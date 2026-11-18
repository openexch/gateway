#!/usr/bin/env python3
"""Process tests of oe-projector against a real PostgreSQL in a throwaway container (the image pinned by digest).

The binaries run with a clean environment (nothing inherited but PATH), both media driver modes, and no core cluster:
these cases cover the process shell, the schema and its migration, the writer lease and its fence, and the rebuild
command. The follow-and-apply logic is not part of them.

Cases:
  config                  settings refused in one pass (78) before anything binds; an unknown argument is 64
  schema                  two copies migrate at once; one recorded schema with the file's sha256; the read relations
                          have the read contract's names and types; the heartbeat view has no row before a beat; a
                          database with another schema, or with the reference tables, is refused (78)
  rebuild                 --rebuild truncates every base table and resets the state row in one transaction (the
                          heartbeat view then has no row); behind an open history read it gives up after its lock wait
                          (75, nothing changed, the lease released) and runs once the read is gone; it waits while
                          another copy holds the lease; SIGTERM while it waits is 75 and changes nothing
  lease_takeover          two copies, one writer; the writer killed (SIGKILL), twice: the standby takes over inside
                          2 x lease + poll; no seq is committed twice; the standby held no replay and no session
  lease_race              two copies started at once on a fresh database, three times: exactly one writer, dense probe rows
  slow_commit             a batch whose COMMIT returns later than the lease less one renewal interval: the copy steps
                          down on its own (its lease counts from before the fence, as the database counts it), then
                          takes the lease again once it expired; no seq is committed twice
  hung_writer             the writer stopped (SIGSTOP) inside a batch before its fence, inside a batch after its fence
                          (holding the state row until the server ends its idle transaction), then at an arbitrary
                          point: the standby takes over; resumed, the old writer cannot commit (its fence updates zero
                          rows, or its transaction is gone) and it becomes a standby; no seq is committed twice
  driver_external_killed  external mode: the host driver killed, exit 70 inside the driver timeout; restarted, the
                          copy reads the cursor from the database again
  driver_embedded         embedded mode: serves, a second copy with the same instance id is refused (78), SIGTERM is a
                          clean stop that releases the lease

usage: projector_it.py --bin-dir <dir> --work <dir> --image <name@sha256:...> --source-dir <repo> [--binary <name>] case...
"""
import argparse
import hashlib
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

SECRET = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
LEASE_MS = 1500
POLL_MS = 200
TAKEOVER_BOUND_S = (2 * LEASE_MS + POLL_MS) / 1000.0
# the server ends a stopped writer's open transaction inside one lease, and a statement is below a third of it
IDLE_IN_TX_MS = LEASE_MS
STATEMENT_TIMEOUT_MS = 450
PG_RETRY_MS = 200
# hung_writer: the open transaction outlives the lease (accepted with a WARN), so a writer that resumes after the
# takeover still has its transaction and only the fence can stop it
HUNG_IDLE_IN_TX_MS = 2500
HUNG_BOUND_S = (max(LEASE_MS, HUNG_IDLE_IN_TX_MS) + LEASE_MS + POLL_MS + PG_RETRY_MS + STATEMENT_TIMEOUT_MS) / 1000.0
# a writer steps down once a confirmation is older than the lease less one renewal interval
HOLD_MS = LEASE_MS - LEASE_MS // 3
REBUILD_LOCK_TIMEOUT_MS = 200
DRIVER_TIMEOUT_MS = 3000
CONTRACT_RELATIONS = ["gateway_order_history_v1", "gateway_fills_v1", "gateway_history_heartbeat_v1"]


class Failure(Exception):
    pass


def check(cond, what):
    if not cond:
        raise Failure(what)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def docker(*args, check_rc=True):
    return subprocess.run(["docker", *args], check=check_rc, capture_output=True, text=True)


class Pg:
    def __init__(self, image):
        self.name = f"gw-projector-test-{os.getpid()}-{secrets.token_hex(3)}"
        self.password = secrets.token_hex(16)
        docker("run", "-d", "--rm", "--name", self.name, "-e", "POSTGRES_USER=gw", "-e",
               f"POSTGRES_PASSWORD={self.password}", "-e", "POSTGRES_DB=gw", "-p", "127.0.0.1::5432", image)
        deadline = time.monotonic() + 60
        while docker("exec", self.name, "pg_isready", "-U", "gw", "-d", "gw", "-h", "127.0.0.1",
                     check_rc=False).returncode != 0:
            check(time.monotonic() < deadline, "postgres did not become ready")
            time.sleep(0.3)
        self.port = docker("port", self.name, "5432/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1]

    def url(self, db):
        return f"postgresql://gw:{self.password}@127.0.0.1:{self.port}/{db}"

    def sql(self, db, statement):
        r = docker("exec", "-i", self.name, "psql", "-U", "gw", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c",
                   statement, check_rc=False)
        check(r.returncode == 0, f"psql {statement[:120]}: {r.stderr}")
        return r.stdout.strip()

    def script(self, db, text):
        r = subprocess.run(["docker", "exec", "-i", self.name, "psql", "-U", "gw", "-d", db, "-At", "-v",
                            "ON_ERROR_STOP=1"], input=text, capture_output=True, text=True)
        check(r.returncode == 0, f"psql script: {r.stderr}")
        return r.stdout.strip()

    def fresh(self, db):
        self.sql("postgres", f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        self.sql("postgres", f"CREATE DATABASE {db}")
        return self.url(db)

    def close(self):
        docker("rm", "-f", self.name, check_rc=False)


class Ctx:
    def __init__(self, args, pg):
        self.args = args
        self.pg = pg
        self.work = args.work
        self.procs = []
        self.dirs = []
        self.key_file = os.path.join(self.work, "cluster-key")
        with open(self.key_file, "w") as f:
            f.write(f"11:GATEWAY:{SECRET}\n")

    def binary(self, name=None):
        path = os.path.join(self.args.bin_dir, name or self.args.binary)
        check(os.access(path, os.X_OK), f"{path} is not built")
        return path

    def tmpdir(self, name):
        base = "/dev/shm" if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK) else self.work
        path = tempfile.mkdtemp(prefix=f"gw-projector-{name}-", dir=base)
        self.dirs.append(path)
        return path

    def env(self, url, mode, instance, driver_dir=None, aeron_dir=None, extra=None):
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
               "HISTORY_PROJECTOR_DRIVER_MODE": mode,
               "HISTORY_PROJECTOR_DRIVER_TIMEOUT_MS": str(DRIVER_TIMEOUT_MS),
               "HISTORY_PROJECTOR_INSTANCE_ID": str(instance),
               "HISTORY_PROJECTOR_ADMIN_BIND": f"127.0.0.1:{free_port()}",
               "HISTORY_PROJECTOR_JOURNAL_ARCHIVES": "127.0.0.1:8010,127.0.0.2:8010",
               "HISTORY_PROJECTOR_JOURNAL_CONTROL_STREAM": "10",
               "HISTORY_PROJECTOR_JOURNAL_STREAM": "4102",
               "HISTORY_PROJECTOR_JOURNAL_CHANNEL": "aeron:ipc",
               "HISTORY_PROJECTOR_REPLAY_STREAM": "4202",
               "HISTORY_PROJECTOR_JOURNAL_MTU": "8192",
               "HISTORY_PROJECTOR_LOCAL_HOST": "127.0.0.1",
               "HISTORY_PROJECTOR_CLUSTER_MEMBERS": "0=127.0.0.1:9010",
               "HISTORY_PROJECTOR_CLUSTER_KEY_FILE": self.key_file,
               "HISTORY_PROJECTOR_JOURNAL_NODE_ENDPOINTS": "-,-",
               "HISTORY_PROJECTOR_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=64k|mtu=1408",
               "HISTORY_PROJECTOR_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=64k|mtu=1408",
               "HISTORY_PROJECTOR_PG_URL": url,
               "HISTORY_PROJECTOR_LEASE_MS": str(LEASE_MS),
               "HISTORY_PROJECTOR_LEASE_POLL_MS": str(POLL_MS),
               "HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS": str(STATEMENT_TIMEOUT_MS),
               "HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS": str(IDLE_IN_TX_MS),
               "HISTORY_PROJECTOR_PG_RETRY_MS": str(PG_RETRY_MS),
               "HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS": str(REBUILD_LOCK_TIMEOUT_MS),
               "HISTORY_PROJECTOR_DUTY_WATCHDOG_MS": "2000"}
        if mode == "embedded":
            env["HISTORY_PROJECTOR_DRIVER_DIR"] = driver_dir or self.tmpdir(f"emb{instance}")
        else:
            env["AERON_DIR"] = aeron_dir
        env.update(extra or {})
        return env

    def start(self, args, env, log_name):
        log = open(os.path.join(self.work, log_name), "a+")
        log.write(f"---- start {' '.join(args)}\n")
        log.flush()
        proc = subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        proc.log = log
        proc.log_path = log.name
        proc.log_start = log.tell()
        proc.env = env
        self.procs.append(proc)
        return proc

    def projector(self, url, mode, instance, log_name, binary=None, **kw):
        env = self.env(url, mode, instance, **kw)
        return self.start([self.binary(binary)], env, log_name)

    def test_driver(self, name):
        parent = self.tmpdir(name)
        driver_dir = os.path.join(parent, "aeron")
        proc = self.start([self.binary("gw_test_media_driver"), driver_dir], {"PATH": os.environ.get("PATH", "")},
                          f"{name}-driver.log")
        check(wait_for(lambda: "driver ready" in text_of(proc), 15), f"the test media driver did not start")
        return proc, driver_dir

    def cleanup(self):
        for proc in self.procs:
            if proc.poll() is None:
                try:
                    os.kill(proc.pid, signal.SIGCONT)
                except OSError:
                    pass
                proc.kill()
                proc.wait()
            proc.log.close()
        for d in self.dirs:
            shutil.rmtree(d, ignore_errors=True)


def text_of(proc):
    with open(proc.log_path) as f:
        f.seek(proc.log_start)
        return f.read()


def wait_for(pred, timeout, step=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def wait_exit(proc, timeout):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def http(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except (urllib.error.URLError, ConnectionError, OSError):
        return None, ""


def admin(proc):
    return int(proc.env["HISTORY_PROJECTOR_ADMIN_BIND"].rsplit(":", 1)[1])


def health(proc):
    status, body = http(admin(proc), "/health")
    if status is None or not body:
        return None
    try:
        return json.loads(body)
    except ValueError:
        return None


def role(proc):
    h = health(proc)
    return None if h is None else h["projector"]["role"]


def stopped(proc):
    try:
        with open(f"/proc/{proc.pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "T"
    except OSError:
        return False


def thread_names(pid):
    names = []
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{tid}/comm") as f:
                names.append(f.read().strip())
        except OSError:
            pass
    return names


def stop_clean(proc, what):
    proc.send_signal(signal.SIGTERM)
    code = wait_exit(proc, 10)
    check(code == 0, f"{what}: SIGTERM exit {code}, expected 0:\n{text_of(proc)}")


# ---- the probe rows of the test build (tests/support/ProjectorFenceMain.cpp)

def probe_rows(ctx, db):
    # one statement: the probe rows and the cursor from one snapshot
    out = ctx.pg.sql(db, "SELECT count(*), COALESCE(max(seq), 0), count(DISTINCT seq), (SELECT applied_seq FROM "
                         "oe_projection_state) FROM oe_projection_fence_probe")
    count, top, distinct, applied = (int(x) for x in out.split("|"))
    return count, top, distinct, applied


def no_double_write(ctx, db):
    dup = ctx.pg.sql(db, "SELECT seq || ':' || string_agg(left(token, 8), ',') FROM oe_projection_fence_probe "
                         "GROUP BY seq HAVING count(*) > 1 ORDER BY seq LIMIT 5")
    check(dup == "", f"two writers committed: seq committed twice (seq:tokens) {dup}")
    count, top, distinct, applied = probe_rows(ctx, db)
    check(count == top and distinct == top and applied == top,
          f"seq not dense: {count} rows, {distinct} distinct, max {top}, applied_seq {applied}")
    return top


def wait_progress(ctx, db, beyond, more=3, timeout=10):
    ok = wait_for(lambda: probe_rows(ctx, db)[1] >= beyond + more, timeout, 0.05)
    check(ok, f"no batches committed past seq {beyond}")


def activity(ctx, db):
    return ctx.pg.sql(db, "SELECT pid || ' ' || coalesce(state, '') || ' ' || coalesce(wait_event_type, '') || ' ' || "
                          "coalesce(wait_event, '') || ' ' || left(query, 100) FROM pg_stat_activity WHERE "
                          "application_name = 'oe-projector'")


def make_probe_table(ctx, db):
    ctx.pg.sql(db, "CREATE TABLE oe_projection_fence_probe (seq BIGINT NOT NULL, token TEXT NOT NULL)")


def expect_standby(proc, what):
    h = health(proc)
    check(h is not None, f"{what}: no /health")
    p = h["projector"]
    check(p["role"] == "standby", f"{what}: role {p['role']}")
    check(p["follow"]["replaysOpen"] == 0 and p["follow"]["sessionOpen"] is False,
          f"{what}: a standby holds a replay or a session: {p['follow']}")
    status, body = http(admin(proc), "/ready")
    check(status == 503 and json.loads(body)["role"] == "standby" and "lease_held" in json.loads(body)["failing"],
          f"{what}: /ready {status} {body}")


def one_writer(a, b, timeout=10):
    def pick():
        ra, rb = role(a), role(b)
        return {ra, rb} == {"writer", "standby"}
    check(wait_for(pick, timeout, 0.05), f"no single writer: {role(a)} / {role(b)}")
    return (a, b) if role(a) == "writer" else (b, a)


# ---- cases

def case_config(ctx):
    proc = ctx.start([ctx.binary()], {"PATH": os.environ.get("PATH", "")}, "config-empty.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78, f"exit {code}, expected 78:\n{out}")
    required = ["DRIVER_MODE", "INSTANCE_ID", "ADMIN_BIND", "JOURNAL_ARCHIVES", "JOURNAL_CONTROL_STREAM",
                "JOURNAL_STREAM", "JOURNAL_CHANNEL", "REPLAY_STREAM", "LOCAL_HOST", "JOURNAL_MTU", "CLUSTER_MEMBERS",
                "CLUSTER_KEY_FILE", "PG_URL", "JOURNAL_NODE_ENDPOINTS", "CORE_INGRESS_CHANNEL", "CORE_RESPONSE_CHANNEL"]
    for key in required:
        check(f"HISTORY_PROJECTOR_{key} is required" in out, f"HISTORY_PROJECTOR_{key} not named:\n{out}")
    check(f"refusing to start: {len(required)} configuration problems (exit 78)" in out, f"refusal line:\n{out}")
    env = ctx.env("postgresql://gw@127.0.0.1:1/x", "embedded", 1,
                  extra={"HISTORY_PROJECTOR_TYPO": "1", "HISTORY_PROJECTOR_LEASE_POLL_MS": str(LEASE_MS)})
    proc = ctx.start([ctx.binary()], env, "config-bad.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78 and "HISTORY_PROJECTOR_TYPO is not a setting" in out and
          "must be above HISTORY_PROJECTOR_LEASE_POLL_MS" in out, f"exit {code}:\n{out}")
    check("admin listening" not in out, "the admin listener bound before the configuration was accepted")
    proc = ctx.start([ctx.binary(), "--rebuld"], env, "config-arg.log")
    check(wait_exit(proc, 10) == 64 and "unknown argument '--rebuld'" in text_of(proc), text_of(proc))


def case_schema(ctx):
    url = ctx.pg.fresh("schema1")
    a = ctx.projector(url, "embedded", 1, "schema-a.log")
    b = ctx.projector(url, "embedded", 2, "schema-b.log")
    for p in (a, b):
        check(wait_for(lambda: (health(p) or {}).get("projector", {}).get("schema", {}).get("state")
                       in ("applied", "present"), 20), f"schema not ready:\n{text_of(p)}")
    with open(os.path.join(ctx.args.source_dir, "sql", "history_projection_v1.sql"), "rb") as f:
        sha = hashlib.sha256(f.read()).hexdigest()
    rows = ctx.pg.sql("schema1", "SELECT version || ':' || sha256 FROM oe_projection_schema")
    check(rows == f"1:{sha}", f"oe_projection_schema: {rows}, expected 1:{sha}")
    states = sorted(health(p)["projector"]["schema"]["state"] for p in (a, b))
    print(f"schema states of the two copies started at once: {states}")
    # the read relations against the read contract's reference DDL, column by column
    with open(os.path.join(ctx.args.source_dir, "sql", "history_read_contract_v1.sql")) as f:
        ddl = f.read()
    ctx.pg.script("schema1", "CREATE SCHEMA ref; SET search_path TO ref;\n" + ddl)
    columns = ("SELECT column_name || ' ' || data_type FROM information_schema.columns WHERE table_schema = '{}' AND "
               "table_name = '{}' ORDER BY ordinal_position")
    compared = 0
    for rel in CONTRACT_RELATIONS:
        want = ctx.pg.sql("schema1", columns.format("ref", rel)).splitlines()
        got = ctx.pg.sql("schema1", columns.format("public", rel)).splitlines()
        check(len(want) > 0 and got[:len(want)] == want,
              f"projected history differs from the read contract: {rel}\n contract {want}\n projected {got}")
        compared += len(want)
        check(got == want, f"projected relation {rel} has columns the read contract does not: {got}")
    print(f"read relations: {compared} contract columns equal")
    check(ctx.pg.sql("schema1", "SELECT count(*) FROM gateway_history_heartbeat_v1") == "0",
          "the heartbeat view has a row before any beat")
    ctx.pg.sql("schema1", "UPDATE oe_projection_state SET epoch = 5, beat_at = clock_timestamp()")
    hb = ctx.pg.sql("schema1", "SELECT applied_order_id || '|' || applied_event_seq || '|' || epoch FROM "
                               "gateway_history_heartbeat_v1")
    check(hb == "0|0|5", f"heartbeat view row {hb}")
    check(ctx.pg.sql("schema1", "SELECT count(*) FROM pg_tables WHERE tablename = 'oe_projection_fence_probe'") == "0",
          "a probe table in a production schema")
    stop_clean(a, "copy a")
    stop_clean(b, "copy b")
    # another schema recorded: refused, never applied over
    ctx.pg.sql("schema1", "UPDATE oe_projection_schema SET sha256 = 'not-this-one'")
    c = ctx.projector(url, "embedded", 3, "schema-c.log")
    code = wait_exit(c, 20)
    check(code == 78 and "holds something else" in text_of(c) and "not-this-one" in text_of(c),
          f"another schema: exit {code}:\n{text_of(c)}")
    # a database holding the reference tables (the gateway's test DDL): refused
    url2 = ctx.pg.fresh("schema2")
    ctx.pg.script("schema2", ddl)
    d = ctx.projector(url2, "embedded", 4, "schema-d.log")
    code = wait_exit(d, 20)
    check(code == 78 and "holds something else" in text_of(d), f"reference tables: exit {code}:\n{text_of(d)}")
    check(ctx.pg.sql("schema2", "SELECT count(*) FROM pg_tables WHERE tablename LIKE 'oe_%'") == "0",
          "a refused schema left relations behind")


SEED = """
UPDATE oe_projection_state SET epoch = 5, applied_seq = 2, applied_log_position = 640, applied_order_id = 10,
    head_seq = 2, beat_at = clock_timestamp(), halt_reason = 'GAP', halt_detail = 'test';
INSERT INTO oe_order_events VALUES
    (1, 10, 7, 'ACCEPTED', 'NEW', 320, 'NONE', 0, 0, 0, 0, 0, 0, 0, 100, 1000, decode(repeat('00', 216), 'hex')),
    (2, 10, 7, 'FILL', 'FILLED', 640, 'TAKER', 1, 5, 99, 1001, 3, 100, 1, 0, 1001, decode(repeat('01', 216), 'hex'));
INSERT INTO oe_orders VALUES (10, 7, NULL, 1, 'BUY', 'LIMIT', 'GTC', 100, NULL, 1, 1, 0, 'FILLED', NULL, NULL, 77, 2,
    100, 0, 1000, 1001, 2, 640);
INSERT INTO oe_fills VALUES (3, 10, 7, 1, 'BUY', 100, 1, false, 1001, 2, 640, 1, 5, 99, 1001);
INSERT INTO oe_projection_source_memo (source, archive_id, recording_id, start_timestamp, initial_term_id, session_id,
    term_buffer_length, mtu_length, stream_id, end_position, seq, epoch) VALUES
    ('127.0.0.1:8010', 1, 4, 5, 6, 7, 65536, 8192, 4102, 640, 2, 5);
INSERT INTO oe_projection_replay (source, archive_id, replay_session_id) VALUES ('127.0.0.1:8010', 1, 99);
"""
BASE_TABLES = ["oe_order_events", "oe_orders", "oe_fills", "oe_projection_source_memo", "oe_projection_replay"]


def rebuild_proc(ctx, url, log_name):
    return ctx.start([ctx.binary(), "--rebuild"], ctx.env(url, "embedded", 9), log_name)


def hold_read(ctx, db, seconds):
    """An open history read: a transaction that read oe_orders and sleeps, holding its lock."""
    proc = ctx.start(["docker", "exec", "-i", ctx.pg.name, "psql", "-U", "gw", "-d", db, "-At", "-c",
                      f"BEGIN; SELECT count(*) FROM oe_orders; SELECT pg_sleep({seconds}); COMMIT;"],
                     {"PATH": os.environ.get("PATH", "")}, "rebuild-reader.log")
    check(wait_for(lambda: ctx.pg.sql(db, "SELECT count(*) FROM pg_stat_activity WHERE query LIKE "
                                          "'%pg_sleep(%' AND pid <> pg_backend_pid()") != "0", 10),
          "the open read did not start")
    return proc


def case_rebuild(ctx):
    url = ctx.pg.fresh("rebuild1")
    p = ctx.projector(url, "embedded", 1, "rebuild-service.log")
    check(wait_for(lambda: role(p) == "writer", 20), f"no writer:\n{text_of(p)}")
    stop_clean(p, "service")
    ctx.pg.script("rebuild1", SEED)
    check(ctx.pg.sql("rebuild1", "SELECT count(*) FROM gateway_history_heartbeat_v1") == "1", "seeded heartbeat")
    r = rebuild_proc(ctx, url, "rebuild-1.log")
    code = wait_exit(r, 30)
    check(code == 0, f"--rebuild exit {code}:\n{text_of(r)}")
    for table in BASE_TABLES:
        check(ctx.pg.sql("rebuild1", f"SELECT count(*) FROM {table}") == "0", f"{table} not truncated")
    state = ctx.pg.sql("rebuild1", "SELECT coalesce(epoch::text, 'null') || '|' || applied_seq || '|' || "
                                   "applied_log_position || '|' || applied_order_id || '|' || "
                                   "coalesce(head_seq::text, 'null') || '|' || (beat_at IS NULL) || '|' || "
                                   "coalesce(halt_reason, 'null') || '|' || (lease_until IS NULL) FROM oe_projection_state")
    check(state == "null|0|0|0|null|true|null|true", f"state row after the rebuild: {state}")
    check(ctx.pg.sql("rebuild1", "SELECT count(*) FROM gateway_history_heartbeat_v1") == "0",
          "rebuilding history served as complete: the heartbeat view has a row after the rebuild")
    # behind an open history read: the lock wait is bounded, nothing changes, the lease is released; run again later
    ctx.pg.script("rebuild1", SEED)
    reader = hold_read(ctx, "rebuild1", 30)
    started = time.monotonic()
    r = rebuild_proc(ctx, url, "rebuild-locked.log")
    code = wait_exit(r, 30)
    elapsed = time.monotonic() - started
    check(code == 75 and "lock timeout" in text_of(r) and "safe to run again" in text_of(r),
          f"--rebuild behind an open read: exit {code}:\n{text_of(r)}")
    check(ctx.pg.sql("rebuild1", "SELECT count(*) FROM oe_order_events") == "2", "a rebuild that gave up changed rows")
    check(ctx.pg.sql("rebuild1", "SELECT lease_until IS NULL FROM oe_projection_state") == "t",
          "a rebuild that gave up kept the lease")
    print(f"--rebuild behind an open read gave up after {elapsed:.2f} s (lock wait {REBUILD_LOCK_TIMEOUT_MS} ms)")
    os.killpg(reader.pid, signal.SIGKILL)
    reader.wait()
    ctx.pg.sql("rebuild1", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%pg_sleep(%' "
                           "AND pid <> pg_backend_pid()")
    r = rebuild_proc(ctx, url, "rebuild-again.log")
    code = wait_exit(r, 30)
    check(code == 0, f"--rebuild run again: exit {code}:\n{text_of(r)}")
    check(ctx.pg.sql("rebuild1", "SELECT count(*) FROM oe_order_events") == "0", "not truncated when run again")
    # a writer holds the lease: the rebuild waits; SIGTERM while waiting is 75 and changes nothing
    ctx.pg.script("rebuild1", SEED)
    p = ctx.projector(url, "embedded", 2, "rebuild-service.log")
    check(wait_for(lambda: role(p) == "writer", 20), f"no writer:\n{text_of(p)}")
    r = rebuild_proc(ctx, url, "rebuild-2.log")
    check(wait_for(lambda: "another copy holds the writer lease" in text_of(r), 10), f"no wait line:\n{text_of(r)}")
    r.send_signal(signal.SIGTERM)
    code = wait_exit(r, 10)
    check(code == 75 and "nothing changed" in text_of(r), f"SIGTERM while waiting: exit {code}:\n{text_of(r)}")
    check(ctx.pg.sql("rebuild1", "SELECT count(*) FROM oe_order_events") == "2", "a waiting rebuild changed rows")
    # waits, then runs once the writer stops (a clean stop releases the lease)
    r = rebuild_proc(ctx, url, "rebuild-3.log")
    check(wait_for(lambda: "another copy holds the writer lease" in text_of(r), 10), f"no wait line:\n{text_of(r)}")
    stop_clean(p, "service")
    code = wait_exit(r, 20)
    check(code == 0, f"--rebuild after the writer stopped: exit {code}:\n{text_of(r)}")
    check(ctx.pg.sql("rebuild1", "SELECT count(*) FROM oe_order_events") == "0", "not truncated after the wait")


def case_lease_takeover(ctx):
    url = ctx.pg.fresh("lease1")
    a = ctx.projector(url, "embedded", 1, "lease-a.log")
    check(wait_for(lambda: role(a) is not None, 20), f"a did not serve:\n{text_of(a)}")
    make_probe_table(ctx, "lease1")
    b = ctx.projector(url, "embedded", 2, "lease-b.log")
    check(wait_for(lambda: role(b) is not None, 20), f"b did not serve:\n{text_of(b)}")
    time.sleep(1.0)
    no_double_write(ctx, "lease1")
    writer, standby = one_writer(a, b)
    expect_standby(standby, "the standby")
    top = no_double_write(ctx, "lease1")
    wait_progress(ctx, "lease1", top)
    takeovers = []
    for round_no in (1, 2):
        top = no_double_write(ctx, "lease1")
        killed = time.monotonic()
        writer.kill()
        writer.wait()
        check(wait_for(lambda: role(standby) == "writer", TAKEOVER_BOUND_S + 2, 0.02),
              f"round {round_no}: the standby did not take over:\n{text_of(standby)}")
        elapsed = time.monotonic() - killed
        takeovers.append(round(elapsed, 3))
        check(elapsed <= TAKEOVER_BOUND_S, f"round {round_no}: takeover took {elapsed:.2f} s, bound {TAKEOVER_BOUND_S} s")
        wait_progress(ctx, "lease1", top)
        no_double_write(ctx, "lease1")
        # the killed copy comes back as a standby (same instance id: its embedded driver dir is reclaimed)
        restarted = ctx.start([ctx.binary()], writer.env, os.path.basename(writer.log_path))
        check(wait_for(lambda: role(restarted) == "standby", 20), f"round {round_no}: restart not standby")
        expect_standby(restarted, f"round {round_no}: the restarted copy")
        writer, standby = standby, restarted
    top = no_double_write(ctx, "lease1")
    print(f"takeover times (s): {takeovers}, bound {TAKEOVER_BOUND_S}; probe rows dense to seq {top}")
    stop_clean(writer, "writer")
    stop_clean(standby, "standby")
    no_double_write(ctx, "lease1")


def case_lease_race(ctx):
    for round_no in (1, 2, 3):
        db = f"race{round_no}"
        url = ctx.pg.fresh(db)
        make_probe_table(ctx, db)
        a = ctx.projector(url, "embedded", 1, f"race{round_no}-a.log")
        b = ctx.projector(url, "embedded", 2, f"race{round_no}-b.log")
        writer, standby = one_writer(a, b, 20)
        expect_standby(standby, f"round {round_no}: the standby")
        wait_progress(ctx, db, 0)
        top = no_double_write(ctx, db)
        takeovers = [health(p)["projector"]["lease"]["takeovers"] for p in (writer, standby)]
        check(takeovers == [1, 0], f"round {round_no}: takeovers (writer, standby) {takeovers}")
        print(f"round {round_no}: one writer of two copies started at once; probe rows dense to seq {top}")
        stop_clean(writer, "writer")
        stop_clean(standby, "standby")


def case_slow_commit(ctx):
    url = ctx.pg.fresh("slow1")
    make_probe_table(ctx, "slow1")
    # the stop sits between the fence and the COMMIT; the server ends that open transaction after a whole lease, later
    # than this stop
    p = ctx.projector(url, "embedded", 1, "slow-a.log")
    check(wait_for(lambda: role(p) == "writer", 20), f"no writer:\n{text_of(p)}")
    top = no_double_write(ctx, "slow1")
    wait_progress(ctx, "slow1", top)
    pause_s = (HOLD_MS + (LEASE_MS - HOLD_MS) / 2) / 1000.0  # past the step-down point, inside the lease
    p.send_signal(signal.SIGRTMIN + 3)
    check(wait_for(lambda: stopped(p), 5), f"the writer did not stop before its COMMIT:\n{text_of(p)}")
    time.sleep(pause_s)
    p.send_signal(signal.SIGCONT)
    check(wait_for(lambda: "writer lease lost" in text_of(p), 3),
          f"a COMMIT that returned {pause_s:.2f} s after its fence left the copy a writer past the lease less one "
          f"renewal interval ({HOLD_MS} ms):\n{text_of(p)}")
    check(wait_for(lambda: (health(p) or {}).get("projector", {}).get("lease", {}).get("lost") == 1, 3),
          f"lease lost counter: {health(p)}")
    # the database still names this copy's old token until the lease ends; then it takes the lease again
    check(wait_for(lambda: role(p) == "writer", TAKEOVER_BOUND_S + 2), f"did not take the lease again:\n{text_of(p)}")
    top = no_double_write(ctx, "slow1")
    wait_progress(ctx, "slow1", top)
    no_double_write(ctx, "slow1")
    print(f"stepped down after a COMMIT {pause_s:.2f} s late (hold {HOLD_MS} ms, lease {LEASE_MS} ms), then writer again")
    stop_clean(p, "writer")


def case_hung_writer(ctx):
    url = ctx.pg.fresh("hung1")
    # a transaction the stopped writer left open (it holds the state row's lock after its fence) ends after this, later
    # than the lease, so the old writer resumes with its transaction alive; the driver timeout is above every stop of
    # this case: a process paused longer than it exits 70 on resume (the conductor heartbeat rule), which is safe (a
    # dead writer commits nothing) but is not what this case measures
    extra = {"HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS": str(HUNG_IDLE_IN_TX_MS),
             "HISTORY_PROJECTOR_DRIVER_TIMEOUT_MS": "10000"}
    a = ctx.projector(url, "embedded", 1, "hung-a.log", extra=extra)
    check(wait_for(lambda: role(a) is not None, 20), f"a did not serve:\n{text_of(a)}")
    check("WARN config: HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS (2500) is above HISTORY_PROJECTOR_LEASE_MS" in
          text_of(a), f"no WARN for an idle timeout above the lease:\n{text_of(a)}")
    make_probe_table(ctx, "hung1")
    b = ctx.projector(url, "embedded", 2, "hung-b.log", extra=extra)
    check(wait_for(lambda: role(b) is not None, 20), f"b did not serve:\n{text_of(b)}")
    writer, standby = one_writer(a, b)
    top = no_double_write(ctx, "hung1")
    wait_progress(ctx, "hung1", top)
    takeovers = []
    # (where, the signal, the takeover bound): a writer stopped before its fence holds no lock the successor needs, so
    # the successor waits out the lease; stopped after its fence it holds the state row until the server ends its
    # idle transaction; stopped anywhere, at most the larger of the two
    points = [("inside a batch, before its fence", signal.SIGUSR2, TAKEOVER_BOUND_S),
              ("inside a batch, after its fence, before its COMMIT", signal.SIGRTMIN + 3, HUNG_BOUND_S),
              ("an arbitrary point", signal.SIGSTOP, HUNG_BOUND_S)]
    for point, sig, bound in points:
        top = no_double_write(ctx, "hung1")
        writer.send_signal(sig)
        check(wait_for(lambda: stopped(writer), 5), f"the writer did not stop {point}:\n{text_of(writer)}")
        held = time.monotonic()
        check(wait_for(lambda: role(standby) == "writer", bound + 2, 0.02),
              f"{point}: the standby did not take over:\n{text_of(standby)}\nsessions:\n{activity(ctx, 'hung1')}")
        elapsed = time.monotonic() - held
        takeovers.append(round(elapsed, 3))
        check(elapsed <= bound, f"{point}: takeover took {elapsed:.2f} s, bound {bound} s")
        wait_progress(ctx, "hung1", top)
        writer.send_signal(signal.SIGCONT)
        check(wait_for(lambda: "writer lease lost" in text_of(writer), 10),
              f"{point}: the resumed writer did not lose its lease:\n{text_of(writer)}")
        time.sleep(0.5)
        no_double_write(ctx, "hung1")
        check(wait_for(lambda: role(writer) == "standby", 5), f"{point}: the resumed writer is not a standby")
        expect_standby(writer, f"{point}: the resumed writer")
        writer, standby = standby, writer
    top = no_double_write(ctx, "hung1")
    print(f"takeover after the stop (s): {takeovers}; probe rows dense to seq {top}")
    stop_clean(writer, "writer")
    stop_clean(standby, "standby")


def case_driver_external_killed(ctx):
    url = ctx.pg.fresh("driver1")
    driver, driver_dir = ctx.test_driver("ext")
    p = ctx.projector(url, "external", 1, "driver-ext.log", aeron_dir=driver_dir)
    check(wait_for(lambda: role(p) == "writer", 20), f"no writer:\n{text_of(p)}")
    check(health(p)["driver"]["mode"] == "external", "driver mode")
    ctx.pg.sql("driver1", "UPDATE oe_projection_state SET epoch = 7, applied_seq = 1234, applied_order_id = 99")
    cursor = lambda proc: (health(proc) or {}).get("projector", {}).get("cursor", {})
    check(wait_for(lambda: cursor(p).get("appliedSeq") == 1234, 5), f"cursor not read: {cursor(p)}")
    killed = time.monotonic()
    driver.send_signal(signal.SIGKILL)
    driver.wait()
    code = wait_exit(p, DRIVER_TIMEOUT_MS / 1000 + 1.5)
    elapsed = time.monotonic() - killed
    if code is None:
        raise Failure(f"projector alive with a dead driver ({elapsed:.1f} s after the kill, driver timeout "
                      f"{DRIVER_TIMEOUT_MS} ms)")
    check(code == 70, f"exit {code} after the driver was killed, expected 70:\n{text_of(p)}")
    print(f"exit 70 {elapsed:.2f} s after the driver was killed (driver timeout {DRIVER_TIMEOUT_MS} ms)")
    driver, driver_dir = ctx.test_driver("ext2")
    p2 = ctx.projector(url, "external", 1, "driver-ext.log", aeron_dir=driver_dir)
    check(wait_for(lambda: role(p2) == "writer", 20), f"restart: no writer:\n{text_of(p2)}")
    c = cursor(p2)
    check(c.get("appliedSeq") == 1234 and c.get("epoch") == 7 and c.get("appliedOrderId") == 99,
          f"restart did not resume from the cursor: {c}")
    stop_clean(p2, "restarted copy")
    check(ctx.pg.sql("driver1", "SELECT lease_until IS NULL FROM oe_projection_state") == "t",
          "a clean stop did not release the lease")
    driver.send_signal(signal.SIGTERM)
    wait_exit(driver, 10)


def case_driver_embedded(ctx):
    url = ctx.pg.fresh("driver2")
    parent = ctx.tmpdir("emb")
    p = ctx.projector(url, "embedded", 11, "driver-emb.log", driver_dir=parent)
    check(wait_for(lambda: role(p) == "writer", 20), f"no writer:\n{text_of(p)}")
    h = health(p)
    check(h["status"] == "UP" and h["driver"]["mode"] == "embedded" and h["process"] == "oe-projector", f"{h}")
    check(h["settings"]["HISTORY_PROJECTOR_LEASE_MS"] == {"value": str(LEASE_MS), "source": "set"} and
          h["settings"]["HISTORY_PROJECTOR_BATCH_EVENTS"] == {"value": "256", "source": "default"}, "settings in /health")
    check(ctx.pg.password not in json.dumps(h), "/health leaks the database password")
    status, body = http(admin(p), "/ready")
    ready = json.loads(body)
    check(status == 503 and ready["role"] == "writer" and
          ready["failing"] == ["session_registered", "head_probe_fresh", "beat_fresh"], f"/ready {status} {body}")
    status, body = http(admin(p), "/metrics")
    check(status == 200 and "\nprojector_lease_held 1\n" in body and "\nprojector_up 1\n" in body, "/metrics")
    names = thread_names(p.pid)
    for name in ("pj-duty", "pj-admin", "pj-pg"):
        check(name in names, f"thread {name} missing: {names}")
    driver_dir = os.path.join(parent, "oe-projector-11")
    check(os.path.isfile(os.path.join(driver_dir, "cnc.dat")), f"no embedded driver at {driver_dir}")
    second = ctx.projector(url, "embedded", 11, "driver-emb-second.log", driver_dir=parent)
    code = wait_exit(second, 15)
    check(code == 78 and "embedded media driver" in text_of(second), f"same instance: exit {code}:\n{text_of(second)}")
    check(p.poll() is None and role(p) == "writer", "the first copy was disturbed")
    stop_clean(p, "embedded copy")
    check(not os.path.exists(driver_dir), "the embedded driver dir was left behind")
    check(ctx.pg.sql("driver2", "SELECT lease_until IS NULL FROM oe_projection_state") == "t",
          "a clean stop did not release the lease")


CASES = {
    "config": case_config,
    "schema": case_schema,
    "rebuild": case_rebuild,
    "lease_takeover": case_lease_takeover,
    "lease_race": case_lease_race,
    "slow_commit": case_slow_commit,
    "hung_writer": case_hung_writer,
    "driver_external_killed": case_driver_external_killed,
    "driver_embedded": case_driver_embedded,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--binary", default="oe-projector")
    parser.add_argument("cases", nargs="+")
    args = parser.parse_args()
    for name in args.cases:
        if name not in CASES:
            print(f"unknown case {name} (known: {', '.join(sorted(CASES))})")
            return 2
    os.makedirs(args.work, exist_ok=True)
    pg = Pg(args.image)
    failures = 0
    try:
        for name in args.cases:
            ctx = Ctx(args, pg)
            try:
                CASES[name](ctx)
                print(f"PASS {name} ({args.binary})")
            except Failure as e:
                print(f"FAIL {name} ({args.binary}): {e}")
                failures += 1
            finally:
                ctx.cleanup()
    finally:
        pg.close()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
