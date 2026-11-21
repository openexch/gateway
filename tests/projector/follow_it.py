#!/usr/bin/env python3
"""T1 rows of oe-projector's follow side against a real three-member core cluster (a heavy job: run it under the lock).

Starts, on this host: three core members from the core jar of the pinned money schema (production Main, the order
journal recorded by every member, the real authenticator with a GATEWAY and an ADMIN key), a throwaway PostgreSQL
container (the image pinned by digest), the test media driver for the external mode, gw_session_it for the order flow
(a GATEWAY session), and oe-projector itself. The projector reaches the members only over the network: archive control
and replay (UDP), the cluster session, and each member's node endpoint (HTTP).

Cases (one per run, each starts its own cluster):
  follow       the projection reaches the core's head; it reads a follower's archive, not the leader's; a projector
               restart resumes from its stored memo (the first replay position equals the memo's of the source it
               attaches); the attached member killed and restarted (the switch), twice; the leader killed (an
               election); applied_seq reaches the head after each; rows dense from 1, one per journalSeq; batches of 16
               records, so every catch-up spans many transactions
  long_overlap a projection whose cursor is far below the core's head, read by a source with no memo (the whole
               overlap compared from the chain start) through a slow writer: the compares count as delivery, BEHIND
               does not leave the source, applied_seq reaches the head
  regenesis    same-archive re-genesis: every member's cluster state wiped with its archive kept, a new genesis; an
               empty database holds only the new genesis's records (the old recordings counted as foreign); a
               projection of the old genesis halts EPOCH_MISMATCH, and after --rebuild holds only the new genesis
  successor    one source only: its member killed and restarted while the projector holds a live replay of its recording
               (which then reads ACTIVE with no stop position), twice: the projector finds the successor recording and
               reaches the head again. It passes without the timed re-list (the successor is also found on reconnect;
               the re-list rules are the archive follow library's, pinned there with their seam controls). Its control
               is oe-projector-nc-pin-recording: the first recording read is the only one ever read (no successor, no
               other source's recording), so after the restart applied_seq stays where it was
  replays      the counters of /health follow the real attach and detach (replaysOpen, sessionOpen; a standby holds
               neither); the member's own replay count (core_archive_replays_active) goes back to its baseline at once
               after SIGTERM, and the clean stop leaves no replay row; after SIGKILL the copy that follows (a restart, then a standby taking over) reads other
               members only, so the killed copy's member counts its orphan alone: it is back at its baseline inside the
               stop bound (below the archive's own client liveness timeout), the archive having answered the
               stopReplay of the recorded session (stopRequestsAnswered); a second projector replaying the same member
               throughout keeps its replay (untouched)
  epoch        an empty database and no genesis recorded in the cluster: nothing written, the epoch stays NULL; the
               genesis recorded: the cluster's genesisId becomes the epoch; a database of another genesis:
               EPOCH_MISMATCH, nothing written
  mtu          a projector whose HISTORY_PROJECTOR_JOURNAL_MTU differs from the recordings': every source refused with
               "replay MTU differs from the recording", nothing read, /ready false, the halt MTU

On oe-projector-tc-faults (the frame sink drops or delays records of a configured source before the forward rule):
  holes        both followers holed, each lacking a record the other holds: read past each hole from the other, no
               halt, rows dense, a follower read at the end
  above_one    the follower read first holds records from 11 only (a member wiped and rejoined): BELOW_RANGE, the
               projection starts at 1 from the other follower; once that range is applied the first serves the rest
  slow         the follower read first delivers 4 records a second under a load of about 10: BEHIND leaves it, the other
               follower closes the lag
  writer_reset the writer starts a generation while the source is attached and the cluster idle: it is read again at
               once, the head reached well inside the BEHIND window
  beat         the beat rule through six states of one projection (on the fault build: a slow source keeps the cursor
               below the head): beats while idle and under continuous load, none while the cursor is below the head,
               while the probe goes unanswered (every member frozen for 3 s), while the session is down (two members
               killed: no quorum), while halted (a copy with another journal MTU: halt MTU at the head)
  beat_epoch   a database of another genesis whose cursor is far above this core's head: EPOCH_MISMATCH, no beat,
               not ready
  halt_restart a halt in the state row is not a latch: GAP (every source lacks one record) then a restart with the
               fault gone, GAP again then a standby taking over from the killed writer, MTU at the head then a restart
               on the idle cluster: each time the next writer clears the halt and beats again. Its control is
               oe-projector-tc-faults-nc-halt-latch: a new writer generation does not carry the stored halt
follow also fills a crossing pair through a BRIDGE session (gw_stream_it_bridge), so the projection holds fill rows,
and the cases on the fault build check that one switch is counted once (never more switches than attaches).

--core-driver external runs each core member on its own external media driver (the test driver), the projector in
the mode --mode names: both mixed combinations (projector external with embedded members, and the reverse).
  replay_cap   each member's archive caps replays at 1 with none reserved (a reader budget of 1); a second projector of
               another database on the same single source: skipped while the member's budget is full (its node shows
               it); with no node endpoint it asks, the archive refuses (MAX_REPLAYS), the refusal is counted and the
               requests back off (counted per minute)

usage: follow_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar> --image <pg image>
                    --mode embedded|external --work <dir> [--port-base N] [--binary oe-projector]
                    [--core-driver embedded|external] case
"""
import argparse
import json
import os
import queue
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

SECRET_ADMIN = "11" * 32
SECRET_GATEWAY = "55" * 32
SECRET_BRIDGE = "77" * 32  # obviously fake fixture keys
CORE_KEYS = (f"1:ADMIN:{SECRET_ADMIN}::users=*;5:GATEWAY:{SECRET_GATEWAY}::users=*;"
             f"7:BRIDGE:{SECRET_BRIDGE}::users=*")
GATEWAY_KEY = f"5:GATEWAY:{SECRET_GATEWAY}"
ADMIN_KEY = f"1:ADMIN:{SECRET_ADMIN}"
BRIDGE_KEY = f"7:BRIDGE:{SECRET_BRIDGE}"
JAVA_OPTS = ["--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens", "java.base/java.nio=ALL-UNNAMED",
             "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "-Xms128m", "-Xmx512m", "-XX:+UseZGC"]
FP = 100_000_000
GENESIS = 7
REPLAY_STREAM = 4302
JOURNAL_MTU = 1408            # the recorded IPC publication's MTU (the members' driver default)
CATCHUP_BOUND_S = 60          # applied_seq reaches the core's head after each fault
ORPHAN_BOUND_S = 30           # every member's replay count back at its baseline at the end of the replays case
STOP_BOUND_S = 8              # a killed writer's recorded replay is stopped by the copy that follows inside this (below
                              # the archive's own client liveness timeout, 10 s, after which it may end it by itself)
LINGER_BOUND_S = 10           # a stopped replay's publication leaves the member's driver after its linger
LEASE_MS = 3000
MARK_FILE_STALE_S = 12        # a killed member's archive mark file reads alive for 10 s
POLL_MS = 300

FAILED = []
STARTED = []  # every projector and helper process a case started: stopped in main's finally, whatever the case raised


def check(ok, name, detail=""):
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": {detail}"), flush=True)
    if not ok:
        FAILED.append(name)
    return ok


def docker(*args, check_rc=True):
    return subprocess.run(["docker", *args], check=check_rc, capture_output=True, text=True)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except (urllib.error.URLError, ConnectionError, OSError):
        return None, ""


def wait_for(pred, timeout, step=0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


class Pg:
    def __init__(self, image):
        self.name = f"gw-follow-it-{os.getpid()}-{secrets.token_hex(3)}"
        self.password = secrets.token_hex(16)
        docker("run", "-d", "--rm", "--name", self.name, "-e", "POSTGRES_USER=gw", "-e",
               f"POSTGRES_PASSWORD={self.password}", "-e", "POSTGRES_DB=gw", "-p", "127.0.0.1::5432", image)
        deadline = time.monotonic() + 60
        while docker("exec", self.name, "pg_isready", "-U", "gw", "-d", "gw", "-h", "127.0.0.1",
                     check_rc=False).returncode != 0:
            if time.monotonic() > deadline:
                raise RuntimeError("postgres did not become ready")
            time.sleep(0.3)
        self.port = docker("port", self.name, "5432/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1]

    def url(self, db):
        return f"postgresql://gw:{self.password}@127.0.0.1:{self.port}/{db}"

    def sql(self, db, statement):
        r = docker("exec", "-i", self.name, "psql", "-U", "gw", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c",
                   statement, check_rc=False)
        if r.returncode != 0:
            raise RuntimeError(f"psql {statement[:120]}: {r.stderr}")
        return r.stdout.strip()

    def fresh(self, db):
        self.sql("postgres", f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        self.sql("postgres", f"CREATE DATABASE {db}")
        return self.url(db)

    def close(self):
        docker("rm", "-f", self.name, check_rc=False)


class Cluster:
    def __init__(self, args, extra_env=None):
        self.args = args
        self.procs = {}
        self.extra = extra_env or {}
        self.shm = os.path.join("/dev/shm", f"gw-follow-it-{os.getpid()}-{args.mode}")
        self.restarts = {i: 0 for i in range(3)}
        self.killed_at = {}
        self.drivers = {}

    def node_port(self, i):
        return self.args.port_base + 90 + i

    def archive(self, i):
        return f"localhost:{self.args.port_base + 100 * i + 1}"

    def start(self, i=None):
        os.makedirs(self.shm, exist_ok=True)
        for m in ([i] if i is not None else range(3)):
            # the archive and the driver of a killed member refuse a restart while their mark files still look alive
            if m in self.killed_at:
                time.sleep(max(0.0, MARK_FILE_STALE_S - (time.monotonic() - self.killed_at.pop(m))))
            d = os.path.join(self.args.work, f"core{m}")
            os.makedirs(d, exist_ok=True)
            env = dict(os.environ, CLUSTER_NODE=str(m), CLUSTER_ADDRESSES="localhost,localhost,localhost",
                       CLUSTER_PORT_BASE=str(self.args.port_base), CLUSTER_PORT_STRIDE="100", BASE_DIR=d,
                       CORE_AUTH_KEYS=CORE_KEYS, CORE_NODE_PORT=str(self.node_port(m)),
                       CORE_ARCHIVE_REPLAY_PROBE_STREAM=str(REPLAY_STREAM), **self.extra)
            aeron_dir = f"{self.shm}/core{m}"
            if self.args.core_driver == "external":
                # the member's own external driver (its host's aeronmd): it outlives a member crash, like on a host
                aeron_dir = f"{self.shm}/drv{m}"
                if m not in self.drivers or self.drivers[m].poll() is not None:
                    dlog = open(os.path.join(self.args.work, f"drv{m}.log"), "a")
                    self.drivers[m] = subprocess.Popen([os.path.join(self.args.bin_dir, "gw_test_media_driver"),
                                                        aeron_dir], stdout=dlog, stderr=subprocess.STDOUT)
                    wait_for(lambda: os.path.exists(os.path.join(aeron_dir, "cnc.dat")), 10, 0.1)
                env.update(TRANSPORT_DRIVER_MODE="external", AERON_DIR=aeron_dir)
            log = open(os.path.join(self.args.work, f"core{m}.log"), "a")
            log.write(f"---- start {self.restarts[m]}\n")
            log.flush()
            self.procs[m] = subprocess.Popen(["java"] + JAVA_OPTS + [f"-Daeron.dir={aeron_dir}", "-jar",
                                                                     self.args.core_jar],
                                             env=env, stdout=log, stderr=subprocess.STDOUT)
            self.restarts[m] += 1

    def kill(self, i):
        p = self.procs[i]
        try:
            os.kill(p.pid, signal.SIGCONT)
        except OSError:
            pass
        p.kill()
        p.wait()
        self.killed_at[i] = time.monotonic()
        # the killed member's embedded driver died with it: its directory goes, so a restart does not wait out the
        # driver's heartbeat (the core refuses a directory whose driver looks alive)
        for name in os.listdir(self.shm) if os.path.isdir(self.shm) else []:
            if name == f"core{i}" or name.startswith(f"core{i}-"):
                shutil.rmtree(os.path.join(self.shm, name), ignore_errors=True)

    def role(self, i):
        status, body = http(self.node_port(i), "/health")
        m = re.search(r"role=(\w+)", body or "")
        return m.group(1) if m else None

    def leader(self):
        for i in range(3):
            if self.procs[i].poll() is None and self.role(i) == "LEADER":
                return i
        return None

    def replays_active(self, i):
        status, body = http(self.node_port(i), "/metrics")
        m = re.search(r"^core_archive_replays_active (-?\d+)", body or "", re.M)
        return int(m.group(1)) if m else None

    def stop(self):
        for i in list(self.procs):
            self.kill(i)
        for d in self.drivers.values():
            d.terminate()
            try:
                d.wait(timeout=10)
            except subprocess.TimeoutExpired:
                d.kill()
        shutil.rmtree(self.shm, ignore_errors=True)


def probe(args, verb, **kv):
    env = dict(os.environ, CORE_ADDRESSES="localhost,localhost,localhost", CORE_PORT_BASE=str(args.port_base),
               CLUSTER_PORT_STRIDE="100", CORE_LOCAL_HOST="127.0.0.1", CORE_AUTH_CREDENTIAL=ADMIN_KEY)
    cmd = ["java"] + JAVA_OPTS + ["-cp", f"{args.core_jar}:{args.probe_jar}", "CoreProbe", verb] + \
        [f"{k}={v}" for k, v in kv.items()]
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=120)
    answer = [line for line in r.stdout.splitlines() if line.startswith("{")]
    print(f"probe {verb} {kv}: exit {r.returncode} {answer[-1] if answer else r.stderr[-400:]}", flush=True)
    return r.returncode, (json.loads(answer[-1]) if answer else None)


class Orders:
    """gw_session_it: creates orders through a GATEWAY session (the order flow that makes order events)."""

    def __init__(self, args):
        self.log = open(os.path.join(args.work, "orders.stderr"), "a")
        self.p = subprocess.Popen([os.path.join(args.bin_dir, "gw_session_it"), "--mode", args.mode, "--dir",
                                   args.driver_dir, "--ingress", args.ingress, "--host", "127.0.0.1",
                                   "--timeout-ms", "30000", "--instance", "71"],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True,
                                  env=dict(os.environ, CORE_AUTH_CREDENTIAL=GATEWAY_KEY))
        self.q = queue.Queue()
        self.key = 1000
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            self.q.put(line.rstrip("\n"))

    def status(self):
        self.p.stdin.write("status\n")
        self.p.stdin.flush()
        end = time.time() + 10
        while time.time() < end:
            try:
                line = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            if line.startswith("STATUS "):
                return json.loads(line[len("STATUS "):])
        return None

    def create(self, n, user=1001, side="BUY"):
        return len(self.create_ids(n, user, side))

    def create_ids(self, n, user=1001, side="BUY"):
        """n limit orders of `user` at one price; the order ids the core acked."""
        base = self.key  # every order takes a key of its own: a reused key is answered as a duplicate
        self.key += n + 10
        self.p.stdin.write(f"create {n} {user} 1 {100 * FP} {FP} {base} {side}\n")
        self.p.stdin.flush()
        ids = []
        outcomes = 0
        end = time.time() + 60
        while outcomes < n and time.time() < end:
            try:
                line = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            if line.startswith("OUTCOME "):
                outcomes += 1
                m = re.search(r" order=(\d+)", line)
                if "kind=REPLY" in line and m and " dup=0" in line:
                    ids.append(int(m.group(1)))
                else:
                    print(f"order not acked as new: {line}", flush=True)
        return ids

    def quit(self):
        try:
            self.p.stdin.write("quit\n")
            self.p.stdin.flush()
            self.p.wait(timeout=15)
        except Exception:
            self.p.kill()


class Bridge:
    """gw_stream_it_bridge: a BRIDGE session that sends MeJournalTrade frames, so two orders fill with no matching
    engine (the core runs behaviour 2, where a trade is applied as it comes)."""

    def __init__(self, args):
        self.log = open(os.path.join(args.work, "bridge.stderr"), "a")
        self.p = subprocess.Popen([os.path.join(args.bin_dir, "gw_stream_it_bridge"), "--mode", args.mode, "--dir",
                                   args.driver_dir, "--ingress", args.ingress, "--host", "127.0.0.1", "--instance", "72"],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True,
                                  env=dict(os.environ, CORE_AUTH_CREDENTIAL=BRIDGE_KEY))
        STARTED.append(self.p)
        self.lines = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            self.lines.put(line.strip())

    def wait_line(self, text, timeout):
        end = time.time() + timeout
        while time.time() < end:
            try:
                if self.lines.get(timeout=0.5) == text:
                    return True
            except queue.Empty:
                pass
        return False

    def trade(self, trade_id, taker, taker_user, maker, maker_user, qty, taker_is_buy):
        self.p.stdin.write(f"trade {trade_id} 1 {taker} {taker_user} {maker} {maker_user} {100 * FP} {qty} "
                           f"{1 if taker_is_buy else 0}\n")
        self.p.stdin.flush()
        return self.wait_line(f"SENT {trade_id}", 30)

    def quit(self):
        try:
            self.p.stdin.write("quit\n")
            self.p.stdin.flush()
            self.p.wait(timeout=15)
        except Exception:
            self.p.kill()


class Projector:
    def __init__(self, args, cluster, url, instance, name, archives=None, nodes=None, extra=None, binary=None):
        self.args = args
        self.name = name
        self.admin = free_port()
        idx = archives if archives is not None else [0, 1, 2]
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HISTORY_PROJECTOR_DRIVER_MODE": args.mode,
            "HISTORY_PROJECTOR_INSTANCE_ID": str(instance),
            "HISTORY_PROJECTOR_ADMIN_BIND": f"127.0.0.1:{self.admin}",
            "HISTORY_PROJECTOR_JOURNAL_ARCHIVES": ",".join(cluster.archive(i) for i in idx),
            "HISTORY_PROJECTOR_JOURNAL_CONTROL_STREAM": "10",
            "HISTORY_PROJECTOR_JOURNAL_STREAM": "4102",
            "HISTORY_PROJECTOR_JOURNAL_CHANNEL": "aeron:ipc",
            "HISTORY_PROJECTOR_REPLAY_STREAM": str(REPLAY_STREAM),
            "HISTORY_PROJECTOR_JOURNAL_MTU": str(JOURNAL_MTU),
            "HISTORY_PROJECTOR_LOCAL_HOST": "127.0.0.1",
            "HISTORY_PROJECTOR_JOURNAL_NODE_ENDPOINTS": ",".join(
                (f"127.0.0.1:{cluster.node_port(i)}" if nodes is None or nodes else "-") for i in idx),
            "HISTORY_PROJECTOR_CLUSTER_MEMBERS": args.ingress,
            "HISTORY_PROJECTOR_CLUSTER_KEY_FILE": args.key_file,
            "HISTORY_PROJECTOR_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=4m|mtu=8192",
            "HISTORY_PROJECTOR_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=4m|mtu=8192",
            "HISTORY_PROJECTOR_PG_URL": url,
            "HISTORY_PROJECTOR_LEASE_MS": str(LEASE_MS),
            "HISTORY_PROJECTOR_LEASE_POLL_MS": str(POLL_MS),
            "HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS": "900",
            "HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS": "500",
            "HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS": str(LEASE_MS),
            "HISTORY_PROJECTOR_HEAD_PROBE_MS": "500",
            "HISTORY_PROJECTOR_HEAD_FRESH_MS": "2000",
            "HISTORY_PROJECTOR_READY_LAG_MS": "3000",
            "HISTORY_PROJECTOR_SOURCE_BACKOFF_MS": "500",
            "HISTORY_PROJECTOR_SOURCE_BACKOFF_MAX_MS": "4000",
            "HISTORY_PROJECTOR_GAP_RETRY_MS": "3000",
            "HISTORY_PROJECTOR_ARCHIVE_TIMEOUT_MS": "3000",
            "HISTORY_PROJECTOR_ARCHIVE_PROBE_MS": "2000",
            "HISTORY_PROJECTOR_NODE_POLL_MS": "500",
            "HISTORY_PROJECTOR_NODE_POLL_TIMEOUT_MS": "400",
            "HISTORY_PROJECTOR_BATCH_EVENTS": "16",
        }
        if args.mode == "embedded":
            self.env["HISTORY_PROJECTOR_DRIVER_DIR"] = os.path.join(cluster.shm, f"pj-{name}")
            os.makedirs(self.env["HISTORY_PROJECTOR_DRIVER_DIR"], exist_ok=True)
        else:
            self.env["AERON_DIR"] = args.driver_dir
        self.env.update(extra or {})
        self.binary = os.path.join(args.bin_dir, binary or args.binary)
        self.p = None
        self.log_path = os.path.join(args.work, f"projector-{name}.log")

    def start(self):
        log = open(self.log_path, "a")
        log.write("---- start\n")
        log.flush()
        self.p = subprocess.Popen([self.binary], env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                  start_new_session=True)
        STARTED.append(self.p)
        return self

    def health(self):
        status, body = http(self.admin, "/health")
        try:
            return json.loads(body)
        except (ValueError, TypeError):
            return None

    def follow(self):
        h = self.health()
        return ((h or {}).get("projector") or {}).get("follow") or {}

    def ready(self):
        status, _ = http(self.admin, "/ready")
        return status == 200

    def send(self, sig):
        os.kill(self.p.pid, sig)

    def stop(self, sig=signal.SIGTERM, timeout=20):
        if self.p is None or self.p.poll() is not None:
            return self.p.returncode if self.p else None
        self.send(sig)
        try:
            return self.p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.p.kill()
            return self.p.wait()

    def log_text(self):
        with open(self.log_path) as f:
            return f.read()

    def run_log(self):
        """The log of the running process (after its last start)."""
        return self.log_text().split("---- start\n")[-1]


def caught_up(pj, timeout=CATCHUP_BOUND_S, at_least=1):
    """applied_seq reaches the core's head (as the projector's own head probe reads it), the head at least `at_least`."""
    def done():
        f = pj.follow()
        return f.get("headSeq", -1) >= at_least and f.get("appliedSeq") == f.get("headSeq")
    t0 = time.monotonic()
    ok = wait_for(done, timeout)
    return ok, time.monotonic() - t0, pj.follow()


def dense(pg, db):
    row = pg.sql(db, "SELECT count(*) || ',' || coalesce(max(journal_seq), 0) || ',' || (SELECT applied_seq FROM "
                     "oe_projection_state) FROM oe_order_events")
    count, mx, applied = (int(x) for x in row.split(","))
    return count == mx == applied and count > 0, row


def genesis(args, genesis_id=GENESIS):
    rc1 = 0
    for version in (1, 2):  # CoreConfig 1 (behaviour 1) records the genesis, CoreConfig 2 moves to behaviour 2
        rc, _ = probe(args, "core-config", version=version, behavior=version, genesis=genesis_id, tick=1, min=1,
                      max=1_000_000_000_000_000, lot=1)
        rc1 = rc1 or rc
    rc2, _ = probe(args, "risk", version=1, market=1, min_qty=1, max_qty=10**6 * FP, collar_bps=5000, per_sec=1000,
                   max_open=5000, slippage_bps=100)
    rc3, _ = probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
    rc4, _ = probe(args, "deposit", user=1001, asset=0, amount=10**7 * FP, rh=9, rl=1)
    return rc1 == rc2 == rc3 == rc4 == 0


def head_now(pj):
    return max(1, pj.follow().get("headSeq", 0))


def created(pj, orders, n):
    """n orders acked; the head the projector must reach is then above the head before them by at least n."""
    before = head_now(pj)
    return orders.create(n) == n, before + n


def on_follower(cluster, pj, timeout=20):
    def ok():
        f = pj.follow()
        m = member_of(cluster, f.get("endpoint"))
        return f.get("phase") == "FOLLOWING" and m is not None and m != cluster.leader()
    return wait_for(ok, timeout, 0.5)


def member_of(cluster, endpoint):
    for i in range(3):
        if cluster.archive(i) == endpoint:
            return i
    return None


def switches_accounted(pj, tag):
    """A switch is one detach of the attached source, counted once: every switch is followed by an attach, so the
    switches of a run never outnumber its attaches."""
    switches = pj.follow().get("counts", {}).get("switches", 0)
    time.sleep(1)  # the log is written behind the counters
    attaches = pj.run_log().count("pj-duty: attaching source ")
    if switches > attaches:
        return check(False, f"{tag}: one switch counted twice ({switches} switches, {attaches} attaches)")
    return check(True, f"{tag}: {switches} switches, {attaches} attaches")


def halts_logged(pj):
    return re.findall(r"halt (GAP|PAYLOAD_CONFLICT|SCHEMA|MTU|EPOCH_MISMATCH): ", pj.run_log())


# ---------------------------------------------------------------------------------------------------------------- cases

def case_follow(args, cluster, pg, orders):
    url = pg.fresh("pj_follow")
    first = cluster.leader()  # the leader's archive first in the list: the configured order alone would pick it
    pj = Projector(args, cluster, url, 1, "a", archives=[first] + [i for i in range(3) if i != first]).start()
    check(orders.create(20) == 20, "20 orders acked")
    ok, took, f = caught_up(pj)
    check(ok, f"applied_seq reaches the head ({took:.1f} s)", json.dumps(f))
    leader = cluster.leader()
    m = member_of(cluster, f.get("endpoint"))
    if m == leader:
        check(False, "the leader's archive read while a follower could be", json.dumps(f))
    check(m is not None and m != leader, f"reads a follower's archive (member {m}, leader {leader})", json.dumps(f))
    print(f"sources {f.get('sources')}", flush=True)
    # a crossing pair filled by one trade: the projection holds the fills (a fill row per order)
    bridge = Bridge(args)
    try:
        check(bridge.wait_line("REGISTERED", 60), "the test bridge registered (BRIDGE session)")
        rc, _ = probe(args, "deposit", user=1002, asset=1, amount=10**6 * FP, rh=9, rl=2)
        check(rc == 0, "the seller holds the base asset")
        before = head_now(pj)
        buy = orders.create_ids(1, 1001, "BUY")
        sell = orders.create_ids(1, 1002, "SELL")
        sent = len(buy) == 1 and len(sell) == 1 and bridge.trade(1, sell[0], 1002, buy[0], 1001, FP, False)
        check(sent, f"a crossing pair (buy {buy}, sell {sell}) and its trade sent")
        filled = wait_for(lambda: pg.sql("pj_follow", "SELECT count(*) FROM oe_fills") == "2", CATCHUP_BOUND_S, 0.5)
        ok, took, f = caught_up(pj, at_least=before + 3)
        fills = pg.sql("pj_follow", "SELECT coalesce(string_agg(trade_id || '/' || order_id || '/' || side || '/' || "
                                    "quantity || '/' || maker, ' ' ORDER BY order_id), 'none') FROM oe_fills")
        check(filled and ok, f"the trade projected: two fill rows ({fills}), applied_seq at the head ({took:.1f} s)",
              json.dumps(f))
    finally:
        bridge.quit()
    # restart: the first replay position equals the stored memo's
    pj.stop()
    memos = {}
    for line in pg.sql("pj_follow", "SELECT source || ' ' || end_position || ' ' || seq FROM "
                                    "oe_projection_source_memo").splitlines():
        src, end, seq = line.split()
        memos[src] = (int(end), int(seq))
    pj.start()
    check(wait_for(lambda: pj.follow().get("phase") == "FOLLOWING", 30), "attached after the restart", json.dumps(pj.follow()))
    f = pj.follow()
    attached = f.get("endpoint")
    check(attached in memos and f.get("lastAttachFromMemo") and f.get("lastAttachFrom") == memos[attached][0],
          f"the first replay position after the restart equals the stored memo of the source attached (memos {memos}; "
          f"attached {attached} at {f.get('lastAttachFrom')}, from the memo: {f.get('lastAttachFromMemo')})",
          json.dumps(f))
    acked, want = created(pj, orders, 10)
    check(acked, "10 more orders acked")
    ok, took, f = caught_up(pj, at_least=want)
    check(ok, f"caught up after the restart ({took:.1f} s)", json.dumps(f))
    # the attached member killed and restarted, twice: the projector switches and catches up each time
    for n in (1, 2):
        if not check(on_follower(cluster, pj), f"switch {n}: attached to a follower before the fault",
                     json.dumps(pj.follow())):
            break
        f = pj.follow()
        m = member_of(cluster, f.get("endpoint"))
        switches = f.get("counts", {}).get("switches", 0)
        print(f"kill -9 member {m} (attached)", flush=True)
        cluster.kill(m)
        acked, want = created(pj, orders, 10)
        check(acked, f"switch {n}: orders acked while member {m} is down")
        ok, took, f = caught_up(pj, at_least=want)
        check(ok and f.get("counts", {}).get("switches", 0) > switches and member_of(cluster, f.get("endpoint")) != m,
              f"switch {n}: another source, caught up ({took:.1f} s)", json.dumps(f))
        cluster.start(m)
        check(wait_for(lambda: cluster.role(m) == "FOLLOWER", 90, 0.5), f"switch {n}: member {m} rejoined")
    # the leader killed: an election; history continues
    leader = cluster.leader()
    print(f"kill -9 the leader, member {leader}", flush=True)
    cluster.kill(leader)
    time.sleep(3)
    acked, want = created(pj, orders, 10)
    ok, took, f = caught_up(pj, CATCHUP_BOUND_S + 30, at_least=want)
    check(acked and ok, f"after the leader's death: caught up ({took:.1f} s)", json.dumps(f))
    cluster.start(leader)
    check(wait_for(lambda: cluster.role(leader) == "FOLLOWER", 90, 0.5), f"member {leader} rejoined")
    d, row = dense(pg, "pj_follow")
    check(d, f"rows dense from 1 (count, max, applied_seq = {row})")
    h = pj.health() or {}
    check(((h.get("projector") or {}).get("halt") or {}).get("reason") is None, "no halt set",
          json.dumps((h.get("projector") or {}).get("halt")))
    switches_accounted(pj, "follow")
    pj.stop()


def case_successor(args, cluster, pg, orders):
    url = pg.fresh("pj_successor")
    leader = cluster.leader()
    m = next(i for i in range(3) if i != leader)
    pj = Projector(args, cluster, url, 2, "s", archives=[m]).start()
    check(orders.create(10) == 10, "10 orders acked")
    ok, took, f = caught_up(pj)
    check(ok, f"caught up on member {m} alone ({took:.1f} s)", json.dumps(f))
    for n in (1, 2):
        rec = f.get("recordingId")
        print(f"kill -9 member {m} (the only source) with a live replay of recording {rec}", flush=True)
        cluster.kill(m)
        time.sleep(2)
        cluster.start(m)
        check(wait_for(lambda: cluster.role(m) == "FOLLOWER", 90, 0.5), f"round {n}: member {m} rejoined")
        acked, want = created(pj, orders, 10)
        check(acked, f"round {n}: orders acked")
        ok, took, f = caught_up(pj, CATCHUP_BOUND_S + 30, at_least=want)
        if not ok:
            check(False, f"round {n}: projector did not follow the successor recording (applied_seq "
                         f"{f.get('appliedSeq')}, head {f.get('headSeq')}, recording {rec} -> {f.get('recordingId')})",
                  json.dumps(f))
            break  # the next round cannot show more than this one did
        else:
            check(True, f"round {n}: the successor recording followed (recording {rec} -> {f.get('recordingId')}, "
                        f"{took:.1f} s)")
    d, row = dense(pg, "pj_successor")
    check(d, f"rows dense from 1 ({row})")
    pj.stop()


def case_replays(args, cluster, pg, orders):
    url = pg.fresh("pj_replays")
    baseline = {i: cluster.replays_active(i) for i in range(3)}
    print(f"replay-count baseline {baseline}", flush=True)
    check(all(v is not None and v >= 0 for v in baseline.values()), "every member serves its replay count",
          json.dumps(baseline))
    a = Projector(args, cluster, url, 3, "a").start()
    check(orders.create(10) == 10, "10 orders acked")
    ok, took, f = caught_up(a)
    check(ok and wait_for(lambda: a.follow().get("replaysOpen") == 1, 20) and a.follow().get("sessionOpen") is True,
          "writer: replaysOpen 1 and sessionOpen true from the real attach", json.dumps(a.follow()))
    f = a.follow()
    m = member_of(cluster, f.get("endpoint"))
    check(m is not None and cluster.replays_active(m) == baseline[m] + 1,
          f"member {m} counts the projector's replay", str(cluster.replays_active(m)))
    b = Projector(args, cluster, url, 4, "b").start()
    check(wait_for(lambda: (b.health() or {}).get("projector", {}).get("role") == "standby", 20), "a second copy is a standby")
    fb = b.follow()
    check(fb.get("replaysOpen") == 0 and fb.get("sessionOpen") is False, "standby: no replay and no session",
          json.dumps(fb))
    b.stop()
    # graceful: SIGTERM ends the replay at once, and the clean stop forgets its row before the lease goes
    a.stop()
    t0 = time.monotonic()
    rows = pg.sql("pj_replays", "SELECT count(*) FROM oe_projection_replay")
    if rows != "0":
        check(False, f"SIGTERM: a replay row left after a clean stop ({rows} rows)")
    else:
        check(True, "SIGTERM: the clean stop forgot its replay row")
    back = wait_for(lambda: cluster.replays_active(m) == baseline[m], LINGER_BOUND_S, 0.05)
    check(back, f"SIGTERM: the member's replay count is back at its baseline ({time.monotonic() - t0:.2f} s after the "
          f"exit; the member's driver keeps a closed replay's sender position for its publication linger)",
          str(cluster.replays_active(m)))
    # a second projector (another database) replays one member throughout: its replay must stay untouched
    others = [i for i in range(3) if i != cluster.leader()]
    shared = others[0]
    r = Projector(args, cluster, pg.fresh("pj_replays_r"), 11, "r", archives=[shared]).start()
    ok, took, fr = caught_up(r)
    check(ok and wait_for(lambda: r.follow().get("replaysOpen") == 1, 20), f"a second reader replays member {shared}",
          json.dumps(fr))
    base = dict(baseline)
    base[shared] += 1  # the second reader's replay is part of that member's baseline from here on

    def orphan_stopped(member, follower, t0, tag):
        """The killed writer's replay on `member` is the only one besides the baseline: the copy that follows reads
        other members, so the member is back at its baseline only when that replay was stopped."""
        stayed = []
        back = wait_for(lambda: cluster.replays_active(member) == base[member], STOP_BOUND_S, 0.1)
        took = time.monotonic() - t0
        counts = follower.follow().get("counts", {})
        if not back:
            stayed.append(cluster.replays_active(member))
            check(False, f"{tag}: replay left open on the source (member {member} counts {stayed[-1]}, baseline "
                         f"{base[member]}, {took:.1f} s after the kill)", json.dumps(counts))
            return False
        if counts.get("stopRequestsAnswered", 0) < 1:
            # with an embedded driver the killed copy's replay loses its receiver and the archive ends it by itself
            # within seconds, so the member's count alone does not show that the copy asked; the archive's answer to
            # its stopReplay does (an answer is not proof of a stop: the archive answers OK for a session it does not
            # know; the count back at its baseline inside the bound, in external mode, is that proof)
            check(False, f"{tag}: replay left open on the source: no stopReplay sent and answered (member "
                         f"{member} back at its baseline {took:.1f} s after the kill by the archive's own timeout)",
                  json.dumps(counts))
            return False
        return check(True, f"{tag}: the recorded replay's stopReplay answered, member {member} back at its baseline "
                           f"{took:.1f} s after the kill (stop requests answered {counts.get('stopRequestsAnswered')}, "
                           f"unknown {counts.get('orphanUnknown')})")

    # SIGKILL then a restart reading the other members only: the recorded replay is stopped by its session id
    a.start()
    for n in (1, 2):
        acked, want = created(a, orders, 5)
        check(acked, f"kill {n}: orders acked")
        ok, took, f = caught_up(a, at_least=want)
        check(ok and wait_for(lambda: a.follow().get("replaysOpen") == 1, 20), f"kill {n}: the writer follows",
              json.dumps(a.follow()))
        m = member_of(cluster, a.follow().get("endpoint"))
        rows = pg.sql("pj_replays", "SELECT count(*) FROM oe_projection_replay")
        check(m is not None and rows == "1" and cluster.replays_active(m) == base[m] + 1,
              f"kill {n}: the open replay on member {m} is recorded ({rows} row) and counted by the member",
              str(cluster.replays_active(m) if m is not None else None))
        t0 = time.monotonic()
        a.stop(signal.SIGKILL)
        a = Projector(args, cluster, url, 3, f"a{n}", archives=[i for i in range(3) if i != m]).start()
        orphan_stopped(m, a, t0, f"kill {n}")
        ok, took, f = caught_up(a)
        check(ok, f"kill {n}: caught up again on another member", json.dumps(f))
    # SIGKILL with a standby taking over: the standby reads the other members only
    check(wait_for(lambda: a.follow().get("replaysOpen") == 1, 20), "the writer holds a replay before its kill",
          json.dumps(a.follow()))
    m = member_of(cluster, a.follow().get("endpoint"))
    b = Projector(args, cluster, url, 4, "b2", archives=[i for i in range(3) if i != m]).start()
    check(wait_for(lambda: (b.health() or {}).get("projector", {}).get("role") == "standby", 20), "standby again")
    rows = pg.sql("pj_replays", "SELECT string_agg(source || '/' || replay_session_id, ' ') FROM oe_projection_replay")
    print(f"recorded replays before the kill: {rows}; the writer holds {a.follow().get('archiveId')}", flush=True)
    t0 = time.monotonic()
    a.stop(signal.SIGKILL)
    took_over = wait_for(lambda: (b.health() or {}).get("projector", {}).get("role") == "writer",
                         2 * LEASE_MS / 1000 + POLL_MS / 1000 + 5)
    check(took_over, f"the standby takes the lease ({time.monotonic() - t0:.1f} s)")
    orphan_stopped(m, b, time.monotonic(), "takeover")
    ok, took, f = caught_up(b)
    check(ok and wait_for(lambda: b.follow().get("replaysOpen") == 1, 20), "the new writer follows",
          json.dumps(b.follow()))
    # the second reader was never touched: one replay throughout, still following
    acked, want = created(b, orders, 5)
    ok, took, fr = caught_up(r, at_least=want)
    check(ok and fr.get("counts", {}).get("replaysStarted") == 1 and r.follow().get("replaysOpen") == 1,
          "the second reader's replay untouched throughout (one replay, still following)", json.dumps(fr))
    b.stop()
    r.stop()
    check(wait_for(lambda: all(cluster.replays_active(i) == baseline[i] for i in range(3)), ORPHAN_BOUND_S),
          "every member's replay count back at its baseline", json.dumps({i: cluster.replays_active(i) for i in range(3)}))

SLOW_FENCE = ("CREATE FUNCTION slow_fence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.02); "
              "RETURN NEW; END $$; CREATE TRIGGER slow_fence BEFORE UPDATE ON oe_projection_state FOR EACH ROW "
              "EXECUTE FUNCTION slow_fence()")


def case_long_overlap(args, cluster, pg, orders):
    """A cursor of 100 under a head of 250, a source with no memo: its 100 records are compared before anything is
    applied, through a writer slowed to about 20 ms a transaction of one record, so the compare alone outlasts the BEHIND
    window several times."""
    leader = cluster.leader()
    f1, f2 = [i for i in range(3) if i != leader]
    url = pg.fresh("pj_long")
    a = Projector(args, cluster, url, 13, "a", archives=[f1]).start()
    check(orders.create(100) == 100, "100 orders acked")
    ok, took, f = caught_up(a, at_least=100)
    check(ok, f"a caught up at 100 ({took:.1f} s)", json.dumps(f))
    a.stop()
    check(orders.create(150) == 150, "150 more orders acked while no projector runs")
    pg.sql("pj_long", SLOW_FENCE)
    slow = {"HISTORY_PROJECTOR_BATCH_EVENTS": "1", "HISTORY_PROJECTOR_SOURCE_BEHIND_MS": "800",
            "HISTORY_PROJECTOR_HEAD_PROBE_MS": "100", "HISTORY_PROJECTOR_HEAD_FRESH_MS": "2000",
            "HISTORY_PROJECTOR_READY_LAG_MS": "3000", "HISTORY_PROJECTOR_SOURCE_BACKOFF_MS": "200",
            "HISTORY_PROJECTOR_SOURCE_BACKOFF_MAX_MS": "400"}
    b = Projector(args, cluster, url, 14, "b", archives=[f2], extra=slow).start()
    ok, took, f = caught_up(b, 45, at_least=250)
    counts = f.get("counts", {})
    if not ok:
        check(False, f"BEHIND left a source that was comparing the overlap (applied {f.get('appliedSeq')}, head "
                     f"{f.get('headSeq')}, behind {counts.get('behind')}, replays {counts.get('replaysStarted')})",
              json.dumps(f))
    else:
        check(counts.get("behind", 0) == 0 and counts.get("skips", 0) >= 100,
              f"the overlap compared ({counts.get('skips')} equal) and applied_seq reached the head in {took:.1f} s "
              f"with no BEHIND", json.dumps(counts))
    b.stop()


EPOCHS = ("SELECT coalesce(string_agg(e || ':' || n, ' ' ORDER BY e), 'none') FROM (SELECT get_byte(payload, 16) e, "
          "count(*) n FROM oe_order_events GROUP BY 1) t")


def payload_epochs(pg, db):
    out = {}
    text = pg.sql(db, EPOCHS)
    for item in ([] if text == "none" else text.split()):
        e, n = item.split(":")
        out[int(e)] = int(n)
    return out


def case_regenesis(args, cluster, pg, orders):
    old, new = GENESIS, GENESIS + 1
    url_old = pg.fresh("pj_regen_old")
    pj = Projector(args, cluster, url_old, 15, "o").start()
    check(orders.create(20) == 20, f"genesis {old}: 20 orders acked")
    ok, took, f = caught_up(pj, at_least=20)
    check(ok, f"a projection of genesis {old} at 20 ({took:.1f} s)", json.dumps(f))
    pj.stop()
    orders.quit()
    # every member's cluster state goes, its archive (the order journal recordings of the old genesis) stays
    for i in range(3):
        cluster.kill(i)
    for i in range(3):
        shutil.rmtree(os.path.join(args.work, f"core{i}", "cluster"), ignore_errors=True)
    cluster.start()
    check(wait_for(lambda: cluster.leader() is not None, 120, 0.5), "re-genesis: a leader on the kept archives")
    check(genesis(args, new), f"a new genesis {new} recorded")
    o2 = Orders(args)
    try:
        check(wait_for(lambda: (o2.status() or {}).get("registered"), 60, 0.5), "the order session registered")
        check(o2.create(5) == 5, f"genesis {new}: 5 orders acked")
        # an empty database
        url = pg.fresh("pj_regen")
        pj = Projector(args, cluster, url, 16, "r").start()
        ok, took, f = caught_up(pj, at_least=5)
        epochs = payload_epochs(pg, "pj_regen")
        state = pg.sql("pj_regen", "SELECT coalesce(epoch::text, 'null') || ' ' || applied_seq FROM oe_projection_state")
        if set(epochs) - {new}:
            check(False, f"projection built from a previous genesis (payload epochs {epochs}, state {state})",
                  json.dumps(f))
        check(ok and epochs == {new: 5} and state == f"{new} 5" and
              f.get("counts", {}).get("foreignEpochRecordings", 0) >= 1,
              f"an empty database holds only genesis {new} (payload epochs {epochs}, state {state}, foreign recordings "
              f"{f.get('counts', {}).get('foreignEpochRecordings')})", json.dumps(f))
        pj.stop()
        # the projection of the old genesis: EPOCH_MISMATCH; --rebuild is its exit
        pj = Projector(args, cluster, url_old, 15, "o2").start()
        halted = wait_for(lambda: ((pj.health() or {}).get("projector", {}).get("halt") or {}).get("reason") ==
                          "EPOCH_MISMATCH", 30)
        check(halted and payload_epochs(pg, "pj_regen_old") == {old: 20},
              f"the projection of genesis {old}: EPOCH_MISMATCH, nothing added", json.dumps(pj.follow()))
        pj.stop()
        rb = subprocess.run([pj.binary, "--rebuild"], env=pj.env, capture_output=True, text=True, timeout=60)
        check(rb.returncode == 0, f"--rebuild exits 0 ({rb.returncode})", rb.stderr[-600:])
        pj = Projector(args, cluster, url_old, 15, "o3").start()
        ok, took, f = caught_up(pj, at_least=5)
        epochs = payload_epochs(pg, "pj_regen_old")
        if set(epochs) - {new}:
            check(False, f"projection built from a previous genesis after --rebuild (payload epochs {epochs})",
                  json.dumps(f))
        check(ok and epochs == {new: 5}, f"after --rebuild: only genesis {new} ({epochs})", json.dumps(f))
        pj.stop()
    finally:
        o2.quit()



def case_epoch(args, cluster, pg, orders):
    # before the genesis is recorded: no genesisId on the cluster, nothing written
    url = pg.fresh("pj_epoch")
    pj = Projector(args, cluster, url, 5, "e").start()
    time.sleep(8)
    epoch = pg.sql("pj_epoch", "SELECT coalesce(epoch::text, 'null') || ' ' || applied_seq FROM oe_projection_state")
    rows = pg.sql("pj_epoch", "SELECT count(*) FROM oe_order_events")
    check(epoch == "null 0" and rows == "0", f"no genesis on the cluster: nothing written (state {epoch}, {rows} rows)")
    check(genesis(args), "genesis recorded (CoreConfig with genesisId)")
    check(orders.create(5) == 5, "5 orders acked")
    ok, took, f = caught_up(pj)
    epoch = pg.sql("pj_epoch", "SELECT epoch FROM oe_projection_state")
    check(ok and epoch == str(GENESIS), f"the cluster's genesisId {GENESIS} became the epoch ({epoch})", json.dumps(f))
    pj.stop()
    # a database of another genesis
    url2 = pg.fresh("pj_epoch_other")
    pj2 = Projector(args, cluster, url2, 6, "o")
    pj2.start()
    def migrated():
        try:
            return pg.sql("pj_epoch_other", "SELECT count(*) FROM oe_projection_state") == "1"
        except RuntimeError:
            return False
    check(wait_for(migrated, 30), "the second database migrated")
    pj2.stop()
    pg.sql("pj_epoch_other", "UPDATE oe_projection_state SET epoch = 999, applied_seq = 0")
    pj2.start()
    halted = wait_for(lambda: ((pj2.health() or {}).get("projector", {}).get("halt") or {}).get("reason") ==
                      "EPOCH_MISMATCH", 30)
    rows = pg.sql("pj_epoch_other", "SELECT count(*) FROM oe_order_events")
    reason = pg.sql("pj_epoch_other", "SELECT coalesce(halt_reason, '') FROM oe_projection_state")
    if rows != "0":
        check(False, "rows of two genesis ids in one projection", rows)
    check(halted and rows == "0" and reason == "EPOCH_MISMATCH" and not pj2.ready(),
          f"another genesis: EPOCH_MISMATCH, nothing written, not ready (rows {rows}, state {reason})",
          json.dumps((pj2.health() or {}).get("projector", {}).get("halt")))
    pj2.stop()


def case_mtu(args, cluster, pg, orders):
    url = pg.fresh("pj_mtu")
    pj = Projector(args, cluster, url, 7, "m", extra={"HISTORY_PROJECTOR_JOURNAL_MTU": str(JOURNAL_MTU * 2)}).start()
    check(orders.create(5) == 5, "5 orders acked")
    halted = wait_for(lambda: ((pj.health() or {}).get("projector", {}).get("halt") or {}).get("reason") == "MTU", 60)
    rows = pg.sql("pj_mtu", "SELECT count(*) FROM oe_order_events")
    text = pj.log_text()
    f = pj.follow()
    check(halted and rows == "0" and "replay MTU differs from the recording" in text and not pj.ready() and
          f.get("counts", {}).get("mtuRefusals", 0) >= 3,
          f"every source refused: replay MTU differs from the recording; nothing read ({rows} rows), not ready",
          json.dumps(f))
    if rows != "0":
        check(False, "replay opened with a different MTU")
    pj.stop()


def case_replay_cap(args, cluster, pg, orders):
    leader = cluster.leader()
    m = next(i for i in range(3) if i != leader)
    a = Projector(args, cluster, pg.fresh("pj_cap_a"), 8, "a", archives=[m]).start()
    check(orders.create(5) == 5, "5 orders acked")
    ok, took, f = caught_up(a)
    check(ok and cluster.replays_active(m) == 1, f"projector a holds the one reader slot of member {m}", json.dumps(f))
    # b sees the member's budget used up on its node: it does not ask
    b = Projector(args, cluster, pg.fresh("pj_cap_b"), 9, "b", archives=[m]).start()
    time.sleep(8)
    fb = b.follow()
    check(fb.get("replaysOpen") == 0 and "budget-full" in " ".join(fb.get("sources", [])) and
          fb.get("counts", {}).get("refusedReplays", 0) == 0,
          "b: the member's reader budget is full, no replay asked", json.dumps(fb))
    b.stop()
    # c has no node endpoint: it asks, the archive refuses, it backs off (requests per minute under the back-off's rate)
    c = Projector(args, cluster, pg.fresh("pj_cap_c"), 10, "c", archives=[m], nodes=False).start()
    t0 = time.monotonic()
    time.sleep(60)
    fc = c.follow()
    refused = fc.get("counts", {}).get("refusedReplays", 0)
    per_min = refused * 60 / (time.monotonic() - t0)
    # the back-off from 500 ms doubling to a 4000 ms cap: at most 1 + 60 / 0.5 ... bounded by ~20 a minute
    bound = 25
    check(refused >= 1, f"c: the archive refused its replay ({refused} refusals)", json.dumps(fc))
    if per_min > bound:
        check(False, f"replay requests flood the source archive ({per_min:.1f} a minute, bound {bound})")
    else:
        check(True, f"c: {per_min:.1f} refused requests a minute (bound {bound})")
    c.stop()
    a.stop()


# ---- rows on the test build with source faults (oe-projector-tc-faults: PROJ_TC_SOURCE_FAULTS drops or delays records
# of a configured source in the frame sink, before the forward rule; the archives are listed in member order, so a source
# index is a member index)

def faults(spec):
    return {"PROJ_TC_SOURCE_FAULTS": spec}


def case_holes(args, cluster, pg, orders):
    """Both followers holed, each lacking a record the other holds: the projection reads past each hole from the other
    follower, never halts, rows dense, a follower read at the end."""
    leader = cluster.leader()
    f1, f2 = [i for i in range(3) if i != leader]
    check(orders.create(30) == 30, "30 orders acked")
    url = pg.fresh("pj_holes")
    pj = Projector(args, cluster, url, 17, "h", extra=faults(f"{f1}:drop=10;{f2}:drop=20")).start()
    ok, took, f = caught_up(pj, 45, at_least=30)
    halts = halts_logged(pj)
    if not ok or halts:
        check(False, f"halted on a hole another source could fill (halts {halts}, applied {f.get('appliedSeq')}, head "
                     f"{f.get('headSeq')})", json.dumps(f))
    else:
        check(f.get("counts", {}).get("holes", 0) >= 2,
              f"member {f1} lacks 10, member {f2} lacks 20: read past both from the other follower, no halt "
              f"({took:.1f} s, holes {f.get('counts', {}).get('holes')})", json.dumps(f))
    # the leader may be read while both followers are marked (the first follower's mark is cleared by the commit of the
    # record it lacked, which can come after the second follower's hole); the reader leaves it once a follower can serve
    read_leader = f"attaching source {cluster.archive(leader)}" in pj.run_log()
    on_f = wait_for(lambda: member_of(cluster, pj.follow().get("endpoint")) in (f1, f2), 15, 0.5)
    check(on_f, f"reads a follower at the end (the leader read meanwhile: {read_leader})", json.dumps(pj.follow()))
    d, row = dense(pg, "pj_holes")
    check(d, f"rows dense from 1 ({row})")
    switches_accounted(pj, "holes")
    pj.stop()


def case_above_one(args, cluster, pg, orders):
    """A member wiped and rejoined: its recordings start above 1 (here records 1..10 dropped from the follower read
    first). An empty projection starts at 1 from another follower (BELOW_RANGE marks the first), never at the first
    record a source holds; once the range it lacked is applied, the first follower serves what follows."""
    leader = cluster.leader()
    f1, f2 = [i for i in range(3) if i != leader]
    check(orders.create(30) == 30, "30 orders acked")
    url = pg.fresh("pj_above")
    pj = Projector(args, cluster, url, 18, "w", extra=faults(f"{f1}:below=11")).start()
    ok, took, f = caught_up(pj, 45, at_least=30)
    first = pg.sql("pj_above", "SELECT coalesce(min(journal_seq), 0) FROM oe_order_events")
    refused = "not the next record" in pj.run_log()
    if first not in ("0", "1") or refused:
        check(False, f"projection started above 1 (first row {first}, the writer refused an APPLY above its cursor: "
                     f"{refused})", json.dumps(f))
    check(ok and f.get("counts", {}).get("belowRange", 0) >= 1 and first == "1",
          f"member {f1} holds 11.. only: BELOW_RANGE, the projection starts at 1 from member {f2} ({took:.1f} s)",
          json.dumps(f))
    # the range member f1 lacked is applied: with member f2 down, f1 serves the rest (the leader is not read)
    cluster.kill(f2)
    acked, want = created(pj, orders, 10)
    check(acked, f"10 orders acked with member {f2} down")
    ok, took, f = caught_up(pj, at_least=want)
    check(ok and member_of(cluster, f.get("endpoint")) == f1,
          f"member {f1} serves what follows the range it lacked ({took:.1f} s)", json.dumps(f))
    d, row = dense(pg, "pj_above")
    check(d, f"rows dense from 1 ({row})")
    switches_accounted(pj, "above_one")
    pj.stop()


def case_slow(args, cluster, pg, orders):
    """The follower read first delivers 4 records a second while the core takes about 10 a second: BEHIND leaves it for
    the other follower, which closes the lag."""
    leader = cluster.leader()
    f1, f2 = [i for i in range(3) if i != leader]
    url = pg.fresh("pj_slow")
    extra = faults(f"{f1}:rate=4")
    extra.update({"HISTORY_PROJECTOR_SOURCE_BEHIND_MS": "1500", "HISTORY_PROJECTOR_HEAD_PROBE_MS": "200",
                  "HISTORY_PROJECTOR_HEAD_FRESH_MS": "1000"})
    pj = Projector(args, cluster, url, 19, "s", extra=extra).start()
    check(orders.create(4) == 4, "4 orders acked")
    ok, took, f = caught_up(pj, 30, at_least=4)
    check(ok and member_of(cluster, f.get("endpoint")) == f1, f"attached to member {f1}, caught up", json.dumps(f))
    t0 = time.monotonic()
    left_at = None
    slow_left = f"source {cluster.archive(f1)} left: BEHIND"
    for _ in range(12):  # 60 orders over about 6 s
        orders.create(5)
        if left_at is None and slow_left in pj.run_log():
            left_at = time.monotonic() - t0
        time.sleep(0.4)
    want = head_now(pj)
    if left_at is None:
        left_at = time.monotonic() - t0 if wait_for(lambda: slow_left in pj.run_log(), 4, 0.2) else None
    if left_at is None:
        f = pj.follow()
        check(False, f"attached to a lagging source while another was current (member {f1} still read "
                     f"{time.monotonic() - t0:.1f} s into the load, applied {f.get('appliedSeq')}, head "
                     f"{f.get('headSeq')}, behind {f.get('counts', {}).get('behind')})", json.dumps(f))
    else:
        check(True, f"BEHIND left member {f1} {left_at:.1f} s into the load")
    ok, took, f = caught_up(pj, 30, at_least=max(want, 64))
    check(ok and f.get("counts", {}).get("behind", 0) >= 1,
          f"applied_seq at the head ({took:.1f} s after the load, read from member {member_of(cluster, f.get('endpoint'))})",
          json.dumps(f))
    d, row = dense(pg, "pj_slow")
    check(d, f"rows dense from 1 ({row})")
    switches_accounted(pj, "slow")
    pj.stop()


def case_writer_reset(args, cluster, pg, orders):
    """The writer starts a generation while the follow side is attached and the cluster is idle (the batch applying the
    last record fails once, as a belt fails it): the source is chosen again at once and the projection reaches the head
    with no new order and well inside the BEHIND window."""
    leader = cluster.leader()
    m = next(i for i in range(3) if i != leader)
    check(orders.create(20) == 20, "20 orders acked")
    url = pg.fresh("pj_reset")
    pj = Projector(args, cluster, url, 20, "r", archives=[m],
                   extra={"PROJ_TC_RELOAD_AT": "20", "HISTORY_PROJECTOR_SOURCE_BEHIND_MS": "30000"}).start()
    ok, took, f = caught_up(pj, 15, at_least=20)
    fired = "test fault: the batch applying journalSeq 20 fails once" in pj.run_log()
    check(fired, "the writer failed the batch with journalSeq 20 once and started a generation")
    if not ok:
        check(False, f"the source stayed attached and idle after a writer reset (applied {f.get('appliedSeq')}, head "
                     f"{f.get('headSeq')}, phase {f.get('phase')}, generation {f.get('generation')})", json.dumps(f))
    else:
        check(f.get("generation", 0) >= 2, f"read again after the reset: at the head in {took:.1f} s (generation "
                                            f"{f.get('generation')})", json.dumps(f))
    d, row = dense(pg, "pj_reset")
    check(d, f"rows dense from 1 ({row})")
    pj.stop()


def beat_row(pg, db):
    """(beat_at as text or None, applied_seq, the database clock less beat_at in ms or None, halt_reason)."""
    row = pg.sql(db, "SELECT coalesce(beat_at::text, '-') || '|' || applied_seq || '|' || coalesce((extract(epoch FROM "
                     "clock_timestamp() - beat_at) * 1000)::bigint::text, '-') || '|' || coalesce(halt_reason, '-') "
                     "FROM oe_projection_state")
    beat, applied, age, halt = row.split("|")
    return (None if beat == "-" else beat), int(applied), (None if age == "-" else int(age)), (None if halt == "-" else halt)


def stop_members(cluster, sig):
    for i in range(3):
        p = cluster.procs.get(i)
        if p is not None and p.poll() is None:
            os.kill(p.pid, sig)


def no_beat_over(pg, db, seconds, what, settle=0.6):
    """beat_at does not move for `seconds` (after `settle` for a beat already in flight); the values seen."""
    time.sleep(settle)
    first = beat_row(pg, db)[0]
    end = time.monotonic() + seconds
    moved = None
    while time.monotonic() < end:
        now = beat_row(pg, db)[0]
        if now != first:
            moved = now
            break
        time.sleep(0.2)
    if moved is not None:
        check(False, f"beat without a fresh head ({what}: beat_at moved from {first} to {moved})")
        return False
    check(True, f"{what}: no beat over {seconds:.1f} s (beat_at {first})")
    return True


def case_beat(args, cluster, pg, orders):
    """The beat rule, one projection of one follower through six states: beats while idle and under continuous load;
    none while the cursor is below the core's head, while the probe goes unanswered, while the session is down, while
    a halt is set. Under load the distance between beat_at and the database clock is recorded as data."""
    leader = cluster.leader()
    f1 = next(i for i in range(3) if i != leader)
    check(orders.create(200) == 200, "200 orders acked")
    db = "pj_beat"
    url = pg.fresh(db)
    probe_ms = 200
    extra = faults("0:rate=25")  # the one source delivers 25 records a second: the catch-up of the backlog is slow
    extra.update({"HISTORY_PROJECTOR_HEAD_PROBE_MS": str(probe_ms), "HISTORY_PROJECTOR_HEAD_FRESH_MS": "1000",
                  "HISTORY_PROJECTOR_READY_LAG_MS": "1500", "HISTORY_PROJECTOR_SOURCE_BEHIND_MS": "60000",
                  "HISTORY_PROJECTOR_REQUEST_TIMEOUT_MS": "1000"})
    pj = Projector(args, cluster, url, 31, "beat", archives=[f1], extra=extra).start()
    check(wait_for(lambda: pg.sql(db, "SELECT to_regclass('oe_projection_state') IS NOT NULL") == "t", 30, 0.2),
          "the projection schema applied")
    # 1. applied below head: the backlog is read at 25 a second, the head stays where it is; no beat until the cursor
    # reaches a probed head
    samples = 0
    below = 0
    early = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 30:
        f = pj.follow()
        beat, applied, age, halt = beat_row(pg, db)
        head = f.get("headSeq", -1)
        if head > 0 and applied < head:
            below += 1
            if beat is not None and early is None:
                early = (beat, applied, head)
            if pj.ready():
                early = early or ("ready", applied, head)
        samples += 1
        if head > 0 and applied >= head:
            break
        time.sleep(0.25)
    if below < 10:
        check(False, f"INCONCLUSIVE: the cursor was below the head in {below} samples only (precondition)")
    elif early is not None:
        check(False, f"beat without a fresh head (applied below head: beat_at {early[0]} at applied_seq {early[1]}, "
                     f"head {early[2]})")
    else:
        check(True, f"applied below head: no beat and not ready in {below} samples while the cursor was below the head")
    ok, took, f = caught_up(pj, 30, at_least=200)
    beat_seen = wait_for(lambda: beat_row(pg, db)[0] is not None, 5, 0.1)
    check(ok and beat_seen, f"at the head: the first beat ({took:.1f} s)", json.dumps(f))
    # 2. idle: beats keep coming, young, and /ready is true
    values = set()
    ages = []
    for _ in range(15):
        beat, applied, age, halt = beat_row(pg, db)
        values.add(beat)
        ages.append(age)
        time.sleep(0.2)
    ready = wait_for(pj.ready, 5, 0.2)
    check(len(values) >= 5 and max(a for a in ages if a is not None) < 1500 and ready,
          f"idle: {len(values)} distinct beats in 3 s, ages {min(ages)}..{max(ages)} ms, ready {ready}")
    # 3. continuous load (about 12 orders a second, below the source's 25): beats occur, lag never has to reach zero
    stop = threading.Event()

    def load():
        while not stop.is_set():
            orders.create(3)
            time.sleep(0.25)
    loader = threading.Thread(target=load, daemon=True)
    loader.start()
    time.sleep(1.0)
    values = set()
    distances = []
    lag_events = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < 6:
        beat, applied, age, halt = beat_row(pg, db)
        values.add(beat)
        distances.append(age)
        f = pj.follow()
        lag_events.append(max(0, f.get("headSeq", 0) - f.get("appliedSeq", 0)))
        time.sleep(0.2)
    stop.set()
    loader.join(timeout=30)
    dist = [d for d in distances if d is not None]
    print(f"DATA under load: beat_at distance to the database clock ms min {min(dist)} median "
          f"{sorted(dist)[len(dist) // 2]} max {max(dist)}; lag in events max {max(lag_events)}", flush=True)
    check(len(values) >= 5, f"continuous load: {len(values)} distinct beats in 6 s")
    ok, took, f = caught_up(pj, 30, at_least=200)
    check(ok, "at the head after the load", json.dumps(f))
    # 4. probe unanswered: every member frozen for 3 s (no answer to a probe sent meanwhile)
    stop_members(cluster, signal.SIGSTOP)
    try:
        answered_before = pj.follow().get("headSeq")
        no_beat_over(pg, db, 2.4, "probe unanswered")
        not_ready = not pj.ready()
    finally:
        stop_members(cluster, signal.SIGCONT)
    check(not_ready, "probe unanswered: not ready")
    check(wait_for(lambda: cluster.leader() is not None, 60, 0.5), "the cluster answers again")
    check(wait_for(lambda: beat_row(pg, db)[2] is not None and beat_row(pg, db)[2] < 1500, 30, 0.3),
          "beats again after the freeze")
    # 5. session down: two members killed (no quorum: no leader, the session cannot stay registered) until the session
    # has read down for 2 s; the member the projector reads stays up. Then both restart and the cluster elects again.
    # (Freezing every member for this long costs the cluster its recovery in this rig, which is not this row.)
    victims = [i for i in range(3) if i != f1]
    for i in victims:
        cluster.kill(i)
    session_down = None
    time.sleep(0.6)
    first = beat_row(pg, db)[0]
    moved = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 30:
        h = pj.health() or {}
        s = h.get("session") or {}
        if session_down is None and (s.get("registered") is False or s.get("connected") is False):
            session_down = time.monotonic() - t0
        now = beat_row(pg, db)[0]
        if now != first:
            moved = now
            break
        if session_down is not None and time.monotonic() - t0 > session_down + 2:
            break
        time.sleep(0.2)
    down_ready = pj.ready()
    print(f"DATA session read down {session_down} s after the quorum was lost", flush=True)
    for i in victims:
        cluster.start(i)
    if moved is not None:
        check(False, f"beat without a fresh head (session down: beat_at moved from {first} to {moved})")
    elif session_down is None:
        check(False, "INCONCLUSIVE: the session never read down in 30 s without a quorum (precondition)")
    else:
        check(not down_ready, f"session down: no beat, not ready (beat_at {first})")
    check(wait_for(lambda: cluster.leader() is not None, 120, 0.5), "the cluster elects again")
    check(wait_for(lambda: beat_row(pg, db)[2] is not None and beat_row(pg, db)[2] < 1500, 90, 0.3),
          "beats again after the session came back", json.dumps(pj.follow()))
    # 6. halt: the same projection under a copy whose journal MTU differs (every source refused: halt MTU) while the
    # cluster answers its probes and the cursor is at the head
    pj.stop()
    extra2 = dict(extra)
    extra2["HISTORY_PROJECTOR_JOURNAL_MTU"] = str(JOURNAL_MTU * 2)
    pj2 = Projector(args, cluster, url, 32, "beat-mtu", archives=[f1], extra=extra2).start()
    halted = wait_for(lambda: beat_row(pg, db)[3] == "MTU", 60, 0.3)
    f = pj2.follow()
    probed = wait_for(lambda: pj2.follow().get("headSeq", -1) >= 0 and
                      pj2.follow().get("appliedSeq", 0) >= pj2.follow().get("headSeq", 0), 10, 0.2)
    if not halted or not probed:
        check(False, f"INCONCLUSIVE: halt {beat_row(pg, db)[3]}, probe answered at the head {probed} (precondition)",
              json.dumps(f))
    else:
        no_beat_over(pg, db, 3.0, "halt MTU with the probe answered at the head")
        check(not pj2.ready(), "halted: not ready")
    pj2.stop()


def case_beat_epoch(args, cluster, pg, orders):
    """A database of the old genesis whose cursor is far above the new genesis's head: EPOCH_MISMATCH, no beat, not
    ready, however the two sequences compare."""
    check(orders.create(10) == 10, "10 orders acked")
    db = "pj_beat_epoch"
    url = pg.fresh(db)
    pj = Projector(args, cluster, url, 33, "be").start()
    ok, took, f = caught_up(pj, 45, at_least=10)
    check(ok, "a projection at the head", json.dumps(f))
    pj.stop()
    # the projection of another genesis: its epoch another id, its cursor far above anything this core emitted
    pg.sql(db, "UPDATE oe_projection_state SET epoch = 4242, applied_seq = 1000000, applied_order_id = 1000000, "
               "beat_at = clock_timestamp() - interval '1 hour', halt_reason = NULL, halt_detail = NULL, lease_until = NULL")
    pj = Projector(args, cluster, url, 34, "be2").start()
    halted = wait_for(lambda: ((pj.health() or {}).get("projector", {}).get("halt") or {}).get("reason") ==
                      "EPOCH_MISMATCH", 60, 0.3)
    probed = wait_for(lambda: 0 <= pj.follow().get("headSeq", -1) < 1000000, 10, 0.2)
    f = pj.follow()
    if not halted or not probed:
        check(False, f"INCONCLUSIVE: halt EPOCH_MISMATCH {halted}, probe answered {probed} (precondition)", json.dumps(f))
    else:
        first = beat_row(pg, db)[0]
        moved = None
        t0 = time.monotonic()
        while time.monotonic() - t0 < 4:
            now = beat_row(pg, db)[0]
            if now != first:
                moved = now
                break
            time.sleep(0.2)
        if moved is not None:
            check(False, f"beat across genesis ids (beat_at moved from {first} to {moved}; the cluster's genesis "
                         f"{f.get('genesisId')}, the projection's epoch {f.get('epoch')}, head {f.get('headSeq')}, "
                         f"applied {f.get('appliedSeq')})")
        else:
            check(not pj.ready(), f"EPOCH_MISMATCH: no beat in 4 s and not ready (head {f.get('headSeq')} far below "
                                  f"applied {f.get('appliedSeq')})", json.dumps(f))
    pj.stop()


def halt_cleared(pj, pg, db, what, timeout=45):
    """The copy that writes after a halt clears it and beats again: halt_reason NULL, a young beat_at, /ready true."""
    def done():
        beat, applied, age, halt = beat_row(pg, db)
        return halt is None and age is not None and age < 1500 and pj.ready()
    t0 = time.monotonic()
    ok = wait_for(done, timeout, 0.3)
    took = time.monotonic() - t0
    beat, applied, age, halt = beat_row(pg, db)
    f = pj.follow()
    if not ok:
        check(False, f"halt latched across a {what} (halt_reason {halt}, beat_at {beat}, applied {applied}, head "
                     f"{f.get('headSeq')}, ready {pj.ready()}, after {took:.1f} s)", json.dumps(f))
    else:
        check(True, f"{what}: halt cleared, beat_at {age} ms old, ready ({took:.1f} s, applied {applied})")
    return ok


def case_halt_restart(args, cluster, pg, orders):
    """A halt recorded in the state row is not a latch: the copy that writes next (a restart, a standby taking over)
    starts with it and clears it by the rule that clears it in a running copy (an APPLY; on an idle cluster, the
    attached source verified up to the cursor at the core's head), then beats again."""
    check(orders.create(30) == 30, "30 orders acked")
    db = "pj_halt"
    url = pg.fresh(db)
    # 1. a restart: every source lacks record 10 (GAP at 9); the next copy reads with the fault gone
    a = Projector(args, cluster, url, 35, "halt-a", extra=faults("0:drop=10;1:drop=10;2:drop=10")).start()
    check(wait_for(lambda: pg.sql(db, "SELECT to_regclass('oe_projection_state') IS NOT NULL") == "t", 30, 0.2),
          "the projection schema applied")
    halted = wait_for(lambda: beat_row(pg, db)[3] == "GAP", 60, 0.3)
    a.stop()
    if not halted:
        check(False, f"INCONCLUSIVE: no GAP with record 10 missing on every source (precondition, {beat_row(pg, db)})")
        return
    check(beat_row(pg, db)[1] == 9, f"GAP at applied_seq 9 ({beat_row(pg, db)})")
    b = Projector(args, cluster, url, 36, "halt-b").start()
    halt_cleared(b, pg, db, "restart")
    ok, took, f = caught_up(b, at_least=30)
    check(ok, "the restarted copy at the head", json.dumps(f))
    base = f.get("headSeq", 30)
    b.stop()
    # 2. a standby taking over: every source lacks the fifth record after the cursor under the writer (GAP); a
    # fault-free standby waits; the writer killed
    check(orders.create(10) == 10, "10 more orders acked")
    want = base + 10
    drop = base + 5
    c = Projector(args, cluster, url, 37, "halt-c", extra=faults(f"0:drop={drop};1:drop={drop};2:drop={drop}")).start()
    halted = wait_for(lambda: beat_row(pg, db)[3] == "GAP" and beat_row(pg, db)[1] == drop - 1, 60, 0.3)
    d = Projector(args, cluster, url, 38, "halt-d").start()
    standby = wait_for(lambda: "standby: another copy holds the writer lease" in d.run_log(), 20, 0.3)
    if not halted or not standby:
        check(False, f"INCONCLUSIVE: GAP at {drop - 1} {halted}, a standby waiting {standby} (precondition, "
                     f"{beat_row(pg, db)})")
        c.stop()
        d.stop()
        return
    c.stop(signal.SIGKILL)
    halt_cleared(d, pg, db, "takeover")
    ok, took, f = caught_up(d, at_least=want)
    check(ok, "the copy that took over at the head", json.dumps(f))
    d.stop()
    # 3. a restart on an idle cluster: a copy whose journal MTU differs halts MTU with the cursor at the head; the next
    # copy has no record to apply, so its source's compares up to the cursor clear the halt
    e = Projector(args, cluster, url, 39, "halt-e", extra={"HISTORY_PROJECTOR_JOURNAL_MTU": str(JOURNAL_MTU * 2)}).start()
    halted = wait_for(lambda: beat_row(pg, db)[3] == "MTU", 60, 0.3)
    e.stop()
    if not halted:
        check(False, f"INCONCLUSIVE: no MTU halt (precondition, {beat_row(pg, db)})")
        return
    g = Projector(args, cluster, url, 40, "halt-g").start()
    halt_cleared(g, pg, db, "restart on an idle cluster")
    check(beat_row(pg, db)[1] >= want, f"at the head on the idle cluster (applied_seq {beat_row(pg, db)[1]})")
    d_ok, row = dense(pg, db)
    check(d_ok, f"rows dense from 1 ({row})")
    g.stop()


CASES = {"follow": case_follow, "successor": case_successor, "replays": case_replays, "epoch": case_epoch,
         "mtu": case_mtu, "replay_cap": case_replay_cap, "long_overlap": case_long_overlap,
         "regenesis": case_regenesis, "holes": case_holes, "above_one": case_above_one, "slow": case_slow,
         "writer_reset": case_writer_reset, "beat": case_beat, "beat_epoch": case_beat_epoch,
         "halt_restart": case_halt_restart}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--image", required=True)
    ap.add_argument("--mode", required=True, choices=["embedded", "external"])
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=9800)
    ap.add_argument("--binary", default="oe-projector")
    ap.add_argument("--core-driver", default="embedded", choices=["embedded", "external"])
    ap.add_argument("case", choices=sorted(CASES))
    args = ap.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    base = args.port_base
    args.ingress = f"0=localhost:{base + 2},1=localhost:{base + 102},2=localhost:{base + 202}"
    args.key_file = os.path.join(args.work, "cluster-key")
    with open(args.key_file, "w") as f:
        f.write(GATEWAY_KEY + "\n")
    print(f"follow_it {args.case} mode={args.mode} core-driver={args.core_driver} binary={args.binary} "
          f"work={args.work} ports={base}", flush=True)
    extra = {}
    if args.case == "replay_cap":
        # one replay per archive and none reserved: the archive itself refuses a second reader (its cap is the only one)
        extra = {"CORE_ARCHIVE_MAX_CONCURRENT_REPLAYS": "1", "CORE_ARCHIVE_CLUSTER_RESERVED_REPLAYS": "0"}
    cluster = Cluster(args, extra)
    driver = None
    pg = None
    orders = None
    try:
        os.makedirs(cluster.shm, exist_ok=True)
        if args.mode == "external":
            args.driver_dir = os.path.join(cluster.shm, "external-driver")
            dlog = open(os.path.join(args.work, "driver.log"), "w")
            driver = subprocess.Popen([os.path.join(args.bin_dir, "gw_test_media_driver"), args.driver_dir],
                                      stdout=dlog, stderr=subprocess.STDOUT)
            wait_for(lambda: os.path.exists(os.path.join(args.driver_dir, "cnc.dat")), 10, 0.1)
        else:
            args.driver_dir = os.path.join(cluster.shm, "embedded")
            os.makedirs(args.driver_dir, exist_ok=True)
        pg = Pg(args.image)
        cluster.start()
        check(wait_for(lambda: cluster.leader() is not None, 90, 0.5), "the core elected a leader")
        if args.case != "epoch":
            check(genesis(args), "genesis recorded (CoreConfig with genesisId, risk, gate OPEN, deposit)")
        orders = Orders(args)
        check(wait_for(lambda: (orders.status() or {}).get("registered"), 60, 0.5), "the order session registered")
        CASES[args.case](args, cluster, pg, orders)
    except Exception as e:  # a crash is never a pass
        check(False, f"exception: {type(e).__name__}: {e}")
    finally:
        for p in STARTED:  # a case that raised leaves no projector or helper behind
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except OSError:
                    p.kill()
                p.wait()
        if orders is not None:
            orders.quit()
        cluster.stop()
        if pg is not None:
            pg.close()
        if driver is not None:
            driver.terminate()
            try:
                driver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                driver.kill()
    print(("FOLLOW IT PASS " if not FAILED else "FOLLOW IT FAIL ") + args.case + " " + args.mode +
          ("" if args.core_driver == "embedded" else " core-external") +
          (" " + " | ".join(FAILED) if FAILED else ""), flush=True)
    return 0 if not FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
