#!/usr/bin/env python3
"""oe-marketdata's trade path through the real binary (T1): an ME-journal-shaped Aeron archive (the cluster-tools
JournalArchiveFixture on the pinned aeron-all jar), a throwaway PostgreSQL container holding the market history, and
oe-marketdata in the driver mode given. No ME cluster: the book path has no session, the trade path does not need one.

Cases (each prints PASS/FAIL lines; a control build must fail with its text):
  exactly_once    the writer killed twice inside a commit (the test build pauses each commit before
                  COMMIT and prints a line; the test kills -9 on it); md_trades equals the journal's trade set by
                  trade id, md_candles equals a recomputation, md_state.last_journal_seq is the last record
  epoch           a history of genesis A, then a journal of genesis B: EPOCH_MISMATCH with
                  `market history belongs to another genesis`, no row written, the book path serves; after the reset
                  (the md_* relations truncated with the service stopped) a clean start on B
  purged          the records after the committed cursor purged from the archive: the halt
                  JOURNAL_PREFIX_PURGED, nothing stored, nothing handed to the live path; the reset starts a partial
                  history: first_journal_seq above 1 and md_history_partial 1
  schema          a frame of journal schema 3 at the next record: the halt SOURCE_SCHEMA, nothing of it
                  stored
  second_instance a second oe-marketdata on the same database exits 75 with
                  `market history writer lock is held` before its public port opens; the first is untouched
  unknown_market  a trade of a market outside MD_EXPECTED_MARKETS: the halt UNKNOWN_MARKET, neither
                  stored nor skipped; a restart with the market in the set stores it once

usage: md_trade_it.py --bin-dir <dir> --work <dir> --image <postgres image> --aeron-jar <aeron-all.jar>
                      --fixture-src <JournalArchiveFixture.java> --fixture-dir <mejournal fixtures>
                      --mode embedded|external [--binary <name>] <case>
"""
import argparse
import json
import os
import secrets
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

SECRET = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
EPOCH_A = 7
EPOCH_B = 8
TS0 = 1_790_000_000_000
INTERVALS = (60, 300, 900, 3600, 14400, 86400)


class Failure(Exception):
    pass


class Inconclusive(Exception):
    pass


def check(cond, what):
    if not cond:
        raise Failure(what)


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
    except OSError as e:
        return 0, str(e)


def wait_until(cond, timeout, step=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(step)
    return cond()


# ---- journal records (schema 4, version 1): record 1 Trade (block 109) and record 2 Terminal (block 62)

def trade_frame(seq, epoch, trade_id, market, price, qty, taker_is_buy, ts, schema=4):
    head = struct.pack("<HHHH", 109, 1, schema, 1)
    block = struct.pack("<qqqqi", seq, epoch, 64 * seq, trade_id, market)
    block += struct.pack("<qqqq", 900001, 900002, 900003, 900004)  # the parties: marker values, never read
    block += struct.pack("<qqBq", price, qty, 1 if taker_is_buy else 0, ts)
    block += struct.pack("<qq", 900005, 900006)
    assert len(head + block) == 117
    return head + block


def terminal_frame(seq, epoch):
    head = struct.pack("<HHHH", 62, 2, 4, 1)
    block = struct.pack("<qqqqqiBqqB", seq, epoch, 64 * seq, 900007, 900008, 1, 3, TS0, 900009, 0)
    block += b"\x00" * (62 - len(block))
    return head + block[:62]


class Journal:
    """What the test appended, so the expected trade set is known exactly."""

    def __init__(self):
        self.trades = {}  # seq -> (market, trade_id, price, qty, taker_is_buy, ts)
        self.head = 0

    def trade(self, seq, epoch, trade_id, market=1, schema=4):
        price = 100_000_000 * (40_000 + (seq * 37) % 900)
        qty = 1_000_000 * (1 + (seq * 13) % 500)
        ts = TS0 + seq * 7_000
        buy = seq % 2 == 0
        if schema == 4:
            self.trades[seq] = (market, trade_id, price, qty, buy, ts)
        return trade_frame(seq, epoch, trade_id, market, price, qty, buy, ts, schema)


def expected_candles(trades):
    """md_candles recomputed from the trade set (journal order): {(market, interval, bucket): row}."""
    out = {}
    for seq in sorted(trades):
        market, trade_id, price, qty, _, ts = trades[seq]
        for interval in INTERVALS:
            width = interval * 1000
            key = (market, interval, ts // width * width)
            if key not in out:
                out[key] = [price, price, price, price, 0, 0, 0, 0]
            row = out[key]
            row[1] = max(row[1], price)
            row[2] = min(row[2], price)
            row[3] = price
            row[4] += qty
            row[5] += price * qty
            row[6] += 1
            row[7] = trade_id
    return {k: tuple(v) for k, v in out.items()}


# ---- the pieces

class Postgres:
    def __init__(self, image, work):
        self.name = f"md-trade-it-{os.getpid()}-{secrets.token_hex(3)}"
        self.admin_password = secrets.token_hex(16)
        self.password = secrets.token_hex(16)
        self.password_file = os.path.join(work, "md-writer-password")
        with open(self.password_file, "w") as f:
            f.write(self.password + "\n")
        subprocess.run(["docker", "run", "-d", "--rm", "--name", self.name, "-e", "POSTGRES_USER=admin", "-e",
                        f"POSTGRES_PASSWORD={self.admin_password}", "-e", "POSTGRES_DB=postgres", "-p",
                        "127.0.0.1::5432", image], check=True, capture_output=True, text=True)
        ok = wait_until(lambda: subprocess.run(["docker", "exec", self.name, "pg_isready", "-U", "admin", "-h",
                                                "127.0.0.1"], capture_output=True).returncode == 0, 60, 0.3)
        check(ok, "postgres did not become ready")
        out = subprocess.run(["docker", "port", self.name, "5432/tcp"], check=True, capture_output=True, text=True)
        self.port = out.stdout.strip().splitlines()[0].rsplit(":", 1)[1]
        # the console's part (B5): the writer role and its database
        self.sql("postgres", f"CREATE ROLE oe_marketdata_writer LOGIN PASSWORD '{self.password}'")
        self.sql("postgres", "CREATE DATABASE r01_marketdata OWNER oe_marketdata_writer")
        self.url = f"postgresql://127.0.0.1:{self.port}/r01_marketdata"

    def sql(self, db, statement):
        out = subprocess.run(["docker", "exec", "-e", f"PGPASSWORD={self.admin_password}", self.name, "psql", "-h",
                              "127.0.0.1", "-U", "admin", "-d", db, "-At", "-v", "ON_ERROR_STOP=1", "-c", statement],
                             capture_output=True, text=True)
        check(out.returncode == 0, f"psql {statement[:80]}: {out.stderr}")
        return out.stdout.strip()

    def md(self, statement):
        return self.sql("r01_marketdata", statement)

    def reset(self):
        # the console's marketdata-reset and the genesis reset, the service stopped first
        self.md("TRUNCATE md_trades, md_candles, md_state")

    def close(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


class Archive:
    """One JournalArchiveFixture: a MediaDriver and an Archive in a JVM, driven over stdin/stdout."""

    def __init__(self, ctx, name):
        self.ctx = ctx
        self.port = free_port()
        self.dir = tempfile.mkdtemp(prefix=f"md-trade-it-{name}-", dir="/dev/shm")
        log = open(os.path.join(ctx.work, f"archive-{name}.log"), "w")
        self.proc = subprocess.Popen(["java", "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "-Xmx512m", "-cp", f"{ctx.jar}:{ctx.classes}", "JournalArchiveFixture",
                                      os.path.join(self.dir, "aeron"), os.path.join(self.dir, "archive"),
                                      str(self.port), ctx.fixture_dir], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=log, text=True, bufsize=1)
        ready = self.proc.stdout.readline().strip()
        check(ready.startswith("READY"), f"the archive fixture did not start: {ready}")
        self.cmd("RECORD aeron:ipc?term-length=64k")

    def cmd(self, line):
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()
        answer = self.proc.stdout.readline().strip()
        check(not answer.startswith("ERROR") and answer, f"archive {line[:40]}: {answer}")
        return answer

    def append(self, frame):
        return self.cmd("HEX " + frame.hex())

    def close(self):
        if self.proc.poll() is None:
            try:
                self.cmd("QUIT")
            except Exception:
                pass
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


class Md:
    def __init__(self, ctx, name, binary, archive, pg, extra=None, instance=1):
        self.ctx = ctx
        self.name = name
        self.admin = free_port()
        self.public = free_port()
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        env.update({
            "MD_DRIVER_MODE": ctx.mode,
            "MD_DRIVER_TIMEOUT_MS": "5000",
            "MD_INSTANCE_ID": str(instance),
            "MD_ME_CLUSTER_ADDRESSES": "127.0.0.1",
            "MD_ME_PORT_BASE": str(free_port()),
            "CLUSTER_PORT_STRIDE": "0",
            "MD_ME_EGRESS_ENDPOINT": f"127.0.0.1:{free_port()}",
            "ME_AUTH_CREDENTIAL": f"7:GATEWAY:{SECRET}",
            "MD_ME_JOURNAL_ARCHIVES": f"127.0.0.1:{archive.port}",
            "MD_JOURNAL_CONTROL_STREAM": "4010",
            "MD_JOURNAL_STREAM": "4001",
            "MD_JOURNAL_CHANNEL": "aeron:ipc",
            "MD_REPLAY_STREAM": "9101",
            "MD_JOURNAL_MTU": "1408",
            "MD_LOCAL_HOST": "127.0.0.1",
            "MD_PUBLIC_BIND": f"127.0.0.1:{self.public}",
            "MD_ADMIN_BIND": f"127.0.0.1:{self.admin}",
            "MD_PG_URL": pg.url,
            "MD_PG_USER": "oe_marketdata_writer",
            "MD_PG_PASSWORD_FILE": pg.password_file,
            "MD_CORS_ORIGINS": "none",
            "MD_EXPECTED_MARKETS": "1,2,3",
            "MD_ARCHIVE_TIMEOUT_MS": "2000",
            "MD_SOURCE_PROBE_MS": "1000",
            "MD_GAP_RESCAN_MS": "1000",
            "MD_COMMIT_INTERVAL_MS": "50",
            "MD_COMMIT_BATCH": "64",
        })
        if ctx.mode == "embedded":
            env["MD_DRIVER_DIR"] = ctx.driver_parent
        else:
            env["AERON_DIR"] = ctx.aeron_dir
        env.update(extra or {})
        self.log_path = os.path.join(ctx.work, f"md-{name}.log")
        self.log = open(self.log_path, "w")
        self.proc = subprocess.Popen([os.path.join(ctx.bin_dir, binary)], env=env, stdout=self.log,
                                     stderr=subprocess.STDOUT, start_new_session=True)
        ctx.mds.append(self)

    def text(self):
        with open(self.log_path) as f:
            return f.read()

    def health(self):
        status, body = http(self.admin, "/health")
        if status not in (200, 503):
            return None
        try:
            return json.loads(body)
        except ValueError:
            return None

    def trades(self):
        h = self.health()
        return None if h is None else h.get("trades")

    def wait_started(self, timeout=30):
        ok = wait_until(lambda: (self.trades() or {}).get("started"), timeout, 0.1)
        check(ok, f"{self.name}: the trade path did not start:\n{self.text()[-3000:]}")

    def wait_committed(self, seq, timeout=60):
        ok = wait_until(lambda: (self.trades() or {}).get("committedSeq") == seq, timeout, 0.1)
        check(ok, f"{self.name}: committedSeq {(self.trades() or {}).get('committedSeq')} never reached {seq}:\n"
                  f"{json.dumps(self.trades())}\n{self.text()[-3000:]}")

    def wait_halt(self, kind, timeout=30):
        ok = wait_until(lambda: kind in ((self.trades() or {}).get("halts") or []), timeout, 0.1)
        check(ok, f"{self.name}: no halt {kind}: {json.dumps(self.trades())}\n{self.text()[-3000:]}")

    def wait_halt_or_commit(self, kind, seq, timeout=20):
        """Waits for the halt, or for the cursor to reach `seq` (what a build without the rule does); the caller
        checks the stored rows first (a control's text), then the halt."""
        def done():
            t = self.trades() or {}
            return kind in (t.get("halts") or []) or t.get("committedSeq", 0) >= seq
        wait_until(done, timeout, 0.1)
        time.sleep(1)  # anything a build without the rule commits, it commits by now

    def kill(self):
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                code = self.proc.wait(20)
            except subprocess.TimeoutExpired:
                self.kill()
                raise Failure(f"{self.name}: did not stop on SIGTERM")
            check(code == 0, f"{self.name}: SIGTERM exit {code}:\n{self.text()[-3000:]}")


class Ctx:
    def __init__(self, args):
        self.bin_dir = args.bin_dir
        self.work = args.work
        self.mode = args.mode
        self.jar = args.aeron_jar
        self.fixture_dir = args.fixture_dir
        self.binary = args.binary
        self.classes = os.path.join(self.work, "classes")
        self.mds = []
        self.archives = []
        self.driver = None
        self.driver_parent = tempfile.mkdtemp(prefix="md-trade-it-drv-", dir="/dev/shm")
        self.aeron_dir = os.path.join(self.driver_parent, "aeron")
        out = subprocess.run(["javac", "-proc:none", "-d", self.classes, "-cp", self.jar, args.fixture_src],
                             capture_output=True, text=True)
        check(out.returncode == 0, f"the archive fixture does not compile: {out.stderr}")
        if self.mode == "external":
            log = open(os.path.join(self.work, "driver.log"), "w+")
            self.driver = subprocess.Popen([os.path.join(self.bin_dir, "gw_test_media_driver"), self.aeron_dir],
                                           stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            ok = wait_until(lambda: "driver ready" in open(log.name).read(), 15)
            check(ok, "the test media driver did not start")
        self.pg = Postgres(args.image, self.work)

    def archive(self, name):
        a = Archive(self, name)
        self.archives.append(a)
        return a

    def md(self, name, archive, binary=None, extra=None, instance=1):
        return Md(self, name, binary or self.binary, archive, self.pg, extra, instance)

    def stored(self):
        """md_trades as {trade_id: (market, price, qty, buy, ts, seq)}, failing on a trade id stored twice."""
        rows = self.pg.md("SELECT market_id, trade_id, price, quantity, taker_is_buy::int, executed_at_ms, journal_seq "
                          "FROM md_trades ORDER BY journal_seq")
        out = {}
        for line in filter(None, rows.splitlines()):
            m, tid, p, q, b, ts, seq = (int(x) for x in line.split("|"))
            if tid in out:
                raise Failure(f"duplicate trade {tid}")
            out[tid] = (m, p, q, b == 1, ts, seq)
        return out

    def check_trade_set(self, journal):
        stored = self.stored()
        want = {v[1]: (v[0], v[2], v[3], v[4], v[5], seq) for seq, v in journal.trades.items()}
        for tid in sorted(want):
            if tid not in stored:
                raise Failure(f"missing trade {tid}")
            check(stored[tid] == want[tid], f"trade {tid} differs: stored {stored[tid]}, journal {want[tid]}")
        for tid in sorted(stored):
            if tid not in want:
                raise Failure(f"duplicate trade {tid} (not in the journal's trade set)")
        rows = self.pg.md("SELECT market_id, interval_s, bucket_start_ms, open, high, low, close, volume_base, "
                          "volume_quote, trade_count, last_trade_id FROM md_candles")
        got = {}
        for line in filter(None, rows.splitlines()):
            v = [int(x) for x in line.split("|")]
            got[(v[0], v[1], v[2])] = tuple(v[3:])
        check(got == expected_candles(journal.trades), "candles differ from a recomputation from the trade set")
        return len(want)

    def cleanup(self):
        for m in self.mds:
            m.kill()
            m.log.close()
        for a in self.archives:
            a.close()
        if self.driver and self.driver.poll() is None:
            self.driver.terminate()
            self.driver.wait()
        self.pg.close()
        shutil.rmtree(self.driver_parent, ignore_errors=True)


def append_records(archive, journal, first, last, epoch, market_of=lambda seq: 1, trade_id_of=lambda seq: seq):
    for seq in range(first, last + 1):
        if seq % 5 == 0:
            archive.append(terminal_frame(seq, epoch))  # a record 2: only the sequence advances
        else:
            archive.append(journal.trade(seq, epoch, trade_id_of(seq), market_of(seq)))
    journal.head = last


# ---- cases

def case_exactly_once(ctx):
    a = ctx.archive("a")
    j = Journal()
    append_records(a, j, 1, 120, EPOCH_A)
    kills = 0
    cursors = []
    for round_no in (1, 2):
        # the first commits of the process go through (64 records each), the next one is killed inside
        pause = {"MDTC_COMMIT_PAUSE_MS": "4000", "MDTC_COMMIT_PAUSE_SKIP": str(3 - round_no)}
        md = ctx.md(f"kill-{round_no}", a, extra=pause)
        md.wait_started()
        first = j.head + 1
        append_records(a, j, first, first + 150, EPOCH_A)
        seen = wait_until(lambda: "paused 4000 ms before COMMIT" in md.text(), 30)
        check(seen, f"round {round_no}: no commit pause line (is this the MD_TC_COMMIT_PAUSE build?):\n{md.text()[-2000:]}")
        pending = (md.trades() or {}).get("pending", 0)
        md.kill()
        kills += 1
        cursors.append(int(ctx.pg.md("SELECT last_journal_seq FROM md_state")))
        print(f"PASS round {round_no}: killed -9 inside a commit ({pending} records read and not committed, cursor "
              f"{cursors[-1]})")
    if kills < 2:
        raise Inconclusive("writer was never killed")
    check(0 < cursors[0] < cursors[1] < j.head, f"the kills did not fall between commits: cursors {cursors}")
    md = ctx.md("final", a)
    md.wait_started()
    append_records(a, j, j.head + 1, j.head + 80, EPOCH_A)
    md.wait_committed(j.head)
    n = ctx.check_trade_set(j)
    check(int(ctx.pg.md("SELECT last_journal_seq FROM md_state")) == j.head, "md_state.last_journal_seq is not the head")
    print(f"PASS restarts {kills}: md_trades equals the journal's {n} trades by trade id; candles equal a recomputation")
    _, metrics = http(md.admin, "/metrics")
    check(f"\nmd_journal_seq {j.head}\n" in metrics, "md_journal_seq is not the committed cursor")
    md.stop()


def case_epoch(ctx):
    a = ctx.archive("genesis-a")
    ja = Journal()
    append_records(a, ja, 1, 50, EPOCH_A)
    md = ctx.md("a", a)
    md.wait_started()
    md.wait_committed(50)
    md.stop()
    a.close()
    b = ctx.archive("genesis-b")
    jb = Journal()
    append_records(b, jb, 1, 80, EPOCH_B, trade_id_of=lambda seq: 100_000 + seq)
    md = ctx.md("b-halted", b)
    md.wait_started()
    md.wait_halt_or_commit("EPOCH_MISMATCH", 80)
    time.sleep(2)  # nothing is written while parked
    stored = ctx.stored()
    if any(tid >= 100_000 for tid in stored):
        raise Failure("trades of two genesis epochs in one history")
    md.wait_halt("EPOCH_MISMATCH")
    check("market history belongs to another genesis" in md.text(), "no `market history belongs to another genesis` line")
    check(len(stored) == len(ja.trades) and ctx.pg.md("SELECT me_epoch || ':' || last_journal_seq FROM md_state") ==
          f"{EPOCH_A}:50", f"the parked writer wrote: {ctx.pg.md('SELECT * FROM md_state')}")
    status, body = http(md.admin, "/ready")
    check(status == 503 and "no_halt" in json.loads(body)["failing"], f"/ready {status} {body}")
    check(md.trades()["liveEmitted"] == 0, "a trade went to the live path under the halt")
    ws = socket.create_connection(("127.0.0.1", md.public), timeout=5)  # the book path serves: the public port is open
    ws.close()
    print("PASS parked: EPOCH_MISMATCH, `market history belongs to another genesis`, no row written, /ready not ready "
          "on no_halt, the public port serves")
    md.stop()
    ctx.pg.reset()
    md = ctx.md("b-clean", b)
    md.wait_started()
    md.wait_committed(80)
    check(not md.trades()["halts"], f"halts after the reset: {md.trades()['halts']}")
    check(ctx.pg.md("SELECT me_epoch || ':' || first_journal_seq FROM md_state") == f"{EPOCH_B}:1", "state after reset")
    ctx.check_trade_set(jb)
    print("PASS after the reset: a clean start on the new genesis, its trades exactly")
    md.stop()


def case_purged(ctx):
    a = ctx.archive("a")
    j = Journal()
    a.cmd(f"SYNTH 1 100 {EPOCH_A}")
    md = ctx.md("first", a)
    md.wait_started()
    md.wait_committed(100)
    md.stop()
    a.cmd(f"SYNTH 101 9000 {EPOCH_A}")
    purged = a.cmd("PURGE 1048576")
    print(f"archive: {purged}")
    md = ctx.md("purged", a)
    md.wait_started()
    md.wait_halt_or_commit("JOURNAL_PREFIX_PURGED", 9100)
    time.sleep(2)
    above = int(ctx.pg.md("SELECT count(*) FROM md_trades WHERE journal_seq > 100"))
    if above:
        raise Failure("trades skipped over a purged prefix")
    md.wait_halt("JOURNAL_PREFIX_PURGED")
    check(int(ctx.pg.md("SELECT last_journal_seq FROM md_state")) == 100, "the cursor moved under the halt")
    check(md.trades()["liveEmitted"] == 0, "a trade went to the live path under the halt")
    print(f"PASS halt JOURNAL_PREFIX_PURGED: {md.trades()['haltDetail']}")
    md.stop()
    ctx.pg.reset()  # marketdata-reset
    md = ctx.md("after-reset", a)
    md.wait_started()
    md.wait_committed(9100)
    first = int(ctx.pg.md("SELECT first_journal_seq FROM md_state"))
    _, metrics = http(md.admin, "/metrics")
    partial = "\nmd_history_partial 1\n" in metrics
    if first <= 1 or not partial:
        raise Failure(f"partial history reported as whole (first_journal_seq {first}, md_history_partial "
                      f"{'1' if partial else '0'})")
    count = int(ctx.pg.md("SELECT count(*) FROM md_trades"))
    check(count == 9100 - first + 1, f"{count} trades stored from {first} to 9100")
    print(f"PASS after marketdata-reset: first_journal_seq {first}, md_history_partial 1, {count} trades")
    md.stop()


def case_schema(ctx):
    a = ctx.archive("a")
    j = Journal()
    append_records(a, j, 1, 20, EPOCH_A)
    a.append(j.trade(21, EPOCH_A, 21, schema=3))
    append_records(a, j, 22, 30, EPOCH_A)
    md = ctx.md("schema", a)
    md.wait_started()
    md.wait_halt_or_commit("SOURCE_SCHEMA", 21)
    if "21" in ctx.pg.md("SELECT trade_id FROM md_trades WHERE trade_id = 21"):
        raise Failure("journal decoded under the wrong schema")
    md.wait_halt("SOURCE_SCHEMA")
    check(int(ctx.pg.md("SELECT last_journal_seq FROM md_state")) == 20, "the cursor is not 20 under the halt")
    print(f"PASS halt SOURCE_SCHEMA: {md.trades()['haltDetail']}")
    md.stop()


def case_second_instance(ctx):
    a = ctx.archive("a")
    j = Journal()
    append_records(a, j, 1, 40, EPOCH_A)
    first = ctx.md("first", a)
    first.wait_started()
    first.wait_committed(40)
    second = ctx.md("second", a, instance=2)
    try:
        code = second.proc.wait(20)
    except subprocess.TimeoutExpired:
        code = None
    if code is None:
        raise Failure("two market history writers")
    check(code == 75, f"the second exited {code}, expected 75:\n{second.text()}")
    check("market history writer lock is held" in second.text(), f"no lock text:\n{second.text()}")
    check("listening" not in second.text() and http(second.public, "/")[0] == 0,
          f"the second bound a listener before it refused:\n{second.text()}")
    append_records(a, j, 41, 60, EPOCH_A)
    first.wait_committed(60)
    check(first.proc.poll() is None, "the first writer is gone")
    ctx.check_trade_set(j)
    print("PASS the second exits 75 with `market history writer lock is held` before its public port; the first goes on")
    first.stop()


def case_unknown_market(ctx):
    a = ctx.archive("a")
    j = Journal()
    append_records(a, j, 1, 10, EPOCH_A)
    a.append(j.trade(11, EPOCH_A, 11, market=4))
    append_records(a, j, 12, 14, EPOCH_A)
    md = ctx.md("halted", a)
    md.wait_started()
    md.wait_halt_or_commit("UNKNOWN_MARKET", 11)
    if ctx.pg.md("SELECT count(*) FROM md_trades WHERE trade_id = 11") != "0":
        raise Failure("trade stored for a market with no book")
    md.wait_halt("UNKNOWN_MARKET")
    check(int(ctx.pg.md("SELECT last_journal_seq FROM md_state")) == 10, "the cursor is not 10 under the halt")
    print(f"PASS halt UNKNOWN_MARKET: {md.trades()['haltDetail']}")
    md.stop()
    md = ctx.md("market-added", a, extra={"MD_EXPECTED_MARKETS": "1,2,3,4"})
    md.wait_started()
    md.wait_committed(14)
    ctx.check_trade_set(j)
    check(ctx.pg.md("SELECT count(*) FROM md_trades WHERE trade_id = 11") == "1", "trade 11 not stored once")
    print("PASS restarted with market 4 in the set: trade 11 stored once, the trade set exact")
    md.stop()


CASES = {
    "exactly_once": case_exactly_once,
    "epoch": case_epoch,
    "purged": case_purged,
    "schema": case_schema,
    "second_instance": case_second_instance,
    "unknown_market": case_unknown_market,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--aeron-jar", required=True)
    parser.add_argument("--fixture-src", required=True)
    parser.add_argument("--fixture-dir", required=True)
    parser.add_argument("--mode", choices=["embedded", "external"], required=True)
    parser.add_argument("--binary", default="oe-marketdata")
    parser.add_argument("case", choices=sorted(CASES))
    args = parser.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    ctx = None
    try:
        ctx = Ctx(args)
        CASES[args.case](ctx)
        print(f"PASS {args.case} ({args.mode}, {args.binary})")
        return 0
    except Inconclusive as e:
        print(f"INCONCLUSIVE {args.case}: {e}")
        return 1
    except Failure as e:
        print(f"FAIL {args.case}: {e}")
        return 1
    finally:
        if ctx:
            ctx.cleanup()


if __name__ == "__main__":
    sys.exit(main())
