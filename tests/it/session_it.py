#!/usr/bin/env python3
"""T1 rows of the core session wrapper against a real three-member core cluster (a heavy job: run it under the lock).

Starts, on this host: three core members from the core jar of the pinned money schema (production Main, the real
authenticator with a GATEWAY and an ADMIN key), the test media driver for the external mode, and gw_session_it (the
session wrapper over the real Aeron transport, driven by stdin). The ADMIN side (CoreConfig, risk, the gate, the
deposit) goes through CoreProbe, built against the same core jar.

Checks:
  readiness  genesis: /ready fails config_loaded and gate_open while /ready/session holds; the config is read again on
        ConfigApplied; gate SUSPENDED -> only gate_open fails and /ready/session holds, twice; no leader -> not ready
  registration  registered = ack: no command offered while unregistered, across the initial registration and the election
  election  leader SIGSTOPped with K creates offered to it, then SIGKILLed: each answered once after the election, every one
        re-offered, one order per key (a second create with each key answers duplicate with the same order)
  timeout  egress muted past the deadline -> OUTCOME_UNKNOWN; the retry with the same key while the first reply arrives:
        the first reply is dropped and counted, the retry gets its own answer (duplicate, its own correlation id)
  budget  a burst of balance reads above the bucket: queued, then OVERLOADED at the deadline; the core sees at most
        rate x 1.2 per second
  streak  N creates time out in a row (N = the ready timeout streak): /ready fails timeout_streak while /ready/session
        holds; late replies do not clear it, one reply does
  ack loss  a create applied by the core with its ack unread; the session is closed 1..N duty cycles later (random, the
        seed is printed; GW_IT_SEED sets it), twice: the gateway's own re-offer on the new session answers duplicate with
        the stored orderId; the core holds one order and one hold per key
  keepalive  the leader stopped (SIGSTOP) with no traffic: /ready fails keepalive_fresh once no keepalive offer has
        succeeded for the stale bound (the times /ready and keepalive_fresh first fail are printed); the run ends there,
        without quorum
  decode  every session process ends with no egress frame refused by the decoder (the ignored count is printed)

usage: session_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar>
                     --mode embedded|external --work <dir> [--port-base N]
"""
import argparse
import json
import os
import queue
import random
import shutil
import signal
import subprocess
import sys
import threading
import time

SECRET_ADMIN = "11" * 32
SECRET_GATEWAY = "55" * 32
CORE_KEYS = f"1:ADMIN:{SECRET_ADMIN}::users=*;5:GATEWAY:{SECRET_GATEWAY}::users=*"
GATEWAY_KEY = f"5:GATEWAY:{SECRET_GATEWAY}"
ADMIN_KEY = f"1:ADMIN:{SECRET_ADMIN}"
# A core member's HTTP endpoint (/health, /ready, /metrics): CLUSTER_PORT_BASE + node x CLUSTER_PORT_STRIDE + 6
NODE_ENDPOINT_OFFSET = 6
PORT_STRIDE = 100


def node_port(base, node):
    return base + node * PORT_STRIDE + NODE_ENDPOINT_OFFSET


JAVA_OPTS = ["--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens", "java.base/java.nio=ALL-UNNAMED",
             "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", "-Xms128m", "-Xmx512m", "-XX:+UseZGC"]
FP = 100_000_000  # 10^8: the money scale

FAILED = []


def check(ok, name, detail=""):
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": {detail}"), flush=True)
    if not ok:
        FAILED.append(name)
    return ok


class SessionProc:
    """gw_session_it with its stdout read by a thread into a queue."""

    def __init__(self, args, work, name, extra):
        self.log = open(os.path.join(work, f"{name}.stderr"), "w")
        self.out = open(os.path.join(work, f"{name}.stdout"), "w")
        cmd = [os.path.join(args.bin_dir, "gw_session_it"), "--mode", args.mode, "--dir", args.driver_dir,
               "--ingress", args.ingress, "--host", "127.0.0.1"] + extra
        env = dict(os.environ, CORE_AUTH_CREDENTIAL=GATEWAY_KEY)
        self.p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True,
                                  env=env)
        self.q = queue.Queue()
        self.outcomes = {}
        self.submits = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            line = line.rstrip("\n")
            self.out.write(line + "\n")
            self.out.flush()
            self.q.put(line)

    def send(self, line):
        self.p.stdin.write(line + "\n")
        self.p.stdin.flush()

    def _take(self, timeout):
        line = self.q.get(timeout=timeout)
        if line.startswith("OUTCOME "):
            o = dict(kv.split("=", 1) for kv in line.split()[1:])
            self.outcomes[int(o["ticket"])] = o
        elif line.startswith("SUBMIT "):
            s = dict(kv.split("=", 1) for kv in line.split()[1:])
            self.submits.append(int(s["ticket"]))
        return line

    def pump(self, seconds):
        end = time.time() + seconds
        while time.time() < end:
            try:
                self._take(max(0.01, end - time.time()))
            except queue.Empty:
                pass

    def status(self, timeout=10):
        self.send("status")
        end = time.time() + timeout
        while time.time() < end:
            try:
                line = self._take(max(0.01, end - time.time()))
            except queue.Empty:
                break
            if line.startswith("STATUS "):
                return json.loads(line[len("STATUS "):])
        raise RuntimeError("no STATUS line")

    def wait_status(self, pred, timeout, every=0.2):
        end = time.time() + timeout
        last = None
        while time.time() < end:
            last = self.status()
            if pred(last):
                return last
            self.pump(every)
        return last

    def submit(self, line, count):
        before = len(self.submits)
        self.send(line)
        end = time.time() + 10
        while len(self.submits) < before + count and time.time() < end:
            try:
                self._take(0.5)
            except queue.Empty:
                pass
        return self.submits[before:before + count]

    def wait_outcomes(self, tickets, timeout):
        end = time.time() + timeout
        while time.time() < end and not all(t in self.outcomes for t in tickets):
            try:
                self._take(0.2)
            except queue.Empty:
                pass
        return [self.outcomes.get(t) for t in tickets]

    def quit(self):
        try:
            self.send("quit")
            self.p.wait(timeout=15)
        except Exception:
            self.p.kill()
            self.p.wait()
        self.pump(0.2)
        return self.p.returncode


class Cluster:
    def __init__(self, args, work):
        self.args = args
        self.work = work
        self.pids = {}
        self.procs = {}
        self.shm = os.path.join("/dev/shm", f"gw-session-it-{os.getpid()}-{args.mode}")

    def start(self):
        os.makedirs(self.shm, exist_ok=True)
        base = self.args.port_base
        for i in range(3):
            d = os.path.join(self.work, f"core{i}")
            os.makedirs(d, exist_ok=True)
            env = dict(os.environ, CLUSTER_NODE=str(i), CLUSTER_ADDRESSES="localhost,localhost,localhost",
                       CLUSTER_PORT_BASE=str(base), CLUSTER_PORT_STRIDE="100", BASE_DIR=d, CORE_AUTH_KEYS=CORE_KEYS,
                       CORE_NODE_PORT=str(node_port(base, i)))
            log = open(os.path.join(self.work, f"core{i}.log"), "w")
            p = subprocess.Popen(["java"] + JAVA_OPTS + [f"-Daeron.dir={self.shm}/core{i}", "-jar", self.args.core_jar],
                                 env=env, stdout=log, stderr=subprocess.STDOUT)
            self.procs[i] = p
            self.pids[i] = p.pid

    def stop(self):
        for p in self.procs.values():
            try:
                os.kill(p.pid, signal.SIGCONT)
            except OSError:
                pass
            p.kill()
        for p in self.procs.values():
            p.wait()
        shutil.rmtree(self.shm, ignore_errors=True)


def no_refused(proc, name):
    """Every egress frame the core sent this session decoded or was ignored: none refused."""
    s = proc.status()
    check(s is not None and s["decodeRefused"] == 0,
          f"{name}: no egress frame refused (ignored {s['wireIgnored'] if s else '?'})", json.dumps(s))


def probe(args, verb, **kv):
    env = dict(os.environ, CORE_ADDRESSES="localhost,localhost,localhost", CORE_PORT_BASE=str(args.port_base),
               CLUSTER_PORT_STRIDE="100", CORE_LOCAL_HOST="127.0.0.1", CORE_AUTH_CREDENTIAL=ADMIN_KEY)
    cmd = ["java"] + JAVA_OPTS + ["-cp", f"{args.core_jar}:{args.probe_jar}", "CoreProbe", verb] + \
        [f"{k}={v}" for k, v in kv.items()]
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=120)
    answer = [line for line in r.stdout.splitlines() if line.startswith("{")]
    print(f"probe {verb} {kv}: exit {r.returncode} {answer[-1] if answer else r.stderr[-400:]}", flush=True)
    return r.returncode, (json.loads(answer[-1]) if answer else None)


def holds_of(args, user):
    """The core's orders and holds of one user (ADMIN order-dump and money-dump)."""
    _, dump = probe(args, "order-dump")
    _, money = probe(args, "money-dump")
    orders = [o for o in (dump or {}).get("orders", []) if o.get("user") == user]
    holds = [h for h in (money or {}).get("holds", []) if h.get("user") == user]
    return orders, holds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--mode", required=True, choices=["embedded", "external"])
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=9800)
    args = ap.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    base = args.port_base
    args.ingress = f"0=localhost:{base + 2},1=localhost:{base + 102},2=localhost:{base + 202}"
    print(f"session_it mode={args.mode} work={args.work} ports={base}", flush=True)

    cluster = Cluster(args, args.work)
    driver = None
    try:
        os.makedirs(cluster.shm, exist_ok=True)  # the driver's instance lock lives next to its dir
        if args.mode == "external":
            args.driver_dir = os.path.join(cluster.shm, "external-driver")
            dlog = open(os.path.join(args.work, "driver.log"), "w")
            driver = subprocess.Popen([os.path.join(args.bin_dir, "gw_test_media_driver"), args.driver_dir],
                                      stdout=dlog, stderr=subprocess.STDOUT)
            for _ in range(100):
                if os.path.exists(os.path.join(args.driver_dir, "cnc.dat")):
                    break
                time.sleep(0.1)
        else:
            args.driver_dir = os.path.join(cluster.shm, "embedded")
            os.makedirs(args.driver_dir, exist_ok=True)
        cluster.start()
        run(args, cluster)
    finally:
        cluster.stop()
        if driver is not None:
            driver.terminate()
            try:
                driver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                driver.kill()
    print(("SESSION IT PASS " if not FAILED else "SESSION IT FAIL ") + args.mode + (" " + " ".join(FAILED) if FAILED else ""),
          flush=True)
    return 0 if not FAILED else 1


def run(args, cluster):
    # ---------------------------------------------------------------- A: registration, config, gate, election
    a = SessionProc(args, args.work, "a", ["--timeout-ms", "60000", "--instance", "61"])
    s = a.wait_status(lambda s: s["registered"] and s["configReports"] >= 1, 120)
    if not check(s is not None and s["registered"], "registered after the AdminAck", json.dumps(s)):
        a.quit()
        return
    check(not s["configLoaded"] and "config_loaded" in s["notReady"] and "gate_open" in s["notReady"]
          and s["sessionNotReady"] == [], "genesis: /ready fails config_loaded and gate_open, /ready/session holds",
          json.dumps(s))

    # CoreConfig 1 (genesis, behaviour 1), then 2 (behaviour 2): each ConfigApplied replaces the config read (twice)
    for version in (1, 2):
        reads = s["configReads"]
        rc, _ = probe(args, "core-config", version=version, behavior=version, genesis=7, tick=1, min=1,
                      max=1_000_000_000_000_000, lot=1)
        s = a.wait_status(lambda s: s["configLoaded"] and s["configVersion"] == version, 15)
        check(rc == 0 and s["configLoaded"] and s["configVersion"] == version and s["configReads"] > reads,
              f"config read again on ConfigApplied (CoreConfig {version} loaded)", json.dumps(s))
    rc, _ = probe(args, "risk", version=1, market=1, min_qty=1, max_qty=10**6 * FP, collar_bps=5000, per_sec=1000,
                  max_open=5000, slippage_bps=100)
    check(rc == 0, "risk row for market 1 accepted")
    rc, _ = probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
    s = a.wait_status(lambda s: s["notReady"] == [], 15)
    check(rc == 0 and s["gateOpen"] and s["notReady"] == [], "gate OPEN with config and keepalive: ready",
          json.dumps(s))
    check(s["windowMs"] > 0, "the idempotence window is read from the core (requestTtlMs)", json.dumps(s))
    rc, _ = probe(args, "deposit", user=1001, asset=0, amount=10**7 * FP, rh=9, rl=1)
    check(rc == 0, "deposit for user 1001")

    price, qty = 100 * FP, FP
    t = a.submit(f"create 1 1001 1 {price} {qty} 1000", 1)
    o = a.wait_outcomes(t, 30)[0]
    check(o is not None and o["kind"] == "REPLY" and "order" in o and o["dup"] == "0",
          "a create through the session is acked (201 shape: duplicate false)", str(o))

    for n in (1, 2):
        rc, _ = probe(args, "gate", market=0, state="SUSPENDED", reason="ADMIN")
        s = a.wait_status(lambda s: s["notReady"] == ["gate_open"], 15)
        check(rc == 0 and s["notReady"] == ["gate_open"] and s["sessionNotReady"] == [],
              f"gate SUSPENDED ({n}): only gate_open fails, /ready/session holds", json.dumps(s))
        rc, _ = probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
        s = a.wait_status(lambda s: s["notReady"] == [], 15)
        check(rc == 0 and s["notReady"] == [], f"gate OPEN again ({n}): ready", json.dumps(s))

    # the leader stops with K creates offered to it, then dies; a new leader is elected seconds later
    s = a.status()
    leader, registrations, reoffers = s["leader"], s["registrations"], s["reoffers"]
    pid = cluster.pids[leader]
    print(f"SIGSTOP core leader {leader} (pid {pid})", flush=True)
    os.kill(pid, signal.SIGSTOP)
    K = 5
    tickets = a.submit(f"create {K} 1001 1 {price} {qty} 2000", K)
    s = a.wait_status(lambda s: s["inflight"] == K, 5, 0.05)
    check(s["inflight"] == K, "the K creates are offered to the stopped leader", json.dumps(s))
    print(f"SIGKILL core leader {leader} (pid {pid})", flush=True)
    os.kill(pid, signal.SIGKILL)
    killed_at = time.time()
    saw_not_ready = False
    while time.time() - killed_at < 90 and not all(t in a.outcomes for t in tickets):
        s = a.status()
        saw_not_ready = saw_not_ready or "session_connected" in s["notReady"] or "session_registered" in s["notReady"] \
            or s["sessionDownCycles"] > 0
        a.pump(0.2)
    took = time.time() - killed_at
    outs = [a.outcomes.get(t) for t in tickets]
    s = a.status()
    saw_not_ready = saw_not_ready or s["sessionDownCycles"] > 0
    check(saw_not_ready, "no leader: not ready during the election")
    check(all(o is not None and o["kind"] == "REPLY" and "order" in o for o in outs),
          f"every request answered once after the election ({took:.1f} s)", str(outs))
    check(all(o is not None and int(o["offers"]) >= 2 for o in outs), "every request was re-offered", str(outs))
    orders = [o.get("order") for o in outs if o]
    check(len(set(orders)) == K, "one order per key", str(orders))
    s = a.status()
    check(s["reoffers"] - reoffers >= K and s["registrations"] > registrations,
          "re-offers after a new registration on the new leader", json.dumps(s))
    check(s["offeredUnregistered"] == 0, "no command offered while unregistered", json.dumps(s))
    again = a.submit(f"create {K} 1001 1 {price} {qty} 2000", K)
    outs2 = a.wait_outcomes(again, 30)
    check(all(o is not None and o["kind"] == "REPLY" and o.get("dup") == "1" for o in outs2)
          and [o.get("order") for o in outs2] == orders,
          "the same keys again: duplicate with the stored orderIds", str(outs2))
    no_refused(a, "A")
    a.quit()

    # ---------------------------------------------------------------- B: the query budget
    rate, burst, deadline_s = 20, 5, 10
    b = SessionProc(args, args.work, "b", ["--timeout-ms", str(deadline_s * 1000), "--instance", "62", "--query-rate",
                                           str(rate), "--query-burst", str(burst)])
    s = b.wait_status(lambda s: s["registered"] and s["configLoaded"], 60)
    check(s["registered"] and s["configLoaded"], "B registered and config loaded", json.dumps(s))
    N = 400
    start = time.time()
    tickets = b.submit(f"balances {N} 1001", N)
    outs = b.wait_outcomes(tickets, deadline_s + 20)
    answered = [o for o in outs if o and o["kind"] == "REPLY"]
    overloaded = [o for o in outs if o and o["kind"] == "OVERLOADED"]
    seconds = max(float(max(int(o["ms"]) for o in answered)) / 1000.0, deadline_s) if answered else deadline_s
    per_s = len(answered) / seconds
    print(f"query budget: {len(answered)} answered, {len(overloaded)} OVERLOADED, {per_s:.1f}/s over {seconds:.1f} s "
          f"(bound {rate * 1.2:.1f}/s), wall {time.time() - start:.1f} s", flush=True)
    check(len(answered) + len(overloaded) == N and len(overloaded) > 0, "queued, then OVERLOADED at the deadline",
          f"{len(answered)} + {len(overloaded)}")
    check(per_s <= rate * 1.2, "the core sees at most rate x 1.2 queries per second", f"{per_s:.1f}/s")
    check(all(int(o["offers"]) == 0 for o in overloaded), "an OVERLOADED read was never offered")
    no_refused(b, "B")
    b.quit()

    # ---------------------------------------------------------------- C: 504 and the late reply
    c = SessionProc(args, args.work, "c", ["--timeout-ms", "1500", "--instance", "63"])
    s = c.wait_status(lambda s: s["notReady"] == [], 60)
    check(s["notReady"] == [], "C ready", json.dumps(s))
    c.send("mute on")
    t1 = c.submit(f"create 1 1001 1 {price} {qty} 3000", 1)
    o1 = c.wait_outcomes(t1, 10)[0]
    check(o1 is not None and o1["kind"] == "OUTCOME_UNKNOWN" and int(o1["offers"]) >= 1,
          "a muted egress yields OUTCOME_UNKNOWN (504), never a 4xx", str(o1))
    t2 = c.submit(f"create 1 1001 1 {price} {qty} 3000", 1)
    c.pump(0.3)
    c.send("mute off")
    o2 = c.wait_outcomes(t2, 10)[0]
    check(o2 is not None and o2["kind"] == "REPLY" and o2.get("dup") == "1" and o2.get("acorr") == o2.get("corr"),
          "the retry gets its own answer (duplicate, its own correlation id)", str(o2))
    s = c.status()
    check(s["late"] >= 1, "the first attempt's reply is dropped and counted", json.dumps(s))
    check(s["streak"] == 0, "one reply clears the timeout streak", json.dumps(s))

    # a timeout streak: N deadlines missed in a row -> /ready 503 until one reply (gw_session_it's bound is the default)
    streak_n = 5
    c.send("mute on")
    ts = c.submit(f"create {streak_n} 1001 1 {price} {qty} 3100", streak_n)
    outs = c.wait_outcomes(ts, 15)
    s = c.status()
    check(all(o is not None and o["kind"] == "OUTCOME_UNKNOWN" for o in outs) and s["streak"] >= streak_n
          and "timeout_streak" in s["notReady"] and s["sessionNotReady"] == [],
          "a timeout streak: /ready fails timeout_streak, /ready/session holds", json.dumps(s) + " " + str(outs))
    c.send("mute off")
    c.pump(1.0)
    s = c.status()
    check(s["late"] >= 1 + streak_n and "timeout_streak" in s["notReady"],
          "late replies do not clear the timeout streak", json.dumps(s))
    t3 = c.submit(f"create 1 1001 1 {price} {qty} 3200", 1)
    o3 = c.wait_outcomes(t3, 10)[0]
    s = c.wait_status(lambda s: s["notReady"] == [], 5)
    check(o3 is not None and o3["kind"] == "REPLY" and s["streak"] == 0 and s["notReady"] == [],
          "one reply clears the timeout streak: ready again", json.dumps(s) + " " + str(o3))
    no_refused(c, "C")
    c.quit()

    # ---------------------------------------------------------------- D: ack loss, the gateway's own re-offer
    seed = int(os.environ.get("GW_IT_SEED") or time.time_ns() % 1_000_000)
    rng = random.Random(seed)
    max_cycles = 50
    print(f"GW_IT_SEED={seed} (drop 1..{max_cycles} duty cycles after the order is in the core)", flush=True)
    d = SessionProc(args, args.work, "d", ["--timeout-ms", "60000", "--instance", "64"])
    s = d.wait_status(lambda s: s["notReady"] == [], 60)
    check(s["notReady"] == [], "D ready", json.dumps(s))
    user = 1002
    rc, _ = probe(args, "deposit", user=user, asset=0, amount=10**7 * FP, rh=9, rl=2)
    check(rc == 0, f"deposit for user {user}")
    for n in (1, 2):
        key = 4000 + n
        d.send("mute on")
        t = d.submit(f"create 1 {user} 1 {price} {qty} {key}", 1)
        orders = []
        end = time.time() + 30
        while time.time() < end and len(orders) < n:
            orders, _ = holds_of(args, user)
            d.pump(0.2)
        check(len(orders) == n and t[0] not in d.outcomes, f"ack loss ({n}): the create is in the core, its ack unread",
              str(orders))
        known = {o["order"] for o in orders}
        cycles = rng.randint(1, max_cycles)
        print(f"drop the session {cycles} duty cycles later", flush=True)
        d.send(f"drop-session {cycles}")
        o = d.wait_outcomes(t, 60)[0]
        orders, holds = holds_of(args, user)
        mine = [x for x in orders if x["order"] in known]
        check(o is not None and o["kind"] == "REPLY" and o.get("dup") == "1" and int(o["offers"]) >= 2
              and int(o.get("order", 0)) in known, f"ack loss ({n}): the re-offer answers duplicate with the stored orderId",
              str(o) + " " + str(orders))
        check(len(orders) == n and len(holds) == n and {h["order"] for h in holds} == {x["order"] for x in orders},
              f"ack loss ({n}): one order and one hold per key", f"orders {orders} holds {holds} mine {mine}")
        s = d.wait_status(lambda s: s["notReady"] == [], 30)
        check(s["notReady"] == [] and s["offeredUnregistered"] == 0, f"ack loss ({n}): ready again on the new session",
              json.dumps(s))

    # ---------------------------------------------------------------- E: no successful keepalive inside the bound
    s = d.status()
    leader = s["leader"]
    pid = cluster.pids[leader]
    stale_ms = 3000  # GatewayReadyBounds{} in gw_session_it
    print(f"SIGSTOP core leader {leader} (pid {pid}), no traffic", flush=True)
    os.kill(pid, signal.SIGSTOP)
    stopped_at = time.time()
    first_ready_lost = None
    keepalive_failed_at = None
    keepalive_failed_status = None
    try:
        while time.time() - stopped_at < 30:
            s = d.status()
            if s["notReady"] and first_ready_lost is None:
                first_ready_lost = (time.time() - stopped_at, list(s["notReady"]))
            if "keepalive_fresh" in s["notReady"]:
                keepalive_failed_at = time.time() - stopped_at
                keepalive_failed_status = s
                break
            d.pump(0.1)
    finally:
        os.kill(pid, signal.SIGCONT)
    print(f"leader stopped: /ready first failed after {first_ready_lost[0]:.1f} s {first_ready_lost[1]}" if first_ready_lost
          else "leader stopped: /ready never failed", flush=True)
    print(f"leader stopped: keepalive_fresh failed after "
          f"{keepalive_failed_at:.1f} s" if keepalive_failed_at is not None else "keepalive_fresh never failed", flush=True)
    check(keepalive_failed_at is not None and keepalive_failed_status["keepaliveAgeMs"] > stale_ms,
          "no successful keepalive inside the bound: /ready fails keepalive_fresh", json.dumps(keepalive_failed_status))
    # A member stopped longer than its media driver's service interval (5 s) dies when it resumes (conductor service
    # timeout), and A already killed one of the three: the cluster has no quorum from here, so readiness is not expected
    # back. The session keeps retrying and the process stays up.
    s = d.status()
    check("session_connected" in s["notReady"] and "keepalive_fresh" in s["notReady"],
          "no quorum after the stop: still not ready", json.dumps(s))
    no_refused(d, "D")
    check(d.quit() == 0, "the session process stayed up and stopped cleanly")


if __name__ == "__main__":
    sys.exit(main())
