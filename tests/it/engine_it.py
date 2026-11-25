#!/usr/bin/env python3
"""T1 rows of oe-stream's engine status feed against a real three-member core cluster (a heavy job: run it under the lock).

Starts, on this host: three core members (production Main; each serves /health, /ready and /metrics on its own probe
port) and oe-stream with STREAM_CORE_NODE_ENDPOINTS naming those three ports (no matching-engine endpoints: that block
must be all null). A WebSocket client with no subscription reads the `engine` and `election` frames. The leader and the
roles the frames show are checked against each member's own /metrics, read by this script independently.

Rows:
  connect   an engine frame on connect; every configured node has a row; the matching-engine block is unconfigured and
            all null; the fields no endpoint supplies are null (snapshot state hash, version, build commit, entries)
  roles     one leader and two followers, the leader the one whose own /metrics says core_cluster_role 2; every row's
            role and term equal what that node's own /health says (a follower's row comes from the follower); positions
            and behindBytes are numbers (behindBytes is sampled from two answers, so it is not clamped)
  cap       with STREAM_ENGINE_FRAME_MIN_MS=1000 under a moving log, at most one engine frame per second (+1 slack) and
            no two frames closer than the cap; run at a 250 ms poll and again on a second stream at the default poll
            (1000 ms), where a frame held by the cap must not wait a whole extra round
  follower  a follower SIGKILLed: its row turns unreachable from its own failed poll (connection refused), the leader and
            the other follower keep their rows; restarted, its row comes back as a follower
  election  the leader SIGKILLed: an election frame phase end names the old leader and the new one (the member whose own
            /metrics says leader), the new term is higher; the engine frame's lastElection agrees; the old leader's row
            is unreachable. A `start` frame is reported when a round saw the election in progress.
  cost      requests per second per stream instance, bytes read per round and bytes per engine frame (printed)

usage: engine_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar>
                    --mode embedded|external --work <dir> [--port-base N]
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_it  # noqa: E402
import stream_it  # noqa: E402  (the stream process, the WebSocket client, the loop; it adds the stream's core key)

check = session_it.check
POLL_MS = 250
FRAME_MIN_MS = 1000


class EngineWs(stream_it.Ws):
    """A client that keeps every engine and election frame with the time it arrived."""

    def __init__(self, user):
        super().__init__(user)
        self.engine = []    # (time, frame)
        self.elections = []

    def on_frame(self, text):
        o = json.loads(text)
        if o["type"] == "engine":
            self.engine.append((time.time(), o))
        elif o["type"] == "election":
            self.elections.append((time.time(), o))
        else:
            super().on_frame(text)


def own_role(args, member):
    """The member's role as its own /metrics says (independent of oe-stream): 2 leader, 0 follower, None no answer."""
    code, body = stream_it.admin_get(session_it.node_port(args.port_base, member), "/metrics")
    if code != 200:
        return None
    for line in body.splitlines():
        if line.startswith("core_cluster_role "):
            return int(float(line.split()[1]))
    return None


def own_health(args, member):
    """The member's role and term as its own /health says (any status): (role, term) or None when it does not answer."""
    code, body = stream_it.admin_get(session_it.node_port(args.port_base, member), "/health")
    if code == 0:
        return None
    tokens = dict(t.split("=", 1) for t in body.split() if "=" in t)
    if "role" not in tokens:
        return None
    term = int(tokens["term"]) if tokens.get("term", "-1").lstrip("-").isdigit() else -1
    return tokens["role"].lower(), (term if term >= 0 else None)


def cap_row(ws, label, window=10.0):
    """Engine frames over `window` seconds under a moving log: count, smallest and largest gap between two frames."""
    before = len(ws.engine)
    time.sleep(window)
    times = [t for t, _ in ws.engine[before:]]
    sent = len(times)
    bound = int(window * 1000 / FRAME_MIN_MS) + 1
    gaps = [1000 * (b - a) for a, b in zip(times, times[1:])]
    check(0 < sent <= bound, f"cap {label}: {sent} engine frames in {window:.0f} s with the log moving (bound {bound}, "
          "at least one)")
    if gaps:
        # the client's arrival times carry scheduling jitter: 100 ms of slack below the cap
        check(min(gaps) >= FRAME_MIN_MS - 100, f"cap {label}: smallest gap between two frames {min(gaps):.0f} ms "
              f"(cap {FRAME_MIN_MS} ms)")
        print(f"cap {label}: gaps {min(gaps):.0f}..{max(gaps):.0f} ms", flush=True)
    return gaps


def latest(ws):
    return ws.engine[-1][1] if ws.engine else None


def row(frame, member):
    for n in frame["core"]["nodes"]:
        if n["id"] == member:
            return n
    return None


def full_of(ws):
    f = latest(ws)
    return f is not None and all(n["role"] in ("leader", "follower") for n in f["core"]["nodes"])


def restart_member(args, cluster, i):
    base = args.port_base
    d = os.path.join(args.work, f"core{i}")
    env = dict(os.environ, CLUSTER_NODE=str(i), CLUSTER_ADDRESSES="localhost,localhost,localhost",
               CLUSTER_PORT_BASE=str(base), CLUSTER_PORT_STRIDE="100", BASE_DIR=d, CORE_AUTH_KEYS=session_it.CORE_KEYS,
               CORE_NODE_PORT=str(session_it.node_port(base, i)))
    log = open(os.path.join(args.work, f"core{i}.log"), "a")
    p = subprocess.Popen(["java"] + session_it.JAVA_OPTS + [f"-Daeron.dir={cluster.shm}/core{i}", "-jar", args.core_jar],
                         env=env, stdout=log, stderr=subprocess.STDOUT)
    cluster.procs[i] = p
    cluster.pids[i] = p.pid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--mode", required=True, choices=["embedded", "external"])
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=9800)
    args = ap.parse_args()
    args.connections = 0
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    print(f"engine_it mode={args.mode} work={args.work} ports={args.port_base}", flush=True)
    cluster = session_it.Cluster(args, args.work)
    procs = []
    driver = None
    try:
        os.makedirs(cluster.shm, exist_ok=True)
        args.driver_parent = os.path.join(cluster.shm, "gw")
        os.makedirs(args.driver_parent, exist_ok=True)
        if args.mode == "external":
            args.driver_dir = os.path.join(cluster.shm, "external-driver")
            dlog = open(os.path.join(args.work, "driver.log"), "w")
            driver = subprocess.Popen([os.path.join(args.bin_dir, "gw_test_media_driver"), args.driver_dir],
                                      stdout=dlog, stderr=subprocess.STDOUT)
            for _ in range(100):
                if os.path.exists(os.path.join(args.driver_dir, "cnc.dat")):
                    break
                time.sleep(0.1)
        cluster.start()
        run(args, cluster, procs)
    finally:
        for p in procs:
            p.stop()
        cluster.stop()
        if driver is not None:
            driver.terminate()
            try:
                driver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                driver.kill()
    print(("ENGINE IT PASS " if not session_it.FAILED else "ENGINE IT FAIL ") + args.mode +
          (" " + " ".join(session_it.FAILED) if session_it.FAILED else ""), flush=True)
    return 0 if not session_it.FAILED else 1


def run(args, cluster, procs):
    wait_until = stream_it.wait_until
    base = args.port_base
    silent = [i for i in range(3) if not wait_until(lambda i=i: own_role(args, i) is not None, 90)]
    if not check(not silent, f"every core node's own /metrics answers (silent: {silent})"):
        return
    endpoints = ",".join(f"{i}@127.0.0.1:{session_it.node_port(base, i)}" for i in range(3))
    st = stream_it.Stream(args, "stream", 93, {
        "STREAM_CORE_NODE_ENDPOINTS": endpoints,
        "STREAM_ENGINE_POLL_MS": str(POLL_MS),
        "STREAM_ENGINE_POLL_TIMEOUT_MS": "200",
        "STREAM_ENGINE_FRAME_MIN_MS": str(FRAME_MIN_MS),
        "STREAM_HEAD_PROBE_MS": "100",  # a log entry every 100 ms: the positions move, so the frame keeps changing
    })
    procs.append(st)
    loop = stream_it.Loop()
    if not check(wait_until(lambda: st.ready()[0] == 200, 120), "stream ready", st.ready()[1]):
        return
    leader = None
    if not check(wait_until(lambda: [own_role(args, i) for i in range(3)].count(2) == 1, 60), "the core has one leader"):
        return
    leader = [i for i in range(3) if own_role(args, i) == 2][0]

    # ---- connect: an engine frame at once, a row per node, the matching-engine block all null
    ws = EngineWs(5)

    async def go():
        return await ws.open(st.ws, {"Authorization": "Bearer dev:5"})
    t0 = time.time()
    loop.run(go())
    check(wait_until(lambda: ws.engine, 5), "connect: an engine frame on connect")
    if ws.engine:
        print(f"first engine frame {1000 * (ws.engine[0][0] - t0):.0f} ms after connect", flush=True)
    check(wait_until(lambda: full_of(ws), 30), "connect: every core node answered with a role", json.dumps(latest(ws)))
    f = latest(ws)
    me = f["matchingEngine"]
    check(me["configured"] is False and me["nodes"] == [] and me["term"] is None and me["leaderId"] is None,
          "connect: no matching-engine endpoint configured -> that block all null", json.dumps(me))
    check(f["bridge"]["consumedJournalSeq"] is not None and f["bridge"]["entriesBehind"] is None,
          "connect: the bridge line has the core's cursor and no matching-engine head (null)", json.dumps(f["bridge"]))
    nodes = f["core"]["nodes"]
    check(len(nodes) == 3 and sorted(n["id"] for n in nodes) == [0, 1, 2], "connect: a row per configured node")
    # the core's /health ends with version= and build= (cluster-kit BuildInfo, core #158): a dev jar without
    # Implementation-Version says version=unknown; build= is the jar's Build-Sha. stateHash is null until a snapshot.
    check(all(isinstance(n["version"], str) and n["version"] and isinstance(n["buildCommit"], str) and n["buildCommit"]
              for n in nodes),
          "connect: version and build commit come from each node's /health", json.dumps(nodes))
    check(all(n["entriesBehind"] is None and (n["snapshot"]["stateHash"] is None or n["snapshot"]["stateHash"].isdigit())
              for n in nodes),
          "connect: entries behind null (no source), snapshot state hash null or decimal", json.dumps(nodes))

    # ---- roles against each member's own /metrics
    ok = wait_until(lambda: latest(ws)["core"]["leaderId"] == leader, 10)
    f = latest(ws)
    check(ok, f"roles: leaderId {f['core']['leaderId']} is the member whose own /metrics says leader ({leader})")
    check([n["role"] for n in f["core"]["nodes"]].count("follower") == 2, "roles: two followers")
    check(isinstance(f["core"]["term"], int), "roles: the term is a number", json.dumps(f["core"]))
    check(all(isinstance(n["commitPosition"], int) and isinstance(n["appliedPosition"], int) and
              isinstance(n["behindBytes"], int) for n in f["core"]["nodes"]),
          "roles: positions and behindBytes are numbers", json.dumps(f["core"]["nodes"]))
    # each row against that node's own /health, read by this script (role and term do not move without an election)
    mismatch = []

    def rows_match_own_health():
        mismatch.clear()
        frame = latest(ws)
        for i in range(3):
            own = own_health(args, i)
            r = row(frame, i) or {}
            if own is None or (r.get("role"), r.get("term")) != own:
                mismatch.append((i, r.get("role"), r.get("term"), own))
        return not mismatch
    ok = wait_until(rows_match_own_health, 10)
    check(ok, "roles: every row's role and term equal that node's own /health (a follower's row from the follower)",
          json.dumps(mismatch))

    # ---- cap: at most one engine frame per FRAME_MIN_MS while the log moves
    cap_row(ws, f"poll {POLL_MS} ms")
    rounds0, req0, read0 = st.m("engine_rounds_total"), st.m("engine_requests_total"), st.m("engine_read_bytes_total")
    time.sleep(5)
    rounds1, req1, read1 = st.m("engine_rounds_total"), st.m("engine_requests_total"), st.m("engine_read_bytes_total")
    frame_bytes = st.m("engine_last_frame_bytes")
    if None not in (rounds0, rounds1, req0, req1, read0, read1) and rounds1 > rounds0:
        print(f"cost: {(req1 - req0) / 5:.1f} requests/s per stream instance (3 nodes, poll {POLL_MS} ms), "
              f"{(read1 - read0) / (rounds1 - rounds0):.0f} bytes read per round, engine frame {frame_bytes:.0f} bytes",
              flush=True)
    check(frame_bytes is not None and frame_bytes > 0, "cost: stream_engine_last_frame_bytes is reported")

    # ---- cap at the default poll (1000 ms) on a second stream: the boot frame takes the cap slot, the first frame with
    # data must come by the cap, and a held frame must not wait for an extra round
    st2 = stream_it.Stream(args, "stream-default", 94, {
        "STREAM_CORE_NODE_ENDPOINTS": endpoints,
        "STREAM_ENGINE_FRAME_MIN_MS": str(FRAME_MIN_MS),
        "STREAM_HEAD_PROBE_MS": "100",
    })
    procs.append(st2)
    ws2 = EngineWs(6)

    async def go2():
        return await ws2.open(st2.ws, {"Authorization": "Bearer dev:6"})
    if check(wait_until(lambda: st2.ready()[0] == 200, 120), "cap default poll: second stream ready", st2.ready()[1]):
        loop.run(go2())
        if check(wait_until(lambda: full_of(ws2), 10), "cap default poll: a frame with every role filled"):
            gaps = cap_row(ws2, "poll 1000 ms (default)")
            if gaps:
                # cap 1000 ms, poll 1000 ms: a frame is due once a second; an extra round would show as a ~2000 ms gap
                check(max(gaps) < FRAME_MIN_MS + 700, f"cap default poll: largest gap {max(gaps):.0f} ms (a held frame "
                      "waited for an extra round if near 2000 ms)")
        loop.run(ws2.close())
    st2.stop()
    procs.remove(st2)

    # ---- follower: SIGKILL -> unreachable from its own poll; restarted -> a follower again
    follower = [i for i in range(3) if i != leader][0]
    other = [i for i in range(3) if i not in (leader, follower)][0]
    fails0 = st.m("engine_request_failures_total") or 0
    os.kill(cluster.pids[follower], signal.SIGKILL)
    cluster.procs[follower].wait()
    print(f"SIGKILL core follower {follower}", flush=True)
    t_kill = time.time()
    ok = wait_until(lambda: (row(latest(ws), follower) or {}).get("role") == "unreachable", 10, every=0.05)
    r = row(latest(ws), follower) or {}
    check(ok, f"follower: node {follower} unreachable {time.time() - t_kill:.2f} s after its SIGKILL", json.dumps(r))
    check("refused" in (r.get("error") or ""), f"follower: the reason is its own failed poll ({r.get('error')})")
    check(r.get("commitPosition") is None and r.get("term") is None, "follower: no value shown for the dead node")
    f = latest(ws)
    check(f["core"]["leaderId"] == leader and row(f, leader)["role"] == "leader" and row(f, other)["role"] == "follower",
          "follower: the leader and the other follower keep their rows", json.dumps(f["core"]["nodes"]))
    check((st.m("engine_request_failures_total") or 0) > fails0, "follower: the failed requests are counted")
    # the killed member's embedded driver directory reads live for the core's 10 s bound: restart after it, and the row
    # must stay unreachable all along (nothing fills it in from the leader)
    flips = []
    end = time.time() + 11
    while time.time() < end:
        r = row(latest(ws), follower) or {}
        if r.get("role") != "unreachable":
            flips.append(r.get("role"))
        time.sleep(0.1)
    check(not flips, f"follower: node {follower} stays unreachable while dead (saw {flips[:3]})")
    restart_member(args, cluster, follower)
    ok = wait_until(lambda: (row(latest(ws), follower) or {}).get("role") == "follower" and
                    (row(latest(ws), follower) or {}).get("recovery") == "ready", 120)
    check(ok, f"follower: node {follower} back as a ready follower after its restart",
          json.dumps(row(latest(ws), follower)))
    if not ok:
        return

    # ---- election: SIGKILL the leader -> an election end naming the old and the new leader
    term_before = latest(ws)["core"]["term"]
    n_elections = len(ws.elections)
    os.kill(cluster.pids[leader], signal.SIGKILL)
    cluster.procs[leader].wait()
    t_kill = time.time()
    print(f"SIGKILL core leader {leader} (term {term_before})", flush=True)
    ended = lambda: any(e["phase"] == "end" for _, e in ws.elections[n_elections:])
    ok = wait_until(ended, 90, every=0.1)
    check(ok, f"election: an election end frame within {time.time() - t_kill:.1f} s of the leader's SIGKILL",
          json.dumps([e for _, e in ws.elections]))
    if not ok:
        return
    new_leader = None
    wait_until(lambda: [own_role(args, i) for i in range(3) if i != leader].count(2) == 1, 30)
    owners = [i for i in range(3) if i != leader and own_role(args, i) == 2]
    new_leader = owners[0] if len(owners) == 1 else None
    events = [e for _, e in ws.elections[n_elections:]]
    end = [e for e in events if e["phase"] == "end"][0]
    starts = [e for e in events if e["phase"] == "start"]
    print(f"election frames: {json.dumps(events)}", flush=True)
    check(end["previousLeaderId"] == leader, f"election: previousLeaderId {end['previousLeaderId']} is the killed leader {leader}")
    check(new_leader is not None and end["leaderId"] == new_leader,
          f"election: leaderId {end['leaderId']} is the member whose own /metrics says leader ({new_leader})")
    check(isinstance(end["term"], int) and isinstance(term_before, int) and end["term"] > term_before,
          f"election: the new term {end['term']} is above {term_before}")
    check(end["durationMs"] is None and isinstance(end["observedGapMs"], int),
          f"election: durationMs null (no source), observedGapMs {end['observedGapMs']} ms (poll resolution)")
    check("orderEntryReopened" in end and end["orderEntryReopened"] is None and
          "firstEntryPosition" in end and end["firstEntryPosition"] is None,
          "election: firstEntryPosition and orderEntryReopened are explicit nulls (no source)")
    if starts:
        s = starts[0]
        check(s["previousLeaderId"] == leader and isinstance(s["lastCommitPosition"], int),
              f"election: start frame names the old leader and its last polled commit (candidates {s['candidates']})")
    else:
        print("election: no round saw the election in progress (start frame not sent)", flush=True)
    ok = wait_until(lambda: (latest(ws)["core"].get("lastElection") or {}).get("leaderId") == end["leaderId"], 5)
    le = latest(ws)["core"]["lastElection"]
    check(ok and le["previousLeaderId"] == leader, "election: the engine frame's lastElection agrees", json.dumps(le))
    check((row(latest(ws), leader) or {}).get("role") == "unreachable", "election: the old leader's row is unreachable")
    loop.run(ws.close())


if __name__ == "__main__":
    sys.exit(main())
