#!/usr/bin/env python3
"""T1: closing the fills gap of a stream resync with GET /api/v2/fills (a heavy job: run it under the lock).

Starts, on this host: three core members (as follow_it.py), a throwaway PostgreSQL container, oe-projector writing the
projection, oe-gateway (dev principals) reading history from it, oe-stream (dev principals), gw_session_it for the
orders and gw_stream_it_bridge for the trades (the core runs behaviour 2: a trade is applied as it comes).

The client rule: a client keeps the last eventSeq of any frame. After a `resync` its anchor E is the eventSeq of the
first frame that follows: the orders snapshot for a connection with `orders`, the first `heartbeat` for a fills-only
connection. It reads GET /api/v2/fills from the newest down to its last known fill, again until an answer's
asOf.eventSeq is at least E, and drops duplicates by (tradeId, orderId).

Row: for an orders-and-fills connection and for a fills-only connection of one user, trades before a disconnect, while
disconnected (the core's event ring and the stream's window made small, so the resume is RESUME_TOO_OLD) and after the
resync: every fill of the user exactly once across the stream and /fills.

Control: oe-gateway-nc-no-asof-event-seq (asOf without eventSeq) must fail with "resync fill gap not closable".

usage: fills_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar> --image <pg image>
                   --mode embedded|external --work <dir> [--port-base N] [--gateway-binary oe-gateway]
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "it"))
import follow_it as fi  # noqa: E402  (the core cluster, PostgreSQL, the projector, the order and trade sessions)

check = fi.check
FP = fi.FP
USER = 1001     # the user whose fills are followed (buys)
SELLER = 1002


def free_port():
    return fi.free_port()


def get(port, path, user):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers={"Authorization": f"Bearer dev:{user}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except ValueError:
            return e.code, None
    except (urllib.error.URLError, OSError):
        return 0, None


def edge_env(args, prefix, instance):
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        f"{prefix}_DRIVER_MODE": args.mode,
        f"{prefix}_CORE_INGRESS": args.ingress,
        f"{prefix}_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=4m|mtu=8192",
        f"{prefix}_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=4m|mtu=8192",
        "CORE_AUTH_CREDENTIAL": fi.GATEWAY_KEY,
        f"{prefix}_INSTANCE_ID": str(instance),
        f"{prefix}_AUTH_MODES": "dev",
        "GATEWAY_ALLOW_DEV_AUTH": "1",
    }
    if args.mode == "external":
        env["AERON_DIR"] = args.driver_dir
    else:
        env[f"{prefix}_DRIVER_DIR"] = args.edge_dir
    return env


def start(args, name, binary, env):
    log = open(os.path.join(args.work, f"{name}.log"), "a")
    p = subprocess.Popen([os.path.join(args.bin_dir, binary)], env=env, stdout=log, stderr=subprocess.STDOUT,
                         start_new_session=True)
    fi.STARTED.append(p)
    return p


def admin(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except (urllib.error.URLError, OSError):
        return 0


def read_gap(gw_port, last_known, anchor, timeout=60):
    """The client rule: GET /fills newest first down to the last known fill (exclusive), until asOf.eventSeq >= E.
    Returns (fills as (tradeId, orderId), the asOf.eventSeq that closed it, rounds) or (None, why, rounds)."""
    deadline = time.monotonic() + timeout
    rounds = 0
    while time.monotonic() < deadline:
        rounds += 1
        got = []
        target = "/api/v2/fills?limit=50"
        seq = None
        while True:
            status, body = get(gw_port, target, USER)
            if status != 200 or not body:
                break
            as_of = body.get("asOf") or {}
            if "eventSeq" not in as_of:
                return None, f"asOf has no eventSeq: {as_of}", rounds
            seq = as_of["eventSeq"] if seq is None else min(seq, as_of["eventSeq"])
            stop = False
            for item in body["items"]:
                key = (int(item["tradeId"]), int(item["orderId"]))
                if last_known is not None and key == last_known:
                    stop = True
                    break
                got.append(key)
            if stop or body.get("nextCursor") is None:
                break
            target = "/api/v2/fills?limit=50&cursor=" + body["nextCursor"]
        if seq is not None and seq >= anchor:
            return got, seq, rounds
        time.sleep(0.3)
    return None, f"asOf.eventSeq never reached {anchor}", rounds


def anchor_after_resync(ws, kind):
    """E: the eventSeq of the first frame after the last resync (the snapshot, or the first heartbeat)."""
    idx = max(i for i, f in enumerate(ws.frames) if f["type"] == "resync")
    for f in ws.frames[idx + 1:]:
        if kind == "orders" and f["type"] == "snapshot" and f.get("channel") == "orders":
            return f["asOf"]["eventSeq"]
        if kind == "fills" and f["type"] == "heartbeat" and isinstance(f.get("eventSeq"), int):
            return f["eventSeq"]
    return None


def run(args, cluster, pg, orders, procs):
    import stream_it  # noqa: E402  (the WebSocket client)
    url = pg.fresh("pj_fills")
    pj = fi.Projector(args, cluster, url, 41, "fills").start()
    st_ws, st_admin = free_port(), free_port()
    env = edge_env(args, "STREAM", 91)
    env.update({"STREAM_WS_BIND": f"127.0.0.1:{st_ws}", "STREAM_ADMIN_BIND": f"127.0.0.1:{st_admin}",
                "STREAM_HEAD_PROBE_MS": "500", "STREAM_HEARTBEAT_MS": "300", "STREAM_EVENT_WINDOW": "16",
                "STREAM_REPLAY_PAGE": "8", "STREAM_USER_LINGER_MS": "1000"})
    procs.append(start(args, "stream", "oe-stream", env))
    gw_http, gw_admin = free_port(), free_port()
    env = edge_env(args, "GATEWAY", 81)
    env.update({"GATEWAY_HTTP_BIND": f"127.0.0.1:{gw_http}", "GATEWAY_ADMIN_BIND": f"127.0.0.1:{gw_admin}",
                "GATEWAY_PG_URL": url, "GATEWAY_PRINCIPAL_RATE": "10000/s", "GATEWAY_PRINCIPAL_BURST": "10000",
                "GATEWAY_HISTORY_PROBE_MS": "500"})
    procs.append(start(args, "gateway", args.gateway_binary, env))
    bridge = fi.Bridge(args)
    check(bridge.wait_line("REGISTERED", 60), "the test bridge registered (BRIDGE session)")
    rc, _ = fi.probe(args, "deposit", user=SELLER, asset=1, amount=10**6 * FP, rh=9, rl=2)
    check(rc == 0, "the seller funded")
    check(fi.wait_for(lambda: admin(st_admin, "/ready") == 200, 60, 0.3), "the stream is ready")
    check(fi.wait_for(lambda: admin(gw_admin, "/ready/session") == 200, 60, 0.3), "the gateway session registered")
    buys = orders.create_ids(8, USER, "BUY")
    sells = orders.create_ids(8, SELLER, "SELL")
    check(len(buys) == 8 and len(sells) == 8, "8 crossing pairs created")
    loop = stream_it.Loop()
    both = loop.connect(st_ws, USER, ["orders", "fills"], name="orders-and-fills")
    only = loop.connect(st_ws, USER, ["fills"], name="fills-only")
    check(fi.wait_for(lambda: both.snapshots == 1, 15, 0.1), "orders-and-fills: subscribed, snapshot")
    trade = [0]
    expected = set()

    def fill(pair, qty):
        trade[0] += 1
        # taker the seller, maker the buyer: the user's fill row is (trade, buy order)
        ok = bridge.trade(trade[0], sells[pair], SELLER, buys[pair], USER, qty, False)
        expected.add((trade[0], buys[pair]))
        return ok
    for i in range(4):
        fill(0, FP // 4)  # four fills of the first pair, live on both connections
    check(fi.wait_for(lambda: len(both.fills) == 4 and len(only.fills) == 4, 20, 0.1),
          f"four fills live ({len(both.fills)}, {len(only.fills)})")
    before = {"orders": set(both.fills), "fills": set(only.fills)}
    resume = {"orders": both.last_seq, "fills": only.last_seq}
    last_fill = (4, buys[0])  # the newest fill each client saw before it went away
    loop.close(both)
    loop.close(only)
    time.sleep(0.5)
    # while disconnected: the core's event ring and the stream's window made small, fills and other events past both
    rc, _ = fi.probe(args, "core-limits", version=1, ttl_ms=900000, cap=100000, ring=16)
    check(rc == 0, "CoreLimitsSet: the event ring at 16")
    for pair in range(1, 5):
        for _ in range(4):
            fill(pair, FP // 4)
    check(len(orders.create_ids(30, SELLER, "SELL")) == 30, "30 more events while the clients are away")
    time.sleep(1.0)
    both = loop.connect(st_ws, USER, ["orders", "fills"], resume=resume["orders"], name="orders-and-fills")
    only = loop.connect(st_ws, USER, ["fills"], resume=resume["fills"], name="fills-only")
    got_resync = fi.wait_for(lambda: both.resyncs and only.resyncs and both.snapshots >= 1 and
                             anchor_after_resync(only, "fills") is not None, 20, 0.1)
    if not got_resync:
        check(False, f"INCONCLUSIVE: no resync on resume (orders-and-fills {both.resyncs}, fills-only {only.resyncs}) "
                     f"(precondition)")
        return
    check(True, f"resume older than the ring: resync {both.resyncs} / {only.resyncs}")
    for pair in range(5, 8):
        fill(pair, FP // 2)
        fill(pair, FP // 2)
    after_ok = fi.wait_for(lambda: len([t for t, _ in expected if t > 20]) <= len(only.fills) and
                           len([t for t, _ in expected if t > 20]) <= len(both.fills), 20, 0.1)
    check(after_ok, f"the fills after the resync live ({len(both.fills)}, {len(only.fills)})")
    closable = True
    for kind, ws in (("orders", both), ("fills", only)):
        anchor = anchor_after_resync(ws, kind)
        recovered, closed_at, rounds = read_gap(gw_http, last_fill, anchor)
        if recovered is None:
            closable = False
            check(False, f"{ws.name}: the gap after the resync could not be closed: {closed_at} (anchor {anchor})")
            continue
        order_of = dict(expected)
        stream_trades = before[kind] | ws.fills
        union = set(recovered) | {(t, order_of.get(t)) for t in stream_trades}  # the rule: one per (tradeId, orderId)
        gap = expected - {(t, order_of.get(t)) for t in stream_trades}
        if not gap:
            check(False, f"INCONCLUSIVE: {ws.name}: the stream lost no fill across the resync (precondition)")
        missing = sorted(expected - union)
        extra = sorted(union - expected)
        repeated = len(recovered) - len(set(recovered))
        check(not missing and not extra and not ws.dup_fills and repeated == 0,
              f"{ws.name}: every fill exactly once across the stream and /fills (anchor E {anchor}, closed at asOf "
              f"{closed_at} in {rounds} reads; {len(expected)} fills, {len(gap)} of them only in /fills, "
              f"{len(recovered)} from /fills, {len(stream_trades)} from the stream)",
              f"missing {missing} extra {extra} stream duplicates {ws.dup_fills} /fills repeated {repeated}")
    if not closable:
        check(False, "resync fill gap not closable")
    loop.close(both)
    loop.close(only)
    bridge.quit()
    pj.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--image", required=True)
    ap.add_argument("--mode", required=True, choices=["embedded", "external"])
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=27000)
    ap.add_argument("--gateway-binary", default="oe-gateway")
    args = ap.parse_args()
    args.binary = "oe-projector"
    args.core_driver = "embedded"
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    base = args.port_base
    args.ingress = f"0=localhost:{base + 2},1=localhost:{base + 102},2=localhost:{base + 202}"
    args.key_file = os.path.join(args.work, "cluster-key")
    with open(args.key_file, "w") as f:
        f.write(fi.GATEWAY_KEY + "\n")
    print(f"fills_it mode={args.mode} work={args.work} ports={base}", flush=True)
    cluster = fi.Cluster(args)
    driver = None
    pg = None
    orders = None
    procs = []
    try:
        os.makedirs(cluster.shm, exist_ok=True)
        args.edge_dir = os.path.join(cluster.shm, "edge")
        os.makedirs(args.edge_dir, exist_ok=True)
        if args.mode == "external":
            args.driver_dir = os.path.join(cluster.shm, "external-driver")
            dlog = open(os.path.join(args.work, "driver.log"), "w")
            driver = subprocess.Popen([os.path.join(args.bin_dir, "gw_test_media_driver"), args.driver_dir],
                                      stdout=dlog, stderr=subprocess.STDOUT)
            fi.wait_for(lambda: os.path.exists(os.path.join(args.driver_dir, "cnc.dat")), 10, 0.1)
        else:
            args.driver_dir = os.path.join(cluster.shm, "embedded")
            os.makedirs(args.driver_dir, exist_ok=True)
        pg = fi.Pg(args.image)
        cluster.start()
        check(fi.wait_for(lambda: cluster.leader() is not None, 90, 0.5), "the core elected a leader")
        check(fi.genesis(args), "genesis recorded (CoreConfig with genesisId, risk, gate OPEN, deposit)")
        orders = fi.Orders(args)
        check(fi.wait_for(lambda: (orders.status() or {}).get("registered"), 60, 0.5), "the order session registered")
        run(args, cluster, pg, orders, procs)
    except Exception as e:  # a crash is never a pass
        check(False, f"exception: {type(e).__name__}: {e}")
    finally:
        for p in fi.STARTED:  # every process this run started, whatever raised
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
    print(("FILLS IT PASS " if not fi.FAILED else "FILLS IT FAIL ") + args.mode +
          (" " + " | ".join(fi.FAILED) if fi.FAILED else ""), flush=True)
    return 0 if not fi.FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
