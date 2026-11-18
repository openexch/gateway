#!/usr/bin/env python3
"""T1 rows of the REST routes through the real oe-gateway binary against a real three-member core cluster (as
session_it.py; a heavy job: run it under the lock). The principal is a dev credential (Bearer dev:<userId>, behind
GATEWAY_ALLOW_DEV_AUTH=1), so no users database is needed.

Checks, with the treasury lane off (gateway A):
  markets/status  /markets lists the core's markets and assets; /status is ready with the idempotence window
  create  POST /orders with an Idempotency-Key: 201 PENDING_NEW with a hold
  retry   the same key again: 200 duplicate with the same orderId; the core holds one order and one hold for it; the
        same key with another payload: 409 REQUEST_ID_PAYLOAD_MISMATCH
  read    GET /orders lists it; GET /orders/{id} answers it
  cancel  DELETE: 200 PENDING_CANCEL; a cancel of another user's order: 404 and the order stays open; an unknown id: 404
  bulk    {orderIds} with own open, own closed (an armed stop, cancelled), foreign and unknown ids: CANCEL_PENDING,
        NOT_FOUND (the core keeps no terminal order: a closed order answers UNKNOWN_ORDER), NOT_FOUND, NOT_FOUND, and
        zero requestId entries consumed (the user holds three entries, its funding deposit and two keyed creates; with
        the core's per-user request cap set to 4 exactly one more keyed create fits after the bulk cancels);
        {all:true} above GATEWAY_BULK_CANCEL_MAX: truncated with remaining
  stored  a keyFinal true reject (SIZE_TOO_LARGE) replays under its key after the risk limit is raised, a new key is
        evaluated again; a keyFinal false reject (GATE_CLOSED) is evaluated again under the same key
  balances  GET /balances: every asset of the config, the hold locked
  lane off  deposit and withdraw: 404 FEATURE_DISABLED
  credential without the flag: exit 78
And with the lane on (gateway B, a TREASURY key next to its GATEWAY key):
  /health says treasuryLane on; deposit 200, the same key replays duplicate; withdraw 200; a withdrawal above the balance
  422 INSUFFICIENT_FUNDS; above the faucet cap 422; an asset the core does not list 400 (never sent); GET /balances shows
  the result; the core's ledger agrees (money-dump)
Every gateway ends with no egress frame refused by the decoder.

usage: rest_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar>
                  --mode embedded|external --work <dir> [--port-base N]
"""
import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_it  # noqa: E402  (the core cluster, the probe and the check helpers)

SECRET_TREASURY = "66" * 32  # an obviously fake fixture key
TREASURY_KEY = f"6:TREASURY:{SECRET_TREASURY}"
session_it.CORE_KEYS = session_it.CORE_KEYS + f";6:TREASURY:{SECRET_TREASURY}::users=*"
check = session_it.check


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(port, path, method="GET", body=None, user=None, key=None, raw_body=None):
    data = raw_body.encode() if raw_body is not None else (json.dumps(body).encode() if body is not None else None)
    h = {}
    if data is not None:
        h["Content-Type"] = "application/json"
    if user is not None:
        h["Authorization"] = f"Bearer dev:{user}"
    if key is not None:
        h["Idempotency-Key"] = key
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None), raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw), raw
        except ValueError:
            return e.code, None, raw


class Gateway:
    def __init__(self, args, cluster, name, instance, extra):
        self.public, self.admin = free_port(), free_port()
        ingress = (f"0=localhost:{args.port_base + 2},1=localhost:{args.port_base + 102},"
                   f"2=localhost:{args.port_base + 202}")
        self.env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "GATEWAY_DRIVER_MODE": args.mode,
            "GATEWAY_CORE_INGRESS": ingress,
            "GATEWAY_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=4m|mtu=8192",
            "GATEWAY_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=4m|mtu=8192",
            "CORE_AUTH_CREDENTIAL": session_it.GATEWAY_KEY,
            "GATEWAY_INSTANCE_ID": str(instance),
            "GATEWAY_HTTP_BIND": f"127.0.0.1:{self.public}",
            "GATEWAY_ADMIN_BIND": f"127.0.0.1:{self.admin}",
            "GATEWAY_AUTH_MODES": "dev",
            "GATEWAY_ALLOW_DEV_AUTH": "1",
            "GATEWAY_BULK_CANCEL_MAX": "2",
            "GATEWAY_QUERY_MIN_INTERVAL_MS": "1",
        }
        if args.mode == "external":
            self.env["AERON_DIR"] = args.driver_dir
        else:
            self.env["GATEWAY_DRIVER_DIR"] = args.driver_parent
        self.env.update(extra)
        self.log = open(os.path.join(args.work, f"{name}.log"), "w")
        self.p = subprocess.Popen([os.path.join(args.bin_dir, "oe-gateway")], env=self.env, stdout=self.log,
                                  stderr=subprocess.STDOUT)

    def admin_get(self, path):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.admin}{path}", timeout=5) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()
        except (urllib.error.URLError, OSError):
            return 0, ""

    def wait_ready(self, path, timeout):
        end = time.time() + timeout
        while time.time() < end:
            if self.admin_get(path)[0] == 200:
                return True
            time.sleep(0.3)
        return False

    def stop(self):
        self.p.terminate()
        try:
            self.p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()


def no_refused(gw, name):
    _, metrics = gw.admin_get("/metrics")
    check("\ngateway_decode_refused_total 0\n" in metrics, f"{name}: no egress frame refused by the decoder",
          [line for line in metrics.splitlines() if "decode_refused" in line])


def orders_of(args, user):
    orders, holds = session_it.holds_of(args, user)
    return orders, holds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=9800)
    ap.add_argument("--mode", choices=["embedded", "external"], default="embedded")
    args = ap.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    print(f"rest_it mode={args.mode} work={args.work} ports={args.port_base}", flush=True)
    cluster = session_it.Cluster(args, args.work)
    driver = None
    gateways = []
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
        run(args, gateways)
    finally:
        for gw in gateways:
            gw.stop()
        cluster.stop()
        if driver is not None:
            driver.terminate()
            try:
                driver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                driver.kill()
    print(("REST IT PASS " if not session_it.FAILED else "REST IT FAIL ") + args.mode +
          (" " + " ".join(session_it.FAILED) if session_it.FAILED else ""), flush=True)
    return 0 if not session_it.FAILED else 1


def run(args, gateways):
    FP = session_it.FP
    a = Gateway(args, None, "gateway-a", 81, {})
    gateways.append(a)
    if not check(a.wait_ready("/ready/session", 120), "gateway A registered with the core (/ready/session 200)"):
        return
    rc1, _ = session_it.probe(args, "core-config", version=1, behavior=1, genesis=7, tick=1, min=1,
                              max=1_000_000_000_000_000, lot=1)
    rc2, _ = session_it.probe(args, "core-config", version=2, behavior=2, genesis=7, tick=1, min=1,
                              max=1_000_000_000_000_000, lot=1)
    rc3, _ = session_it.probe(args, "risk", version=1, market=1, min_qty=1, max_qty=10**6 * FP, collar_bps=5000,
                              per_sec=1000, max_open=5000, slippage_bps=100)
    rc4, _ = session_it.probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
    rc5, _ = session_it.probe(args, "deposit", user=1001, asset=0, amount=10**7 * FP, rh=9, rl=11)
    rc6, _ = session_it.probe(args, "deposit", user=1002, asset=0, amount=10**7 * FP, rh=9, rl=12)
    rc7, _ = session_it.probe(args, "deposit", user=1004, asset=0, amount=10**7 * FP, rh=9, rl=13)
    rc8, _ = session_it.probe(args, "deposit", user=1005, asset=0, amount=10**7 * FP, rh=9, rl=14)
    check(all(rc == 0 for rc in (rc1, rc2, rc3, rc4, rc5, rc6, rc7, rc8)),
          "core configured, gate open, users 1001, 1002, 1004 and 1005 funded")
    check(a.wait_ready("/ready", 30), "gateway A ready")

    # ---- markets and status
    status, body, raw = http(a.public, "/api/v2/markets")
    check(status == 200 and body["configVersion"] == 2 and any(m["marketId"] == "1" for m in body["markets"])
          and len(body["assets"]) > 0, "/markets: the core's markets and assets", raw)
    assets = [x["assetId"] for x in body["assets"]] if body else []
    status, body, raw = http(a.public, "/api/v2/status")
    check(status == 200 and body["ready"] is True and body["idempotencyWindowMs"] > 0
          and body["admission"][0]["marketId"] == "0" and body["admission"][0]["gate"] == "OPEN",
          "/status: ready, the idempotence window, the global gate OPEN", raw)

    # ---- create and the idempotent retry
    order = {"marketId": "1", "side": "BUY", "type": "LIMIT", "timeInForce": "GTC", "price": "100", "quantity": "1"}
    k1 = str(uuid.uuid4())
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=1001, key=k1)
    created = body["orderId"] if status == 201 and body else None
    check(status == 201 and body["duplicate"] is False and body["status"] == "PENDING_NEW"
          and body["hold"]["amount"] == "100", "create: 201 PENDING_NEW with the hold", raw)
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=1001, key=k1)
    check(status == 200 and body["duplicate"] is True and body["orderId"] == created,
          "the same Idempotency-Key again: 200 duplicate with the same orderId", raw)
    orders, holds = orders_of(args, 1001)
    mine = [o for o in orders if str(o["order"]) == created]
    check(len(orders) == 1 and len(mine) == 1 and len(holds) == 1, "the core holds one order and one hold for the key",
          f"orders {orders} holds {holds}")
    status, body, raw = http(a.public, "/api/v2/orders", "POST", dict(order, quantity="2"), user=1001, key=k1)
    check(status == 409 and body["code"] == "CONFLICT" and body["reason"] == "REQUEST_ID_PAYLOAD_MISMATCH",
          "the same key with another payload: 409 REQUEST_ID_PAYLOAD_MISMATCH", raw)
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=1001)
    check(status == 400 and body["code"] == "MISSING_IDEMPOTENCY_KEY", "a create without a key: 400", raw)
    status, body, raw = http(a.public, "/api/v2/orders", "POST", dict(order, orderType="LIMIT"), user=1001, key=k1)
    check(status == 400 and body["code"] == "VALIDATION" and "orderType" in raw, "an unknown field: 400", raw)

    # ---- reads
    status, body, raw = http(a.public, "/api/v2/orders", user=1001)
    check(status == 200 and [o["orderId"] for o in body["orders"]] == [created]
          and body["asOf"]["logPosition"] > 0, "GET /orders lists the open order", raw)
    status, body, raw = http(a.public, f"/api/v2/orders/{created}", user=1001)
    check(status == 200 and body["orderId"] == created and body["price"] == "100" and body["quantity"] == "1",
          "GET /orders/{id} answers the open order", raw)

    # ---- cancel
    status, body, raw = http(a.public, f"/api/v2/orders/{created}", "DELETE", user=1002)
    check(status == 404 and body["code"] == "NOT_FOUND", "another user's cancel: 404", raw)
    orders, _ = orders_of(args, 1001)
    check(len(orders) == 1, "the order stays open after the foreign cancel", str(orders))
    status, body, raw = http(a.public, f"/api/v2/orders/{created}", "DELETE", user=1001)
    check(status == 200 and body["status"] == "PENDING_CANCEL" and body["duplicate"] is False,
          "cancel: 200 PENDING_CANCEL", raw)
    status, body, raw = http(a.public, "/api/v2/orders/987654321", "DELETE", user=1001)
    check(status == 404 and body["reason"] == "UNKNOWN_ORDER", "cancel of an unknown id: 404", raw)

    # ---- bulk cancel (GATEWAY_BULK_CANCEL_MAX 2 on this gateway)
    ids = []
    for _ in range(3):
        status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=1001, key=str(uuid.uuid4()))
        ids.append(body["orderId"] if body else "0")
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=1002, key=str(uuid.uuid4()))
    foreign = body["orderId"] if body else "0"
    # the mixed set of user 1004: own open, own closed, foreign, unknown (two requests: the bound is 2 here)
    bu = 1004
    stop_order = dict(order, type="STOP_LIMIT", stopPrice="110")
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=bu, key=str(uuid.uuid4()))
    own_open = body["orderId"] if status == 201 and body else "0"
    status2, body2, raw2 = http(a.public, "/api/v2/orders", "POST", stop_order, user=bu, key=str(uuid.uuid4()))
    armed = body2["orderId"] if status2 == 201 and body2 else "0"
    check(status == 201 and status2 == 201, "user 1004: a resting order and an armed stop", raw + " " + raw2)
    status, body, raw = http(a.public, f"/api/v2/orders/{armed}", "DELETE", user=bu)  # an armed stop ends at its cancel
    check(status == 200 and body["status"] == "PENDING_CANCEL", "user 1004: the armed stop cancelled (now closed)", raw)
    # user 1004 holds three requestId entries (its funding deposit and two keyed creates; the unkeyed cancel takes
    # none): with the per-user cap at 4 exactly one more fits, unless the bulk cancels below take an entry
    rc, _ = session_it.probe(args, "core-limits", version=1, ttl_ms=900_000, cap=4, ring=65_536)
    check(rc == 0, "core limits: the per-user request cap set to 4")
    status, body, raw = http(a.public, "/api/v2/orders/cancel", "POST", {"orderIds": [own_open, armed]}, user=bu)
    states = [r["state"] for r in body["results"]] if status == 200 and body else []
    # the core keeps no terminal order: the cancelled stop answers UNKNOWN_ORDER, so own closed reads NOT_FOUND
    # (NOT_OPEN is the answer for an order the core still holds but that is no longer open)
    check(status == 200 and states == ["CANCEL_PENDING", "NOT_FOUND"] and body["accepted"] == 1,
          "bulk cancel: own open CANCEL_PENDING, own closed NOT_FOUND", raw)
    status, body, raw = http(a.public, "/api/v2/orders/cancel", "POST", {"orderIds": [foreign, "987654321"]}, user=bu)
    states = [r["state"] for r in body["results"]] if status == 200 and body else []
    check(status == 200 and states == ["NOT_FOUND", "NOT_FOUND"] and body["accepted"] == 0,
          "bulk cancel: foreign NOT_FOUND, unknown NOT_FOUND", raw)
    answers = [http(a.public, "/api/v2/orders", "POST", order, user=bu, key=str(uuid.uuid4())) for _ in range(2)]
    codes = [x[0] for x in answers]
    check(codes == [201, 429] and answers[1][1]["reason"] == "TOO_MANY_PENDING_REQUESTS",
          "bulk cancel: zero requestId entries consumed (cap 4, three held: one more keyed create fits, then refused)",
          " | ".join(x[2] for x in answers))
    rc, _ = session_it.probe(args, "core-limits", version=2, ttl_ms=900_000, cap=256, ring=65_536)
    check(rc == 0, "core limits: the per-user request cap back to 256")
    status, body, raw = http(a.public, "/api/v2/orders/cancel", "POST", {"all": True}, user=1001)
    check(status == 200 and body["requested"] >= 3 and body["truncated"] is True and len(body["results"]) == 2
          and body["remaining"] == body["requested"] - 2, "bulk cancel all:true above the bound: truncated, remaining",
          raw)
    orders, _ = orders_of(args, 1002)
    check(len(orders) == 1, "the other user's order is untouched by the bulk cancels", str(orders))

    # ---- stored rejects: keyFinal true replays under its key, keyFinal false is evaluated again
    su = 1005
    big = dict(order, price="1", quantity="2000000")
    ks = str(uuid.uuid4())
    status, body, raw = http(a.public, "/api/v2/orders", "POST", big, user=su, key=ks)
    check(status == 422 and body["reason"] == "SIZE_TOO_LARGE" and body["keyFinal"] is True,
          "stored: SIZE_TOO_LARGE is 422 keyFinal true", raw)
    rc, _ = session_it.probe(args, "risk", version=2, market=1, min_qty=1, max_qty=10**7 * FP, collar_bps=5000,
                             per_sec=1000, max_open=5000, slippage_bps=100)
    check(rc == 0, "risk: the size limit raised to 10^7")
    status, body, raw = http(a.public, "/api/v2/orders", "POST", big, user=su, key=ks)
    check(status == 422 and body["reason"] == "SIZE_TOO_LARGE",
          "stored: the same key replays SIZE_TOO_LARGE after the limit is raised", raw)
    status, body, raw = http(a.public, "/api/v2/orders", "POST", big, user=su, key=str(uuid.uuid4()))
    check(status == 201, "stored: a new key is evaluated again (201)", raw)
    rc, _ = session_it.probe(args, "gate", market=0, state="SUSPENDED", reason="ADMIN")
    kg = str(uuid.uuid4())
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=su, key=kg)
    check(rc == 0 and status == 503 and body["reason"] == "GATE_CLOSED" and body["keyFinal"] is False,
          "stored: GATE_CLOSED is 503 keyFinal false", raw)
    rc, _ = session_it.probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
    status, body, raw = http(a.public, "/api/v2/orders", "POST", order, user=su, key=kg)
    check(rc == 0 and status == 201 and body["duplicate"] is False,
          "stored: the same key after GATE_CLOSED is evaluated again (201)", raw)

    # ---- balances
    status, body, raw = http(a.public, "/api/v2/balances", user=1001)
    rows = {r["assetId"]: r for r in body["assets"]} if status == 200 and body else {}
    check(status == 200 and set(assets) <= set(rows) and rows.get("0", {}).get("locked") not in (None, "0")
          and body["asOf"]["logPosition"] > 0, "GET /balances: every asset, the holds locked", raw)

    # ---- the lane off
    for path in ("/api/v2/balances/deposit", "/api/v2/balances/withdraw"):
        status, body, raw = http(a.public, path, "POST", {"assetId": "0", "amount": "1"}, user=1001, key=str(uuid.uuid4()))
        check(status == 404 and body["code"] == "FEATURE_DISABLED", f"lane off: {path} 404 FEATURE_DISABLED", raw)
    _, health = a.admin_get("/health")
    check('"treasuryLane":"off"' in health, "lane off: /health says treasuryLane off")
    no_refused(a, "A")
    a.stop()
    gateways.remove(a)

    # ---- a credential without the flag: exit 78
    bad = Gateway(args, None, "gateway-refused", 83, {"GATEWAY_TREASURY_CREDENTIAL": TREASURY_KEY})
    try:
        code = bad.p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        bad.stop()
        code = None
    check(code == 78, "a TREASURY credential without GATEWAY_ALLOW_TREASURY_LANE=1: exit 78", str(code))

    # ---- the lane on
    caps = ",".join(f"{asset}:1000" for asset in assets)
    b = Gateway(args, None, "gateway-b", 82, {"GATEWAY_TREASURY_CREDENTIAL": TREASURY_KEY,
                                               "GATEWAY_ALLOW_TREASURY_LANE": "1",
                                               "GATEWAY_FAUCET_MAX_AMOUNT": caps,
                                               "GATEWAY_FAUCET_RATE": "100/s"})
    gateways.append(b)
    if not check(b.wait_ready("/ready", 120), "gateway B ready"):
        return
    _, health = b.admin_get("/health")
    check('"treasuryLane":"on"' in health, "lane on: /health says treasuryLane on")
    user = 1003
    asset = assets[0]
    dk = str(uuid.uuid4())
    status, body, raw = http(b.public, "/api/v2/balances/deposit", "POST", {"assetId": asset, "amount": "5"}, user=user,
                             key=dk)
    if status == 503:  # the TREASURY session may still be registering
        time.sleep(3)
        status, body, raw = http(b.public, "/api/v2/balances/deposit", "POST", {"assetId": asset, "amount": "5"},
                                 user=user, key=dk)
    check(status == 200 and body["available"] == "5" and body["duplicate"] is False, "lane on: deposit 200", raw)
    status, body, raw = http(b.public, "/api/v2/balances/deposit", "POST", {"assetId": asset, "amount": "5"}, user=user,
                             key=dk)
    check(status == 200 and body["duplicate"] is True and body["available"] == "5",
          "lane on: the same deposit key replays duplicate, credited once", raw)
    status, body, raw = http(b.public, "/api/v2/balances/withdraw", "POST", {"assetId": asset, "amount": "2"},
                             user=user, key=str(uuid.uuid4()))
    check(status == 200 and body["available"] == "3", "lane on: withdraw 200", raw)
    status, body, raw = http(b.public, "/api/v2/balances/withdraw", "POST", {"assetId": asset, "amount": "100"},
                             user=user, key=str(uuid.uuid4()))
    check(status == 422 and body["reason"] == "INSUFFICIENT_FUNDS" and body["keyFinal"] is False,
          "lane on: a withdrawal above the balance: 422 INSUFFICIENT_FUNDS", raw)
    status, body, raw = http(b.public, "/api/v2/balances/deposit", "POST", {"assetId": asset, "amount": "1000.1"},
                             user=user, key=str(uuid.uuid4()))
    check(status == 422 and body["code"] == "REJECTED" and "1000" in body["detail"], "lane on: above the faucet cap: 422",
          raw)
    status, body, raw = http(b.public, "/api/v2/balances/deposit", "POST", {"assetId": "4000", "amount": "1"},
                             user=user, key=str(uuid.uuid4()))
    check(status == 400 and body["code"] == "VALIDATION" and "4000" in body["detail"],
          "lane on: an asset the core does not list: 400, never sent", raw)
    status, body, raw = http(b.public, "/api/v2/balances", user=user)
    rows = {r["assetId"]: r for r in body["assets"]} if status == 200 and body else {}
    check(status == 200 and rows.get(asset, {}).get("available") == "3", "lane on: GET /balances shows 3", raw)
    _, money = session_it.probe(args, "money-dump")
    ledger = [x for x in (money or {}).get("balances", []) if x.get("user") == user]
    print(f"money-dump balances of {user}: {ledger}", flush=True)
    check(any(x.get("asset") == int(asset) and x.get("available") == 3 * FP for x in ledger),
          "lane on: the core's ledger agrees (3)", str(ledger))
    no_refused(b, "B")


if __name__ == "__main__":
    sys.exit(main())
