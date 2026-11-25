#!/usr/bin/env python3
"""T1 rows of oe-stream against a real three-member core cluster (as session_it.py; a heavy job: run it under the lock).

Starts, on this host: three core members (production Main, the real authenticator), oe-gateway (dev principals, the
treasury lane on: it funds the users and carries the order flow), oe-stream (dev principals), and gw_stream_it_bridge, a
BRIDGE session that sends MeJournalTrade frames so that orders fill with no matching engine (the core runs behaviour 2,
where a trade is applied as it comes). The ADMIN side (CoreConfig, risk, the gate, the limits record, the oracle dumps)
goes through CoreProbe. The WebSocket clients are a minimal client of this file (no dependency), all on one asyncio loop.

Rows:
  readiness  /ready 200 with the global gate still SUSPENDED; every core member stopped -> 503 head_probe_fresh; an
       open gap (a replay in pages) -> 503 event_gap
  binding    first-frame auth; no token from a URL (400); a userId in a frame closes 4003; no frame of another user
       ever reaches a connection (checked on every connection of every phase, the storm's included)
  stitch     under continuous order flow (creates, partial and full fills), STITCH_CLIENTS subscribes: each client's
       final open orders equal the core's, no duplicate event, no missing fill; one user above 512 open orders with its
       orders created and filled while its pages are read
  balances   deposits and fills racing BALANCE_CLIENTS balance subscribes: each client's final balances equal the ledger
  cache      a second connection and a reconnect inside the linger cost no core query and get a correct snapshot; the
       cache is dropped on session loss (the next connection reads again)
  resume     resume from a heartbeat after a quiet period: served from the window, no core query; resume after a
       stream restart inside the core ring: one shared backfill, then served; resume older than the ring: RESUME_TOO_OLD
  gap        the stream's session closed by the core (SIGSTOP past the session timeout) and a leader change, both
       mid-flow: the hole is filled by replay, no client resyncs; with the ring made small by CoreLimitsSet every client
       resyncs and converges (the storm below)
  slow       a client that stops reading is closed 4001 at the bound; another client's frame latency and the duty max
       cycle stay inside their bounds
  storm      --connections connections of distinct users, forced overrun: the stream offers at most
       STREAM_QUERY_RATE x 1.2 snapshot queries per second (its own counter, sampled), the leader's log time advances in
       every 100 ms sample, no core session is closed at its egress ceiling, gateway orders are answered inside their
       deadline throughout, every client converges within users / rate + 20 s

usage: stream_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar>
                    --mode embedded|external --work <dir> [--port-base N] [--connections N]
"""
import argparse
import asyncio
import base64
import json
import os
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_it  # noqa: E402  (the core cluster, the probe and the check helpers)

check = session_it.check
FP = session_it.FP
SECRET_TREASURY = "66" * 32  # obviously fake fixture keys
SECRET_BRIDGE = "77" * 32
SECRET_STREAM = "88" * 32
session_it.CORE_KEYS = session_it.CORE_KEYS + (f";6:TREASURY:{SECRET_TREASURY}::users=*;7:BRIDGE:{SECRET_BRIDGE}::users=*"
                                               f";8:GATEWAY:{SECRET_STREAM}::users=*")
TREASURY_KEY = f"6:TREASURY:{SECRET_TREASURY}"
BRIDGE_KEY = f"7:BRIDGE:{SECRET_BRIDGE}"
STREAM_KEY = f"8:GATEWAY:{SECRET_STREAM}"
PRICE = 100  # whole units; FP-scaled on the wire
STITCH_CLIENTS = 200
BALANCE_CLIENTS = 200
QUERY_RATE = 50
TERMINAL = {"FILLED", "CANCELLED", "REJECTED", "EXPIRED"}


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(port, path, method="GET", body=None, user=None, key=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    h = {}
    if data is not None:
        h["Content-Type"] = "application/json"
    if user is not None:
        h["Authorization"] = f"Bearer dev:{user}"
    if key is not None:
        h["Idempotency-Key"] = key
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, None
    except (urllib.error.URLError, OSError):
        return 0, None


def admin_get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except (urllib.error.URLError, OSError):
        return 0, ""


def metric(port, name):
    _, body = admin_get(port, "/metrics")
    m = re.search(r"^" + re.escape(name) + r" (\S+)$", body, re.M)
    return float(m.group(1)) if m else None


class Proc:
    def __init__(self, args, name, binary, env, stdin=False):
        self.name = name
        self.log = open(os.path.join(args.work, f"{name}.log"), "a")
        self.cmd = binary
        self.env = env
        self.stdin = stdin
        self.start()

    def start(self):
        self.p = subprocess.Popen(self.cmd, env=self.env, stdout=self.log if not self.stdin else subprocess.PIPE,
                                  stderr=self.log, stdin=subprocess.PIPE if self.stdin else None, text=True)

    def stop(self):
        try:
            os.kill(self.p.pid, signal.SIGCONT)
        except OSError:
            pass
        self.p.terminate()
        try:
            self.p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()


def base_env(args, prefix, instance, credential):
    ingress = f"0=localhost:{args.port_base + 2},1=localhost:{args.port_base + 102},2=localhost:{args.port_base + 202}"
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        f"{prefix}_DRIVER_MODE": args.mode,
        f"{prefix}_CORE_INGRESS": ingress,
        f"{prefix}_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=4m|mtu=8192",
        f"{prefix}_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=4m|mtu=8192",
        "CORE_AUTH_CREDENTIAL": credential,
        f"{prefix}_INSTANCE_ID": str(instance),
        f"{prefix}_AUTH_MODES": "dev",
        "GATEWAY_ALLOW_DEV_AUTH": "1",
    }
    if args.mode == "external":
        env["AERON_DIR"] = args.driver_dir
    else:
        env[f"{prefix}_DRIVER_DIR"] = args.driver_parent
    return env


class Stream(Proc):
    def __init__(self, args, name, instance, extra=None):
        self.ws, self.admin = free_port(), free_port()
        env = base_env(args, "STREAM", instance, STREAM_KEY)
        env.update({
            "STREAM_WS_BIND": f"127.0.0.1:{self.ws}",
            "STREAM_ADMIN_BIND": f"127.0.0.1:{self.admin}",
            "STREAM_HEAD_PROBE_MS": "500",
            "STREAM_READY_STALE_MS": "3000",
            "STREAM_HEARTBEAT_MS": "300",
            "STREAM_USER_LINGER_MS": "5000",
            "STREAM_WS_QUEUE_MESSAGES": "64",
            "STREAM_WS_SEND_BUFFER_BYTES": "16384",
            "STREAM_WS_PING_MS": "2000",
            "STREAM_WS_IDLE_MS": "60000",
            "STREAM_REPLAY_PAGE": "16",
            "STREAM_MAX_CONNECTIONS": str(max(4000, args.connections + 500)),
            "STREAM_QUERY_RATE": f"{QUERY_RATE}/s",
            "STREAM_IO_THREADS": "2",
            # a 12 s SIGSTOP (past the core's 10 s session timeout) must cost the core session, not the Aeron client
            "STREAM_DRIVER_TIMEOUT_MS": "30000",
            "AERON_CLIENT_LIVENESS_TIMEOUT": "40000000000",
            "AERON_PUBLICATION_UNBLOCK_TIMEOUT": "60000000000",
        })
        env.update(extra or {})
        super().__init__(args, name, [os.path.join(args.bin_dir, "oe-stream")], env)

    def ready(self):
        return admin_get(self.admin, "/ready")

    def m(self, name):
        return metric(self.admin, "stream_" + name)


class Gateway(Proc):
    def __init__(self, args, name, instance):
        self.public, self.admin = free_port(), free_port()
        env = base_env(args, "GATEWAY", instance, session_it.GATEWAY_KEY)
        env.update({
            "GATEWAY_HTTP_BIND": f"127.0.0.1:{self.public}",
            "GATEWAY_ADMIN_BIND": f"127.0.0.1:{self.admin}",
            "GATEWAY_TREASURY_CREDENTIAL": TREASURY_KEY,
            "GATEWAY_ALLOW_TREASURY_LANE": "1",
            "GATEWAY_FAUCET_MAX_AMOUNT": "0:100000000,1:100000000",
            "GATEWAY_FAUCET_RATE": "1000/s",
            "GATEWAY_PRINCIPAL_RATE": "10000/s",
            "GATEWAY_PRINCIPAL_BURST": "10000",
            "GATEWAY_REQUEST_TIMEOUT_MS": "5000",
        })
        super().__init__(args, name, [os.path.join(args.bin_dir, "oe-gateway")], env)


class Bridge:
    def __init__(self, args):
        env = dict(os.environ, CORE_AUTH_CREDENTIAL=BRIDGE_KEY)
        ingress = f"0=localhost:{args.port_base + 2},1=localhost:{args.port_base + 102},2=localhost:{args.port_base + 202}"
        cmd = [os.path.join(args.bin_dir, "gw_stream_it_bridge"), "--mode", args.mode, "--dir",
               args.driver_dir if args.mode == "external" else args.driver_parent, "--ingress", ingress, "--host",
               "127.0.0.1", "--instance", "71"]
        self.log = open(os.path.join(args.work, "bridge.stderr"), "w")
        self.p = subprocess.Popen(cmd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log, text=True)
        self.lines = []
        self.lock = threading.Lock()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            with self.lock:
                self.lines.append(line.strip())

    def registered(self, timeout):
        end = time.time() + timeout
        while time.time() < end:
            with self.lock:
                if "REGISTERED" in self.lines:
                    return True
            time.sleep(0.2)
        return False

    def trade(self, trade_id, taker, taker_user, maker, maker_user, qty):
        self.p.stdin.write(f"trade {trade_id} 1 {taker} {taker_user} {maker} {maker_user} {PRICE * FP} {qty} 1\n")
        self.p.stdin.flush()

    def stop(self):
        try:
            self.p.stdin.write("quit\n")
            self.p.stdin.flush()
            self.p.wait(timeout=10)
        except Exception:
            self.p.kill()


# ---------------------------------------------------------------- a minimal WebSocket client and its oracle

class Ws:
    """One client connection and the state a UI keeps from its frames."""

    def __init__(self, user, name=""):
        self.user = user
        self.name = name
        self.frames = []
        self.orders = {}       # orderId -> filledQuantity
        self.balances = {}     # assetId -> available
        self.fills = set()
        self.dup_fills = []
        self.duplicates = []
        self.seen = set()
        self.snapshot_as_of = -1
        self.snapshots = 0
        self.balance_snapshots = 0
        self.resyncs = []
        self.last_seq = -1
        self.close_code = None
        self.closed = False
        self.foreign = []
        self.order_ids = set()  # every order the client saw (for the foreign check)
        self.status = None
        self.subprotocol = None
        self.reading = True
        self.read_event = None
        self.first_frame_at = {}

    async def open(self, port, headers=None, target="/ws/v2", rcvbuf=None):
        self.read_event = asyncio.Event()
        self.read_event.set()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if rcvbuf:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        sock.setblocking(False)
        await asyncio.get_running_loop().sock_connect(sock, ("127.0.0.1", port))
        # a client that stops reading must stop the socket too: a small reader limit pauses the transport
        self.reader, self.writer = await asyncio.open_connection(sock=sock, limit=(2048 if rcvbuf else 1 << 24))
        key = base64.b64encode(os.urandom(16)).decode()
        lines = [f"GET {target} HTTP/1.1", f"Host: 127.0.0.1:{port}", "Upgrade: websocket", "Connection: Upgrade",
                 f"Sec-WebSocket-Key: {key}", "Sec-WebSocket-Version: 13"]
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        self.writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await self.writer.drain()
        head = await self.reader.readuntil(b"\r\n\r\n")
        first = head.split(b"\r\n")[0].decode()
        self.status = int(first.split()[1])
        for h in head.decode(errors="replace").split("\r\n")[1:]:
            if h.lower().startswith("sec-websocket-protocol:"):
                self.subprotocol = h.split(":", 1)[1].strip()
        if self.status != 101:
            self.closed = True
            self.writer.close()
            return False
        asyncio.get_running_loop().create_task(self._loop())
        return True

    async def send(self, obj):
        payload = (obj if isinstance(obj, str) else json.dumps(obj)).encode()
        await self._send_frame(0x1, payload)

    async def _send_frame(self, opcode, payload):
        mask = os.urandom(4)
        n = len(payload)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        try:
            self.writer.write(head + mask + masked)
            await self.writer.drain()
        except (ConnectionError, OSError):
            self.closed = True

    async def _loop(self):
        message = b""
        message_opcode = 0
        try:
            while True:
                await self.read_event.wait()
                b0, b1 = await self.reader.readexactly(2)
                fin = b0 & 0x80
                opcode = b0 & 0x0F
                n = b1 & 0x7F
                if n == 126:
                    n = struct.unpack(">H", await self.reader.readexactly(2))[0]
                elif n == 127:
                    n = struct.unpack(">Q", await self.reader.readexactly(8))[0]
                payload = await self.reader.readexactly(n)
                if opcode == 0x8:
                    self.close_code = struct.unpack(">H", payload[:2])[0] if len(payload) >= 2 else 1005
                    try:
                        await self._send_frame(0x8, payload[:2])
                    except Exception:
                        pass
                    break
                if opcode == 0x9:
                    await self._send_frame(0xA, payload)
                    continue
                if opcode in (0x0, 0x1):
                    # a message may come in fragments: a first frame, continuations, the last one with FIN
                    if opcode == 0x1:
                        message, message_opcode = b"", 0x1
                    message += payload
                    if fin and message_opcode == 0x1:
                        self.on_frame(message.decode())
                        message = b""
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        self.closed = True

    def on_frame(self, text):
        o = json.loads(text)
        self.frames.append(o)
        t = o["type"]
        self.first_frame_at.setdefault(t, time.time())
        if t == "snapshot" and o["channel"] == "orders":
            self.orders = {int(x["orderId"]): x["filledQuantity"] for x in o["orders"]}
            for x in o["orders"]:
                self.order_ids.add(int(x["orderId"]))
            self.snapshot_as_of = o["asOf"]["eventSeq"]
            self.last_seq = max(self.last_seq, self.snapshot_as_of)
            self.snapshots += 1
            self.seen = set()
        elif t == "snapshot":
            self.balances = {x["assetId"]: x["available"] for x in o["assets"]}
            self.balance_snapshots += 1
        elif t == "order":
            s = o["eventSeq"]
            if (self.snapshots and s <= self.snapshot_as_of) or s in self.seen:
                self.duplicates.append(s)
            self.seen.add(s)
            self.last_seq = max(self.last_seq, s)
            oid = int(o["order"]["orderId"])
            self.order_ids.add(oid)
            if o["order"]["status"] in TERMINAL:
                self.orders.pop(oid, None)
            else:
                self.orders[oid] = o["order"]["filledQuantity"]
        elif t == "fill":
            self.last_seq = max(self.last_seq, o["eventSeq"])
            tid = int(o["fill"]["tradeId"])
            self.order_ids.add(int(o["fill"]["orderId"]))
            if tid in self.fills:
                self.dup_fills.append(tid)
            self.fills.add(tid)
        elif t == "balance":
            self.balances[o["assetId"]] = o["available"]
        elif t in ("heartbeat", "pong"):
            if isinstance(o.get("eventSeq"), int):
                self.last_seq = max(self.last_seq, o["eventSeq"])
        elif t == "resync":
            self.resyncs.append(o["reason"])

    def pause(self):
        self.read_event.clear()

    def resume(self):
        self.read_event.set()

    async def close(self):
        try:
            await self._send_frame(0x8, struct.pack(">H", 1000))
            self.writer.close()
        except Exception:
            pass


class Loop:
    """The asyncio loop of every client, on its own thread."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()

    def run(self, coro, timeout=120):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def connect(self, port, user, channels=None, resume=None, headers=None, name="", rcvbuf=None):
        ws = Ws(user, name)

        async def go():
            h = dict(headers) if headers is not None else {"Authorization": f"Bearer dev:{user}"}
            ok = await ws.open(port, h, rcvbuf=rcvbuf)
            if ok and channels:
                msg = {"op": "subscribe", "channels": channels}
                if resume is not None:
                    msg["resume"] = {"eventSeq": resume}
                await ws.send(msg)
            return ok
        self.run(go())
        return ws

    def connect_many(self, port, users, channels, rate=400):
        clients = [Ws(u) for u in users]

        async def go():
            async def one(ws, delay):
                await asyncio.sleep(delay)
                try:
                    if await ws.open(port, {"Authorization": f"Bearer dev:{ws.user}"}):
                        await ws.send({"op": "subscribe", "channels": channels})
                except Exception:
                    ws.closed = True
            await asyncio.gather(*(one(ws, i / rate) for i, ws in enumerate(clients)))
        self.run(go(), timeout=600)
        return clients

    def send(self, ws, obj):
        self.run(ws.send(obj))

    def close(self, ws):
        self.run(ws.close())


# ---------------------------------------------------------------- order flow

class Flow:
    """Creates, fills and the oracle side of them: every order this run created, with its user."""

    def __init__(self, gw, bridge, buyers, sellers):
        self.gw = gw
        self.bridge = bridge
        self.buyers = buyers
        self.sellers = sellers
        self.user_of = {}
        self.remaining = {}
        self.side = {}
        self.trade_id = 0
        self.lock = threading.Lock()
        self.running = False
        self.thread = None
        self.latencies = []
        self.errors = []

    def create(self, user, side, qty=10):
        t0 = time.time()
        status, body = http(self.gw.public, "/api/v2/orders", "POST",
                            {"marketId": "1", "side": side, "type": "LIMIT", "timeInForce": "GTC", "price": str(PRICE),
                             "quantity": str(qty)}, user=user, key=str(uuid.uuid4()))
        self.latencies.append(time.time() - t0)
        if status not in (200, 201) or not body or (status == 200 and not body.get("duplicate")):
            self.errors.append((status, body))
            return None
        oid = int(body["orderId"])
        with self.lock:
            self.user_of[oid] = user
            self.remaining[oid] = qty * FP
            self.side[oid] = side
        return oid

    def fill(self, buy, sell, qty_units):
        with self.lock:
            qty = min(qty_units * FP, self.remaining.get(buy, 0), self.remaining.get(sell, 0))
            if qty <= 0:
                return False
            self.trade_id += 1
            tid = self.trade_id
            self.remaining[buy] -= qty
            self.remaining[sell] -= qty
        self.bridge.trade(tid, buy, self.user_of[buy], sell, self.user_of[sell], qty)
        return True

    def open_of(self, side, users=None):
        with self.lock:
            return [o for o, r in self.remaining.items() if r > 0 and self.side[o] == side
                    and (users is None or self.user_of[o] in users)]

    def step(self, rnd):
        r = rnd.random()
        if r < 0.45:
            self.create(rnd.choice(self.buyers), "BUY", rnd.randint(1, 10))
        elif r < 0.75:
            self.create(rnd.choice(self.sellers), "SELL", rnd.randint(1, 10))
        else:
            buys, sells = self.open_of("BUY", set(self.buyers)), self.open_of("SELL")
            if buys and sells:
                self.fill(rnd.choice(buys), rnd.choice(sells), rnd.randint(1, 6))

    def start(self, rate=30, seed=7):
        self.running = True

        def run():
            rnd = random.Random(seed)
            while self.running:
                self.step(rnd)
                time.sleep(1.0 / rate)
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=30)


def oracle_orders(args):
    _, dump = session_it.probe(args, "order-dump")
    out = {}
    for o in (dump or {}).get("orders", []):
        filled = o["filled"]
        out.setdefault(o["user"], {})[o["order"]] = format_money(filled)
    return out


def oracle_balances(args):
    _, dump = session_it.probe(args, "money-dump")
    out = {}
    for b in (dump or {}).get("balances", []):
        out.setdefault(b["user"], {})[str(b["asset"])] = format_money(b["available"])
    return out


def oracle_fills(args, flow):
    """FILL events per user: [(eventSeq, tradeId)] (the core's event ring, the default capacity)."""
    _, rep = session_it.probe(args, "order-events", **{"from": 0})
    out = {}
    for e in (rep or {}).get("events", []):
        if e["kind"] == "FILL":
            user = flow.user_of.get(e["order"])
            if user is not None:
                out.setdefault(user, []).append((e["seq"], e["trade"]))
    return out


def format_money(units):
    whole, frac = divmod(abs(units), FP)
    s = str(whole) + (("." + f"{frac:08d}".rstrip("0")) if frac else "")
    return ("-" if units < 0 else "") + s


def converged(clients, oracle, what, fills=None):
    bad = []
    for c in clients:
        want = oracle.get(c.user, {})
        if c.closed and c.close_code not in (None,):
            bad.append(f"{c.user}: closed {c.close_code}")
            continue
        if {int(k): v for k, v in want.items()} != c.orders:
            missing = set(want) - set(c.orders)
            extra = set(c.orders) - set(int(k) for k in want)
            wrong = [k for k in want if k in c.orders and c.orders[k] != want[k]]
            bad.append(f"{c.user}: client state differs from oracle (missing {sorted(missing)[:5]}, extra "
                       f"{sorted(extra)[:5]}, filled differs {sorted(wrong)[:5]})")
            for k in sorted(missing)[:2] + sorted(extra)[:2]:
                seen = [f for f in c.frames if f.get("type") in ("order", "fill") and
                        int((f.get("order") or f.get("fill"))["orderId"]) == k]
                print(f"debug {c.user} order {k}: oracle {want.get(k)} snapshot asOf {c.snapshot_as_of} frames "
                      f"{[(f['type'], f['eventSeq'], (f.get('order') or {}).get('status')) for f in seen]}", flush=True)
        if c.duplicates:
            bad.append(f"{c.user}: duplicate event {c.duplicates[:3]}")
        if fills is not None:
            for seq, tid in fills.get(c.user, []):
                if seq > c.snapshot_as_of >= 0 and tid not in c.fills:
                    bad.append(f"{c.user}: fill {tid} missing")
                    break
        if c.dup_fills:
            bad.append(f"{c.user}: fill {c.dup_fills[0]} delivered twice")
    check(not bad, what, "; ".join(bad[:8]) + (f" (+{len(bad) - 8})" if len(bad) > 8 else ""))
    return not bad


def no_foreign(clients, flow, what):
    bad = []
    for c in clients:
        for oid in c.order_ids:
            owner = flow.user_of.get(oid)
            if owner is not None and owner != c.user:
                bad.append(f"order {oid} of {owner} on {c.user}'s connection")
    check(not bad, f"{what}: no foreign event delivered", "; ".join(bad[:5]))


def wait_until(pred, timeout, every=0.2):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(every)
    return pred()


# ---------------------------------------------------------------- the run

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--mode", required=True, choices=["embedded", "external"])
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=9800)
    ap.add_argument("--connections", type=int, default=int(os.environ.get("GW_IT_STREAM_CONNECTIONS", "2000")))
    args = ap.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    print(f"stream_it mode={args.mode} work={args.work} ports={args.port_base} connections={args.connections}", flush=True)
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
                                      stdout=dlog, stderr=subprocess.STDOUT,
                                      env=dict(os.environ, AERON_CLIENT_LIVENESS_TIMEOUT="40000000000",
                                               AERON_PUBLICATION_UNBLOCK_TIMEOUT="60000000000"))
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
    print(("STREAM IT PASS " if not session_it.FAILED else "STREAM IT FAIL ") + args.mode +
          (" " + " ".join(session_it.FAILED) if session_it.FAILED else ""), flush=True)
    return 0 if not session_it.FAILED else 1


def run(args, cluster, procs):
    gw = Gateway(args, "gateway", 81)
    procs.append(gw)
    st = Stream(args, "stream", 91)
    procs.append(st)
    bridge = Bridge(args)
    procs.append(bridge)
    loop = Loop()
    if not check(wait_until(lambda: admin_get(gw.admin, "/ready/session")[0] == 200, 120), "gateway registered"):
        return
    # every core node's endpoint answers (the leader change and the log-position rows read it); a port taken by another
    # process would otherwise turn those rows vacuous
    silent = [i for i in range(3) if not wait_until(lambda i=i: admin_get(session_it.node_port(args.port_base, i), "/metrics")[0] == 200,
                                                     60)]
    if not check(not silent, f"every core node's /metrics answers (silent: {silent}; is a port of the base taken?)"):
        return

    # ---- readiness: ready on a genesis core, gate still SUSPENDED (the gate is not a condition of the stream)
    ok = wait_until(lambda: st.ready()[0] == 200, 60)
    check(ok, "readiness: stream /ready 200 with the global gate SUSPENDED (genesis)", st.ready()[1])

    for version in (1, 2):
        rc, _ = session_it.probe(args, "core-config", version=version, behavior=version, genesis=7, tick=1, min=1,
                                 max=1_000_000_000_000_000, lot=1)
        check(rc == 0, f"CoreConfig {version}")
    rc, _ = session_it.probe(args, "risk", version=1, market=1, min_qty=1, max_qty=10**6 * FP, collar_bps=5000,
                             per_sec=100000, max_open=100000, slippage_bps=100)
    check(rc == 0, "risk row for market 1")
    # the per-user request cap above this run's keyed creates of one user (the big user's, the slow client's)
    rc, _ = session_it.probe(args, "core-limits", version=1, ttl_ms=900000, cap=100000, ring=65536)
    check(rc == 0, "CoreLimitsSet: the per-user request cap at 100000")
    rc, _ = session_it.probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
    check(rc == 0 and wait_until(lambda: admin_get(gw.admin, "/ready")[0] == 200, 30), "gate OPEN, gateway ready")
    check(bridge.registered(60), "the test bridge registered (BRIDGE session)")

    buyers = list(range(2001, 2041))
    sellers = list(range(3001, 3041))
    big = 4001
    for u in buyers + [big]:
        http(gw.public, "/api/v2/balances/deposit", "POST", {"assetId": "0", "amount": "50000000"}, user=u,
             key=str(uuid.uuid4()))
    for u in sellers + [big]:
        http(gw.public, "/api/v2/balances/deposit", "POST", {"assetId": "1", "amount": "500000"}, user=u,
             key=str(uuid.uuid4()))
    flow = Flow(gw, bridge, buyers, sellers)
    everyone = []

    # ---- binding at the edge
    c = loop.connect(st.ws, 5001, headers={})
    loop.send(c, {"op": "auth", "token": "dev:5001"})
    loop.send(c, {"op": "subscribe", "channels": ["orders"]})
    check(wait_until(lambda: c.snapshots == 1, 10), "binding: first-frame auth then subscribe: snapshot")
    t = Ws(5002)
    loop.run(t.open(st.ws, {}, target="/ws/v2?token=dev:5002"))
    check(t.status == 400, f"binding: no token from a URL: {t.status}")
    s = loop.connect(st.ws, 5003, headers={"Sec-WebSocket-Protocol": "bearer, dev:5003"}, channels=["fills"])
    check(s.subprotocol == "bearer" and wait_until(lambda: any(f["type"] == "subscribed" for f in s.frames), 10),
          "binding: bearer subprotocol")
    u = loop.connect(st.ws, 5004)
    loop.send(u, {"op": "subscribe", "channels": ["orders"], "userId": "2001"})
    check(wait_until(lambda: u.close_code == 4003, 10), f"binding: a userId in a frame closes 4003 ({u.close_code})")

    # ---- stitch: STITCH_CLIENTS subscribes under continuous flow, plus a user above 512 open orders
    for i in range(600):
        flow.create(big, "BUY", 1)
    flow.start(rate=40)
    time.sleep(2)
    stitch = []
    rnd = random.Random(11)
    big_client = None
    for i in range(STITCH_CLIENTS):
        user = rnd.choice(buyers + sellers)
        stitch.append(loop.connect(st.ws, user, ["orders", "fills"]))
        if i == STITCH_CLIENTS // 2:
            # the big user's pages are read while its own orders are created and filled
            stop_big = threading.Event()

            def big_flow():
                r2 = random.Random(5)
                while not stop_big.is_set():
                    flow.create(big, "BUY", 1)
                    flow.create(big, "BUY", 1)
                    sells = flow.open_of("SELL")
                    mine = flow.open_of("BUY", {big})
                    if sells and mine:
                        flow.fill(r2.choice(mine), r2.choice(sells), 1)
                    time.sleep(0.005)
            th = threading.Thread(target=big_flow, daemon=True)
            th.start()
            big_client = loop.connect(st.ws, big, ["orders", "fills"])
            time.sleep(1.5)
            stop_big.set()
            th.join()
        time.sleep(0.02)
    time.sleep(2)
    flow.stop()
    time.sleep(3)
    oracle = oracle_orders(args)
    fills = oracle_fills(args, flow)
    converged(stitch, oracle, f"stitch: {STITCH_CLIENTS} subscribes under flow: final orders equal the oracle, "
                              "no duplicate, no missing fill", fills)
    check(big_client is not None and len(oracle.get(big, {})) > 512,
          "stitch: the big user holds more than 512 open orders", str(len(oracle.get(big, {}))))
    converged([big_client], oracle, "stitch: the user above 512 open orders (changed between its pages) converges",
              fills)
    everyone += stitch + [big_client]

    # ---- cache: the per-user cache
    target = buyers[0]
    a1 = loop.connect(st.ws, target, ["orders", "balances"])
    wait_until(lambda: a1.snapshots == 1 and a1.balance_snapshots == 1, 10)
    snaps, reads = st.m("snapshot_queries_total"), st.m("balance_queries_total")
    a2 = loop.connect(st.ws, target, ["orders", "balances"])
    wait_until(lambda: a2.snapshots == 1 and a2.balance_snapshots == 1, 10)
    loop.close(a1)
    loop.close(a2)
    time.sleep(0.5)
    a3 = loop.connect(st.ws, target, ["orders"])
    wait_until(lambda: a3.snapshots == 1, 10)
    oracle = oracle_orders(args)
    check(st.m("snapshot_queries_total") == snaps and st.m("balance_queries_total") == reads,
          "cache: a second connection and a reconnect inside the linger cost no core query",
          f"{snaps}->{st.m('snapshot_queries_total')}, {reads}->{st.m('balance_queries_total')}")
    converged([a2, a3], oracle, "cache: the cached snapshots are correct")
    everyone += [a2, a3]

    # ---- resume: resume from a heartbeat after a quiet period: the window, no core query
    quiet = 6001
    q1 = loop.connect(st.ws, quiet, ["orders", "fills"])
    wait_until(lambda: q1.snapshots == 1, 10)
    flow.start(rate=40, seed=21)
    time.sleep(3)
    flow.stop()
    time.sleep(1)
    last = q1.last_seq
    loop.close(q1)
    snaps, backfills = st.m("snapshot_queries_total"), st.m("backfills_total")
    q2 = loop.connect(st.ws, quiet, ["orders", "fills"], resume=last)
    time.sleep(1.5)
    check(q2.resyncs == [] and q2.snapshots == 0 and st.m("snapshot_queries_total") == snaps and
          st.m("backfills_total") == backfills and st.m("window_resumes_total") >= 1,
          f"resume: resume from heartbeat {last} after a quiet period: served from the window, no core query",
          f"resyncs {q2.resyncs} snapshots {q2.snapshots}")

    # ---- resume: a stream restart inside the core ring: one shared backfill, then served
    r1 = loop.connect(st.ws, buyers[1], ["orders", "fills"])
    r2 = loop.connect(st.ws, sellers[1], ["orders", "fills"])
    wait_until(lambda: r1.snapshots == 1 and r2.snapshots == 1, 15)
    flow.start(rate=40, seed=31)
    time.sleep(2)
    flow.stop()
    time.sleep(1)
    l1, l2 = r1.last_seq, r2.last_seq
    st.stop()
    flow.start(rate=40, seed=41)
    time.sleep(2)
    flow.stop()
    st.start()
    check(wait_until(lambda: st.ready()[0] == 200, 60), "the restarted stream is ready")
    r1b = loop.connect(st.ws, buyers[1], ["orders", "fills"], resume=l1)
    r2b = loop.connect(st.ws, sellers[1], ["orders", "fills"], resume=l2)
    time.sleep(3)
    oracle = oracle_orders(args)
    check(st.m("backfills_total") == 1 and r1b.resyncs == [] and r2b.resyncs == [] and r1b.snapshots == 0,
          "resume: resume after a stream restart inside the core ring: one shared backfill, then served",
          f"backfills {st.m('backfills_total')}, resyncs {r1b.resyncs} {r2b.resyncs}")
    everyone += [r1b, r2b]

    # ---- readiness: an open gap is 503 (a replay in pages of STREAM_REPLAY_PAGE), every core member stopped is 503
    live = loop.connect(st.ws, buyers[2], ["orders", "fills"])
    live2 = loop.connect(st.ws, sellers[2], ["orders", "fills"])
    wait_until(lambda: live.snapshots == 1 and live2.snapshots == 1, 15)

    # ---- gap: the stream's session closed by the core mid-flow: the hole is filled by replay
    flow.start(rate=60, seed=51)
    time.sleep(1)
    os.kill(st.p.pid, signal.SIGSTOP)
    time.sleep(12)  # past the core's 10 s session timeout: the session is closed by the core
    os.kill(st.p.pid, signal.SIGCONT)
    saw_gap = False
    end = time.time() + 15
    while time.time() < end:
        code, body = st.ready()
        if code == 503 and "event_gap" in body:
            saw_gap = True
            break
        time.sleep(0.05)
    check(saw_gap, "readiness: an open gap reads 503 event_gap")
    time.sleep(3)
    flow.stop()
    time.sleep(3)
    oracle = oracle_orders(args)
    fills = oracle_fills(args, flow)
    check(live.resyncs == [] and live2.resyncs == [], "gap: session closed by the core: no client resyncs",
          f"{live.resyncs} {live2.resyncs}")
    converged([live, live2], oracle, "gap: session closed by the core: the hole is filled by replay", fills)
    # the cache is dropped on session loss: the next connection reads its snapshot again
    snaps = st.m("snapshot_queries_total")
    a4 = loop.connect(st.ws, target, ["orders"])
    wait_until(lambda: a4.snapshots == 1, 10)
    check(st.m("snapshot_queries_total") > snaps, "cache: the cache is dropped on session loss")
    everyone += [a4]

    # ---- slow: a slow client is closed 4001 at the bound; others are not delayed
    slow_user = 7001
    http(gw.public, "/api/v2/balances/deposit", "POST", {"assetId": "0", "amount": "50000000"}, user=slow_user,
         key=str(uuid.uuid4()))
    slow = loop.connect(st.ws, slow_user, ["orders"], rcvbuf=4096)
    fast = loop.connect(st.ws, buyers[3], ["orders"])
    wait_until(lambda: slow.snapshots == 1 and fast.snapshots == 1, 10)
    slow.pause()
    lat = []
    for i in range(500):
        flow.create(slow_user, "BUY", 1)
        if i % 25 == 0:
            n = len(fast.frames)
            t0 = time.time()
            if flow.create(buyers[3], "BUY", 1) is not None:
                wait_until(lambda: len(fast.frames) > n, 5, every=0.002)
                lat.append(time.time() - t0)
    max_cycle = st.m("duty_max_cycle_us")
    slow.resume()
    check(wait_until(lambda: slow.close_code == 4001, 30), f"slow: the slow client is closed 4001 at the bound "
          f"(close {slow.close_code}, queue high water {st.m('ws_queue_high_water')})")
    check(st.m("ws_queue_high_water") <= 64 + 64 + 1, "slow: queue exceeded bound",
          f"high water {st.m('ws_queue_high_water')}")
    check(lat and max(lat) < 1.0,
          f"slow: another client's create-to-frame latency stays under 1 s (max {max(lat):.3f} s)")
    check(max_cycle is not None and max_cycle < 100_000, f"slow: the stream's duty max cycle stays under 100 ms "
          f"({max_cycle} us)")
    everyone += [fast]

    # ---- balances: deposits and fills racing BALANCE_CLIENTS balance subscribes
    bal = []
    flow.start(rate=40, seed=61)
    rnd = random.Random(13)
    for i in range(BALANCE_CLIENTS):
        user = rnd.choice(buyers + sellers)
        bal.append(loop.connect(st.ws, user, ["balances"]))
        if i % 5 == 0:
            http(gw.public, "/api/v2/balances/deposit", "POST", {"assetId": "0", "amount": "1"},
                 user=rnd.choice(buyers + sellers), key=str(uuid.uuid4()))
    time.sleep(2)
    flow.stop()
    time.sleep(3)
    ledger = oracle_balances(args)
    bad = []
    for b in bal:
        want = {k: v for k, v in ledger.get(b.user, {}).items()}
        got = {k: v for k, v in b.balances.items() if v != "0" or k in want}
        if want != got:
            bad.append(f"{b.user}: stale balance after snapshot (client {got}, ledger {want})")
    check(not bad, f"balances: {BALANCE_CLIENTS} balance subscribes racing deposits and fills: final balances equal "
          "the ledger", "; ".join(bad[:5]))

    # ---- gap: a leader change mid-flow: the hole is filled, no client resyncs
    flow.start(rate=40, seed=71)
    time.sleep(1)
    leader = leader_of(args, cluster)
    if leader is not None:
        os.kill(cluster.pids[leader], signal.SIGKILL)
        print(f"SIGKILL core leader {leader}", flush=True)
    time.sleep(8)
    flow.stop()
    time.sleep(4)
    oracle = oracle_orders(args)
    if check(leader is not None, "gap: the core leader found and killed"):
        check(live.resyncs == [] and live2.resyncs == [], "gap: leader change: no client resyncs", f"{live.resyncs}")
        converged([live, live2], oracle, "gap: leader change: the hole is filled by replay")
    else:
        print("SKIP gap: leader change rows: no leader was killed", flush=True)

    # ---- storm (and gap with the ring made small): N connections of distinct users, forced overrun
    n = args.connections
    users = list(range(100000, 100000 + n))
    t0 = time.time()
    storm = loop.connect_many(st.ws, users, ["orders"])
    check(wait_until(lambda: sum(1 for c in storm if c.snapshots >= 1) == n, max(120, n / QUERY_RATE + 60)),
          f"storm: {n} connections subscribed ({sum(1 for c in storm if c.snapshots >= 1)} snapshots, "
          f"{time.time() - t0:.1f} s)")
    rc, _ = session_it.probe(args, "core-limits", version=2, ttl_ms=900000, cap=100000, ring=16)
    check(rc == 0, "CoreLimitsSet: the event ring at 16")
    pre_snap = st.m("snapshot_queries_total")
    pre_closed = core_ceiling_closed(args, cluster)
    flow.start(rate=40, seed=81)
    time.sleep(1)
    os.kill(st.p.pid, signal.SIGSTOP)
    time.sleep(12)
    os.kill(st.p.pid, signal.SIGCONT)
    t_resync = time.time()
    samples = []
    log_times = []
    gw_late = []
    leader = leader_of(args, cluster)
    stop_sampling = threading.Event()

    def sample():
        last = st.m("snapshot_queries_total") or 0
        tl = time.time()
        while not stop_sampling.is_set():
            time.sleep(1.0)
            v = st.m("snapshot_queries_total") or 0
            now = time.time()
            samples.append((v - last) / (now - tl))
            last, tl = v, now

    def sample_log():
        while not stop_sampling.is_set():
            log_times.append(core_log_time(args, leader))
            time.sleep(0.1)

    def gateway_orders():
        while not stop_sampling.is_set():
            t1 = time.time()
            s_, _ = http(gw.public, "/api/v2/orders", "POST",
                         {"marketId": "1", "side": "BUY", "type": "LIMIT", "timeInForce": "GTC", "price": str(PRICE),
                          "quantity": "1"}, user=buyers[5], key=str(uuid.uuid4()), timeout=10)
            if s_ != 201 or time.time() - t1 > 5.0:
                gw_late.append((s_, time.time() - t1))
            time.sleep(0.2)
    threads = [threading.Thread(target=f, daemon=True) for f in (sample, sample_log, gateway_orders)]
    for th in threads:
        th.start()
    bound = n / QUERY_RATE + 20
    all_resynced = wait_until(lambda: all(c.resyncs for c in storm), 30)
    done = wait_until(lambda: all(c.snapshots >= 2 for c in storm), bound + 10, every=0.5)
    took = time.time() - t_resync
    stop_sampling.set()
    for th in threads:
        th.join(timeout=10)
    flow.stop()
    time.sleep(3)
    oracle = oracle_orders(args)
    check(all_resynced, f"gap/storm: with the ring at 16 every connection got resync "
          f"({sum(1 for c in storm if c.resyncs)} of {n})")
    peak = max(samples) if samples else 0
    total = (st.m("snapshot_queries_total") or 0) - pre_snap
    average = total / took if took > 0 else 0
    check(peak <= QUERY_RATE * 1.2 and average <= QUERY_RATE * 1.2,
          f"storm: snapshot query rate above the bound: peak {peak:.1f}/s in a 1 s sample, {average:.1f}/s over the "
          f"re-snapshot ({total:.0f} in {took:.1f} s), bound {QUERY_RATE * 1.2:.0f}/s", str([round(x, 1) for x in samples]))
    check(done and took <= bound,
          f"storm: every client converged in {took:.1f} s (bound users / rate + 20 = {bound:.0f} s)")
    positions = [x for x in log_times if x is not None]
    stalls = sum(1 for a, b in zip(positions, positions[1:]) if b <= a)
    check(len(positions) > 10 and stalls == 0, f"storm: the leader's log position advanced in every 100 ms sample "
          f"({len(positions)} samples, {stalls} without progress)")
    post_closed = core_ceiling_closed(args, cluster)
    check(pre_closed is not None and post_closed == pre_closed, f"storm: no core session closed at its egress ceiling "
          f"({pre_closed} -> {post_closed})")
    check(not gw_late,
          f"storm: gateway orders answered inside their deadline throughout ({len(gw_late)} late or failed)",
          str(gw_late[:5]))
    converged([c for c in storm[:200]] + [live, live2], oracle, "gap/storm: after the storm the clients converge")
    print(f"storm: {n} connections, {st.m('snapshot_queries_total') - pre_snap:.0f} snapshot queries, {took:.1f} s",
          flush=True)
    everyone += storm + [live, live2]

    # ---- resume: resume older than the ring (16): RESUME_TOO_OLD
    old = loop.connect(st.ws, buyers[1], ["orders"], resume=1)
    check(wait_until(lambda: old.resyncs == ["RESUME_TOO_OLD"] and old.snapshots == 1, 15),
          f"resume: a resume older than the core's ring: resync RESUME_TOO_OLD and a snapshot ({old.resyncs})")

    # ---- readiness: every core member stopped: the head probe goes unanswered -> 503 head_probe_fresh
    for pid in cluster.pids.values():
        try:
            os.kill(pid, signal.SIGSTOP)
        except OSError:
            pass
    t0 = time.time()
    saw = wait_until(lambda: st.ready()[0] == 503 and "head_probe_fresh" in st.ready()[1], 20, every=0.1)
    print(f"head_probe_fresh failed {time.time() - t0:.1f} s after the core stopped: {st.ready()[1]}", flush=True)
    for pid in cluster.pids.values():
        try:
            os.kill(pid, signal.SIGCONT)
        except OSError:
            pass
    check(saw, "readiness: head probe unanswered past STREAM_READY_STALE_MS: 503 head_probe_fresh")

    # ---- binding everywhere: no frame of another user reached any connection; and no frame was refused by the decoder
    no_foreign(everyone, flow, "binding: every connection of the run")
    refused = metric(st.admin, "stream_decode_refused_total")
    check(refused == 0, f"the stream refused no egress frame ({refused})")
    check(not flow.errors, f"every flow create was accepted ({len(flow.errors)} not: {flow.errors[:3]})")


def leader_of(args, cluster):
    for i in range(3):
        code, body = admin_get(session_it.node_port(args.port_base, i), "/metrics")
        if code == 200 and re.search(r"^core_cluster_role 2(\.0)?$", body, re.M):
            return i
    return None


def core_log_time(args, member):
    """The member's log position (core_log_position): the commit position as this node applied it."""
    if member is None:
        return None
    code, body = admin_get(session_it.node_port(args.port_base, member), "/metrics")
    m = re.search(r"^core_log_position (\S+)$", body, re.M)
    return float(m.group(1)) if m else None


def core_ceiling_closed(args, cluster):
    total = 0
    found = False
    for i in range(3):
        code, body = admin_get(session_it.node_port(args.port_base, i), "/metrics")
        m = re.search(r"^core_resumable_sessions_closed_total (\S+)$", body, re.M)
        if m:
            total += float(m.group(1))
            found = True
    return total if found else None


if __name__ == "__main__":
    sys.exit(main())
