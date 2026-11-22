#!/usr/bin/env python3
"""Process tests of oe-marketdata: exit codes, both media driver modes, the admin endpoints, the public WebSocket.

Each case starts the real binary with a clean environment (nothing inherited but PATH) and no ME cluster: the process
shell must start, serve /health, /ready, /ready/book, /ready/session and /metrics on the admin bind only, answer on the
public /ws with no credential, stop cleanly on SIGTERM and exit with its documented code on every failure it owns.

usage: md_process_test.py --bin-dir <build bin dir> --work <dir> <case> [<case> ...]
"""
import argparse
import base64
import json
import os
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
REQUIRED = ["MD_DRIVER_MODE", "MD_INSTANCE_ID", "MD_ME_CLUSTER_ADDRESSES", "MD_ME_PORT_BASE", "CLUSTER_PORT_STRIDE",
            "MD_ME_EGRESS_ENDPOINT", "ME_AUTH_CREDENTIAL", "MD_ME_JOURNAL_ARCHIVES", "MD_JOURNAL_CONTROL_STREAM",
            "MD_JOURNAL_STREAM", "MD_JOURNAL_CHANNEL", "MD_REPLAY_STREAM", "MD_JOURNAL_MTU", "MD_LOCAL_HOST",
            "MD_PUBLIC_BIND", "MD_ADMIN_BIND", "MD_PG_URL", "MD_PG_USER", "MD_PG_PASSWORD_FILE", "MD_CORS_ORIGINS",
            "MD_EXPECTED_MARKETS"]
# no ME behind it: no session, so no book; no archive and no market history database: the trade path waits for both
NOT_READY = ["me_session", "book_fresh", "journal_attached", "journal_fresh", "history_writable"]


class Failure(Exception):
    pass


def check(cond, what):
    if not cond:
        raise Failure(what)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Ctx:
    def __init__(self, bin_dir, work):
        self.bin_dir = bin_dir
        self.work = work
        self.procs = []

    def binary(self, name):
        path = os.path.join(self.bin_dir, name)
        check(os.access(path, os.X_OK), f"{path} is not built")
        return path

    def env(self, mode, driver_dir=None, aeron_dir=None, extra=None):
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        env.update({
            "MD_DRIVER_MODE": mode,
            "MD_DRIVER_TIMEOUT_MS": "3000",
            "MD_INSTANCE_ID": "1",
            "MD_ME_CLUSTER_ADDRESSES": "127.0.0.1",
            "MD_ME_PORT_BASE": str(free_port()),
            "CLUSTER_PORT_STRIDE": "0",
            "MD_ME_EGRESS_ENDPOINT": "127.0.0.1:0",
            "ME_AUTH_CREDENTIAL": f"7:GATEWAY:{SECRET}",
            "MD_ME_JOURNAL_ARCHIVES": "127.0.0.1:1",
            "MD_JOURNAL_CONTROL_STREAM": "10",
            "MD_JOURNAL_STREAM": "4100",
            "MD_JOURNAL_CHANNEL": "aeron:ipc",
            "MD_REPLAY_STREAM": "4300",
            "MD_JOURNAL_MTU": "8192",
            "MD_LOCAL_HOST": "127.0.0.1",
            "MD_PUBLIC_BIND": f"127.0.0.1:{free_port()}",
            "MD_ADMIN_BIND": f"127.0.0.1:{free_port()}",
            "MD_PG_URL": "postgresql://127.0.0.1:1/oe_marketdata",
            "MD_PG_USER": "oe_marketdata_writer",
            "MD_PG_PASSWORD_FILE": os.path.join(self.work, "no-password-needed-yet"),
            "MD_CORS_ORIGINS": "none",
            "MD_EXPECTED_MARKETS": "1,2,3",
            "MD_DUTY_WATCHDOG_MS": "2000",
            "MD_ARCHIVE_TIMEOUT_MS": "1000",
        })
        if mode == "embedded":
            env["MD_DRIVER_DIR"] = driver_dir
        else:
            env["AERON_DIR"] = aeron_dir
        env.update(extra or {})
        return env

    def start(self, args, env, log_name):
        log = open(os.path.join(self.work, log_name), "w+")
        proc = subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        proc.log = log
        proc.log_path = log.name
        self.procs.append(proc)
        return proc

    def cleanup(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            proc.log.close()


def text_of(proc):
    with open(proc.log_path) as f:
        return f.read()


def wait_for_line(proc, needle, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if needle in text_of(proc):
            return True
        if proc.poll() is not None:
            return needle in text_of(proc)
        time.sleep(0.02)
    return False


def wait_exit(proc, timeout):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def http(port, path, method="GET"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def port_of(env, key):
    return int(env[key].rsplit(":", 1)[1])


def tmpfs_parent(ctx, name):
    base = "/dev/shm" if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK) else ctx.work
    return tempfile.mkdtemp(prefix=f"md-test-{name}-", dir=base)


def start_test_driver(ctx, name):
    parent = tmpfs_parent(ctx, name)
    driver_dir = os.path.join(parent, "aeron")
    proc = ctx.start([ctx.binary("gw_test_media_driver"), driver_dir], {"PATH": os.environ.get("PATH", "")},
                     f"{name}-driver.log")
    check(wait_for_line(proc, "driver ready", 15), f"the test media driver did not start: {text_of(proc)}")
    return proc, driver_dir, parent


# ---- a minimal WebSocket client (RFC 6455, text frames, client frames masked), no credential of any kind

class Ws:
    def __init__(self, port, path="/ws"):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n"
                           f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            head += chunk
        self.status_line = head.split(b"\r\n", 1)[0].decode()
        self.buffer = head.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in head else b""

    def send(self, obj):
        payload = json.dumps(obj).encode()
        mask = os.urandom(4)
        header = bytes([0x81])
        n = len(payload)
        if n < 126:
            header += bytes([0x80 | n])
        else:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        self.sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def _read(self, n):
        while len(self.buffer) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise Failure("the WebSocket closed")
            self.buffer += chunk
        out, self.buffer = self.buffer[:n], self.buffer[n:]
        return out

    def recv(self):
        while True:
            b0, b1 = self._read(2)
            n = b1 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._read(8))[0]
            payload = self._read(n)
            op = b0 & 0x0F
            if op == 0x1:
                return json.loads(payload.decode())
            if op == 0x8:
                raise Failure("the WebSocket closed")
            # ping (0x9) and pong: skip

    def close(self):
        self.sock.close()


def expect_serving(ctx, proc, env, mode):
    check(wait_for_line(proc, "Aeron client connected", 20), f"oe-marketdata did not connect to its driver:\n{text_of(proc)}")
    admin = port_of(env, "MD_ADMIN_BIND")
    end = time.time() + 5
    while time.time() < end and http(admin, "/health")[0] != 200:
        time.sleep(0.05)
    status, body = http(admin, "/health")
    check(status == 200, f"/health {status}: {body}")
    health = json.loads(body)
    check(health["status"] == "UP" and health["process"] == "oe-marketdata", f"/health {health['status']}")
    check(health["driver"]["mode"] == mode, f"/health driver mode {health['driver']['mode']}")
    check(health["ready"] is False, "/health says ready without an ME session")
    check(health["expectedMarkets"] == [1, 2, 3], f"/health expectedMarkets {health.get('expectedMarkets')}")
    check(set(health["books"]) == {"1", "2", "3"} and health["books"]["1"]["state"] == "no_snapshot",
          f"/health books {health.get('books')}")
    settings = health["settings"]
    check(settings["MD_WS_QUEUE_BYTES"] == {"value": "524288", "source": "default"}, "a default is not shown in /health")
    check(SECRET not in body, "/health leaks the credential secret")
    status, body = http(admin, "/ready")
    check(status == 503 and json.loads(body) == {"ready": False, "failing": NOT_READY}, f"/ready {status} {body}")
    status, body = http(admin, "/ready/book")
    check(status == 503 and json.loads(body)["failing"] == ["me_session", "book_fresh"], f"/ready/book {status} {body}")
    status, body = http(admin, "/ready/session")
    check(status == 503 and json.loads(body)["failing"] == ["me_session"], f"/ready/session {status} {body}")
    status, body = http(admin, "/metrics")
    check(status == 200 and "\nmd_ready 0\n" in body and "\nmd_up 1\n" in body and
          "md_foreign_market_frames_total 0" in body and 'md_ws_frames_sent_total{type="BOOK_SNAPSHOT"}' in body,
          f"/metrics {status}")
    status, _ = http(admin, "/health", "POST")
    check(status == 405, f"POST /health {status}")
    # the public port: no admin endpoint, /ws with no credential
    public = port_of(env, "MD_PUBLIC_BIND")
    status, _ = http(public, "/health")
    check(status == 404, f"the admin endpoints must not be on the public listener: /health {status}")
    ws = Ws(public)
    check(" 101 " in ws.status_line, f"public /ws upgrade: {ws.status_line}")
    ws.send({"action": "ping"})
    pong = ws.recv()
    check(pong["type"] == "PONG" and isinstance(pong["timestamp"], int), f"ping: {pong}")
    ws.send({"action": "subscribe", "marketId": 2})
    frames = [ws.recv(), ws.recv(), ws.recv()]
    check(frames[0] == {"type": "SUBSCRIPTION_CONFIRMED", "status": "subscribed", "marketId": 2}, f"subscribe: {frames}")
    check(frames[1]["type"] == "BOOK_SNAPSHOT" and frames[1]["stale"] is True and frames[1]["bids"] == [],
          f"no ME: the book is stale and empty: {frames[1]}")
    check(frames[2]["type"] == "ERROR" and frames[2]["code"] == "HISTORY_UNAVAILABLE", f"history: {frames[2]}")
    ws.send({"action": "subscribe", "marketId": 4})
    err = ws.recv()
    check(err["type"] == "ERROR" and err["code"] == "UNKNOWN_MARKET", f"market 4 outside the set: {err}")
    ws.send({"action": "unsubscribe", "marketId": 2})
    err = ws.recv()
    check(err["type"] == "ERROR" and err["code"] == "BAD_REQUEST", f"unknown action: {err}")
    ws.close()
    other = Ws(public, "/ws/v2")
    check(" 404 " in other.status_line, f"a path other than /ws: {other.status_line}")
    other.close()
    _, body = http(admin, "/metrics")
    check('md_ws_frames_sent_total{type="SUBSCRIPTION_CONFIRMED"} 1' in body, "frames sent not counted")
    # idle past the driver timeout: the liveness checks must not read an idle process as dead
    time.sleep(4.5)
    check(proc.poll() is None, f"oe-marketdata exited {proc.returncode} while idle:\n{text_of(proc)}")
    names = []
    for tid in os.listdir(f"/proc/{proc.pid}/task"):
        with open(f"/proc/{proc.pid}/task/{tid}/comm") as f:
            names.append(f.read().strip())
    for name in ("md-duty", "md-admin", "md-io-0", "md-io-1"):
        check(name in names, f"thread {name} missing: {names}")


def stop_clean(proc, what, timeout=10):
    proc.send_signal(signal.SIGTERM)
    code = wait_exit(proc, timeout)
    check(code == 0, f"{what}: SIGTERM exit {code}, expected 0:\n{text_of(proc)}")
    check("stopped (exit 0)" in text_of(proc), f"{what}: no clean stop line")


# ---- cases

def case_config_refused(ctx):
    proc = ctx.start([ctx.binary("oe-marketdata")], {"PATH": os.environ.get("PATH", "")}, "config-refused.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78, f"exit {code}, expected 78:\n{out}")
    for key in REQUIRED:
        check(f"missing setting: {key}" in out, f"{key} not named in one pass:\n{out}")
    check("admin listening" not in out, "the admin listener bound before the configuration was accepted")


def case_config_bad_values(ctx):
    parent = tmpfs_parent(ctx, "bad")
    extra = {"MD_EXPECTED_MARKETS": "1,6", "ME_AUTH_CREDENTIAL": f"7:FORWARDER:{SECRET}", "MD_DRIVER_TIMEOUT_MS": "2000",
             "MD_PUBLIC_BIND": "127.0.0.1:0", "MD_TYPO_SETTING": "1"}
    needles = ["invalid setting: MD_EXPECTED_MARKETS", "ME credential class must be GATEWAY",
               "invalid setting: MD_DRIVER_TIMEOUT_MS", "invalid setting: MD_PUBLIC_BIND",
               "invalid setting: MD_TYPO_SETTING"]
    proc = ctx.start([ctx.binary("oe-marketdata")], ctx.env("embedded", driver_dir=parent, extra=extra), "config-bad.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78, f"exit {code}, expected 78:\n{out}")
    for needle in needles:
        check(needle in out, f"missing '{needle}':\n{out}")
    check(f"refusing to start: {len(needles)} configuration problems (exit 78)" in out, f"refusal line:\n{out}")
    shutil.rmtree(parent, ignore_errors=True)


def case_embedded_start_stop(ctx):
    parent = tmpfs_parent(ctx, "emb")
    env = ctx.env("embedded", driver_dir=parent)
    proc = ctx.start([ctx.binary("oe-marketdata")], env, "embedded.log")
    expect_serving(ctx, proc, env, "embedded")
    check(os.path.isdir(os.path.join(parent, "oe-marketdata-1")), "the embedded driver dir is not <dir>/oe-marketdata-1")
    stop_clean(proc, "embedded")
    shutil.rmtree(parent, ignore_errors=True)


def case_external_start_stop(ctx):
    driver, driver_dir, parent = start_test_driver(ctx, "ext")
    env = ctx.env("external", aeron_dir=driver_dir)
    proc = ctx.start([ctx.binary("oe-marketdata")], env, "external.log")
    expect_serving(ctx, proc, env, "external")
    stop_clean(proc, "external")
    driver.send_signal(signal.SIGTERM)
    wait_exit(driver, 10)
    shutil.rmtree(parent, ignore_errors=True)


def case_external_without_driver(ctx):
    parent = tmpfs_parent(ctx, "nodrv")
    env = ctx.env("external", aeron_dir=os.path.join(parent, "no-driver-here"))
    proc = ctx.start([ctx.binary("oe-marketdata")], env, "external-no-driver.log")
    code = wait_exit(proc, 20)
    out = text_of(proc)
    check(code == 70, f"exit {code} with no driver, expected 70:\n{out}")
    check("no media driver answered" in out, f"no driver line:\n{out}")
    shutil.rmtree(parent, ignore_errors=True)


def case_external_driver_killed(ctx):
    driver, driver_dir, parent = start_test_driver(ctx, "kill")
    env = ctx.env("external", aeron_dir=driver_dir)
    proc = ctx.start([ctx.binary("oe-marketdata")], env, "external-killed.log")
    check(wait_for_line(proc, "Aeron client connected", 20), f"did not connect:\n{text_of(proc)}")
    driver.kill()
    driver.wait()
    code = wait_exit(proc, 20)
    check(code == 70, f"exit {code} after the driver was killed, expected 70:\n{text_of(proc)}")
    shutil.rmtree(parent, ignore_errors=True)


CASES = {
    "config_refused": case_config_refused,
    "config_bad_values": case_config_bad_values,
    "embedded_start_stop": case_embedded_start_stop,
    "external_start_stop": case_external_start_stop,
    "external_without_driver": case_external_without_driver,
    "external_driver_killed": case_external_driver_killed,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("cases", nargs="+")
    args = parser.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    failed = 0
    for name in args.cases:
        ctx = Ctx(args.bin_dir, args.work)
        try:
            CASES[name](ctx)
            print(f"PASS {name}")
        except Failure as e:
            print(f"FAIL {name}: {e}")
            failed += 1
        finally:
            ctx.cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
