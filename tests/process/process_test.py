#!/usr/bin/env python3
"""Process tests of oe-gateway and oe-stream: exit codes, both media driver modes, the admin endpoints.

Each case starts the real binaries with a clean environment (nothing inherited but PATH) and no core cluster: the
process shell must start, serve /health, /ready (not ready, every failing condition named), /ready/session and
/metrics on the admin bind, stop cleanly on SIGTERM, and exit with the code of 2.10 on every failure it owns.

usage: process_test.py --bin-dir <build dir> --work <dir> <case> [<case> ...]
       process_test.py --bin-dir <build dir> --work <dir> footprint     (prints RSS, threads and driver dir size)
"""
import argparse
import json
import os
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
API_KEY = "test-process-key-0001"  # an obviously fake fixture key, written to the work dir per run
GATEWAY_REQUIRED = ["GATEWAY_DRIVER_MODE", "GATEWAY_CORE_INGRESS", "GATEWAY_CORE_INGRESS_CHANNEL",
                    "GATEWAY_CORE_RESPONSE_CHANNEL", "CORE_AUTH_CREDENTIAL", "GATEWAY_INSTANCE_ID", "GATEWAY_HTTP_BIND",
                    "GATEWAY_ADMIN_BIND", "GATEWAY_AUTH_MODES"]
STREAM_REQUIRED = ["STREAM_DRIVER_MODE", "STREAM_CORE_INGRESS", "STREAM_CORE_INGRESS_CHANNEL",
                   "STREAM_CORE_RESPONSE_CHANNEL", "CORE_AUTH_CREDENTIAL", "STREAM_INSTANCE_ID", "STREAM_WS_BIND",
                   "STREAM_ADMIN_BIND", "STREAM_AUTH_MODES"]
GATEWAY_NOT_READY = ["session_connected", "session_registered", "config_loaded", "gate_open", "keepalive_fresh"]
STREAM_NOT_READY = ["session_registered", "head_probe_fresh", "ws_listener"]


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
        self.api_keys = os.path.join(work, "api-keys")
        with open(self.api_keys, "w") as f:
            f.write(f"# test fixture\n{API_KEY}:1001\n")

    def binary(self, name):
        path = os.path.join(self.bin_dir, name)
        check(os.access(path, os.X_OK), f"{path} is not built")
        return path

    def env(self, role, mode, instance=1, driver_dir=None, aeron_dir=None, extra=None):
        p = "GATEWAY" if role == "gateway" else "STREAM"
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        env.update({
            f"{p}_DRIVER_MODE": mode,
            f"{p}_DRIVER_TIMEOUT_MS": "3000",
            f"{p}_CORE_INGRESS": "0=127.0.0.1:1",
            f"{p}_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=64k|mtu=8192",
            f"{p}_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=64k|mtu=8192",
            "CORE_AUTH_CREDENTIAL": f"7:GATEWAY:{SECRET}",
            f"{p}_INSTANCE_ID": str(instance),
            ("GATEWAY_HTTP_BIND" if role == "gateway" else "STREAM_WS_BIND"): f"127.0.0.1:{free_port()}",
            f"{p}_ADMIN_BIND": f"127.0.0.1:{free_port()}",
            f"{p}_AUTH_MODES": "api-key",
            "GATEWAY_API_KEYS_FILE": self.api_keys,
            f"{p}_DUTY_WATCHDOG_MS": "2000",
        })
        if mode == "embedded":
            env[f"{p}_DRIVER_DIR"] = driver_dir
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


def http(port, path, method="GET", headers=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def admin_port(env):
    for key in ("GATEWAY_ADMIN_BIND", "STREAM_ADMIN_BIND"):
        if key in env:
            return int(env[key].rsplit(":", 1)[1])
    raise Failure("no admin bind")


def thread_names(pid):
    names = []
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{tid}/comm") as f:
                names.append(f.read().strip())
        except OSError:
            pass
    return names


def tmpfs_parent(ctx, name):
    base = "/dev/shm" if os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK) else ctx.work
    path = tempfile.mkdtemp(prefix=f"gw-test-{name}-", dir=base)
    return path


def start_test_driver(ctx, name):
    parent = tmpfs_parent(ctx, name)
    driver_dir = os.path.join(parent, "aeron")
    proc = ctx.start([ctx.binary("gw_test_media_driver"), driver_dir], {"PATH": os.environ.get("PATH", "")},
                     f"{name}-driver.log")
    check(wait_for_line(proc, "driver ready", 15), f"the test media driver did not start: {text_of(proc)}")
    return proc, driver_dir, parent


def expect_serving(ctx, role, proc, env, mode):
    prefix = "gw" if role == "gateway" else "st"
    process = "oe-gateway" if role == "gateway" else "oe-stream"
    check(wait_for_line(proc, "Aeron client connected", 20), f"{process} did not connect to its driver:\n{text_of(proc)}")
    port = admin_port(env)
    status, body = http(port, "/health")
    check(status == 200, f"/health {status}: {body}")
    health = json.loads(body)
    check(health["status"] == "UP", f"/health status {health['status']}")
    check(health["process"] == process, f"/health process {health['process']}")
    check(health["driver"]["mode"] == mode, f"/health driver mode {health['driver']['mode']}")
    check(health["ready"] is False, "/health says ready without a session")
    settings = health["settings"]
    p = "GATEWAY" if role == "gateway" else "STREAM"
    check(settings[f"{p}_DRIVER_MODE"] == {"value": mode, "source": "set"}, f"settings: {settings.get(f'{p}_DRIVER_MODE')}")
    check(settings[f"{p}_IO_THREADS"] == {"value": "2", "source": "default"}, "a default is not shown in /health")
    check(SECRET not in body, "/health leaks the credential secret")
    status, body = http(port, "/ready")
    check(status == 503, f"/ready {status}: {body}")
    want = GATEWAY_NOT_READY if role == "gateway" else STREAM_NOT_READY
    check(json.loads(body) == {"ready": False, "failing": want}, f"/ready body {body}")
    status, body = http(port, "/ready/session")
    if role == "gateway":
        check(status == 503 and json.loads(body)["failing"] == ["session_connected", "session_registered"],
              f"/ready/session {status} {body}")
    else:
        check(status == 404, f"oe-stream /ready/session {status}")
    status, body = http(port, "/metrics")
    metric = "gateway" if role == "gateway" else "stream"
    check(status == 200 and f"\n{metric}_ready 0\n" in body and f"\n{metric}_up 1\n" in body, f"/metrics {status}")
    status, _ = http(port, "/health", "POST")
    check(status == 405, f"POST /health {status}")
    names = thread_names(proc.pid)
    expected = [f"{prefix}-duty", f"{prefix}-admin"]
    if role == "gateway":
        # the public listener serves while the session is not connected: auth needs no core
        public = int(env["GATEWAY_HTTP_BIND"].rsplit(":", 1)[1])
        status, body = http(public, "/api/v2/auth/me", headers={"X-API-Key": API_KEY})
        check(status == 200 and json.loads(body) == {"userId": "1001", "username": None}, f"/api/v2/auth/me {status} {body}")
        status, body = http(public, "/api/v2/auth/me")
        check(status == 401 and json.loads(body)["code"] == "UNAUTHORIZED", f"/api/v2/auth/me without a key {status}")
        status, body = http(public, "/health")
        check(status == 404, f"the admin endpoints must not be on the public listener: /health {status}")
        expected += ["gw-io-0", "gw-io-1", "gw-done"]
    for name in expected:
        check(name in names, f"thread {name} missing: {names}")
    # idle past the driver timeout (3 s here): the liveness checks must not read an idle process as dead
    time.sleep(4.5)
    check(proc.poll() is None, f"{process} exited {proc.returncode} while idle:\n{text_of(proc)}")
    status, body = http(port, "/health")
    check(status == 200, f"/health after idling past the driver timeout: {status} {body}")


def stop_clean(proc, what, timeout=10):
    proc.send_signal(signal.SIGTERM)
    code = wait_exit(proc, timeout)
    check(code == 0, f"{what}: SIGTERM exit {code}, expected 0:\n{text_of(proc)}")
    check("stopped (exit 0)" in text_of(proc), f"{what}: no clean stop line")


# ---- cases

def case_config_refused(ctx, role):
    binary = ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")
    proc = ctx.start([binary], {"PATH": os.environ.get("PATH", "")}, f"config-refused-{role}.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78, f"exit {code}, expected 78:\n{out}")
    required = GATEWAY_REQUIRED if role == "gateway" else STREAM_REQUIRED
    for key in required:
        check(f"{key} is required" in out, f"{key} not named in one pass:\n{out}")
    check(f"refusing to start: {len(required)} configuration problems (exit 78)" in out, f"refusal line:\n{out}")


def case_config_bad_values(ctx, role):
    p = "GATEWAY" if role == "gateway" else "STREAM"
    parent = tmpfs_parent(ctx, f"bad-{role}")
    extra = {
        f"{p}_INSTANCE_ID": "gw-1",
        f"{p}_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=4m",
        f"{p}_IDLE": "spin",
        f"{p}_TYPO_SETTING": "1",
    }
    needles = [f"{p}_INSTANCE_ID must be a positive integer", f"{p}_CORE_INGRESS_CHANNEL must name term-length and mtu",
               f"{p}_IDLE must be exactly backoff or busy", f"{p}_TYPO_SETTING is not a setting"]
    if role == "gateway":
        # a value only the session's own check refuses (above the client's 1 h bound) is listed in the same pass
        extra["GATEWAY_REQUEST_TIMEOUT_MS"] = "4000000"
        needles.append("GATEWAY_REQUEST_TIMEOUT_MS: session: requestTimeoutMs must be in 1..3600000")
    env = ctx.env(role, "embedded", driver_dir=parent, extra=extra)
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"config-bad-{role}.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78, f"exit {code}, expected 78:\n{out}")
    for needle in needles:
        check(needle in out, f"missing '{needle}':\n{out}")
    check(f"refusing to start: {len(needles)} configuration problems (exit 78)" in out, f"refusal line:\n{out}")
    check("admin listening" not in out, "the admin listener bound before the configuration was accepted")
    shutil.rmtree(parent, ignore_errors=True)


def case_config_auth_files(ctx, role):
    """The credential files of the enabled modes are read at boot, in the same pass as the settings: 78, all named."""
    p = "GATEWAY" if role == "gateway" else "STREAM"
    parent = tmpfs_parent(ctx, f"authfiles-{role}")
    short_secret = os.path.join(ctx.work, "jwt-secret-short")
    with open(short_secret, "w") as f:
        f.write("too-short-test-secret\n")
    missing_keys = os.path.join(ctx.work, "no-such-api-keys")
    extra = {f"{p}_AUTH_MODES": "api-key,jwt", "GATEWAY_API_KEYS_FILE": missing_keys,
             "GATEWAY_JWT_SECRET_FILE": short_secret, f"{p}_IDLE": "spin"}
    needles = [f"GATEWAY_API_KEYS_FILE: cannot read {missing_keys}", "needs at least 32",
               f"{p}_IDLE must be exactly backoff or busy"]
    env = ctx.env(role, "embedded", driver_dir=parent, extra=extra)
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"auth-files-{role}.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78, f"exit {code}, expected 78:\n{out}")
    for needle in needles:
        check(needle in out, f"missing '{needle}':\n{out}")
    check(f"refusing to start: {len(needles)} configuration problems (exit 78)" in out, f"refusal line:\n{out}")
    check("too-short-test-secret" not in out, "a secret file's content reached the log")
    check("admin listening" not in out, "the admin listener bound before the configuration was accepted")
    # each mode alone: the jwt secret file missing is 78 too
    env = ctx.env(role, "embedded", driver_dir=parent, extra={f"{p}_AUTH_MODES": "jwt",
                                                               "GATEWAY_JWT_SECRET_FILE": missing_keys + "-jwt"})
    env.pop("GATEWAY_API_KEYS_FILE")
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"auth-files-jwt-{role}.log")
    code = wait_exit(proc, 10)
    out = text_of(proc)
    check(code == 78 and f"GATEWAY_JWT_SECRET_FILE: cannot read {missing_keys}-jwt" in out, f"exit {code}:\n{out}")
    shutil.rmtree(parent, ignore_errors=True)


def case_public_bind_in_use(ctx, role):
    parent = tmpfs_parent(ctx, f"pubbind-{role}")
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    env = ctx.env(role, "embedded", driver_dir=parent, extra={"GATEWAY_HTTP_BIND": f"127.0.0.1:{port}"})
    proc = ctx.start([ctx.binary("oe-gateway")], env, f"public-bind-in-use-{role}.log")
    code = wait_exit(proc, 10)
    blocker.close()
    out = text_of(proc)
    check(code == 78, f"exit {code} with the public port taken, expected 78:\n{out}")
    check("GATEWAY_HTTP_BIND" in out, f"the bind failure does not name the setting:\n{out}")
    check("embedded media driver at" not in out, "the driver started before the public bind failed")
    shutil.rmtree(parent, ignore_errors=True)


def case_embedded_start_stop(ctx, role):
    parent = tmpfs_parent(ctx, f"emb-{role}")
    env = ctx.env(role, "embedded", instance=11, driver_dir=parent)
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"embedded-{role}.log")
    expect_serving(ctx, role, proc, env, "embedded")
    driver_dir = os.path.join(parent, ("oe-gateway-" if role == "gateway" else "oe-stream-") + "11")
    check(os.path.isfile(os.path.join(driver_dir, "cnc.dat")), f"no embedded driver at {driver_dir}")
    stop_clean(proc, f"{role} embedded")
    check(not os.path.exists(driver_dir), f"the embedded driver dir {driver_dir} was left behind")
    shutil.rmtree(parent, ignore_errors=True)


def case_external_start_stop(ctx, role):
    driver, driver_dir, parent = start_test_driver(ctx, f"ext-{role}")
    env = ctx.env(role, "external", aeron_dir=driver_dir)
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"external-{role}.log")
    expect_serving(ctx, role, proc, env, "external")
    stop_clean(proc, f"{role} external")
    check(driver.poll() is None, "the host media driver died with the client")
    check(os.path.isfile(os.path.join(driver_dir, "cnc.dat")), "the host media driver dir was removed by the client")
    driver.send_signal(signal.SIGTERM)
    check(wait_exit(driver, 10) == 0, "the test media driver did not stop")
    shutil.rmtree(parent, ignore_errors=True)


def expect_starting(proc, env, role):
    """During the driver wait the admin endpoints answer at once: 503, driver_connected failing, nothing hangs."""
    port = admin_port(env)
    check(wait_for_line(proc, "admin listening", 5), f"the admin listener did not bind:\n{text_of(proc)}")
    asked = time.monotonic()
    status, body = http(port, "/health")
    check(time.monotonic() - asked < 1.0, "/health took over 1 s during the driver wait")
    check(status == 503 and json.loads(body)["status"] == "STARTING", f"/health during the driver wait: {status} {body}")
    check(json.loads(body)["driver"]["connected"] is False, f"/health driver.connected: {body}")
    status, body = http(port, "/ready")
    check(status == 503 and json.loads(body)["failing"][0] == "driver_connected",
          f"/ready during the driver wait: {status} {body}")
    status, body = http(port, "/metrics")
    metric = "gateway" if role == "gateway" else "stream"
    check(status == 200 and f"\n{metric}_driver_connected 0\n" in body, f"/metrics during the driver wait: {status}")


def external_no_driver(ctx, role, name, aeron_dir):
    env = ctx.env(role, "external", aeron_dir=aeron_dir)
    start = time.monotonic()
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"{name}-{role}.log")
    expect_starting(proc, env, role)
    code = wait_exit(proc, 20)
    elapsed = time.monotonic() - start
    out = text_of(proc)
    check(code == 70, f"exit {code}, expected 70:\n{out}")
    check("no media driver answered" in out, f"no named failure:\n{out}")
    check(elapsed < 3.0 + 1.5, f"took {elapsed:.1f} s for a 3 s driver timeout")


def case_external_without_driver(ctx, role):
    parent = tmpfs_parent(ctx, f"nodrv-{role}")
    external_no_driver(ctx, role, "no-driver", parent)
    shutil.rmtree(parent, ignore_errors=True)


def case_external_aeron_dir_missing(ctx, role):
    # after a reboot, or before aeronmd starts, AERON_DIR does not exist: no driver (70, systemd restarts), not 78
    parent = tmpfs_parent(ctx, f"nodir-{role}")
    missing = os.path.join(parent, "aeron-not-yet")
    external_no_driver(ctx, role, "aeron-dir-missing", missing)
    check(not os.path.exists(missing), "the client created the missing AERON_DIR")
    shutil.rmtree(parent, ignore_errors=True)


def case_external_driver_killed(ctx, role):
    driver, driver_dir, parent = start_test_driver(ctx, f"kill-{role}")
    env = ctx.env(role, "external", aeron_dir=driver_dir)
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"driver-killed-{role}.log")
    check(wait_for_line(proc, "Aeron client connected", 20), f"did not connect:\n{text_of(proc)}")
    check(http(admin_port(env), "/health")[0] == 200, "/health before the kill")
    killed = time.monotonic()
    driver.send_signal(signal.SIGKILL)
    code = wait_exit(proc, 20)
    elapsed = time.monotonic() - killed
    check(code == 70, f"exit {code} after the driver was killed, expected 70:\n{text_of(proc)}")
    check(elapsed < 3.0 + 1.5, f"took {elapsed:.1f} s to notice a dead driver with a 3 s timeout")
    shutil.rmtree(parent, ignore_errors=True)


def case_embedded_same_instance_refused(ctx, role):
    parent = tmpfs_parent(ctx, f"dup-{role}")
    binary = ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")
    first_env = ctx.env(role, "embedded", instance=5, driver_dir=parent)
    first = ctx.start([binary], first_env, f"dup-first-{role}.log")
    check(wait_for_line(first, "Aeron client connected", 20), f"first instance did not start:\n{text_of(first)}")
    second = ctx.start([binary], ctx.env(role, "embedded", instance=5, driver_dir=parent), f"dup-second-{role}.log")
    code = wait_exit(second, 15)
    check(code == 78, f"a second instance with the same id exited {code}, expected 78:\n{text_of(second)}")
    check("embedded media driver" in text_of(second), f"no named refusal:\n{text_of(second)}")
    check(first.poll() is None and http(admin_port(first_env), "/health")[0] == 200, "the first instance was disturbed")
    stop_clean(first, "first instance")
    shutil.rmtree(parent, ignore_errors=True)


def case_admin_bind_in_use(ctx, role):
    parent = tmpfs_parent(ctx, f"bind-{role}")
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    p = "GATEWAY" if role == "gateway" else "STREAM"
    env = ctx.env(role, "embedded", driver_dir=parent, extra={f"{p}_ADMIN_BIND": f"127.0.0.1:{port}"})
    proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env, f"bind-in-use-{role}.log")
    code = wait_exit(proc, 10)
    blocker.close()
    check(code == 78, f"exit {code} with the admin port taken, expected 78:\n{text_of(proc)}")
    check(f"{p}_ADMIN_BIND" in text_of(proc), f"the bind failure does not name the setting:\n{text_of(proc)}")
    check("embedded media driver at" not in text_of(proc), "the driver started before the admin bind failed")
    shutil.rmtree(parent, ignore_errors=True)


def case_duty_stall(ctx, role):
    parent = tmpfs_parent(ctx, f"stall-{role}")
    env = ctx.env(role, "embedded", instance=21, driver_dir=parent)
    binary = ctx.binary("oe-gateway-tc-dutystall" if role == "gateway" else "oe-stream-tc-dutystall")
    proc = ctx.start([binary], env, f"duty-stall-{role}.log")
    check(wait_for_line(proc, "Aeron client connected", 20), f"did not start:\n{text_of(proc)}")
    port = admin_port(env)
    check(http(port, "/health")[0] == 200, "/health before the stall")
    time.sleep(0.3)
    stalled_at = time.monotonic()
    proc.send_signal(signal.SIGUSR2)
    seen_503 = False
    while proc.poll() is None and time.monotonic() - stalled_at < 15:
        try:
            status, body = http(port, "/health")
            if status == 503 and '"status":"STALLED"' in body:
                seen_503 = True
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.005)
    code = wait_exit(proc, 5)
    elapsed = time.monotonic() - stalled_at
    out = text_of(proc)
    check(code == 70, f"exit {code} after the duty thread halted, expected 70:\n{out}")
    check("duty heartbeat did not advance" in out, f"no watchdog line:\n{out}")
    check(elapsed >= 2.0, f"exited {elapsed:.2f} s after the stall, before the 2 s watchdog")
    check(seen_503, "/health never read 503 STALLED between the stall and the exit")
    shutil.rmtree(parent, ignore_errors=True)


def footprint(ctx):
    """RSS, threads and driver dir size at idle, per binary and mode (reported, not asserted)."""
    rows = []
    for role in ("gateway", "stream"):
        for mode in ("embedded", "external"):
            driver = None
            parent = None
            if mode == "embedded":
                parent = tmpfs_parent(ctx, f"fp-{role}")
                env = ctx.env(role, "embedded", instance=31, driver_dir=parent)
            else:
                driver, driver_dir, parent = start_test_driver(ctx, f"fp-{role}")
                env = ctx.env(role, "external", aeron_dir=driver_dir)
            proc = ctx.start([ctx.binary("oe-gateway" if role == "gateway" else "oe-stream")], env,
                             f"footprint-{role}-{mode}.log")
            check(wait_for_line(proc, "Aeron client connected", 20), f"did not start:\n{text_of(proc)}")
            time.sleep(5)
            status = {}
            with open(f"/proc/{proc.pid}/status") as f:
                for line in f:
                    key, _, value = line.partition(":")
                    status[key] = value.strip()
            du = subprocess.run(["du", "-sk", parent], capture_output=True, text=True).stdout.split()[0]
            cpu = open(f"/proc/{proc.pid}/stat").read().split()
            ticks = os.sysconf("SC_CLK_TCK")
            rows.append({
                "binary": "oe-gateway" if role == "gateway" else "oe-stream",
                "mode": mode,
                "rss_kib": int(status["VmRSS"].split()[0]),
                "threads": int(status["Threads"]),
                "thread_names": sorted(thread_names(proc.pid)),
                "driver_dir_kib": int(du),
                "cpu_seconds_after_5s_idle": (int(cpu[13]) + int(cpu[14])) / ticks,
            })
            stop_clean(proc, f"footprint {role} {mode}")
            if driver is not None:
                driver.send_signal(signal.SIGTERM)
                wait_exit(driver, 10)
            shutil.rmtree(parent, ignore_errors=True)
    print(json.dumps(rows, indent=2))


CASES = {}
for _role in ("gateway", "stream"):
    for _name, _fn in (("config_refused", case_config_refused), ("config_bad_values", case_config_bad_values),
                       ("embedded_start_stop", case_embedded_start_stop),
                       ("external_start_stop", case_external_start_stop),
                       ("external_without_driver", case_external_without_driver),
                       ("external_aeron_dir_missing", case_external_aeron_dir_missing),
                       ("external_driver_killed", case_external_driver_killed),
                       ("embedded_same_instance_refused", case_embedded_same_instance_refused),
                       ("admin_bind_in_use", case_admin_bind_in_use), ("duty_stall", case_duty_stall),
                       ("config_auth_files", case_config_auth_files)):
        CASES[f"{_role}.{_name}"] = (lambda fn, role: (lambda ctx: fn(ctx, role)))(_fn, _role)
CASES["gateway.public_bind_in_use"] = lambda ctx: case_public_bind_in_use(ctx, "gateway")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("cases", nargs="+")
    args = parser.parse_args()
    os.makedirs(args.work, exist_ok=True)
    failures = 0
    for name in args.cases:
        ctx = Ctx(args.bin_dir, args.work)
        try:
            if name == "footprint":
                footprint(ctx)
            elif name in CASES:
                CASES[name](ctx)
            else:
                raise Failure(f"unknown case {name} (known: {', '.join(sorted(CASES))}, footprint)")
            print(f"PASS {name}")
        except Failure as e:
            print(f"FAIL {name}: {e}")
            failures += 1
        finally:
            ctx.cleanup()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
