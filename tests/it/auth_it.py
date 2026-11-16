#!/usr/bin/env python3
"""T1 rows of the auth slice through the real oe-gateway binary: a three-member core cluster (as session_it.py), a
throwaway PostgreSQL container (the users database, the image pinned by digest), and oe-gateway in demo mode with an
api-key file, its REST listener, its hash and store pools and its duty bridge. A heavy job: run it under the lock.

Checks:
  identity  the users database hands out a userId the core already knows (the core holds a deposit for it): register
        is 503 IDENTITY_STORE_INCONSISTENT, no user row, nothing cached, gateway_user_id_conflict_total 1; the next
        register takes the next id, unknown to the core, and is 201
  round trip  register, me, login (token rotated: the old one is 401), logout (then 401); the users table holds only
        the token's SHA-256
  modes  an api key and a demo token answer the same /auth/me shape; a credential in the URL is refused
  store down  with the container frozen, a cached token is still served and an uncached one is 503 AUTH_UNAVAILABLE
        (never 401); thawed, both work
  decode  /metrics shows no egress frame refused by the decoder

usage: auth_it.py --bin-dir <build>/bin --core-jar <core-cluster.jar> --probe-jar <CoreProbe.jar>
                  --image <postgres@sha256:...> --work <dir> [--port-base N]
"""
import argparse
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import session_it  # noqa: E402  (the core cluster, the probe and the check helpers)

API_KEY = "test-auth-it-key-0001"  # an obviously fake fixture key, written to the work dir per run


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(port, path, method="GET", body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    h = dict(headers or {})
    if data is not None:
        h["Content-Type"] = "application/json"
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


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def psql(name, statement):
    r = docker("exec", name, "psql", "-U", "gw", "-d", "gw", "-At", "-c", statement, check=False)
    return r.stdout.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--core-jar", required=True)
    ap.add_argument("--probe-jar", required=True)
    ap.add_argument("--image", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--port-base", type=int, default=28000)
    ap.add_argument("--mode", choices=["embedded", "external"], default="embedded")
    args = ap.parse_args()
    shutil.rmtree(args.work, ignore_errors=True)
    os.makedirs(args.work)
    base = args.port_base
    ingress = f"0=localhost:{base + 2},1=localhost:{base + 102},2=localhost:{base + 202}"
    print(f"auth_it mode={args.mode} work={args.work} ports={base}", flush=True)

    pg = f"gw-auth-it-{os.getpid()}-{secrets.token_hex(3)}"
    password = secrets.token_hex(16)
    cluster = session_it.Cluster(args, args.work)
    gateway = None
    driver = None
    try:
        docker("run", "-d", "--rm", "--name", pg, "-e", "POSTGRES_USER=gw", "-e", f"POSTGRES_PASSWORD={password}",
               "-e", "POSTGRES_DB=gw", "-p", "127.0.0.1::5432", args.image)
        for _ in range(200):
            if docker("exec", pg, "pg_isready", "-U", "gw", "-d", "gw", "-h", "127.0.0.1", check=False).returncode == 0:
                break
            time.sleep(0.3)
        pg_port = docker("port", pg, "5432/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1]

        os.makedirs(cluster.shm, exist_ok=True)
        if args.mode == "external":
            # a host driver of its own (the test media driver), as aeronmd would be in production
            driver_dir = os.path.join(cluster.shm, "external-driver")
            dlog = open(os.path.join(args.work, "driver.log"), "w")
            driver = subprocess.Popen([os.path.join(args.bin_dir, "gw_test_media_driver"), driver_dir],
                                      stdout=dlog, stderr=subprocess.STDOUT)
            for _ in range(100):
                if os.path.exists(os.path.join(driver_dir, "cnc.dat")):
                    break
                time.sleep(0.1)
        cluster.start()
        keys = os.path.join(args.work, "api-keys")
        with open(keys, "w") as f:
            f.write(f"{API_KEY}:4242\n")
        public, admin = free_port(), free_port()
        driver_parent = os.path.join(cluster.shm, "gw")
        os.makedirs(driver_parent, exist_ok=True)
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "GATEWAY_DRIVER_MODE": args.mode,
            "GATEWAY_CORE_INGRESS": ingress,
            "GATEWAY_CORE_INGRESS_CHANNEL": "aeron:udp?term-length=4m|mtu=8192",
            "GATEWAY_CORE_RESPONSE_CHANNEL": "aeron:udp?endpoint=127.0.0.1:0|term-length=4m|mtu=8192",
            "CORE_AUTH_CREDENTIAL": session_it.GATEWAY_KEY,
            "GATEWAY_INSTANCE_ID": "71",
            "GATEWAY_HTTP_BIND": f"127.0.0.1:{public}",
            "GATEWAY_ADMIN_BIND": f"127.0.0.1:{admin}",
            "GATEWAY_AUTH_MODES": "demo,api-key",
            "GATEWAY_API_KEYS_FILE": keys,
            "GATEWAY_PG_URL": f"postgresql://gw:{password}@127.0.0.1:{pg_port}/gw",
            "GATEWAY_PG_STATEMENT_TIMEOUT_MS": "1000",
            "GATEWAY_PBKDF2_ITERATIONS": "20000",
            "GATEWAY_TOKEN_CACHE_TTL_MS": "60000",
        }
        if args.mode == "external":
            env["AERON_DIR"] = driver_dir
        else:
            env["GATEWAY_DRIVER_DIR"] = driver_parent
        log = open(os.path.join(args.work, "oe-gateway.log"), "w")
        gateway = subprocess.Popen([os.path.join(args.bin_dir, "oe-gateway")], env=env, stdout=log,
                                   stderr=subprocess.STDOUT)

        def ready_session():
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{admin}/ready/session", timeout=3) as r:
                    return r.status == 200
            except (urllib.error.URLError, OSError):
                return False

        end = time.time() + 120
        while time.time() < end and not ready_session():
            time.sleep(0.5)
        if not session_it.check(ready_session(), "oe-gateway registered with the core (/ready/session 200)"):
            return 1
        rc, _ = session_it.probe(args, "core-config", version=1, behavior=1, genesis=7, tick=1, min=1,
                                 max=1_000_000_000_000_000, lot=1)
        rc2, _ = session_it.probe(args, "gate", market=0, state="OPEN", reason="ADMIN")
        # the core knows user 100000: the first id demo_user_seq hands out (a users database reset behind the core)
        rc3, _ = session_it.probe(args, "deposit", user=100000, asset=0, amount=10 ** 8, rh=9, rl=2)
        session_it.check(rc == 0 and rc2 == 0 and rc3 == 0, "core configured, gate open, user 100000 funded")

        # ---- identity
        status, body, raw = http(public, "/api/v2/auth/register", "POST", {"username": "alice", "password": "secret-1"})
        session_it.check(status == 503 and body and body["code"] == "IDENTITY_STORE_INCONSISTENT",
                         "register on an id the core knows: 503 IDENTITY_STORE_INCONSISTENT", f"{status} {raw}")
        session_it.check(psql(pg, "SELECT count(*) FROM users") == "0", "no user row written for the known id")
        with urllib.request.urlopen(f"http://127.0.0.1:{admin}/metrics", timeout=5) as r:
            metrics = r.read().decode()
        session_it.check("\ngateway_user_id_conflict_total 1\n" in metrics, "gateway_user_id_conflict_total 1")
        status, body, raw = http(public, "/api/v2/auth/register", "POST", {"username": "alice", "password": "secret-1"})
        session_it.check(status == 201 and body and body["userId"] == "100001" and body["funded"] is False,
                         "the next id is unknown to the core: 201 with userId 100001", f"{status} {raw}")
        token = body["token"] if body else ""

        # ---- round trip
        status, body, raw = http(public, "/api/v2/auth/me", headers={"Authorization": f"Bearer {token}"})
        session_it.check(status == 200 and body == {"userId": "100001", "username": "alice"}, "me with the demo token",
                         f"{status} {raw}")
        stored = psql(pg, "SELECT encode(token_hash, 'hex') FROM users WHERE user_id = 100001")
        import hashlib
        session_it.check(stored == hashlib.sha256(token.encode()).hexdigest(), "the users table holds SHA-256(token)",
                         stored)
        status, body, raw = http(public, "/api/v2/auth/login", "POST", {"username": "alice", "password": "secret-1"})
        token2 = body["token"] if status == 200 and body else ""
        session_it.check(status == 200 and token2 and token2 != token, "login rotates the token", f"{status} {raw}")
        status, _, raw = http(public, "/api/v2/auth/me", headers={"Authorization": f"Bearer {token}"})
        session_it.check(status == 401, "the rotated-out token is 401", f"{status} {raw}")

        # ---- modes and the URL
        status, body, raw = http(public, "/api/v2/auth/me", headers={"X-API-Key": API_KEY})
        session_it.check(status == 200 and body == {"userId": "4242", "username": None}, "me with an api key",
                         f"{status} {raw}")
        status, body, raw = http(public, f"/api/v2/auth/me?token={token2}")
        session_it.check(status == 400 and body and body["code"] == "VALIDATION", "a token in the URL is refused",
                         f"{status} {raw}")

        # ---- store down: the cached token is served, an uncached one is 503, never 401
        docker("pause", pg)
        try:
            status, body, raw = http(public, "/api/v2/auth/me", headers={"Authorization": f"Bearer {token2}"})
            session_it.check(status == 200, "PG frozen: the cached token is served", f"{status} {raw}")
            status, body, raw = http(public, "/api/v2/auth/me",
                                     headers={"Authorization": f"Bearer {secrets.token_urlsafe(32)}"})
            session_it.check(status == 503 and body and body["code"] == "AUTH_UNAVAILABLE",
                             "PG frozen: an uncached token is 503 AUTH_UNAVAILABLE", f"{status} {raw}")
            status, body, raw = http(public, "/api/v2/auth/login", "POST", {"username": "alice", "password": "secret-1"})
            session_it.check(status == 503 and body and body["code"] == "AUTH_UNAVAILABLE",
                             "PG frozen: login is 503 AUTH_UNAVAILABLE", f"{status} {raw}")
        finally:
            docker("unpause", pg)
        status, _, raw = http(public, "/api/v2/auth/logout", "POST", headers={"Authorization": f"Bearer {token2}"})
        session_it.check(status == 204, "logout after PG is back", f"{status} {raw}")
        status, _, raw = http(public, "/api/v2/auth/me", headers={"Authorization": f"Bearer {token2}"})
        session_it.check(status == 401, "the logged-out token is 401", f"{status} {raw}")

        with urllib.request.urlopen(f"http://127.0.0.1:{admin}/metrics", timeout=5) as r:
            metrics = r.read().decode()
        session_it.check("\ngateway_decode_refused_total 0\n" in metrics, "no egress frame refused by the decoder")
    finally:
        if gateway is not None:
            gateway.terminate()
            try:
                gateway.wait(timeout=15)
            except subprocess.TimeoutExpired:
                gateway.kill()
        cluster.stop()
        if driver is not None:
            driver.terminate()
            try:
                driver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                driver.kill()
        docker("unpause", pg, check=False)
        docker("rm", "-f", pg, check=False)
    print(("AUTH IT PASS " if not session_it.FAILED else "AUTH IT FAIL ") + args.mode +
          (" " + " ".join(session_it.FAILED) if session_it.FAILED else ""), flush=True)
    return 0 if not session_it.FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
