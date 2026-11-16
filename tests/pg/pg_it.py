#!/usr/bin/env python3
"""Runs the users-store tests (gw_pg_tests) against a throwaway PostgreSQL container, then removes the container.

The image is pinned by digest. The password is random per run and lives only in this process and the container.

usage: pg_it.py --binary <gw_pg_tests> --image <name@sha256:...> [case ...]
"""
import argparse
import os
import secrets
import subprocess
import sys
import time


def docker(*args, check=True, capture=True):
    return subprocess.run(["docker", *args], check=check, capture_output=capture, text=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("cases", nargs="*")
    args = parser.parse_args()
    name = f"gw-pg-test-{os.getpid()}-{secrets.token_hex(3)}"
    password = secrets.token_hex(16)
    docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_USER=gw", "-e", f"POSTGRES_PASSWORD={password}",
           "-e", "POSTGRES_DB=gw", "-p", "127.0.0.1::5432", args.image)
    try:
        deadline = time.monotonic() + 60
        while True:
            ready = docker("exec", name, "pg_isready", "-U", "gw", "-d", "gw", "-h", "127.0.0.1", check=False)
            if ready.returncode == 0:
                break
            if time.monotonic() > deadline:
                print(f"FAIL postgres did not become ready: {ready.stdout}{ready.stderr}")
                return 1
            time.sleep(0.3)
        port = docker("port", name, "5432/tcp").stdout.strip().splitlines()[0].rsplit(":", 1)[1]
        base = f"postgresql://gw:{password}@127.0.0.1:{port}"
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "GW_TEST_PG_URL": f"{base}/gw",
            "GW_TEST_PG_ADMIN_URL": f"{base}/postgres",
            "GW_TEST_PG_PAUSE": f"docker pause {name} >/dev/null",
            "GW_TEST_PG_UNPAUSE": f"docker unpause {name} >/dev/null",
        }
        result = subprocess.run([args.binary, *args.cases], env=env)
        return result.returncode
    finally:
        docker("unpause", name, check=False)
        docker("rm", "-f", name, check=False)


if __name__ == "__main__":
    sys.exit(main())
