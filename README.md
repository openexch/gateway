# Open Exchange gateway

`oe-gateway` (REST) and `oe-stream` (WebSocket user streams): two stateless C++20 processes in front of the
Open Exchange core. Each has its own cluster session and no shared state; run N copies of each. They share
one static library, `gw_common`.

This version is the process shell: settings, both media driver modes, the supervisor and the admin
endpoints. Sessions, REST routes, auth and streams come next.

## Build

Requirements: a C++20 compiler, CMake 3.30 or later (the Aeron 1.53 floor), Java 21 (SBE code generation
inside cluster-tools), `uuid-dev`, `python3`, `curl`.

```
scripts/build-cluster-tools.sh .deps/cache .deps/prefix
cmake -S . -B build -DCMAKE_PREFIX_PATH=$PWD/.deps/prefix
cmake --build build
ctest --test-dir build --output-on-failure
```

`dependencies.lock` pins cluster-tools by commit sha and source tarball sha256, and pins the money schema by
core commit, id, version and sha256. `scripts/fetch-cluster-tools.sh` refuses any mismatch, and the CMake
configure refuses a package built from another commit or carrying another schema. To re-pin:
`scripts/repin-cluster-tools.sh <40-hex commit>`, review the diff, commit the lock.

## Run

Every setting comes from the environment. Every problem is reported in one pass and the process exits 78.
`/health` lists every setting in force with its source, secrets redacted. Required settings for both
binaries (`<P>` is `GATEWAY` or `STREAM`):

- `<P>_DRIVER_MODE`: `embedded` (dev and demo) or `external` (production, the host `aeronmd`).
- The driver directory for the chosen mode: `AERON_DIR` for external, `<P>_DRIVER_DIR` for embedded.
- `<P>_CORE_INGRESS`.
- `<P>_CORE_INGRESS_CHANNEL`: must name `term-length` and `mtu`.
- `<P>_CORE_RESPONSE_CHANNEL`: must name `endpoint`, `term-length` and `mtu`.
- `CORE_AUTH_CREDENTIAL`: a gateway key.
- `<P>_INSTANCE_ID`: a positive int64.
- `GATEWAY_HTTP_BIND` or `STREAM_WS_BIND`.
- `<P>_ADMIN_BIND`: IPv4, never the public bind.
- `<P>_AUTH_MODES`.

Sizes, timeouts and cadences are settings with defaults, not limits; `/health` shows the value in force.
`<P>_DRIVER_TIMEOUT_MS` (default 10000) must be 3000 to 60000: below that a driver hiccup reads as a
dead driver.

In external mode a missing `AERON_DIR` means the driver is not up yet: the process exits 70 after the
driver timeout so the service manager retries it. Order the unit after `aeronmd` anyway.

Admin endpoints are served on the admin bind only, from the moment the process binds: `/health`, `/ready`
(503 lists the failing conditions by name, `driver_connected` first), `/ready/session` (oe-gateway) and
`/metrics` (Prometheus).

Exit codes: 0 clean stop; 70 process health (driver death or timeout, conductor heartbeat, duty watchdog, a
stop bound hit); 75 temporary; 78 configuration (set `RestartPreventExitStatus=78`).

## Footprint

Idle, defaults, no session yet: an embedded driver holds about 48 MB in its driver directory (`cnc.dat`
47 MB, an Aeron default, plus `loss-report.dat` 1 MB); the process RSS is about 56 MB embedded and 6.5 MB
external. Session terms come on top of that. These are measurements, not limits.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
