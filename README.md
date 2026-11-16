# Open Exchange gateway

`oe-gateway` (REST) and `oe-stream` (WebSocket user streams): two stateless C++20 processes in front of the
Open Exchange core. Each has its own cluster session and no shared state; run N copies of each. They share
one static library, `gw_common`.

This version has the process shell (settings, both media driver modes, the supervisor, the admin
endpoints), the wire codec, both core sessions, auth, the REST routes, the user streams and the engine status
feed.

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

The lock also pins Boost, OpenSSL and libpq by source tarball and sha256. Build them with
`scripts/build-deps.sh <cache> <prefix>` and pass `-DGW_DEPS_PREFIX=<prefix>`; configure checks the build stamp
and the version headers against the lock. `-DGW_PG_TESTS=ON` runs the PostgreSQL tests in docker. A security
fix in one of them is a re-pin of the lock.

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
`<P>_RECONNECT_DELAY_MS` (default 1000) spaces session reconnects; `<P>_REGISTER_RESEND_MS` (default 5000)
resends an unanswered registration.

In external mode a missing `AERON_DIR` means the driver is not up yet: the process exits 70 after the
driver timeout so the service manager retries it. Order the unit after `aeronmd` anyway.

Admin endpoints are served on the admin bind only, from the moment the process binds: `/health`, `/ready`
(503 lists the failing conditions by name, `driver_connected` first), `/ready/session` (oe-gateway) and
`/metrics` (Prometheus).

`/ready` conditions, each failing one listed by name: `driver_connected`, `storage_space`,
`session_connected`, `session_registered`, `config_loaded`, `gate_open`, `keepalive_fresh`, `timeout_streak`,
`public_listener`. `/ready/session` checks only the driver and the session. `config_loaded` needs a core
that has applied a configuration (config version above 0).

## Core session

`oe-gateway` holds one GATEWAY cluster session. It counts as registered only after the core's AdminAck
arrives at the exact money schema version, and it registers again on every new session and every new
leader; nothing is offered before that. Requests in flight are offered once more after a new leader, with
the same correlation id and request id, so a keyed command replays its stored decision. Reads share a query
budget; above it the gateway answers overloaded instead of queueing.

A session whose connect attempts the media driver refuses for storage space reads not ready
(`storage_space`) and counts `gateway_storage_starved_seconds_total`. After 30 s of continuous refusal it
exits 70, so the service manager restarts it.

## Wire

Both binaries speak exactly the money schema `dependencies.lock` pins (id 2, version 8), through the
cluster-tools codecs. Every egress frame is checked before any field is read. A wrong schema, any other
version, an unknown template, wrong lengths, trailing bytes and unnamed enum values are refused. Refusals
are counted in `gateway_decode_refused_total` and logged rate-limited (the first, then every 1000th, with
the count). Frames the core sends on a subscribed channel that the gateway does not read (`OrderFault`, a
CONTROL broadcast, and `SettlementApplied`, `SettleFault` and `SettlementAppliedBatch`, which the core can
broadcast to a new session before its registration sets the channel mask) are counted in `gateway_wire_ignored_total{template}` and never logged. A frame reaches
its handler only after it decoded completely.

`tests/golden/regen.sh <core-cluster.jar>` regenerates the golden corpus. It accepts only a jar whose
Build-Sha and `sbe/money-schema.xml` sha256 match the lock.

Exit codes: 0 clean stop; 70 process health (driver death or timeout, conductor heartbeat, duty watchdog, a
stop bound hit); 75 temporary; 78 configuration (set `RestartPreventExitStatus=78`).

## Auth

`oe-gateway` serves `/api/v2` on `GATEWAY_HTTP_BIND` (Boost.Beast, `GATEWAY_IO_THREADS` `gw-io` threads), with
`POST /auth/register`, `POST /auth/login`, `POST /auth/logout` and `GET /auth/me`. `GATEWAY_AUTH_MODES` takes:

- `api-key`: `X-API-Key`, keys from `GATEWAY_API_KEYS_FILE` as `key:userId`.
- `jwt`: `Authorization: Bearer`, HS256, `exp` required, `sub` is the userId, secret from
  `GATEWAY_JWT_SECRET_FILE`.
- `demo`: an opaque token, stored as SHA-256 in PostgreSQL (`GATEWAY_PG_URL`). The schema is `db/migration`;
  `oe-gateway` applies it under an advisory lock and records its sha256, `oe-stream` only verifies it.
- `dev`: only with `GATEWAY_ALLOW_DEV_AUTH=1`.

A missing or bad key or secret file stops the process with exit 78. A credential is never read from a URL, and
no request can name a user. Password hashing (PBKDF2-SHA256, `GATEWAY_PBKDF2_ITERATIONS`, default 600000; a stored hash keeps its own count) runs on `gw-hash`
(`GATEWAY_HASH_THREADS`, `GATEWAY_HASH_QUEUE`; a full queue is 503 AUTH_BUSY). Users-store calls run on
`gw-pg-auth`. Register and login are rate limited globally and per source address (`<n>/s` or `<n>/min` plus a
burst; `GATEWAY_TRUSTED_PROXIES` decides the source; IPv6 sources share a budget per
`GATEWAY_SOURCE_IPV6_PREFIX`, default 64). Registration asks the core whether the new userId already exists and
refuses with 503 IDENTITY_STORE_INCONSISTENT when it does.

Bearer tokens the gateway has not cached cost a users-database lookup, with their own budget per source:
`GATEWAY_TOKEN_LOOKUP_RATE` (60/min) and `GATEWAY_TOKEN_LOOKUP_BURST` (20), above it 429 RATE_LIMITED. At most
`GATEWAY_TOKEN_LOOKUP_INFLIGHT` such lookups wait at once (default half of `GATEWAY_PG_QUEUE`), so random tokens
cannot starve login or register. After a failed database connect, calls fail at once for `GATEWAY_PG_RETRY_MS`
(1000); cached tokens are still served stale during that time.

Runbook: every refused registration (`gateway_user_id_conflict_total`) still uses up one `demo_user_seq` value.
Alert on any rise, and after a restore set the sequence above the highest userId the core knows.

## REST API

- Public: `GET /markets` and `GET /status`.
- Orders: `POST /orders` (`Idempotency-Key` required), `GET /orders?marketId=`, `GET|PUT|DELETE /orders/{orderId}`,
  `POST /orders/cancel` (`{orderIds}` or `{all:true}`, at most `GATEWAY_BULK_CANCEL_MAX` per request).
- `GET /balances`.
- `POST /balances/deposit|withdraw`: only with the treasury lane, otherwise 404 FEATURE_DISABLED.
- `/orders/history` and `/fills` answer 503 HISTORY_UNAVAILABLE for now. So does `GET /orders/{id}` for an order
  that is not open, with `Retry-After: 60`.

Money is a decimal string (at most 8 fraction digits) and ids are decimal strings. Errors are
`{error, code, reason, detail, keyFinal}`. `keyFinal: true` appears only on a reject of a request that carried
an `Idempotency-Key`, and means a retry with the same key gets the same answer. A 504 OUTCOME_UNKNOWN means retry
with the same key inside `idempotencyWindowMs` (shown in `/status`).

Bulk cancel takes one principal token per order id plus one, never more than one full bucket
(`GATEWAY_PRINCIPAL_BURST`); near the bucket size it can get 429 shortly after other requests: wait for
`Retry-After` and resend, nothing was sent. `all:true` is charged one token for the read, then at most
`burst - 1` for the cancels.

Treasury lane (demo only): `GATEWAY_TREASURY_CREDENTIAL` plus `GATEWAY_ALLOW_TREASURY_LANE=1` plus
`GATEWAY_FAUCET_MAX_AMOUNT` (`assetId:amount,...`), rate limited by `GATEWAY_FAUCET_RATE`. A deposit or withdraw
retried after a 504 also takes a faucet token. A TREASURY key can credit any user; the caps in the gateway only
guard against bugs. Registration funding re-sends a deposit whose outcome is unknown only when the core's
`idempotencyWindowMs` is longer than twice `GATEWAY_REQUEST_TIMEOUT_MS`; otherwise `funded` is false and
nothing is re-sent, because a re-send after the window could credit twice.

## User stream

`oe-stream` serves each user's own orders, fills and balances over WebSocket at `/ws/v2`, from its own
GATEWAY session to the core. It holds no state that survives a restart, and it does not link the order or
money encoders (`tests/link` checks the compile, the link and the symbols).

Credential, exactly one of:

- `Sec-WebSocket-Protocol: bearer, <token>`
- `Authorization` or `X-API-Key`
- a first frame `{"op":"auth","token":"..."}` within `STREAM_WS_AUTH_TIMEOUT_MS`

A query string is refused with 400. Any `userId` in a client frame closes the connection with 4003. Other
close codes: 1008 auth failure or timeout, 1013 users store unavailable or ring full, 1001 idle.

Client frames: `subscribe` `{channels: orders|fills|balances, resume:{eventSeq}?}`, `unsubscribe`, `ping`.
Server frames: `subscribed`, `snapshot`, `order`, `fill`, `balance`, `admission`, `heartbeat` `{eventSeq}`,
`pong`, `resync` `{reason}`, `error`, `engine`, `election`.

Resume. Keep the last `eventSeq` of any frame, heartbeats included, and resume from it. A resume the stream
cannot serve gets `resync` `RESUME_TOO_OLD` and a snapshot. After a resync, fills between your last frame and
the snapshot may be missing until history is served.

Snapshots. A user's open orders come from the core in pages and are stitched by log position: the first page
exclusive, the last inclusive, so no event is lost or doubled between pages. No snapshot is read until the
first head probe has set the watermark; a failed first probe is asked again at once. Order caches are dropped
on every re-registration, a leader change included. Re-snapshots after an overrun are paced by
`STREAM_QUERY_RATE` and `STREAM_QUERY_BURST`.

Slow clients. Each connection's bound (`STREAM_WS_QUEUE_MESSAGES`, `STREAM_WS_QUEUE_BYTES`) counts the frames
waiting in `oe-stream` and the frames the writer holds unwritten. The byte test leaves out the largest frame
sent since the connection was last empty, so a snapshot larger than the byte bound still goes out; what
queues behind it must fit. At the bound the connection is closed 4001. Queued order frames for one order
coalesce to the latest; fill frames never do. If the client has stopped reading, the close frame cannot pass
the blocked write and the connection ends as a socket close after `STREAM_WS_IDLE_MS`. The duty thread and
other clients are not affected.

`/ready` requires a registered session, no open event gap, a head probe answered within
`STREAM_READY_STALE_MS` and the listener accepting. A closed admission gate does not affect it.

Engine status. Every connection gets an `engine` frame on connect and again on change, at most once per
`STREAM_ENGINE_FRAME_MIN_MS` (a held frame goes out as soon as the cap allows), and `election` frames with
phase `start` and `end`. Values come from each node's own endpoint (`GET /health`, `GET /metrics`), polled
every `STREAM_ENGINE_POLL_MS` from `STREAM_CORE_NODE_ENDPOINTS` and `STREAM_ME_NODE_ENDPOINTS`
(`<memberId>@<host>:<port>`, an IPv6 host in brackets); an unset list leaves that block null. A whole round,
name lookups included, ends by `STREAM_ENGINE_POLL_TIMEOUT_MS`, which must be below the poll interval (boot
refuses otherwise, exit 78). A node that does not answer is `unreachable` with null values; a follower's row
always comes from that follower. The leader is the live node (`/health` 200) that reports itself leader.
`behindBytes` and `bridge.entriesBehind` compare two answers sampled at different instants, so they can
briefly read negative. Fields no node supplies are null and render as a dash: snapshot state hash, version,
build commit, entries behind, election duration, first entry position, order entry reopened. Each instance
makes 2 x nodes HTTP requests per poll and adds nothing to the cluster log.

New settings with defaults: `STREAM_REQUEST_TIMEOUT_MS` 5000, `STREAM_WS_MAX_MESSAGE_BYTES` 65536,
`STREAM_PG_QUEUE` 64, `STREAM_PG_STATEMENT_TIMEOUT_MS` 2000, `STREAM_PG_RETRY_MS` 1000,
`STREAM_WS_SEND_BUFFER_BYTES` (default: the OS buffer), `STREAM_ENGINE_POLL_MS` 1000,
`STREAM_ENGINE_POLL_TIMEOUT_MS` 500, `STREAM_ENGINE_FRAME_MIN_MS` 1000, `STREAM_ENGINE_MAX_BODY_BYTES` 1048576.

T1: `ctest -L t1` with `-DGW_IT_CORE_JAR` and `-DGW_IT_PROBE_JAR`. `GW_IT_STREAM_CONNECTIONS` sets the storm
size (default 2000). The T1 suites take port bases in 24000..31299, below the usual Linux ephemeral range, and
`it.stream` fails at start if a core node's endpoint does not answer.

## Footprint

Idle, defaults, no session yet: an embedded driver holds about 48 MB in its driver directory (`cnc.dat`
47 MB, an Aeron default, plus `loss-report.dat` 1 MB); the process RSS is about 56 MB embedded and 6.5 MB
external. Session terms come on top of that. These are measurements, not limits.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
