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

Both binaries speak exactly the money schema `dependencies.lock` pins (id 2, version 9), through the
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

The full client reference (every route, body, status code, error string, stream frame and the migration from the OMS v1 API) is [docs/api.md](docs/api.md).

- Public: `GET /markets` and `GET /status`.
- Orders: `POST /orders` (`Idempotency-Key` required), `GET /orders?marketId=`, `GET|PUT|DELETE /orders/{orderId}`,
  `POST /orders/cancel` (`{orderIds}` or `{all:true}`, at most `GATEWAY_BULK_CANCEL_MAX` per request).
- `GET /balances`.
- `POST /balances/deposit|withdraw`: only with the treasury lane, otherwise 404 FEATURE_DISABLED.
- History: `GET /orders/history?limit=&cursor=&marketId=&status=` and `GET /fills?limit=&cursor=&marketId=&orderId=`,
  and `GET /orders/{orderId}` for an order that is no longer open (see History below).

Money is a decimal string (at most 8 fraction digits) and ids are decimal strings. Errors are
`{error, code, reason, detail, keyFinal}`. `keyFinal: true` appears only on a reject of a request that carried
an `Idempotency-Key`, and means a retry with the same key gets the same answer. A 504 OUTCOME_UNKNOWN means retry
with the same key inside `idempotencyWindowMs` (shown in `/status`).

Bulk cancel takes one principal token per order id plus one, never more than one full bucket
(`GATEWAY_PRINCIPAL_BURST`); near the bucket size it can get 429 shortly after other requests: wait for
`Retry-After` and resend, nothing was sent. `all:true` is charged one token for the read, then at most
`burst - 1` for the cancels.

History. The four history reads come from the history projection's relations, defined in
`sql/history_read_contract_v1.sql`; the projection owns them and the gateway's role needs SELECT on those four
relations only. `sql/history_projection_v1.sql` grants them to the role `oe_history_reader`: make the gateway's login
role a member (`GRANT oe_history_reader TO <gateway role>`). The schema creates that role when the projector's role
has CREATEROLE; otherwise create it first (`CREATE ROLE oe_history_reader NOLOGIN`), or the projector refuses to start
and names the role. They are read when `GATEWAY_PG_URL` is set, on `GATEWAY_PG_HISTORY_POOL` read-only connections of
their own with their own queue (sized by `GATEWAY_PG_QUEUE`), so a history storm never takes a connection from login.
No order-path request touches PostgreSQL.

- Pages are `{items, nextCursor, asOf:{ageMs, stale, eventSeq}}`, newest first, keyset only: send `nextCursor` back unchanged with the
  same filters (a cursor from another route or filter is 400). `nextCursor: null` is the last page. `limit` defaults
  to `GATEWAY_HISTORY_LIMIT_DEFAULT` (100); above `GATEWAY_HISTORY_LIMIT_MAX` (1000) is 400. A row whose key does not
  change is served exactly once; an order updated during a walk moves to the top, so a walk that has not reached it
  yet does not serve it and a walk started over from the top does.
- `asOf.ageMs` is the age of the projection's last beat, measured by the database clock: history is complete as of that long ago. `asOf.stale` is true above `GATEWAY_HISTORY_STALE_MS` (5000). `asOf.eventSeq` is the projection's cursor: every fill or order event up to it is in the answer. With `GATEWAY_HISTORY_MAX_AGE_MS` set (default 0, off; must be above the stale bound) older history is 503 HISTORY_UNAVAILABLE. History of another core genesis than the gateway's last CoreConfigReport, or before the gateway has read one, is always 503. `/health` shows `history:{ageMs, stale, cause, eventSeq, probedAgoMs}` from a probe every `GATEWAY_HISTORY_PROBE_MS` (5000); the metric is `gateway_history_age_ms`.
- `GET /orders/{orderId}` of an order that is not open answers from history; an unknown order and another user's
  order both answer the same 404. An id above the projection's `applied_order_id` with no row yet answers 503
  HISTORY_UNAVAILABLE with `Retry-After` from `GATEWAY_HISTORY_LAG_RETRY_AFTER_MS` (1000), never 404.
- `GET /orders/{orderId}/trail` lists every journal record of the caller's own order, oldest first by `eventSeq`
  (keyset pages, the same limits), with `asOf`. Unknown and another user's order are the same 404; the lag rule above
  applies.
- Each read has one deadline, `GATEWAY_PG_STATEMENT_TIMEOUT_MS`, counted from when it is queued. A full queue, or a
  read still queued at its deadline, is 503 HISTORY_BUSY (`Retry-After` from `GATEWAY_HISTORY_BUSY_RETRY_AFTER_MS`,
  1000). No URL, no projection, no heartbeat, an unreachable database or a timeout is 503 HISTORY_UNAVAILABLE
  (`Retry-After` from `GATEWAY_HISTORY_RETRY_AFTER_MS`, 60000); the cause is in `gateway_history_reads_total`. A
  connection the server dropped is retried once on a fresh connection.

Treasury lane (demo only): `GATEWAY_TREASURY_CREDENTIAL` plus `GATEWAY_ALLOW_TREASURY_LANE=1` plus
`GATEWAY_FAUCET_MAX_AMOUNT` (`assetId:amount,...`; an amount above 92233720368.54775807 stops the process at boot), rate limited by `GATEWAY_FAUCET_RATE`. A deposit or withdraw
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
the snapshot may be missing; read them from `GET /fills`.

Snapshots. A user's open orders come from the core in pages and are stitched by log position: the first page
exclusive, the last inclusive, so no event is lost or doubled between pages. No snapshot is read until the
first head probe has set the watermark; a failed first probe is asked again at once. Order caches are dropped
on every re-registration, a leader change included. Re-snapshots after an overrun are paced by
`STREAM_QUERY_RATE` and `STREAM_QUERY_BURST` (default 10, below the rate, so a storm second stays within 1.2x the rate).

Slow clients. Each connection's bound (`STREAM_WS_QUEUE_MESSAGES`, `STREAM_WS_QUEUE_BYTES`) counts the frames
waiting in `oe-stream` and the frames the writer holds unwritten. The byte test leaves out the largest frame
sent since the connection was last empty, so a snapshot larger than the byte bound still goes out; what
queues behind it must fit. At the bound the connection is closed 4001. Queued order frames for one order
coalesce to the latest; fill frames never do. If the client has stopped reading, the close frame cannot pass
the blocked write and the connection ends as a socket close after `STREAM_WS_IDLE_MS`. The duty thread and
other clients are not affected.

`/ready` requires a registered session, no open event gap, a head probe answered within
`STREAM_READY_STALE_MS` and the listener accepting. A closed admission gate does not affect it. `STREAM_READY_STALE_MS` must be above `STREAM_HEAD_PROBE_MS` and `STREAM_WS_PING_MS` below `STREAM_WS_IDLE_MS`; otherwise the process stops at boot.

Engine status. Every connection gets an `engine` frame on connect and again on change, at most once per
`STREAM_ENGINE_FRAME_MIN_MS` (a held frame goes out as soon as the cap allows), and `election` frames with
phase `start` and `end`. Values come from each node's own endpoint (`GET /health`, `GET /metrics`), polled
every `STREAM_ENGINE_POLL_MS` from `STREAM_CORE_NODE_ENDPOINTS` and `STREAM_ME_NODE_ENDPOINTS`
(`<memberId>@<host>:<port>`, an IPv6 host in brackets); an unset list leaves that block null. A whole round,
name lookups included, ends by `STREAM_ENGINE_POLL_TIMEOUT_MS`, which must be below the poll interval (boot
refuses otherwise, exit 78). A node that does not answer is `unreachable` with null values; a follower's row
always comes from that follower. The leader is the live node (`/health` 200) that reports itself leader.
`behindBytes` and `bridge.entriesBehind` compare two answers sampled at different instants, so they can
briefly read negative. The snapshot block reads `<prefix>snapshot_last_log_position` and
`<prefix>snapshot_state_hash` (prefix `core_` or `match_`); the hash is a CRC32C of the snapshot payload, taken
only when it is all digits, and nodes that report one hash at one position read `matched`. A node that cannot
name its hash omits the series. `version` and `buildCommit` come from the `version=` and `build=` tokens at the
end of `/health` (`commit=` stays the commit log position); an engine build without them shows null, and a
build without a release stamp shows `unknown`. Fields no node supplies are null and render as a dash: entries
behind, election duration, first entry position, order entry reopened. Each instance
makes 2 x nodes HTTP requests per poll and adds nothing to the cluster log.

New settings with defaults: `STREAM_REQUEST_TIMEOUT_MS` 5000, `STREAM_WS_MAX_MESSAGE_BYTES` 65536,
`STREAM_PG_QUEUE` 64, `STREAM_PG_STATEMENT_TIMEOUT_MS` 2000, `STREAM_PG_RETRY_MS` 1000,
`STREAM_WS_SEND_BUFFER_BYTES` (default: the OS buffer), `STREAM_ENGINE_POLL_MS` 1000,
`STREAM_ENGINE_POLL_TIMEOUT_MS` 500, `STREAM_ENGINE_FRAME_MIN_MS` 1000, `STREAM_ENGINE_MAX_BODY_BYTES` 1048576.

T1: `ctest -L t1` with `-DGW_IT_CORE_JAR` and `-DGW_IT_PROBE_JAR`. `GW_IT_STREAM_CONNECTIONS` sets the storm
size (default 2000). The T1 suites take port bases in 24000..31299, below the usual Linux ephemeral range, and
`it.stream` fails at start if a core node's endpoint does not answer.

## History projector (`oe-projector`)

`oe-projector` is the history projector, the only binary in this repo that writes a database. It follows the core's order journal from the member archives over the network and writes the relations in `sql/history_projection_v1.sql`. It applies that file at boot under a transaction-scoped advisory lock and records its sha256; a different recorded schema is refused (exit 78). The read relations are views with the exact names and types of `sql/history_read_contract_v1.sql`, so the history reads above keep working.

**Single writer.** N copies run on N hosts. One holds a lease in `oe_projection_state` (`writer_token`, `lease_until`, on the database clock). Every write transaction ends with `UPDATE ... WHERE writer_token = <mine>`; zero rows rolls back and the copy becomes a standby. A standby polls every `HISTORY_PROJECTOR_LEASE_POLL_MS` and holds no replay and no session. A clean stop drops the replays first, then releases the lease.

The writer treats its lease as held until the lease less one renewal interval (2/3 of `HISTORY_PROJECTOR_LEASE_MS`), counted from when its last confirming statement was sent, so it stops acting as the writer before the database can give the lease to another copy, even when a COMMIT is slow. `HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS` must be below a third of the lease (default 3000 with the 10000 lease). An idle-in-transaction timeout above the lease is accepted with a WARN: the fence still stops a stopped writer, but a takeover can then wait that long. Through a transaction-mode pooler the session `SET` of `idle_in_transaction_session_timeout` is not kept: set it on the projector's role (`ALTER ROLE ... SET idle_in_transaction_session_timeout = ...`) or on the server. The fence does not depend on it. The default lease (10 s) equals the default driver timeout (10 s), so a writer paused past its lease usually exits 70 on resume rather than stepping down; both outcomes are safe.

**Following the journal.** `oe-projector` follows the core's order journal over the network: archive control and replay from `HISTORY_PROJECTOR_JOURNAL_ARCHIVES`, the cluster session (`_CLUSTER_MEMBERS`, `_CORE_INGRESS_CHANNEL`, `_CORE_RESPONSE_CHANNEL`) for the genesisId and the head, and each member's node endpoint (`_JOURNAL_NODE_ENDPOINTS`, one per archive or `-`) for its role and reader replay budget. It reads a follower's archive; the leader's archive is read only when every other source is unreachable, marked, or at its replay budget, never because a follower is backing off. It holds one replay at a time, recorded in `oe_projection_replay` before it is read. A clean stop ends the replay and forgets its row before the lease is released; after a crash the next writer sends `stopReplay` by session id (`stopRequestsAnswered` counts the OK answers; an archive also answers OK for a session it no longer knows). For the budget, set `CORE_ARCHIVE_REPLAY_PROBE_STREAM` on the members to the projector's `_REPLAY_STREAM`.

A source that lacks records (a hole, or recordings starting above 1 after a wipe) is marked and another source fills the range; the projection never starts at the first record a source happens to hold. A source re-reading history the projection already holds counts as delivering; BEHIND measures the lag from the highest sequence of the read that the writer verified or applied. Records at or below the cursor are compared byte for byte; a difference marks that source, and PAYLOAD_CONFLICT halts only when no source agrees. The writer applies only the record after its cursor; anything else fails the batch and the projection reloads from the database. Records of another genesis, or of epoch 0, are never applied. A gap no source can fill is GAP; another genesis is EPOCH_MISMATCH; a record it cannot name is SCHEMA. None of these is a latch, and each is shown in `/health` and in the state row.

`_DUTY_WATCHDOG_MS` defaults to 10000, above `_ARCHIVE_TIMEOUT_MS` (5000); a watchdog at or below the archive timeout is accepted with a WARN. Other settings: `_NODE_FRESH_MS` 3000, `_NODE_FIRST_ROUND_MS` 3000, `_SESSION_MAX_INFLIGHT` 64, `_QUERY_RATE_COUNT` 100, `_QUERY_RATE_PERIOD_MS` 1000, `_QUERY_BURST` 200. Building it needs cluster-tools with `WITH_BRIDGE` (`scripts/build-cluster-tools.sh` does this) and the core feed harness jar, downloaded from the core repository's releases or given as `CT_FEED_HARNESS_JAR`.

**Beat.** The projector writes `beat_at` only when it has applied the core's head as its probe answered it, for its own genesis, with no halt: beat_at is the database clock less the time since that probe was sent. Behind, halted, cut off or of another genesis: no beat, `/ready` false. `/health` shows `lagMs` (beat age) and the beat counts.

**Rebuild.** `oe-projector --rebuild` waits for the lease, then in one transaction resets the state row and truncates every base table. History answers 503 until the projection beats again. It waits at most `HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS` behind open history reads; past that it changes nothing and exits 75 (run it again). Exit codes: 0 done, 75 stopped or not committed (safe to repeat), 78 config.

**Settings.** All settings use the `HISTORY_PROJECTOR_` prefix and are listed with their defaults in `projector/ProjectorSettings.h`. Both driver modes are supported. `/health`, `/ready` (with `"role"`) and `/metrics` (`projector_`) are served on `HISTORY_PROJECTOR_ADMIN_BIND`. Threads: `pj-duty`, `pj-pg`, `pj-admin`.

**Tests.** `projector` (T0) and `projector.*` (T1, a PostgreSQL container, with `GW_PG_TESTS=ON` and `GW_TEST_BUILDS=ON`).

## Footprint

Idle, defaults, no session yet: an embedded driver holds about 48 MB in its driver directory (`cnc.dat`
47 MB, an Aeron default, plus `loss-report.dat` 1 MB); the process RSS is about 56 MB embedded and 6.5 MB
external. Session terms come on top of that. These are measurements, not limits.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
