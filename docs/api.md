# Open Exchange gateway API (v2)

This document describes the public API of the two gateway processes:

- `oe-gateway` serves REST under `/api/v2` on `GATEWAY_HTTP_BIND`.
- `oe-stream` serves the user stream over WebSocket at `/ws/v2` on `STREAM_WS_BIND`.

Both processes also serve `/health`, `/ready` and `/metrics` on their admin bind (`GATEWAY_ADMIN_BIND`,
`STREAM_ADMIN_BIND`). These are operator endpoints and are never served on the public bind.

Every size, rate, bound and timeout named below is a setting with a default. A deployment can change it, and `/health`
on the admin bind lists every setting in force with its source. The values in this document are the defaults.

## 1. Conventions

- **JSON only.** A request body must be one JSON object sent with `Content-Type: application/json`. Responses with a
  body are `application/json` and carry `Cache-Control: no-store`.
- **Money** is a decimal string with at most 8 fraction digits, no sign and no exponent, for example `"65000.5"` or
  `"0.00000001"`. A JSON number is refused. Responses print money without trailing zeros (`"1"`, `"0.25"`).
- **Ids** (`orderId`, `marketId`, `assetId`, `userId`, `tradeId`, `meOrderId`) are decimal strings of integers, with no
  sign and no leading zero.
- **Times** (`acceptedAt`, `createdAt`, `updatedAt`, `executedAt`, `expiresAt`) are JSON integers, epoch milliseconds of
  the core's cluster time.
- **Sequence numbers and log positions** (`eventSeq`, `asOf.logPosition`) are JSON integers.
- **Strict input.** A body field, query parameter or client frame key that the route does not name is refused with
  400 `VALIDATION`, naming it. An old client's `orderType` is never silently ignored.
- **No request names a user.** A `userId` or `user_id` key anywhere in a body (at any depth), in a query string or in a
  WebSocket client frame is refused. The authenticated credential decides the user and nothing else does.
- **No credential in a URL.** Query parameters named `token`, `access_token`, `apikey`, `api_key`, `api-key`, `jwt`,
  `authorization` or `bearer` (any case) are refused with 400. The user stream refuses any query string.
- **CORS.** Only origins listed in `GATEWAY_CORS_ORIGINS` (default: none) get `Access-Control-Allow-Origin`. A
  preflight (`OPTIONS`) from a listed origin answers 204 with `Access-Control-Allow-Methods: GET, POST, PUT, DELETE`,
  `Access-Control-Allow-Headers: Authorization, Content-Type, Idempotency-Key, X-API-Key` and
  `Access-Control-Max-Age: 600`; from any other origin it answers 403 `FORBIDDEN` "origin not allowed".
- **Order of checks.** A request is validated (query, body, fields, order shape, `Idempotency-Key`) before its
  credential is checked. A malformed request therefore answers 400 even without a credential.

### 1.1 Error body

Every error answer has this body:

```json
{"error":"rejected by the core","code":"REJECTED","reason":"INSUFFICIENT_FUNDS","detail":null,"keyFinal":true}
```

| Field | Meaning |
|---|---|
| `error` | Human text. Stable per case (listed in section 4), but match on `code`, not on this text. |
| `code` | The machine code (table below). |
| `reason` | The core's own reject reason name when the core decided, else `null`. |
| `detail` | More text or `null`: which field, which setting, what to do next. |
| `keyFinal` | `true` only on a core reject of a request that carried an `Idempotency-Key`, when the core stored that decision under the key: a retry with the same key gets the same answer until the idempotence window ends, so a new attempt needs a new key. `false` everywhere else: a retry with the same key is evaluated again. |

Every 429 and every 503 from `oe-gateway` carries `Retry-After` in whole seconds (rounded up, at least 1). A 401 carries
`WWW-Authenticate: Bearer`. A 405 carries `Allow`.

### 1.2 Codes

| HTTP | `code` | Meaning |
|---|---|---|
| 400 | `VALIDATION` | Shape errors: bad JSON, unknown field or parameter, bad money or id form, order shape, bad cursor, unknown `assetId` on deposit or withdraw. Also core rejects `MISSING_REQUEST_ID`, `UNSUPPORTED_FLAGS`, `UNSUPPORTED_TIF`. |
| 400 | `MISSING_IDEMPOTENCY_KEY` | A route that needs `Idempotency-Key` did not get one. |
| 401 | `UNAUTHORIZED` | Missing, unknown, expired or malformed credential; credential of a mode that is off; two credentials. |
| 403 | `FORBIDDEN` | The core refused this gateway's session class (`UNAUTHORIZED_CLASS`, a deployment error), or a CORS preflight from an unlisted origin. |
| 404 | `NOT_FOUND` | No such route; an order that is unknown or not yours (one answer for both); core `UNKNOWN_ORDER`, `NOT_OWNER`, `UNKNOWN_MARKET`. |
| 404 | `FEATURE_DISABLED` | Register and login without `demo` auth; logout of a non-demo credential; deposit and withdraw with the treasury lane off. |
| 405 | `METHOD_NOT_ALLOWED` | The path exists with other methods. |
| 409 | `CONFLICT` | Username taken; core `REQUEST_ID_PAYLOAD_MISMATCH`, `DUPLICATE_CLIENT_ORDER_ID`, `NOT_OPEN`, `AMEND_PENDING`, `EXPIRED`; money `REQUEST_ID_MISMATCH`. |
| 413 | `VALIDATION` | Request body above `GATEWAY_MAX_BODY_BYTES` (65536). The connection is closed. |
| 422 | `REJECTED` | A business rule of the core refused the request (section 4.4), or a faucet amount above its cap. |
| 429 | `RATE_LIMITED` | A gateway budget is spent, or the core answered `RATE_LIMIT` or `TOO_MANY_PENDING_REQUESTS`. |
| 431 | `VALIDATION` | Header block above `GATEWAY_MAX_HEADER_BYTES` (8192). The connection is closed. |
| 500 | `INTERNAL` | A gateway defect or a core answer that cannot reach a gateway. Generic text; the cause is in the gateway log. |
| 503 | `ADMISSION_CLOSED` | The core's admission gate is closed (`GATE_CLOSED`) or a circuit breaker tripped (`CIRCUIT_BREAKER`). |
| 503 | `CORE_UNAVAILABLE` | No registered core session before the deadline (nothing was sent), or the core config is not loaded yet. |
| 503 | `OVERLOADED` | The gateway's command ring, request table or query budget is full. Nothing was sent. |
| 503 | `AUTH_UNAVAILABLE` | The users store cannot answer and no cached answer is usable. |
| 503 | `AUTH_BUSY` | The password hashing queue or the users store queue is full. |
| 503 | `IDENTITY_STORE_INCONSISTENT` | Registration refused: the users store handed out a userId the core already knows. |
| 503 | `HISTORY_UNAVAILABLE` | The history projection cannot answer, or an order is not projected yet. |
| 503 | `HISTORY_BUSY` | Too many history reads at once. |
| 504 | `OUTCOME_UNKNOWN` | The command was offered to the core and no answer came before `GATEWAY_REQUEST_TIMEOUT_MS` (5000). It may have been applied. |

## 2. Authentication

`GATEWAY_AUTH_MODES` (and `STREAM_AUTH_MODES` for `oe-stream`, which must be given the same list) enables one or more
modes. There is no default; the process does not start without the list and the material each mode needs.

| Mode | Credential | Setup | Resolves to |
|---|---|---|---|
| `api-key` | `X-API-Key: <key>` | `GATEWAY_API_KEYS_FILE`: lines `key:userId` (`#` comments, blank lines ignored). A key is 16 or more visible ASCII characters without `:`. Compared in constant time. | the file's userId |
| `jwt` | `Authorization: Bearer <header.payload.signature>` | `GATEWAY_JWT_SECRET_FILE` (HS256 secret, at least 32 bytes) | `sub` |
| `demo` | `Authorization: Bearer <token>` | `GATEWAY_PG_URL` (users store). Tokens come from register and login. | the users row |
| `dev` | `Authorization: Bearer dev:<userId>` | Only with `GATEWAY_ALLOW_DEV_AUTH=1`; refused at boot otherwise. Never in production. | `<userId>` |

Rules:

- Exactly one credential per request. `Authorization` and `X-API-Key` together, or either header repeated, is 401.
- The `Authorization` scheme must be `Bearer` (case-insensitive) followed by a non-empty token of visible ASCII
  characters.
- A bearer token that starts with `dev:` is judged as a dev token only; a token that contains a `.` is judged as a JWT
  only (no fall-through to demo); any other bearer token is a demo token.
- JWT: header `alg` must be `HS256` with no `crit`; `exp` is required and must be in the future; `nbf`, when present,
  must not be in the future; `sub` must be a string holding a positive decimal integer (no leading zero, at most
  int64).
- Demo tokens are opaque (43 characters of base64url, 32 random bytes), stored only as their SHA-256, one per user.
  Login rotates the token, logout clears it. A looked-up token is cached for `GATEWAY_TOKEN_CACHE_TTL_MS` (5000); a
  logout through another gateway instance takes effect here when that cache entry expires. When the users store cannot
  answer, a token that was valid keeps being served for up to `GATEWAY_TOKEN_STALE_MS` (600000), after which the answer
  is 503 `AUTH_UNAVAILABLE`, never 401.
- A bearer token the gateway has not cached costs a users-store lookup and takes a token of a per-source budget,
  `GATEWAY_TOKEN_LOOKUP_RATE` (60/min) with `GATEWAY_TOKEN_LOOKUP_BURST` (20); spent is 429 `RATE_LIMITED`. At most
  `GATEWAY_TOKEN_LOOKUP_INFLIGHT` (default half of `GATEWAY_PG_QUEUE`, so 32) such lookups wait at once; above that is
  503 `AUTH_BUSY`.

401 `detail` strings (the `error` is always "unauthorized"):

| `detail` | Cause |
|---|---|
| `no credential (Authorization: Bearer <token> or X-API-Key)` | none sent |
| `two credentials (Authorization and X-API-Key): send one` | both headers |
| `a credential header is repeated` | a header given twice |
| `the Authorization scheme must be Bearer` | other scheme |
| `empty or malformed bearer token` | empty token or invisible characters |
| `api-key authentication is not enabled` / `jwt authentication is not enabled` / `dev authentication is not enabled` | mode off |
| `unknown API key` | key not in the file |
| `dev:<userId> needs a positive integer` | bad dev token |
| `jwt refused: <VERDICT>` | `MALFORMED`, `BAD_ALGORITHM`, `BAD_SIGNATURE`, `MISSING_EXP`, `EXPIRED`, `NOT_YET_VALID`, `BAD_SUBJECT` |
| `unknown bearer token` | demo mode off, or no users store |
| `unknown or expired bearer token` | the users store does not know the token |

### 2.1 Register, login, logout, me

These four routes exist in every mode. Register and login need `demo` in the mode list; without it they answer 404
`FEATURE_DISABLED`.

Register and login take `{username, password}`. `username` is 3 to 20 characters `a-z`, `0-9`, `_`; `password` is 6 or
more characters. Passwords are hashed with PBKDF2-SHA256 (`GATEWAY_PBKDF2_ITERATIONS`, 600000) on a bounded pool
(`GATEWAY_HASH_THREADS` 1, `GATEWAY_HASH_QUEUE` 32). Both routes are rate limited globally (`GATEWAY_REGISTER_RATE`
1/s burst `GATEWAY_REGISTER_BURST` 5; `GATEWAY_LOGIN_RATE` 10/s burst `GATEWAY_LOGIN_BURST` 20) and per source address
(`GATEWAY_AUTH_SOURCE_RATE` 10/min burst `GATEWAY_AUTH_SOURCE_BURST` 10, register and login together). The source is the
socket peer unless the peer is inside `GATEWAY_TRUSTED_PROXIES`; then it is the right-most address of
`GATEWAY_CLIENT_IP_HEADER` (`X-Forwarded-For`) that is not itself a trusted proxy. IPv6 sources share a budget per
`GATEWAY_SOURCE_IPV6_PREFIX` (64) bits.

#### POST /api/v2/auth/register

```http
POST /api/v2/auth/register
Content-Type: application/json

{"username":"alice_01","password":"correct horse"}
```

201:

```json
{"userId":"1007","username":"alice_01","token":"q3W0mB8x...43 chars...","funded":true}
```

`funded` is `true` only when the treasury lane is on and every funding deposit was acknowledged (section 7). With the
lane off it is always `false`.

Before the user row is written, the gateway asks the core whether the new userId already exists. If it does, nothing is
written or funded and the answer is 503 `IDENTITY_STORE_INCONSISTENT` with `Retry-After: 60`.

| Status | code | error |
|---|---|---|
| 400 | `VALIDATION` | `invalid username` (detail `username must be a string of 3 to 20 characters a-z, 0-9 or _`), `invalid password` (detail `password must be a string of 6 or more characters`), and the body errors of 4.1 |
| 404 | `FEATURE_DISABLED` | `registration is not enabled` |
| 409 | `CONFLICT` | `username taken` |
| 429 | `RATE_LIMITED` | `too many requests` |
| 500 | `INTERNAL` | `internal error` |
| 503 | `AUTH_BUSY` | `too many logins and registrations at once`, `the users store queue is full` |
| 503 | `AUTH_UNAVAILABLE` | `the users store cannot answer` |
| 503 | `IDENTITY_STORE_INCONSISTENT` | `registration is unavailable: the users store is behind the core` |
| 503 | `CORE_UNAVAILABLE`, `OVERLOADED` | `the core session is not available`, `the gateway is overloaded` |
| 504 | `OUTCOME_UNKNOWN` | `the core did not answer in time` (detail `nothing was written; retry the request`) |

#### POST /api/v2/auth/login

Same body. 200:

```json
{"userId":"1007","username":"alice_01","token":"Hk2v...43 chars..."}
```

Errors: 400 as register; 401 `UNAUTHORIZED` `invalid username or password` (an unknown username costs the same
hashing time as a wrong password); 404 `FEATURE_DISABLED` `login is not enabled`; 429; 500; 503 `AUTH_BUSY` and
`AUTH_UNAVAILABLE`. Login does not ask the core.

#### POST /api/v2/auth/logout

No body (a body is 400 `logout takes no body`). Needs a demo credential. 204 with no body. A non-demo credential is 404
`FEATURE_DISABLED` `logout applies to demo tokens only`. Errors: section 2, 503 `AUTH_BUSY`, `AUTH_UNAVAILABLE`.

#### GET /api/v2/auth/me

200 `{"userId":"1007","username":"alice_01"}`. `username` is `null` for `api-key`, `jwt` and `dev` credentials.

## 3. Idempotency

`Idempotency-Key` makes a write safe to retry.

- **Form.** A UUID (used as its 128 bits) or 1 to 64 visible ASCII characters (`0x21` to `0x7E`; the first 16 bytes of
  their SHA-256 are used). The nil UUID, and any key that encodes all-zero bits, is refused (400 `VALIDATION` "invalid
  Idempotency-Key"). The header given twice is 400. The gateway never mints a key for a client.
- **Where.** Required on `POST /orders`, `PUT /orders/{orderId}`, `POST /balances/deposit` and
  `POST /balances/withdraw` (missing: 400 `MISSING_IDEMPOTENCY_KEY` "an Idempotency-Key header is required").
  Optional on `DELETE /orders/{orderId}`. Not read by bulk cancel.
- **Scope.** The core remembers a decision per (user, key). Two users can use the same key. One user reusing a key for
  a different request (another command or other fields) gets 409 `CONFLICT` with reason `REQUEST_ID_PAYLOAD_MISMATCH`
  (orders) or `REQUEST_ID_MISMATCH` (deposit, withdraw).
- **Window.** The core keeps a decision for its request TTL, published as `idempotencyWindowMs` in
  `GET /api/v2/status`. It is a replicated core setting (the core limits record), 900000 ms (15 minutes) at genesis.
  After the window the same key is a new request and can create a second order.
- **What a retry returns.**
  - A stored acceptance: the same success status family with `"duplicate": true`. For a create that is 200 instead of
    201. Only `orderId` is authoritative in a duplicate answer: `status` is the order's current status (or
    `PENDING_NEW` with a zero hold when the order is already gone) and `acceptedAt` is the time of the replay.
  - A stored reject: the same error with `"keyFinal": true` (table 4.4). Send a new key for a new attempt.
  - A reject with `"keyFinal": false`: nothing was stored; the same key is evaluated again.
  - `duplicate: true` can appear on a first attempt: the gateway re-offers an in-flight command once after a core
    session loss or leader change, with the same key, and the core replays its stored decision.
- **After a 504 `OUTCOME_UNKNOWN`**, retry with the same key inside the window, never with a new one. The 504 detail
  names the current window: `the command may have been applied: retry with the same Idempotency-Key inside the
  idempotence window (idempotencyWindowMs 900000, also in /api/v2/status); after the window the same key is a new
  request`.
- **Per-user request cap.** The core remembers at most a fixed number of keyed requests per user inside the window (a
  replicated core setting, 256 at genesis). Every remembered request counts (creates, amends, keyed cancels, deposits,
  withdrawals and registration funding deposits); creates and amends are checked against the cap. Above it the core
  answers `TOO_MANY_PENDING_REQUESTS`, which is 429 `RATE_LIMITED`, `keyFinal: false`, `Retry-After: 1`: wait for older
  keys to leave the window, then retry with the same key. Unkeyed cancels and bulk cancels take no slot.

```json
{"error":"too many requests","code":"RATE_LIMITED","reason":"TOO_MANY_PENDING_REQUESTS","detail":null,"keyFinal":false}
```

## 4. REST routes

Base path `/api/v2`. Any other path (the old `/api/v1` included) is 404 `NOT_FOUND` "no such route".

| Method | Path | Auth | Key | Rate cost |
|---|---|---|---|---|
| GET | `/markets` | none | | none |
| GET | `/status` | none | | none |
| POST | `/orders` | yes | required | 1 |
| GET | `/orders?marketId=` | yes | | 1 (0 from the read cache) |
| GET | `/orders/{orderId}` | yes | | 1 |
| PUT | `/orders/{orderId}` | yes | required | 1 |
| DELETE | `/orders/{orderId}` | yes | optional | 1 |
| POST | `/orders/cancel` | yes | not read | ids + 1, at most one full bucket |
| GET | `/orders/history` | yes | | 1 |
| GET | `/fills` | yes | | 1 |
| GET | `/balances` | yes | | 1 (0 from the read cache) |
| POST | `/balances/deposit` | yes | required | 1 + 1 faucet token |
| POST | `/balances/withdraw` | yes | required | 1 + 1 faucet token |
| POST | `/auth/register`, `/auth/login` | none | | auth budgets (2.1) |
| POST | `/auth/logout` | yes | | none |
| GET | `/auth/me` | yes | | none |

### 4.1 Errors every route can return

| Status | code | error (detail) |
|---|---|---|
| 400 | `VALIDATION` | `unknown query parameter`, `a query parameter is repeated`, `malformed query string`, `a request cannot name a user: the credential decides it`, `a credential is never read from a URL` |
| 400 | `VALIDATION` | Routes with a body: `the body must be JSON` (Content-Type), `the body is not valid JSON`, `the body must be a JSON object`, `unknown field`, `invalid field` (detail `field 'x' is required`, `field 'x' must be a string`, `field 'x' must be a decimal string with at most 8 fraction digits (no sign, no exponent), inside the int64 range`, `field 'x' must be a decimal string of an integer from 1`, ...) |
| 401 | `UNAUTHORIZED` | `unauthorized` (section 2) |
| 404 | `NOT_FOUND` | `no such route` |
| 405 | `METHOD_NOT_ALLOWED` | `method not allowed` (detail names the methods) |
| 413, 431 | `VALIDATION` | `request body too large`, `request headers too large` |
| 429 | `RATE_LIMITED` | `too many requests` (detail `the principal's request budget (GATEWAY_PRINCIPAL_RATE) is spent`, or no detail for the token-lookup budget) |
| 503 | `AUTH_UNAVAILABLE`, `AUTH_BUSY` | `the credential cannot be checked now` |

### 4.2 Core outcome errors (routes that ask the core)

| Status | code | error | detail |
|---|---|---|---|
| 504 | `OUTCOME_UNKNOWN` | `the core did not answer in time` | keyed writes: the retry text of section 3; deposit and withdraw add `. The retry also takes a token of the faucet rate (GATEWAY_FAUCET_RATE): a 429 before it carries Retry-After, wait that long and retry with the same key`; unkeyed cancel: `the cancel may have been applied: read the order, or send the cancel again (a cancel of an order that is no longer open answers 404 or 409; send an Idempotency-Key for an exact answer)`; reads: `nothing was changed: retry the read` |
| 503 | `CORE_UNAVAILABLE` | `the core session is not available` | nothing was sent |
| 503 | `CORE_UNAVAILABLE` | `the core config is not loaded yet` | `/markets`, `/balances`, deposit, withdraw |
| 503 | `OVERLOADED` | `the gateway is overloaded` | the command ring, request table (`GATEWAY_MAX_INFLIGHT` 4096) or query budget (`GATEWAY_QUERY_RATE` 100/s, burst 200, per gateway session) is full |
| 500 | `INTERNAL` | `internal error` | |

### 4.3 Core reject mapping

`reason` is the core's name. The `error` text follows the status: 400 `invalid request`, 403 `forbidden`, 404
`not found`, 409 `conflict`, 422 `rejected by the core`, 429 `too many requests`, 503 `order entry is closed`.

| Core reason | HTTP | code | keyFinal (keyed request) | Retry-After |
|---|---|---|---|---|
| `MISSING_REQUEST_ID`, `UNSUPPORTED_FLAGS`, `UNSUPPORTED_TIF` | 400 | `VALIDATION` | false | |
| `UNAUTHORIZED_CLASS` | 403 | `FORBIDDEN` | false | |
| `UNKNOWN_ORDER`, `NOT_OWNER`, `UNKNOWN_MARKET` | 404 | `NOT_FOUND` | false | |
| `REQUEST_ID_PAYLOAD_MISMATCH`, `DUPLICATE_CLIENT_ORDER_ID`, `NOT_OPEN`, `AMEND_PENDING`, `EXPIRED` | 409 | `CONFLICT` | false | |
| `PRICE_OUT_OF_BAND`, `PRICE_OFF_TICK`, `QTY_OFF_LOT` | 422 | `REJECTED` | false | |
| `INSUFFICIENT_FUNDS`, `SIZE_TOO_SMALL`, `SIZE_TOO_LARGE`, `NOTIONAL_TOO_SMALL`, `NOTIONAL_TOO_LARGE`, `PRICE_COLLAR`, `OPEN_ORDER_LIMIT`, `POSITION_LIMIT`, `NO_REFERENCE_PRICE`, `STOP_WOULD_TRIGGER_IMMEDIATELY` | 422 | `REJECTED` | **true** | |
| `INVALID` on a create or amend | 422 | `REJECTED` | **true** | |
| `RATE_LIMIT` | 429 | `RATE_LIMITED` | **true** | 1 s |
| `TOO_MANY_PENDING_REQUESTS` | 429 | `RATE_LIMITED` | false | 1 s |
| `GATE_CLOSED` | 503 | `ADMISSION_CLOSED` | false | 1 s |
| `CIRCUIT_BREAKER` | 503 | `ADMISSION_CLOSED` | **true** | 1 s |
| money `INSUFFICIENT_FUNDS`, `INVALID_AMOUNT`, `BALANCE_OVERFLOW` | 422 | `REJECTED` | false | |
| money `REQUEST_ID_MISMATCH` | 409 | `CONFLICT` | false | |
| any other (`HOLD_FAULT`, the `ME_*` family, `UNKNOWN_HOLD`, ...) | 500 | `INTERNAL` (with `reason`) | false | |

`keyFinal` is never `true` on a read or on an unkeyed cancel. An `ME_*` reason is never a synchronous answer: the
matching engine's decision arrives later as an order event (section 5).

### 4.4 Public routes

#### GET /api/v2/markets

The core's configuration, as the gateway's core session holds it. No query parameters.

```json
{"configVersion":4,
 "markets":[{"marketId":"1","symbol":"BTC-USD","baseAsset":"BTC","quoteAsset":"USD","baseAssetId":"1","quoteAssetId":"2",
   "tickSize":"0.01","lotSize":"0.0001","minPrice":"0.01","maxPrice":"1000000","minQuantity":"0.0001",
   "maxQuantity":"1000","minNotional":"1","maxNotional":"10000000"}],
 "assets":[{"assetId":"1","code":"BTC","scale":8},{"assetId":"2","code":"USD","scale":8}]}
```

(Values are illustrative.) `baseAsset` and `quoteAsset` are `null` when the asset is not in the asset list. Errors: 503
`CORE_UNAVAILABLE` `the core config is not loaded yet`.

#### GET /api/v2/status

```json
{"ready":true,"configVersion":4,"behaviorVersion":4,"idempotencyWindowMs":900000,
 "admission":[{"marketId":"0","gate":"OPEN","reasons":[],"circuitBreaker":false},
              {"marketId":"1","gate":"OPEN","reasons":[],"circuitBreaker":false}]}
```

- `ready`: the config is loaded, the core session is registered and the global gate is open.
- `admission[0]` with `marketId` `"0"` is the global gate; one row per market follows. `gate` is `OPEN`, `SUSPENDED` or
  `null` (not known yet). `reasons` lists the core's gate reasons (`ADMIN`, `RETENTION_BUDGET`, `BRIDGE_HALT`,
  `RECOVERY`, `ME_CAPACITY`, `GENESIS`, `APPLY_FAULT`, `ME_LEG_LIVE`, `ME_BEHAVIOR`).
- Before a config is loaded: `{"ready":false,"configVersion":null,"behaviorVersion":null,"idempotencyWindowMs":null,"admission":[]}`.

Always 200 (except the 400s of 4.1).

### 4.5 Orders

#### Order lifecycle

Create is two-step. A 201 means the core accepted the order and took the hold; the order is `PENDING_NEW` (or
`PENDING_TRIGGER` for an armed stop). The matching engine's decision arrives later as an `order` frame on the user
stream (`event` `ACK_NEW`, `FILL`, or `REJECTED` with a `reason` of the `ME_*` family and `meReason`). A client without
the stream reads `GET /orders/{orderId}`.

| `status` | Meaning | Terminal |
|---|---|---|
| `PENDING_NEW` | accepted by the core with its hold; the matching engine has not acknowledged it yet | no |
| `PENDING_TRIGGER` | a stop or trailing stop waiting for its trigger | no |
| `NEW` | resting in the book | no |
| `PARTIALLY_FILLED` | resting, partly filled | no |
| `PENDING_CANCEL` | a cancel is accepted and on its way | no |
| `PENDING_REPLACE` | an amend is accepted and on its way | no |
| `FILLED` | fully filled | yes |
| `CANCELLED` | cancelled | yes |
| `REJECTED` | refused after acceptance (an `ME_*` reason from the matching engine) | yes |
| `EXPIRED` | expired (for example a GTD order past `expiresAt`) | yes |

The core keeps no terminal order: once an order is terminal, the open-orders read no longer lists it and
`GET /orders/{orderId}` answers from history (4.6).

Order event kinds on the stream (`event`): `ACCEPTED`, `DISPATCHED`, `ACK_NEW`, `FILL`, `CANCELLED`, `EXPIRED`,
`REJECTED`, `REPLACED`, `TRIGGERED`, `HOLD_CHANGED`, `SLICE_SUBMITTED`, `QUARANTINED`. Each event carries the full order
state.

#### The Order object

```json
{"orderId":"1042","clientOrderId":"7a1d3c52-0f4e-4b8e-9c1a-2b3c4d5e6f70","marketId":"1","side":"BUY","type":"LIMIT",
 "timeInForce":"GTC","price":"65000.5","stopPrice":null,"quantity":"0.01","filledQuantity":"0",
 "remainingQuantity":"0.01","status":"NEW","reason":null,"meReason":null,"meOrderId":"880017",
 "hold":{"assetId":"2","amount":"650.005"},"createdAt":1763373600123,"updatedAt":1763373600131,
 "displayQuantity":null,"expiresAt":null,"budget":null}
```

| Field | Notes |
|---|---|
| `clientOrderId` | the UUID the client sent, or `null` |
| `side` | `BUY`, `SELL` |
| `type` | `LIMIT`, `MARKET`, `LIMIT_MAKER`, `STOP_LOSS`, `STOP_LIMIT`, `TRAILING_STOP`, `ICEBERG` |
| `timeInForce` | `GTC`, `IOC`, `FOK`, `GTD` |
| `price` | `null` for `MARKET`, `STOP_LOSS`, `TRAILING_STOP` |
| `stopPrice` | `null` unless set |
| `reason` | the core reject reason name, or `null` |
| `meReason` | the matching engine's numeric reason code, or `null` when not ME-sourced |
| `meOrderId` | the matching engine's id, or `null` before dispatch |
| `hold` | `{assetId, amount}`; rows built from an order event also carry `remaining` |
| `displayQuantity`, `expiresAt`, `budget` | present only on rows from the open-orders read (`GET /orders`, `GET /orders/{orderId}` of an open order, stream snapshot rows that no event changed); `null` when unset |

Rows from history (4.6) have the same fields as event rows: `hold.remaining` when the projection has it, and no
`displayQuantity`, `expiresAt` or `budget`.

#### POST /api/v2/orders

Headers: credential, `Idempotency-Key` (required), `Content-Type: application/json`.

| Field | Type | Rule |
|---|---|---|
| `marketId` | id string | required, 1 to 2147483647 (the core decides whether it exists) |
| `side` | `BUY`, `SELL` | required |
| `type` | order type | required |
| `timeInForce` | `GTC`, `IOC`, `FOK`, `GTD` | required; no default per type |
| `quantity` | money | required, above 0 |
| `price` | money | required and above 0 for `LIMIT`, `LIMIT_MAKER`, `STOP_LIMIT`, `ICEBERG`; refused for `MARKET`, `STOP_LOSS`, `TRAILING_STOP` |
| `budget` | money | only on `MARKET`, `STOP_LOSS`, `TRAILING_STOP` |
| `stopPrice` | money | required on `STOP_LOSS` and `STOP_LIMIT`, refused otherwise |
| `trailingDelta` | money | required on `TRAILING_STOP`, refused otherwise |
| `displayQuantity` | money | required on `ICEBERG` and below `quantity`, refused otherwise |
| `expiresAt` | integer ms | required with `GTD`, refused otherwise |
| `clientOrderId` | UUID string | optional, not the nil UUID |

The gateway checks shape only. Tick, lot, band, balance, risk and market existence are the core's decisions.

```http
POST /api/v2/orders
X-API-Key: k_9f2c8a61b0e34d7a
Idempotency-Key: 5b0e8f2a-3c1d-4e6f-9a7b-1c2d3e4f5a6b
Content-Type: application/json

{"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"65000.5","quantity":"0.01"}
```

201 (first decision) or 200 (`duplicate: true`):

```json
{"orderId":"1042","status":"PENDING_NEW","duplicate":false,"marketId":"1",
 "hold":{"assetId":"2","amount":"650.005"},"acceptedAt":1763373600123,"eventSeq":88211}
```

`eventSeq` is the core's order event sequence number of this acceptance (the same space as the stream's `eventSeq`).

Errors: 4.1, 4.2, 4.3, and 400 `VALIDATION` `invalid order` with detail, one of: `quantity must be positive`,
`price is not part of a <TYPE> order`, `price is required and positive for a <TYPE> order`, `budget is only part of a
MARKET, STOP_LOSS or TRAILING_STOP order`, `stopPrice is required and positive for a stop order`, `stopPrice is only
part of a STOP_LOSS or STOP_LIMIT order`, `trailingDelta is required and positive for a TRAILING_STOP order`,
`trailingDelta is only part of a TRAILING_STOP order`, `displayQuantity is required and positive for an ICEBERG order`,
`displayQuantity is only part of an ICEBERG order`, `displayQuantity must be below quantity`, `expiresAt is required
with GTD`, `expiresAt is only part of a GTD order`; `invalid field` for `side` (`must be BUY or SELL`), `type`,
`timeInForce`, `clientOrderId` (`must be a UUID other than the nil UUID`), `expiresAt` (`must be a positive integer
(epoch milliseconds)`); 400 `MISSING_IDEMPOTENCY_KEY`; 400 `VALIDATION` `invalid Idempotency-Key`.

#### GET /api/v2/orders?marketId=

The caller's open orders, read from the core (every page of the core's snapshot joined), in orderId order. `marketId`
is optional and filters the result; an unknown market gives an empty list.

```json
{"asOf":{"logPosition":51234816},"orders":[{"orderId":"1042", "...": "Order"}]}
```

`asOf.logPosition` is the core log position the read reflects. Read cache: an identical read by the same user within
`GATEWAY_QUERY_MIN_INTERVAL_MS` (250) is answered from the previous answer (same `asOf`) and costs no rate token. The
cache is dropped when this gateway instance completes a command for that user, so a fill, or a write through another
gateway instance, can be up to that interval late here. The user stream is the fast path for fills.

Errors: 4.1, 4.2, 400 `invalid field` for `marketId`.

#### GET /api/v2/orders/{orderId}

`orderId` is a positive decimal integer (else 400 `invalid orderId`). An open order of the caller answers from the core
(200 `Order`, with `displayQuantity`, `expiresAt`, `budget`). Any other id is looked up in history:

- the caller's row in history: 200 `Order` (history shape) with `asOf` (`{ageMs, stale, eventSeq}`, as on the pages of 4.6); an open order from the core has no `asOf`;
- no row, and the id is above the highest order id the projection has applied: 503 `HISTORY_UNAVAILABLE` with
  `Retry-After` `GATEWAY_HISTORY_LAG_RETRY_AFTER_MS` (1000): the order may exist and not be projected yet;
- otherwise 404 `NOT_FOUND` `not found`. An unknown order and another user's order give the same answer.

Errors: 4.1, 4.2, and the history errors of 4.6.

#### PUT /api/v2/orders/{orderId}

Amend. Body `{price?, quantity?}`: at least one, each above 0. `quantity` is the new total quantity. `Idempotency-Key`
required.

```json
{"price":"64990"}
```

200:

```json
{"orderId":"1042","status":"PENDING_REPLACE","duplicate":false}
```

Errors: 4.1 to 4.3, 400 `invalid field` (`field 'price' must be positive`, `field 'quantity' must be positive (the new
total quantity)`, `field 'price' or quantity is required`), `MISSING_IDEMPOTENCY_KEY`. Typical core answers: 404
`UNKNOWN_ORDER`, 409 `NOT_OPEN`, `AMEND_PENDING`, 422 as table 4.3.

#### DELETE /api/v2/orders/{orderId}

Cancel. No body (a body is 400 `a cancel takes no body`). `Idempotency-Key` optional.

200:

```json
{"orderId":"1042","status":"PENDING_CANCEL","duplicate":false}
```

- With a key, a retry gets the stored answer (`duplicate: true`). A cancel of an order already pending cancel is acked
  with `duplicate: true`.
- Without a key: if the gateway had to offer the cancel more than once (core session loss or leader change in flight)
  and the core then answers `UNKNOWN_ORDER` or `NOT_OPEN`, the answer is 200
  `{"orderId":"1042","status":"NOT_OPEN","duplicate":true,"reoffered":true}`: the order is not open, and whether this
  request closed it is not known. An acknowledged cancel that was re-offered also carries `"reoffered":true`.
- A single-offer cancel of an order that is no longer open answers 404 (`UNKNOWN_ORDER`; the core keeps no terminal
  order) or 409 (`NOT_OPEN`). `NOT_OWNER` is always 404.

Send an `Idempotency-Key` when you need the exact answer. A keyed cancel takes a slot of the per-user request cap.

#### POST /api/v2/orders/cancel

Bulk cancel, as a loop of unkeyed cancels in parallel. Body, exactly one of:

- `{"orderIds":["1042","1043"]}`: a non-empty array of distinct order id strings;
- `{"all":true}` or `{"all":true,"marketId":"1"}`: the caller's open orders (of that market), in orderId order.

At most `GATEWAY_BULK_CANCEL_MAX` (200) orders are cancelled per request; the rest is reported, never looped over.

200:

```json
{"results":[{"orderId":"1042","state":"CANCEL_PENDING"},
            {"orderId":"1043","state":"NOT_OPEN","reoffered":true},
            {"orderId":"1044","state":"NOT_FOUND"}],
 "requested":3,"accepted":1,"truncated":false,"remaining":0}
```

| `state` | Meaning | Extra fields |
|---|---|---|
| `CANCEL_PENDING` | the core accepted the cancel | |
| `NOT_OPEN` | the order is not open (also an `UNKNOWN_ORDER` after a re-offer) | |
| `NOT_FOUND` | unknown or not yours | |
| `FAILED` | another core reject | `"code":"REJECTED","reason":"<core reason>"` |
| `OUTCOME_UNKNOWN` | offered, unanswered by the deadline | `"code":"OUTCOME_UNKNOWN"` |
| `NOT_SENT` | never offered | `"code":"INTERNAL"`, `"CORE_UNAVAILABLE"` or `"OVERLOADED"` |

Any item answered after a re-offer carries `"reoffered":true`. `accepted` counts `CANCEL_PENDING`. `truncated` and
`remaining` say how many ids were not sent. `{"all":true}` with nothing open answers
`{"results":[],"requested":0,"accepted":0,"truncated":false,"remaining":0}`.

Rate cost: `orderIds` takes one principal token per id sent plus one; `all` takes one for the read, then one per order
up to `GATEWAY_PRINCIPAL_BURST - 1`. A request never costs more than one full bucket. A 429 here means nothing was sent;
wait for `Retry-After` and resend.

Errors: 4.1, 4.2 (for the open-orders read of `all`), 400 `invalid field` (`field 'orderIds' or all:true is required,
not both`, `must be a non-empty array of order ids`, `must hold decimal strings`, `lists order N twice`,
`field 'marketId' goes with all:true only`, `field 'all' must be true`).

### 4.6 History

`GET /api/v2/orders/history`, `GET /api/v2/fills` and the not-open case of `GET /api/v2/orders/{orderId}` read the
history projection, a PostgreSQL read model written by a separate projector. They are served when `GATEWAY_PG_URL` is
set and the projection's relations exist; otherwise they answer 503 `HISTORY_UNAVAILABLE`. No order or balance route
touches PostgreSQL.

| Route | Query parameters | Items, newest first |
|---|---|---|
| `GET /orders/history` | `limit`, `cursor`, `marketId`, `status` | `Order` (history shape), by `(updatedAt, orderId)` |
| `GET /fills` | `limit`, `cursor`, `marketId`, `orderId` | `Fill`, by `(executedAt, tradeId, orderId)` |

```http
GET /api/v2/fills?limit=2&marketId=1
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMDA3IiwiZXhwIjoxNzYzMzgwMDAwfQ.sig
```

```json
{"items":[{"tradeId":"500231","orderId":"1042","marketId":"1","side":"BUY","price":"65000.5","quantity":"0.004",
           "maker":true,"executedAt":1763373601200},
          {"tradeId":"500230","orderId":"1042","marketId":"1","side":"BUY","price":"65000.5","quantity":"0.006",
           "maker":true,"executedAt":1763373601195}],
 "nextCursor":"ZjF8...","asOf":{"ageMs":120,"stale":false,"eventSeq":88230}}
```

- `limit`: default `GATEWAY_HISTORY_LIMIT_DEFAULT` (100); above `GATEWAY_HISTORY_LIMIT_MAX` (1000) is 400, never
  clamped.
- `status`: one of the ten order statuses (else 400).
- Paging is keyset only: send `nextCursor` back unchanged with the same filters. `nextCursor: null` is the last page.
  A cursor of the other route, of other filters, or one that does not decode is 400 `invalid cursor`. There is no
  offset and no total. A row whose key does not change is served exactly once in a walk; an order updated during a walk
  moves to the top, so that walk does not serve it again, and a walk started over from the top does.
- `asOf.ageMs`: history is complete as of this long ago (database clock). `asOf.stale`: `ageMs` above `GATEWAY_HISTORY_STALE_MS` (5000). `asOf.eventSeq`: every event up to it is in the answer.
- Each history read takes one principal token.
- Each read has one deadline, `GATEWAY_PG_STATEMENT_TIMEOUT_MS` (2000), counted from when it is queued.

| Status | code | error | Retry-After |
|---|---|---|---|
| 400 | `VALIDATION` | `invalid cursor` (detail `send the nextCursor of the previous page unchanged`, `the cursor belongs to another route`, `the cursor was issued for another filter: keep marketId, status and orderId as on the first page`), `malformed query string`, `invalid field` | |
| 503 | `HISTORY_BUSY` | `too many history reads at once` (queue `GATEWAY_PG_QUEUE` 64 full, or the read waited past its deadline) | `GATEWAY_HISTORY_BUSY_RETRY_AFTER_MS` (1000) |
| 503 | `HISTORY_UNAVAILABLE` | `history is not available` (no URL, no relations, no heartbeat, unreachable, timeout, another core genesis, older than `GATEWAY_HISTORY_MAX_AGE_MS` when set) | `GATEWAY_HISTORY_RETRY_AFTER_MS` (60000) |
| 503 | `HISTORY_UNAVAILABLE` | same text, for an order not projected yet | `GATEWAY_HISTORY_LAG_RETRY_AFTER_MS` (1000) |

### 4.7 Balances

#### GET /api/v2/balances

Every asset in the core config, zero balances included, read from the core.

```json
{"asOf":{"logPosition":51234816},
 "assets":[{"assetId":"1","asset":"BTC","available":"0.5","locked":"0","total":"0.5"},
           {"assetId":"2","asset":"USD","available":"9349.995","locked":"650.005","total":"10000"}]}
```

Read cache as for `GET /orders`. Errors: 4.1, 4.2.

## 5. User stream: `/ws/v2`

`oe-stream` serves the caller's own orders, fills and balances. It holds no state that survives a restart.

### 5.1 Connecting

Path `/ws/v2` only (other paths 404). Any query string is 400. A plain HTTP request is 426. Exactly one credential, by
one of:

1. `Sec-WebSocket-Protocol: bearer, <token>` (browsers). The server answers with the `bearer` subprotocol. Any other
   subprotocol value is 401.
2. `Authorization: Bearer <token>` or `X-API-Key: <key>` (non-browser clients).
3. No credential header: the upgrade is accepted and the first client frame must be
   `{"op":"auth","token":"<token>"}` within `STREAM_WS_AUTH_TIMEOUT_MS` (5000). No frame confirms success; a failure
   closes the connection.

The subprotocol and the first frame carry a bearer token (demo, JWT or dev). An API key travels only in `X-API-Key`.
A header credential that fails is answered over HTTP before the upgrade: 401 `UNAUTHORIZED`, 429 `RATE_LIMITED`,
503 `AUTH_UNAVAILABLE` or `AUTH_BUSY`, with the error body of 1.1.

```http
GET /ws/v2 HTTP/1.1
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Version: 13
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Protocol: bearer, q3W0mB8x...
```

At most `STREAM_MAX_CONNECTIONS` (2000) connections per instance; above that the socket is closed at accept.

### 5.2 Client frames

Text frames of one JSON object, at most `STREAM_WS_MAX_MESSAGE_BYTES` (65536).

| Frame | Notes |
|---|---|
| `{"op":"subscribe","channels":["orders","fills","balances"],"resume":{"eventSeq":88211}}` | `channels` a non-empty list; `resume` optional (5.5) |
| `{"op":"unsubscribe","channels":["balances"]}` | |
| `{"op":"ping"}` | answered with `pong` |

A bad frame is answered with an `error` frame and the connection stays open: not a JSON object, no `op`, unknown `op`,
unknown key, unknown channel, a bad `resume`, a second `auth`. A full internal command ring answers `error` with code
`OVERLOADED` ("the stream is overloaded; send the op again").

### 5.3 Server frames

Every server frame has `type`. Ids and money follow section 1; `eventSeq` is a JSON integer.

| `type` | Body | When |
|---|---|---|
| `subscribed` | `{"type":"subscribed","channels":["orders","fills"]}` | after every subscribe and unsubscribe; lists the channels now active |
| `snapshot` (orders) | `{"type":"snapshot","channel":"orders","asOf":{"eventSeq":88200},"orders":[Order,...]}` | on subscribe to `orders` and after a `resync` |
| `snapshot` (balances) | `{"type":"snapshot","channel":"balances","assets":[{"assetId":"2","asset":"USD","available":"9349.995","locked":"650.005"}]}` | on subscribe to `balances`, and after the stream re-registers with the core |
| `order` | `{"type":"order","eventSeq":88212,"event":"FILL","order":Order}` | every order event of the user; `event` is the core event kind |
| `fill` | `{"type":"fill","eventSeq":88212,"fill":{"tradeId":"500231","orderId":"1042","marketId":"1","side":"BUY","price":"65000.5","quantity":"0.004","maker":null,"executedAt":1763373601200}}` | one per `FILL` event with a trade (`tradeId` above 0; a `FILL` without one goes out as an `order` frame only); never coalesced; `maker` is `null` on the stream (it is set in history) |
| `balance` | `{"type":"balance","assetId":"2","asset":"USD","available":"9349.995","locked":"650.005"}` | each change of one of the user's balances |
| `admission` | `{"type":"admission","marketId":"0","gate":"SUSPENDED","reasons":["ADMIN"],"circuitBreaker":false}` | to every connection, subscribed or not |
| `heartbeat` | `{"type":"heartbeat","eventSeq":88230}` | every `STREAM_HEARTBEAT_MS` (5000) to connections with `orders` or `fills` active |
| `pong` | `{"type":"pong","eventSeq":88230}` | answer to `ping`; `eventSeq` is `null` while no event channel is live or a resume is pending |
| `resync` | `{"type":"resync","reason":"EVENT_GAP"}` | the stream cannot continue the sequence (5.5) |
| `error` | `{"type":"error","code":"VALIDATION","error":"unknown channel (orders, fills, balances)"}` | a refused client frame; codes `VALIDATION`, `OVERLOADED`, `FEATURE_DISABLED` |
| `engine` | `{"type":"engine","core":{...},"matchingEngine":{...},"bridge":{...}}` | on connect and on change, at most once per `STREAM_ENGINE_FRAME_MIN_MS` (1000); display telemetry, not user data |
| `election` | `{"type":"election","cluster":"core","phase":"start",...}` | a leader election seen by the engine poller (`phase` `start` or `end`) |

Order rows in an `order` frame carry `hold.remaining` and no `displayQuantity`, `expiresAt` or `budget`. Rows in an
orders snapshot have the open-orders shape unless an event changed them during the snapshot read.

When the `balances` channel is off on the instance (`STREAM_BALANCE_STREAM=off`), a subscribe that names it gets
`{"type":"error","code":"FEATURE_DISABLED","error":"the balances channel is off on this stream (STREAM_BALANCE_STREAM)"}`
and the other channels are subscribed.

Order of frames after `subscribe` to `orders` and `fills`: `subscribed`, the orders `snapshot`, the `fill` frames of
events folded into the snapshot, then live `order` and `fill` frames above `asOf.eventSeq`. Subscribing to `fills`
alone sends no snapshot; fills flow from the next event.

`engine` frame fields: `core` and `matchingEngine` each hold `{configured, term, leaderId, nodes:[{id, role, live,
recovery, term, commitPosition, appliedPosition, behindBytes, entriesBehind, snapshot:{position, atMs, stateHash,
agreement}, version, buildCommit, error}], lastElection, snapshot:{position, atMs, agreement, divergedNodes}}`;
`bridge` is `{consumedJournalSeq, meJournalSeq, entriesBehind}`. Fields with no source are `null`. `election` start
frames carry `atMs, previousTerm, previousLeaderId, lastCommitPosition, candidates`; end frames carry `atMs,
previousTerm, previousLeaderId, term, leaderId, firstEntryPosition, durationMs, observedGapMs, orderEntryReopened`.

### 5.4 Heartbeat and liveness

- `eventSeq` is the exchange-wide, dense order event sequence. The stream's watermark is the highest `eventSeq` up to
  which it has received every event. `heartbeat` and `pong` carry it, so a quiet user's position keeps moving.
- The server sends a WebSocket ping every `STREAM_WS_PING_MS` (15000) while it is not writing. A connection that sent
  nothing (no frame, no pong) for `STREAM_WS_IDLE_MS` (60000) is closed 1001. The ping interval is always below the idle bound.

### 5.5 Resume and resync

Keep the last `eventSeq` of any frame you received (`order`, `fill`, `heartbeat`, `pong`, `snapshot.asOf`). On
reconnect, subscribe with `"resume":{"eventSeq":N}`:

- the stream's window (`STREAM_EVENT_WINDOW`, 65536 events) holds every event above N: your events above N are
  replayed as `order` and `fill` frames, with no snapshot;
- the window is younger than N but the core's event ring still covers N: the stream backfills its window once from the
  core, then replays;
- N is above the stream's watermark: the resume waits until the stream reaches N;
- otherwise (N too old, or ahead of anything the core has): `resync` with reason `RESUME_TOO_OLD`, then a fresh
  snapshot.

`resync` reasons: `EVENT_GAP` (events were lost and could not be replayed), `LEADER_CHANGE` and `SESSION_LOST` (the
stream's core session changed and the replay could not cover the hole), `RESUME_TOO_OLD`. After a `resync` the stream
sends a new orders snapshot to connections subscribed to `orders`; a `fills`-only connection continues with the next
fill. Balances are not sequenced: they are re-sent as a snapshot when the stream registers with the core again, and a
`resync` from an event gap does not resend them.

**Closing the fills gap of a resync.** After a `resync` take E, the `eventSeq` of the first frame that follows it: the orders `snapshot` (`asOf.eventSeq`), or for a fills-only connection the first `heartbeat`. Read `GET /api/v2/fills` from the newest down to your last known fill, again until an answer's `asOf.eventSeq` is at least E, and drop duplicates by (tradeId, orderId). Where no history projection runs, `/fills` answers 503 `HISTORY_UNAVAILABLE` and the gap cannot be closed.

### 5.6 Slow consumers

Each connection has an outbound bound of `STREAM_WS_QUEUE_MESSAGES` (256) frames and `STREAM_WS_QUEUE_BYTES`
(1048576) bytes, counting frames waiting in the stream and frames the writer holds unwritten. The byte test leaves out
the largest frame sent since the connection was last empty, so one snapshot larger than the byte bound still goes out.
Queued `order` frames of one order coalesce to the latest state; `fill` frames never coalesce. At the bound the
connection is closed with code 4001 (`RESYNC`): nothing is dropped silently. Reconnect and resume from your last
`eventSeq`. A client that stopped reading cannot receive the close frame; its socket is closed after
`STREAM_WS_IDLE_MS`. One slow client never delays another.

### 5.7 Close codes

| Code | Reason |
|---|---|
| 1001 | idle (`STREAM_WS_IDLE_MS`) |
| 1003 | a binary frame (text frames only) |
| 1008 | first-frame auth failed (`UNAUTHORIZED`), timed out (`AUTH_TIMEOUT`), or the first frame was not `{"op":"auth","token":"..."}` |
| 1009 | a client frame above `STREAM_WS_MAX_MESSAGE_BYTES` |
| 1013 | first-frame auth could not be checked (`AUTH_UNAVAILABLE`), or the stream could not open the connection (`OVERLOADED`) |
| 4001 | `RESYNC`: the outbound bound was reached (5.6) |
| 4003 | a client frame named a user |

## 6. Rate and size settings a client can hit

| Setting | Default | Effect |
|---|---|---|
| `GATEWAY_PRINCIPAL_RATE`, `GATEWAY_PRINCIPAL_BURST` | 50/s, 100 | per-user bucket for every core-backed and history route; 429 |
| `GATEWAY_QUERY_RATE`, `GATEWAY_QUERY_BURST` | 100/s, 200 | core reads per gateway instance; a read that waits past its deadline is 503 `OVERLOADED` |
| `GATEWAY_QUERY_MIN_INTERVAL_MS` | 250 | read cache of `GET /orders` and `GET /balances` |
| `GATEWAY_BULK_CANCEL_MAX` | 200 | ids cancelled per bulk request |
| `GATEWAY_REQUEST_TIMEOUT_MS` | 5000 | core deadline; 504 or 503 after it |
| `GATEWAY_MAX_INFLIGHT` | 4096 | requests held for the core per instance; 503 `OVERLOADED` |
| `GATEWAY_MAX_BODY_BYTES`, `GATEWAY_MAX_HEADER_BYTES` | 65536, 8192 | 413, 431 |
| `GATEWAY_MAX_CONNECTIONS` | 2000 | HTTP connections per instance; above, closed at accept |
| `GATEWAY_HTTP_IDLE_TIMEOUT_MS` | 60000 | idle keep-alive connection closed |
| `GATEWAY_HISTORY_LIMIT_DEFAULT`, `GATEWAY_HISTORY_LIMIT_MAX` | 100, 1000 | history page size |
| `GATEWAY_PG_QUEUE`, `GATEWAY_PG_STATEMENT_TIMEOUT_MS` | 64, 2000 | history queue and deadline |
| `GATEWAY_REGISTER_RATE`/`_BURST`, `GATEWAY_LOGIN_RATE`/`_BURST`, `GATEWAY_AUTH_SOURCE_RATE`/`_BURST` | 1/s 5, 10/s 20, 10/min 10 | register and login |
| `GATEWAY_TOKEN_LOOKUP_RATE`/`_BURST` | 60/min, 20 | unknown bearer tokens per source |
| `GATEWAY_FAUCET_RATE`, `GATEWAY_FAUCET_MAX_AMOUNT` | 1/s, per asset | treasury lane |
| core request TTL (`idempotencyWindowMs`) and per-user request cap | 900000 ms, 256 | replicated core settings (section 3) |
| `STREAM_WS_QUEUE_MESSAGES`, `STREAM_WS_QUEUE_BYTES` | 256, 1048576 | 4001 close |
| `STREAM_WS_MAX_MESSAGE_BYTES` | 65536 | 1009 close |
| `STREAM_WS_AUTH_TIMEOUT_MS`, `STREAM_WS_PING_MS`, `STREAM_WS_IDLE_MS` | 5000, 15000, 60000 | stream liveness |
| `STREAM_HEARTBEAT_MS` | 5000 | heartbeat frame |
| `STREAM_EVENT_WINDOW` | 65536 | events a resume can be served from without the core |
| `STREAM_USER_LINGER_MS` | 30000 | a reconnect inside it is served from the stream's cache |
| `STREAM_QUERY_RATE`, `STREAM_QUERY_BURST` | 50/s, 10 | core reads per stream instance (snapshots, balances) |
| `STREAM_MAX_CONNECTIONS` | 2000 | WebSocket connections per instance |

Rates are written `<n>/s` or `<n>/min`.

## 7. Treasury lane (demo faucet)

Deposit and withdraw exist for demos that hand out play money. The lane is **off by default**. It is on only when both
`GATEWAY_TREASURY_CREDENTIAL` is set and `GATEWAY_ALLOW_TREASURY_LANE=1`, and then `GATEWAY_FAUCET_MAX_AMOUNT` is
required; any other combination stops the process at boot. `/health` shows `"treasuryLane":"on"` or `"off"`.

A treasury credential can credit and debit any user for any amount. The caps below live in the gateway process, so
they guard against bugs, not against whoever controls that process. Production deployments keep the lane off.

With the lane off both routes answer 404 `FEATURE_DISABLED` `the treasury lane is off` (detail `deposit and withdraw
are a demo feature (GATEWAY_ALLOW_TREASURY_LANE)`), before any other check, and registration funds nothing.

#### POST /api/v2/balances/deposit, POST /api/v2/balances/withdraw

Always for the caller's own userId. `Idempotency-Key` required.

```http
POST /api/v2/balances/deposit
Authorization: Bearer q3W0mB8x...
Idempotency-Key: faucet-2026-11-17-001
Content-Type: application/json

{"assetId":"2","amount":"1000"}
```

200:

```json
{"assetId":"2","amount":"1000","available":"10349.995","duplicate":false}
```

`available` is the new available balance of that asset.

| Status | code | error | detail |
|---|---|---|---|
| 400 | `VALIDATION` | `invalid field` | `field 'assetId' ...` (0 to 2147483647), `field 'amount' must be above zero` |
| 400 | `VALIDATION` | `unknown asset` | `assetId N is not in the core's asset list (/api/v2/markets)` |
| 400 | `MISSING_IDEMPOTENCY_KEY` | | |
| 404 | `FEATURE_DISABLED` | `the treasury lane is off` | |
| 409 | `CONFLICT` | `conflict` | reason `REQUEST_ID_MISMATCH` |
| 422 | `REJECTED` | `above the faucet cap` | `at most <cap> of asset N per request`, or `asset N has no GATEWAY_FAUCET_MAX_AMOUNT entry` |
| 422 | `REJECTED` | `rejected by the core` | reason `INSUFFICIENT_FUNDS` (withdraw), `INVALID_AMOUNT`, `BALANCE_OVERFLOW` |
| 429 | `RATE_LIMITED` | `too many requests` | `the faucet rate (GATEWAY_FAUCET_RATE) is spent`, or the principal budget |
| 503 | `CORE_UNAVAILABLE` | `the core config is not loaded yet` | |
| 504 | `OUTCOME_UNKNOWN` | `the core did not answer in time` | retry with the same key; the retry also takes a faucet token |

Plus 4.1 and 4.2. `GATEWAY_FAUCET_MAX_AMOUNT` is `assetId:amount,...` (each amount at most 92233720368.54775807, or the gateway does not start); an asset without an entry cannot be deposited
or withdrawn through the lane. `GATEWAY_FAUCET_RATE` (1/s) is per user; its burst is one period's count.

Registration funding: when the lane is on, register deposits each capped asset of the config its cap, with a request
id derived from the user and asset (never random). A deposit whose outcome is unknown is re-sent once inside the same
register request, and only when `idempotencyWindowMs` is longer than twice `GATEWAY_REQUEST_TIMEOUT_MS`; otherwise
`funded` is `false`. A user completes a partial funding through the deposit route with a key of their own. Each
funding deposit takes a slot of the user's request cap for the window.

## 8. Operator endpoints

Served on the admin bind only, from the moment the process binds. `GET` only (other methods 405, other paths 404, plain
text).

### /health

200 when the media driver is connected and the duty thread's heartbeat is younger than its watchdog bound, else 503.
JSON:

```json
{"status":"UP","process":"oe-gateway","version":"...","build":"...","clusterTools":"...",
 "moneySchema":{"version":9,"sha256":"..."},
 "driver":{"mode":"external","dir":"...","timeoutMs":10000,"reclaimed":false,"connected":true,"conductorHeartbeatAgeMs":12},
 "duty":{"heartbeatAgeMs":1,"watchdogMs":5000,"maxCycleUs":310,"iterations":123456},
 "session":{"connected":true,"registered":true,"epoch":1,"leader":0,"keepaliveAgeMs":420},
 "config":{"loaded":true,"gateOpen":true,"timeoutStreak":0},
 "storage":{"starved":false,"starvedSeconds":0},
 "treasuryLane":"off",
 "auth":{"modes":["api-key","jwt"],"hashQueue":0,"hashQueueCapacity":32,"userIdConflicts":0},
 "history":{"state":"...","ageMs":420,"stale":false,"cause":null,"eventSeq":88230,"probedAgoMs":800,"queue":0,"queueCapacity":64},
 "ready":true,"conditions":[...],"aeronErrors":0,"logDropped":0,
 "settings":{"GATEWAY_PRINCIPAL_RATE":{"value":"50/s","source":"default"}}}
```

`status` is `STARTING` (no driver yet), `UP` or `STALLED`. `oe-stream` has `events:{gapOpen, headProbeAgeMs}` in place
of `config`, `storage` and `treasuryLane`. Secrets in `settings` are redacted.

### /ready

200 `{"ready":true,"failing":[]}` or 503 `{"ready":false,"failing":["session_registered","gate_open"]}`.

- `oe-gateway`: `driver_connected`, `storage_space`, `session_connected`, `session_registered`, `config_loaded`
  (core config version above 0), `gate_open` (global gate), `keepalive_fresh` (`GATEWAY_READY_STALE_MS` 3000),
  `timeout_streak` (fewer than `GATEWAY_READY_TIMEOUT_STREAK` 5 deadlines missed in a row), `public_listener`.
- `oe-gateway` also serves `/ready/session` (`driver_connected`, `session_connected`, `session_registered`): route on it
  to keep cancels and balance reads reachable while the gate is closed.
- `oe-stream`: `driver_connected`, `session_registered`, `event_gap` (no unresolved gap), `head_probe_fresh`
  (`STREAM_READY_STALE_MS` 15000), `ws_listener`. A closed gate does not affect it.

### /metrics

Prometheus text, prefix `gateway_` or `stream_`. Among them: `requests_total{route,code}`, `principal_rate_limited_total`,
`read_cache_hits_total`, `cancel_reoffered_not_open_total`, `outcome_unknown_total`, `reoffers_total`,
`late_replies_total`, `history_reads_total{query,result,cause}` (causes add `epoch`, `stale`), `history_age_ms`, `user_id_conflict_total`, `inflight`, `ready`,
`ready_condition{name}`; for the stream `resync_total{reason}`, `ws_closed_total{code}`, `ws_closed_at_bound_total`,
`ws_queue_high_water`, `watermark`, `window_first_seq`, `users_cached`.

## 9. Migration from the OMS v1 API

| OMS v1 | v2 | What changes for a client |
|---|---|---|
| `/api/v1/*` and `/ws/v1` on one process | `/api/v2/*` on `oe-gateway`, `/ws/v2` on `oe-stream` | old paths answer 404; two upstreams behind the proxy |
| `POST /orders` without a key | `Idempotency-Key` required | a create without it is 400 `MISSING_IDEMPOTENCY_KEY` |
| body `orderType`, optional `timeInForce`, no expiry | `type`; `timeInForce` required; `expiresAt` (required with GTD); `budget`; `clientOrderId` as a UUID | `orderType` is now an unknown field (400); GTD without `expiresAt` is 400; free-text `clientOrderId` is 400 |
| response `{accepted, omsOrderId, rejectReason}` with 201/200/400 | 201/200 with `orderId`; rejects are 4xx/5xx with `{error, code, reason, detail, keyFinal}` | no `accepted` field; a reject is never 2xx |
| a create answer is final | 201 = accepted by the core, `PENDING_NEW`; the matching engine's decision arrives as an `order` event | handle a later `REJECTED` with an `ME_*` `reason` and `meReason` |
| `omsOrderId`, `clusterOrderId` | `orderId`, `meOrderId` | rename; ids are the core's |
| `filledQty`, `remainingQty`, `createdAtMs`, `updatedAtMs`, `rejectReason` | `filledQuantity`, `remainingQuantity`, `createdAt`, `updatedAt`, `reason` | rename |
| statuses including `PENDING_RISK`, `PENDING_HOLD` | the core's ten statuses | two gone; `EXPIRED`, `PENDING_CANCEL`, `PENDING_REPLACE` are new |
| FOK emulated by a cancel | FOK on LIMIT is native; other FOK combinations are 400 `UNSUPPORTED_TIF` | |
| `DELETE /orders/{id}` always 200 | 200 when accepted (or `reoffered:true` with `NOT_OPEN`); 404 or 409 otherwise | handle 404 and 409 |
| amend refused in journal mode; LIMIT and LIMIT_MAKER only | amend per the core's rules; answer `PENDING_REPLACE` | key required |
| `GET /orders` memory scan, `?userId`, `?status` | open orders only, from the core, `?marketId`, with `asOf` | `userId` and `status` are refused (use `/orders/history?status=`) |
| `GET /orders/history`, `GET /executions`, offset and limit, bare array | `/orders/history`, `/fills`, keyset `cursor`, `{items, nextCursor, asOf}` | new envelope and paging; 503 `HISTORY_UNAVAILABLE` where no history projection runs |
| `GET /accounts/{userId}` | `GET /balances` | no userId in a path; every asset listed, zero included |
| `POST /accounts/{userId}/deposit` and `/withdraw`, no key | `POST /balances/deposit` and `/withdraw`, key required, treasury lane only | 404 `FEATURE_DISABLED` when the lane is off |
| `GET /markets` needs auth | public, from the core config, with `lotSize`, minimum and maximum quantity and notional, and the asset list | |
| `GET /positions`, `/api/v1/admin/*`, gRPC | not served | removed |
| WebSocket: bare order objects, no sequence, a REST seed race, `userId` honoured in subscribe | typed frames, snapshots, `eventSeq`, `heartbeat`, resume, `resync`, `fills` and `balances` channels, first-frame auth | every frame shape changes; a `userId` in a frame closes the connection 4003; after a `resync` fills may be missing (5.5) |
| `dev` auth on by a default switch; `dev:1` was an admin in demo mode | `dev` only with `GATEWAY_ALLOW_DEV_AUTH=1`; no backdoor; API keys carry no role | bots move to API keys and send idempotency keys |
| JWT `exp` optional | required | |
| demo tokens stored in clear | stored as SHA-256 | old demo tokens do not carry over |
| `OMS_CORS_ORIGINS=*` allowed | explicit origins only | |
| `/health`, `/ready`, `/metrics` on the public port | admin bind only; `/api/v2/status` is public | |
| money accepted as a JSON number | strings only | |
