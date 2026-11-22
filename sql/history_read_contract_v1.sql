-- History read contract, version 1.
--
-- The relations oe-gateway reads for GET /api/v2/orders/history, GET /api/v2/fills, GET /api/v2/orders/{orderId} of an
-- order the core no longer holds open, and GET /api/v2/orders/{orderId}/trail. The gateway only reads them, on
-- read-only connections; it never creates, migrates or writes them. The history projection owns them and may provide
-- each one as a table or as a view with exactly these column names and types. This file is the reference: the
-- gateway's tests seed from it.
--
-- Types:
--   ids            BIGINT (market and asset ids INTEGER), rendered as decimal strings
--   amounts        BIGINT integer units at the wire's fixed-point scale 10^8 (1.5 is 150000000); the gateway renders
--                  them as the REST decimal strings ("1.5"). Never NUMERIC, never a float
--   times          BIGINT epoch milliseconds of core cluster time (*_ms), rendered as JSON integers
--   enums          TEXT holding the core's names (the money schema's)
--   NULL           the REST null of that field; a column marked NOT NULL never holds one
--
-- Paging is keyset only: orders newest first by (updated_at_ms, order_id) DESC, fills newest first by
-- (executed_at_ms, trade_id, order_id) DESC, an order's trail oldest first by event_seq. The indexes below serve every
-- filter the routes offer; a projection that provides views must give the planner equivalent indexes on the underlying
-- tables.
--
-- Orders: the projection writes every order from its creation on (an open order too, updated in place as it changes),
-- not only terminal ones, so that GET /api/v2/orders/{orderId} can tell "no such order" from "not projected yet" (see
-- applied_order_id below). An order updated while a client walks the pages takes a newer updated_at_ms: if the walk
-- has not reached it yet, it moves above the cursor and that walk never serves it (a walk that starts over from the top
-- does). Rows whose key does not change are served exactly once.
--
-- Trail: one row per journal record of an order (every record, from the first; event_seq is the core's eventSeq),
-- ordered by event_seq and by nothing else: core_log_position is not unique (the FILL records of one trade batch share
-- one), and the matching-engine fields are attributes of the record, always with me_epoch beside them (me_epoch 0: the
-- record has no matching-engine cause). The trail of an order and its row in gateway_order_history_v1 are written in
-- the same transaction. GET /api/v2/orders/{orderId}/trail reads the caller's rows only (user_id in the statement);
-- an order with no row of the caller follows the applied_order_id rule below (404 at or below it, 503 above it).
--
-- Heartbeat: beat_at means "every event of this genesis is applied as of this instant". The projection moves it only
-- when it has applied the core's head as a cluster probe answered it, with the genesisId of that answer equal to its
-- epoch and no halt set; beat_at is then the database's clock less the time since the probe was sent. A projection
-- that is behind the head, halted, cut off from the core, or of another genesis does not beat, whatever it has read.
-- applied_order_id is the highest order id it has applied (order ids are global, so this says nothing about any user),
-- applied_event_seq its cursor (every event up to it is applied, committed in the same transaction as the rows), epoch
-- the core genesisId it projects. The gateway answers asOf.ageMs = clock_timestamp() - max(beat_at) in whole
-- milliseconds, never below 0, computed by the database (one clock: no skew between the gateway's and the
-- projection's hosts), asOf.eventSeq = applied_event_seq (read before the rows), and asOf.stale above its
-- GATEWAY_HISTORY_STALE_MS. No row means the projection has never reported: every history read answers 503
-- HISTORY_UNAVAILABLE; so does an epoch other than the genesisId of the gateway's last CoreConfigReport.
-- GET /api/v2/orders/{orderId} of an id above applied_order_id that has no row answers 503 HISTORY_UNAVAILABLE with a
-- short Retry-After (the order may exist and not be projected yet), never 404.
--
-- Roles: the history projection owns these relations (creates, migrates and writes them). The gateway's role is
-- granted SELECT on these four relations and nothing else, for example:
--   GRANT SELECT ON gateway_order_history_v1, gateway_fills_v1, gateway_history_heartbeat_v1, gateway_order_trail_v1
--       TO <gateway role>;
-- (sql/history_projection_v1.sql grants them to the role oe_history_reader, of which the gateway's role is a member.)
-- The gateway also opens its history connections read-only (default_transaction_read_only), but that is a session
-- default, not a privilege: the grant is what keeps the gateway from writing.

CREATE TABLE gateway_order_history_v1 (
    order_id            BIGINT      NOT NULL PRIMARY KEY CHECK (order_id > 0),
    user_id             BIGINT      NOT NULL,
    client_order_id     UUID,                                   -- NULL: none
    market_id           INTEGER     NOT NULL,
    side                TEXT        NOT NULL CHECK (side IN ('BUY', 'SELL')),
    type                TEXT        NOT NULL CHECK (type IN ('LIMIT', 'MARKET', 'LIMIT_MAKER', 'STOP_LOSS', 'STOP_LIMIT',
                                                             'TRAILING_STOP', 'ICEBERG')),
    time_in_force       TEXT        NOT NULL CHECK (time_in_force IN ('GTC', 'IOC', 'FOK', 'GTD')),
    price               BIGINT,                                 -- NULL for MARKET, STOP_LOSS, TRAILING_STOP
    stop_price          BIGINT,                                 -- NULL: none
    quantity            BIGINT      NOT NULL,
    filled_quantity     BIGINT      NOT NULL,
    remaining_quantity  BIGINT      NOT NULL,
    status              TEXT        NOT NULL CHECK (status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED',
                                                               'REJECTED', 'EXPIRED', 'PENDING_NEW', 'PENDING_TRIGGER',
                                                               'PENDING_CANCEL', 'PENDING_REPLACE')),
    reason              TEXT,                                   -- the core's CoreRejectReason name; NULL: none
    me_reason           SMALLINT    CHECK (me_reason BETWEEN 0 AND 254), -- the ME's code; NULL: not ME-sourced
    me_order_id         BIGINT,                                 -- NULL: not yet at the ME
    hold_asset_id       INTEGER     NOT NULL,
    hold_amount         BIGINT      NOT NULL,
    hold_remaining      BIGINT,                                 -- NULL: not known (the field is then left out)
    created_at_ms       BIGINT      NOT NULL,
    updated_at_ms       BIGINT      NOT NULL
);

CREATE INDEX gateway_order_history_v1_user_key
    ON gateway_order_history_v1 (user_id, updated_at_ms DESC, order_id DESC);
CREATE INDEX gateway_order_history_v1_user_market_key
    ON gateway_order_history_v1 (user_id, market_id, updated_at_ms DESC, order_id DESC);
CREATE INDEX gateway_order_history_v1_user_status_key
    ON gateway_order_history_v1 (user_id, status, updated_at_ms DESC, order_id DESC);

-- One row per order side of a trade: a self-trade has two rows of one user with one trade_id.
CREATE TABLE gateway_fills_v1 (
    trade_id            BIGINT      NOT NULL CHECK (trade_id > 0),
    order_id            BIGINT      NOT NULL CHECK (order_id > 0),
    user_id             BIGINT      NOT NULL,
    market_id           INTEGER     NOT NULL,
    side                TEXT        NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price               BIGINT      NOT NULL,
    quantity            BIGINT      NOT NULL,
    maker               BOOLEAN     NOT NULL,
    executed_at_ms      BIGINT      NOT NULL,
    PRIMARY KEY (trade_id, order_id)
);

CREATE INDEX gateway_fills_v1_user_key
    ON gateway_fills_v1 (user_id, executed_at_ms DESC, trade_id DESC, order_id DESC);
CREATE INDEX gateway_fills_v1_user_market_key
    ON gateway_fills_v1 (user_id, market_id, executed_at_ms DESC, trade_id DESC, order_id DESC);
CREATE INDEX gateway_fills_v1_user_order_key
    ON gateway_fills_v1 (user_id, order_id, executed_at_ms DESC, trade_id DESC);

CREATE TABLE gateway_history_heartbeat_v1 (
    singleton           BOOLEAN     NOT NULL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    beat_at             TIMESTAMPTZ NOT NULL,
    applied_order_id    BIGINT      NOT NULL CHECK (applied_order_id >= 0), -- 0: no order applied yet
    applied_event_seq   BIGINT      NOT NULL CHECK (applied_event_seq >= 0), -- every event up to it is in the rows
    epoch               BIGINT      NOT NULL CHECK (epoch <> 0)             -- the core genesisId projected
);

-- One row per journal record of an order, oldest first by event_seq.
CREATE TABLE gateway_order_trail_v1 (
    order_id            BIGINT      NOT NULL CHECK (order_id > 0),
    user_id             BIGINT      NOT NULL,
    event_seq           BIGINT      NOT NULL PRIMARY KEY CHECK (event_seq > 0),
    event               TEXT        NOT NULL,                   -- the core's EventKind name (ACCEPTED, FILL, ...)
    status              TEXT        NOT NULL,                   -- the order's OrderStatus after the record
    core_log_position   BIGINT      NOT NULL,                   -- the core log position of the applying entry
    me_epoch            BIGINT      NOT NULL,                   -- 0: no matching-engine cause
    me_log_position     BIGINT      NOT NULL,                   -- the ME egress sequence of the causing record
    me_journal_seq      BIGINT      NOT NULL,
    liquidity           TEXT        NOT NULL CHECK (liquidity IN ('NONE', 'TAKER', 'MAKER')),
    trade_id            BIGINT      NOT NULL,                   -- 0: no trade
    last_price          BIGINT      NOT NULL,
    last_qty            BIGINT      NOT NULL,
    hold_remaining      BIGINT      NOT NULL,
    at_ms               BIGINT      NOT NULL,                   -- the record's updatedAtMs (core cluster time)
    me_timestamp_ms     BIGINT      NOT NULL                    -- the causing ME record's time (ME cluster time)
);

CREATE INDEX gateway_order_trail_v1_order_key
    ON gateway_order_trail_v1 (order_id, event_seq);
