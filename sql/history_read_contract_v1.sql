-- History read contract, version 1.
--
-- The relations oe-gateway reads for GET /api/v2/orders/history, GET /api/v2/fills and GET /api/v2/orders/{orderId}
-- of an order the core no longer holds open. The gateway only reads them, on read-only connections; it never creates,
-- migrates or writes them. The history projection owns them and may provide each one as a table or as a view with
-- exactly these column names and types. This file is the reference: the gateway's tests seed from it.
--
-- Types:
--   ids            BIGINT (market and asset ids INTEGER), rendered as decimal strings
--   amounts        BIGINT integer units at the wire's fixed-point scale 10^8 (1.5 is 150000000); the gateway renders
--                  them as the REST decimal strings ("1.5"). Never NUMERIC, never a float
--   times          BIGINT epoch milliseconds of core cluster time (*_ms), rendered as JSON integers
--   enums          TEXT holding the core's names (the money schema's)
--   NULL           the REST null of that field; a column marked NOT NULL never holds one
--
-- Paging is keyset only, newest first: orders by (updated_at_ms, order_id) DESC, fills by
-- (executed_at_ms, trade_id, order_id) DESC. The indexes below serve every filter the routes offer; a projection that
-- provides views must give the planner equivalent indexes on the underlying tables.
--
-- Orders: the projection writes every order from its creation on (an open order too, updated in place as it changes),
-- not only terminal ones, so that GET /api/v2/orders/{orderId} can tell "no such order" from "not projected yet" (see
-- applied_order_id below). An order updated while a client walks the pages takes a newer updated_at_ms: if the walk
-- has not reached it yet, it moves above the cursor and that walk never serves it (a walk that starts over from the top
-- does). Rows whose key does not change are served exactly once.
--
-- Heartbeat: the projection writes beat_at = now() whenever it has applied everything it has read (also while idle),
-- and applied_order_id = the highest order id it has applied (order ids are global, so this says nothing about any
-- user). The gateway answers asOf.ageMs = clock_timestamp() - max(beat_at) in whole milliseconds, never below 0,
-- computed by the database (one clock: no skew between the gateway's and the projection's hosts). No row means the
-- projection has never reported: every history read answers 503 HISTORY_UNAVAILABLE. GET /api/v2/orders/{orderId} of
-- an id above applied_order_id that has no row answers 503 HISTORY_UNAVAILABLE with a short Retry-After (the order may
-- exist and not be projected yet), never 404.
--
-- Roles: the history projection owns these relations (creates, migrates and writes them). The gateway's role is
-- granted SELECT on these three relations and nothing else, for example:
--   GRANT SELECT ON gateway_order_history_v1, gateway_fills_v1, gateway_history_heartbeat_v1 TO <gateway role>;
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
    applied_order_id    BIGINT      NOT NULL CHECK (applied_order_id >= 0) -- 0: no order applied yet
);
