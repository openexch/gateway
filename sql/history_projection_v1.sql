-- History projection, version 1.
--
-- Written by oe-projector, the one process in this repository that writes a database. It follows the core's order
-- journal and writes the base tables below; oe-gateway reads the history through the read relations at the end of
-- this file, which keep the exact names, columns and types of sql/history_read_contract_v1.sql (each relation may be
-- a table or a view; here they are views).
--
-- oe-projector applies this file at boot, in one transaction, under a transaction-scoped advisory lock (N copies may
-- start at once), and records it in oe_projection_schema with its sha256; a recorded sha256 that differs from the
-- binary's own copy refuses the start. The projector's role owns every relation. The read relations are granted SELECT
-- to the role oe_history_reader and nothing else is granted to it; the gateway's login role is made a member of it
-- (GRANT oe_history_reader TO <gateway role>). This file creates oe_history_reader when it does not exist, which needs
-- CREATEROLE; a projector role without it needs the role created beforehand (CREATE ROLE oe_history_reader NOLOGIN).
--
-- Rules:
--   - a projected row is a function of the journal bytes only: no wall clock, host, source name or arrival order
--     reaches oe_order_events, oe_orders or oe_fills. Wall-clock columns live only in the three projector state
--     tables (oe_projection_state, oe_projection_source_memo, oe_projection_replay);
--   - no key, unique index or ordering uses a matching-engine field: ME sequences and positions are attributes,
--     always stored with me_epoch beside them; the only orderings are journal_seq and the read contract's keys;
--   - core_log_position is not unique (the FILL records of one trade batch share one);
--   - one writer: the state row holds a lease (writer_token, lease_until, both on the database's clock) and every
--     write transaction of the projector ends with a conditional UPDATE of this row on its own token; zero rows
--     updated aborts the transaction.

CREATE TABLE oe_projection_state (
    singleton             BOOLEAN     NOT NULL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    epoch                 BIGINT      CHECK (epoch <> 0),           -- the core's genesisId; NULL: none learned yet
    applied_seq           BIGINT      NOT NULL DEFAULT 0 CHECK (applied_seq >= 0), -- the cursor: every journalSeq
                                                                    -- up to it is applied, in this transaction order
    applied_log_position  BIGINT      NOT NULL DEFAULT 0,
    applied_order_id      BIGINT      NOT NULL DEFAULT 0 CHECK (applied_order_id >= 0),
    head_seq              BIGINT,                                   -- the core's head at the last answered probe
    beat_at               TIMESTAMPTZ,                              -- NULL: this projection has not beaten yet
    writer_token          TEXT,                                     -- the lease holder's token; NULL: released
    lease_until           TIMESTAMPTZ,                              -- NULL: free
    halt_reason           TEXT,
    halt_detail           TEXT,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (applied_seq = 0 OR epoch IS NOT NULL)                    -- nothing is applied before the genesis is known
);
INSERT INTO oe_projection_state (singleton) VALUES (TRUE);

-- One resume memo per source (an archive control endpoint as configured): where the last committed record of that
-- source ended. A hint, never authority, never used on another source.
CREATE TABLE oe_projection_source_memo (
    source                TEXT        NOT NULL PRIMARY KEY,
    archive_id            BIGINT      NOT NULL,
    recording_id          BIGINT      NOT NULL,
    start_timestamp       BIGINT      NOT NULL,
    initial_term_id       INTEGER     NOT NULL,
    session_id            INTEGER     NOT NULL,
    term_buffer_length    INTEGER     NOT NULL,
    mtu_length            INTEGER     NOT NULL,
    stream_id             INTEGER     NOT NULL,
    end_position          BIGINT      NOT NULL,
    seq                   BIGINT      NOT NULL CHECK (seq > 0),
    epoch                 BIGINT      NOT NULL CHECK (epoch <> 0),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- One row per replay this projection started and has not stopped: recorded before any record of it is applied, so a
-- successor (a restart, or a standby taking over) can stop it by its session id.
CREATE TABLE oe_projection_replay (
    source                TEXT        NOT NULL,
    archive_id            BIGINT      NOT NULL,
    replay_session_id     BIGINT      NOT NULL,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source, replay_session_id)
);

-- Every journal record, append only, with its 216 message bytes (the duplicate check compares them).
CREATE TABLE oe_order_events (
    journal_seq           BIGINT      NOT NULL PRIMARY KEY CHECK (journal_seq > 0),
    order_id              BIGINT      NOT NULL CHECK (order_id > 0),
    user_id               BIGINT      NOT NULL,
    event                 TEXT        NOT NULL,
    status                TEXT        NOT NULL,
    core_log_position     BIGINT      NOT NULL,
    liquidity             TEXT        NOT NULL,
    me_epoch              BIGINT      NOT NULL,
    me_journal_seq        BIGINT      NOT NULL,
    me_log_position       BIGINT      NOT NULL,
    me_timestamp_ms       BIGINT      NOT NULL,
    trade_id              BIGINT      NOT NULL,
    last_price            BIGINT      NOT NULL,
    last_qty              BIGINT      NOT NULL,
    hold_remaining        BIGINT      NOT NULL,
    at_ms                 BIGINT      NOT NULL,
    payload               BYTEA       NOT NULL CHECK (octet_length(payload) = 216)
);
CREATE INDEX oe_order_events_order_key ON oe_order_events (order_id, journal_seq);

-- Every order from its creation, updated in place from each record's full state (the read contract's 21 columns).
CREATE TABLE oe_orders (
    order_id              BIGINT      NOT NULL PRIMARY KEY CHECK (order_id > 0),
    user_id               BIGINT      NOT NULL,
    client_order_id       UUID,
    market_id             INTEGER     NOT NULL,
    side                  TEXT        NOT NULL CHECK (side IN ('BUY', 'SELL')),
    type                  TEXT        NOT NULL CHECK (type IN ('LIMIT', 'MARKET', 'LIMIT_MAKER', 'STOP_LOSS', 'STOP_LIMIT',
                                                               'TRAILING_STOP', 'ICEBERG')),
    time_in_force         TEXT        NOT NULL CHECK (time_in_force IN ('GTC', 'IOC', 'FOK', 'GTD')),
    price                 BIGINT,
    stop_price            BIGINT,
    quantity              BIGINT      NOT NULL,
    filled_quantity       BIGINT      NOT NULL,
    remaining_quantity    BIGINT      NOT NULL,
    status                TEXT        NOT NULL CHECK (status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED',
                                                                 'REJECTED', 'EXPIRED', 'PENDING_NEW', 'PENDING_TRIGGER',
                                                                 'PENDING_CANCEL', 'PENDING_REPLACE')),
    reason                TEXT,
    me_reason             SMALLINT    CHECK (me_reason BETWEEN 0 AND 254),
    me_order_id           BIGINT,
    hold_asset_id         INTEGER     NOT NULL,
    hold_amount           BIGINT      NOT NULL,
    hold_remaining        BIGINT      NOT NULL,                     -- every record carries it: never NULL here
    created_at_ms         BIGINT      NOT NULL,
    updated_at_ms         BIGINT      NOT NULL,
    last_event_seq        BIGINT      NOT NULL REFERENCES oe_order_events (journal_seq),
    last_log_position     BIGINT      NOT NULL
);
CREATE INDEX oe_orders_user_key ON oe_orders (user_id, updated_at_ms DESC, order_id DESC);
CREATE INDEX oe_orders_user_market_key ON oe_orders (user_id, market_id, updated_at_ms DESC, order_id DESC);
CREATE INDEX oe_orders_user_status_key ON oe_orders (user_id, status, updated_at_ms DESC, order_id DESC);

-- One row per order side of a trade (a FILL record with tradeId > 0 and lastQty > 0).
CREATE TABLE oe_fills (
    trade_id              BIGINT      NOT NULL CHECK (trade_id > 0),
    order_id              BIGINT      NOT NULL CHECK (order_id > 0) REFERENCES oe_orders (order_id),
    user_id               BIGINT      NOT NULL,
    market_id             INTEGER     NOT NULL,
    side                  TEXT        NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price                 BIGINT      NOT NULL,
    quantity              BIGINT      NOT NULL,
    maker                 BOOLEAN     NOT NULL,
    executed_at_ms        BIGINT      NOT NULL,
    event_seq             BIGINT      NOT NULL REFERENCES oe_order_events (journal_seq),
    core_log_position     BIGINT      NOT NULL,
    me_epoch              BIGINT      NOT NULL,
    me_journal_seq        BIGINT      NOT NULL,
    me_log_position       BIGINT      NOT NULL,
    me_timestamp_ms       BIGINT      NOT NULL,
    PRIMARY KEY (trade_id, order_id)
);
CREATE INDEX oe_fills_user_key ON oe_fills (user_id, executed_at_ms DESC, trade_id DESC, order_id DESC);
CREATE INDEX oe_fills_user_market_key ON oe_fills (user_id, market_id, executed_at_ms DESC, trade_id DESC, order_id DESC);
CREATE INDEX oe_fills_user_order_key ON oe_fills (user_id, order_id, executed_at_ms DESC, trade_id DESC);

-- The read relations of sql/history_read_contract_v1.sql, as views.

CREATE VIEW gateway_order_history_v1 AS
    SELECT order_id, user_id, client_order_id, market_id, side, type, time_in_force, price, stop_price, quantity,
           filled_quantity, remaining_quantity, status, reason, me_reason, me_order_id, hold_asset_id, hold_amount,
           hold_remaining, created_at_ms, updated_at_ms
      FROM oe_orders;

CREATE VIEW gateway_fills_v1 AS
    SELECT trade_id, order_id, user_id, market_id, side, price, quantity, maker, executed_at_ms
      FROM oe_fills;

-- One row per journal record of an order, ordered by event_seq and by nothing else (core_log_position is not unique).
-- oe_order_events_order_key (order_id, journal_seq) serves the gateway's trail read.
CREATE VIEW gateway_order_trail_v1 AS
    SELECT order_id, user_id, journal_seq AS event_seq, event, status, core_log_position, me_epoch, me_log_position,
           me_journal_seq, liquidity, trade_id, last_price, last_qty, hold_remaining, at_ms
      FROM oe_order_events;

-- No row until this projection's first beat: the gateway answers 503 HISTORY_UNAVAILABLE while there is none (also
-- for the whole of a rebuild, which clears beat_at in the same transaction as the truncate).
CREATE VIEW gateway_history_heartbeat_v1 AS
    SELECT singleton, beat_at, applied_order_id, applied_seq AS applied_event_seq, epoch
      FROM oe_projection_state
     WHERE beat_at IS NOT NULL;

-- The reader role: SELECT on the four read relations, nothing else. Roles belong to the server, not the database, so
-- two projections on one server share it; a concurrent create by the other one is not an error.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'oe_history_reader') THEN
        BEGIN
            CREATE ROLE oe_history_reader NOLOGIN;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END IF;
END
$$;
GRANT SELECT ON gateway_order_history_v1, gateway_fills_v1, gateway_history_heartbeat_v1, gateway_order_trail_v1
    TO oe_history_reader;
