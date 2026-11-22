-- Money read contract, version 1.
--
-- The one relation oe-gateway reads from the core money database, for GET /api/v2/balances/movements, on read-only
-- connections of its own (GATEWAY_MONEY_PG_URL). The core projector's packaged migration V002 provides it as a view
-- over the money journal projection and creates the role oe_money_reader with SELECT on it and on nothing else; the
-- gateway's login role is made a member of that role (GRANT oe_money_reader TO <gateway role>). This file is the
-- gateway's reference copy of its columns and types: the gateway's tests seed from it. The gateway never creates,
-- migrates or writes the money database.
--
-- Display only: what a user's balance did, for a person to read. Nothing replays settlement, rebuilds a balance or
-- decides a money movement from it.
--
-- Types: amounts BIGINT integer units at the wire's scale 10^8, rendered as signed REST decimal strings; ids BIGINT
-- (asset and market ids INTEGER) rendered as decimal strings; times BIGINT epoch milliseconds of core cluster time.
-- Paging is keyset only, newest first by (journal_seq, leg_no), the caller's rows only (user_id in the statement).

CREATE TABLE gateway_balance_movements_v1 (
    user_id         BIGINT      NOT NULL,
    journal_seq     BIGINT      NOT NULL,                   -- the money journal's dense sequence of the movement
    leg_no          SMALLINT    NOT NULL,                   -- the leg within the movement, from 0
    log_position    BIGINT      NOT NULL,                   -- the core log position of the entry that produced it
    kind            TEXT,                                   -- opening, deposit, withdrawal, settle
    asset_id        INTEGER,                                -- NULL only for a settle of a market the database lacks
    delta           BIGINT,                                 -- signed change of available + locked; NULL with asset_id
    balance_after   BIGINT      NOT NULL,                   -- available + locked after the movement
    at_ms           BIGINT,                                 -- NULL for opening
    trade_id        BIGINT,                                 -- settle only
    market_id       INTEGER,                                -- settle only
    PRIMARY KEY (journal_seq, leg_no)
);

CREATE INDEX gateway_balance_movements_v1_user_key ON gateway_balance_movements_v1 (user_id, journal_seq, leg_no);
