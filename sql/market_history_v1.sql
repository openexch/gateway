-- Market history, version 1.
--
-- Written by oe-marketdata only, under its own login role (oe_marketdata_writer on the database oe_marketdata), which
-- owns every relation here. oe-marketdata applies this file at start, in one transaction, under a transaction-scoped
-- advisory lock, when none of the three relations exists; a database that holds some of them, or holds them with other
-- columns, or records another schema version in md_state, refuses the start. It names no other role: the console's
-- retention role gets DELETE and TRUNCATE from the default privileges the console sets before the first start.
--
-- Rules:
--   - plain PostgreSQL, exact integers only: prices and quantities are the journal's int64 values, volumes are NUMERIC
--     sums of exact integers (the quote volume is price x quantity computed in 128 bits; a 64-bit product overflows);
--   - a row is a function of the ME journal's trade set: the candle bucket comes from the journal's own timestamp,
--     never from a clock of this process, and nothing here records when or where a row was written;
--   - one transaction per batch: the trades, the candles they touch and the cursor move together. The cursor
--     (md_state.last_journal_seq) is the only cursor: every journal record up to it is stored, and nothing above it;
--   - no order id and no user id is stored anywhere.

-- One row (after a reset that truncated it, the service writes it again at start).
CREATE TABLE md_state (
    singleton          BOOLEAN NOT NULL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    schema_version     INTEGER NOT NULL CHECK (schema_version = 1),
    me_epoch           BIGINT  CHECK (me_epoch > 0),            -- the ME journal's meEpoch (its genesis id), bound once
    last_journal_seq   BIGINT  NOT NULL DEFAULT 0 CHECK (last_journal_seq >= 0), -- the committed cursor
    first_journal_seq  BIGINT  CHECK (first_journal_seq >= 1),  -- the first record stored; above 1: a partial history
    CHECK (last_journal_seq = 0 OR (me_epoch IS NOT NULL AND first_journal_seq IS NOT NULL)),
    CHECK (first_journal_seq IS NULL OR last_journal_seq >= first_journal_seq - 1)
);

-- One row per trade of journal record 1, keyed by the trade id.
CREATE TABLE md_trades (
    market_id       INTEGER NOT NULL CHECK (market_id > 0),
    trade_id        BIGINT  NOT NULL,
    price           BIGINT  NOT NULL,
    quantity        BIGINT  NOT NULL,
    taker_is_buy    BOOLEAN NOT NULL,
    executed_at_ms  BIGINT  NOT NULL,                           -- the journal's timestamp (epoch milliseconds)
    journal_seq     BIGINT  NOT NULL UNIQUE CHECK (journal_seq >= 1),
    PRIMARY KEY (market_id, trade_id)
);
-- the recent tape of a market
CREATE INDEX md_trades_market_seq ON md_trades (market_id, journal_seq);

-- Six intervals, written (not derived at read time): each committed batch upserts the buckets it touched.
CREATE TABLE md_candles (
    market_id        INTEGER NOT NULL CHECK (market_id > 0),
    interval_s       INTEGER NOT NULL CHECK (interval_s IN (60, 300, 900, 3600, 14400, 86400)),
    bucket_start_ms  BIGINT  NOT NULL,                          -- floor(executed_at_ms / interval) * interval
    open             BIGINT  NOT NULL,                          -- the first trade of the bucket in journal order
    high             BIGINT  NOT NULL,
    low              BIGINT  NOT NULL,
    close            BIGINT  NOT NULL,                          -- the last trade of the bucket in journal order
    volume_base      NUMERIC NOT NULL CHECK (volume_base = trunc(volume_base)),   -- sum of quantity
    volume_quote     NUMERIC NOT NULL CHECK (volume_quote = trunc(volume_quote)), -- sum of price x quantity
    trade_count      BIGINT  NOT NULL CHECK (trade_count > 0),
    last_trade_id    BIGINT  NOT NULL,
    PRIMARY KEY (market_id, interval_s, bucket_start_ms),
    CHECK (bucket_start_ms % (interval_s::BIGINT * 1000) = 0)
);
