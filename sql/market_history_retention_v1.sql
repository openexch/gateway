-- Market history retention, version 1: the age-based delete of the trade tape (md_trades).
--
-- oe-marketdata applies this file at every start, in the schema step's transaction, after sql/market_history_v1.sql
-- (an empty database) or after checking that the relations are the ones that file creates (a database it wrote
-- before): CREATE OR REPLACE, so a database written by an older build gets the function in place, with no change to
-- md_trades, md_candles or md_state. The function's body never changes under this name: a different delete gets a new
-- name (market_trades_prune_v2).
--
-- market_trades_prune_v1(before_ms, max_rows) deletes at most max_rows trades, market by market, from the head of
-- each market's tape in journal order: the trades older than before_ms (executed_at_ms, the journal's own time) that
-- come before the market's first trade at or after it, and never one of the newest 1000 trades of the market, however
-- old. 1000 is MARKET_TAPE_KEEP_PER_MARKET (marketdata/MarketHistory.h): oe-marketdata refuses an MD_RECENT_TRADES above
-- it, so the tape its cold start reads (readRecentTrades, the newest MD_RECENT_TRADES of a market) is always whole, also
-- for a market that has had no trade inside the window. Each market's rows are read through md_trades_market_seq, at
-- most max_rows in all. With the journal's time non-decreasing along journal_seq that is every trade older than
-- before_ms but the kept ones; were a trade ever stamped earlier than one before it, it would stay until the one before
-- it is old enough too, never the other way round: no trade at or after before_ms is deleted. It returns the trades
-- deleted; zero means nothing older is left. md_candles and md_state are never touched: a candle is written, not
-- derived from md_trades, and the cursor md_state.last_journal_seq does not depend on the rows below it.
--
-- It runs as its owner (SECURITY DEFINER) with a fixed search_path (pg_catalog, the schema this file is applied in,
-- pg_temp last). The role oe_history_pruner holds EXECUTE on it and no right on any table. Make the retention job's
-- login role a member (GRANT oe_history_pruner TO <role>). The file creates the role when oe-marketdata's role has
-- CREATEROLE; otherwise create it first (CREATE ROLE oe_history_pruner NOLOGIN), or the start is refused, naming the
-- role.

CREATE OR REPLACE FUNCTION market_trades_prune_v1(before_ms BIGINT, max_rows INTEGER)
    RETURNS BIGINT
    LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    keep_per_market CONSTANT INTEGER := 1000; -- MARKET_TAPE_KEEP_PER_MARKET
    left_rows BIGINT := greatest(max_rows, 0);
    deleted BIGINT := 0;
    n BIGINT;
    market INTEGER;
    floor_seq BIGINT;
BEGIN
    -- every market of the tape, through the (market_id, journal_seq) index
    FOR market IN
        WITH RECURSIVE m AS (
            SELECT min(market_id) AS id FROM md_trades
            UNION ALL
            SELECT (SELECT min(t.market_id) FROM md_trades t WHERE t.market_id > m.id) FROM m WHERE m.id IS NOT NULL
        )
        SELECT id FROM m WHERE id IS NOT NULL
    LOOP
        EXIT WHEN left_rows <= 0;
        -- the oldest of the market's newest keep_per_market trades; fewer than that: the market keeps all
        SELECT t.journal_seq INTO floor_seq FROM md_trades t WHERE t.market_id = market
         ORDER BY t.journal_seq DESC OFFSET keep_per_market - 1 LIMIT 1;
        CONTINUE WHEN floor_seq IS NULL;
        WITH head AS (
            SELECT t.journal_seq, t.executed_at_ms FROM md_trades t
             WHERE t.market_id = market AND t.journal_seq < floor_seq
             ORDER BY t.journal_seq LIMIT left_rows
        ), stop AS (
            SELECT min(h.journal_seq) AS journal_seq FROM head h WHERE h.executed_at_ms >= before_ms
        ), gone AS (
            DELETE FROM md_trades t
             WHERE t.journal_seq IN (SELECT h.journal_seq FROM head h, stop s
                                      WHERE s.journal_seq IS NULL OR h.journal_seq < s.journal_seq)
            RETURNING 1
        )
        SELECT count(*) INTO n FROM gone;
        deleted := deleted + n;
        left_rows := left_rows - n;
    END LOOP;
    RETURN deleted;
END
$$;
DO $$
BEGIN
    EXECUTE format('ALTER FUNCTION market_trades_prune_v1(BIGINT, INTEGER) SET search_path = pg_catalog, %I, pg_temp',
        current_schema());
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'oe_history_pruner') THEN
        BEGIN
            CREATE ROLE oe_history_pruner NOLOGIN;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END IF;
END
$$;
REVOKE ALL ON FUNCTION market_trades_prune_v1(BIGINT, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION market_trades_prune_v1(BIGINT, INTEGER) TO oe_history_pruner;
