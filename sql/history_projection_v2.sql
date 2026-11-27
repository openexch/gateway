-- History projection, version 2: age-based retention of old terminal orders.
--
-- This file is a step on top of version 1 (sql/history_projection_v1.sql), not a schema of its own. oe-projector
-- applies it in the same transaction and under the same advisory lock as version 1: on an empty database right after
-- version 1, and on a database that records version 1 in place (no truncate, no replay of the journal). The database
-- then records both versions in oe_projection_schema, each with the sha256 of its own file; a recorded version whose
-- sha256 differs from the binary's file refuses the start, as before. Every statement here can run again over its own
-- result (IF NOT EXISTS, ON CONFLICT DO NOTHING, CREATE OR REPLACE): a database whose version-2 row was deleted (a
-- rollback to a version 1 binary) is upgraded again in place. Needs PostgreSQL 14 or later (CREATE OR REPLACE TRIGGER).
--
-- It adds what an age-based delete of old orders needs, and nothing the projector writes:
--   - the indexes the delete and its foreign key checks read (without them every deleted row would scan a table);
--   - oe_projection_pruned: the greatest journal_seq a delete has removed and the greatest cutoff it was called with
--     (the compare rule below);
--   - gateway_history_prune_v1: the delete, called by the operator's retention job, never by the projector;
--   - the role oe_history_pruner: EXECUTE on the delete and nothing else (no right on any table). The function runs
--     as its owner (SECURITY DEFINER) with a fixed search_path (pg_catalog, then the schema this file was applied in,
--     pg_temp last), so the only thing the role can do is what the function does. Make the retention job's login role
--     a member (GRANT oe_history_pruner TO <role>). Like oe_history_reader, the file creates the role when the
--     projector's role has CREATEROLE; otherwise create it first (CREATE ROLE oe_history_pruner NOLOGIN), or the step
--     fails and the projector refuses to start, naming the role.
--
-- What one call deletes: gateway_history_prune_v1(before_ms, max_orders) takes at most max_orders orders whose status
-- is terminal (FILLED, CANCELLED, REJECTED, EXPIRED) and whose updated_at_ms is below before_ms, oldest first, and
-- deletes each one with every fill row and every event row of it, in one statement: an order is deleted whole or not
-- at all. A non-terminal order, and every row of it, is never touched, however old. It returns the rows deleted
-- (orders_deleted, fills_deleted, events_deleted); zero orders means nothing older is left. The core removes an order
-- once it is terminal and journals nothing more for it, so a deleted order gets no later record.
--   Every row a journal record produces belongs to the order the record names (its event row, that order's row, at
-- most one fill row of that order), so the rows of one order reference no row of another and deleting whole orders
-- keeps every foreign key. The foreign keys are checked at the end of the statement: a row of another order that
-- referenced a deleted row would fail the statement and nothing would be deleted.
--   An order the projector is updating at that moment is locked by its transaction: the delete waits for it and then
-- tests the order again (a newer record changed its updated_at_ms or its status, so it is kept).
--
-- The projector after a delete: it writes only the record after its cursor (oe_projection_state.applied_seq), which a
-- delete never moves, so it never writes a deleted order again, across restarts and replays. A record at or below the
-- cursor that a source delivers again (a read from the start of a source's journal) is compared with the stored bytes
-- of its journal_seq. When no row is stored, the record is a pruned skip (nothing written) only when all three hold:
--   - its journal_seq is at or below oe_projection_pruned.up_to_seq;
--   - its own time (at_ms, the core's updatedAtMs of the record) is below oe_projection_pruned.max_before_ms. The core
--     stamps every record with its log time, which never decreases along the journal, so every record of an order is
--     at or below the order's updated_at_ms (the time of its last record), and a deleted order's updated_at_ms is below
--     the cutoff it was deleted with;
--   - the order it names has no row.
-- Any other missing row is a payload conflict, as before (a lost recent order among old deleted ones included). A
-- trigger keeps up_to_seq: it runs as the owner after every statement that deletes event rows, whoever runs it, and
-- resets the row when oe_order_events is truncated (a rebuild); it writes the row by upsert, so a deleted row comes
-- back (with max_before_ms 0: every later missing row is a conflict until the next delete records its cutoff).
--
-- Rules of version 1 that this keeps: no projected row depends on the wall clock (oe_projection_pruned is projector
-- state, like oe_projection_state, and holds the only wall-clock column added here); nothing orders by a
-- matching-engine field.

-- The trigger first: its table lock is taken before the indexes', in the order the projector's writes take them
-- (events, orders, fills), so a writer of an older binary that is still running waits instead of deadlocking.
CREATE TABLE IF NOT EXISTS oe_projection_pruned (
    singleton             BOOLEAN     NOT NULL PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    up_to_seq             BIGINT      NOT NULL DEFAULT 0 CHECK (up_to_seq >= 0), -- the greatest journal_seq deleted
    max_before_ms         BIGINT      NOT NULL DEFAULT 0,                        -- the greatest cutoff of a delete
    updated_at            TIMESTAMPTZ                                            -- NULL: nothing deleted yet
);
INSERT INTO oe_projection_pruned (singleton) VALUES (TRUE) ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION oe_projection_note_pruned() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        INSERT INTO oe_projection_pruned (singleton, up_to_seq, max_before_ms, updated_at) VALUES (TRUE, 0, 0, NULL)
            ON CONFLICT (singleton) DO UPDATE SET up_to_seq = 0, max_before_ms = 0, updated_at = NULL;
    ELSE
        INSERT INTO oe_projection_pruned (singleton, up_to_seq, updated_at)
            VALUES (TRUE, (SELECT coalesce(max(journal_seq), 0) FROM gone), clock_timestamp())
            ON CONFLICT (singleton) DO UPDATE SET up_to_seq = GREATEST(oe_projection_pruned.up_to_seq, EXCLUDED.up_to_seq),
                updated_at = EXCLUDED.updated_at;
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION oe_projection_note_pruned() FROM PUBLIC;
CREATE OR REPLACE TRIGGER oe_order_events_pruned AFTER DELETE ON oe_order_events REFERENCING OLD TABLE AS gone
    FOR EACH STATEMENT EXECUTE FUNCTION oe_projection_note_pruned();
CREATE OR REPLACE TRIGGER oe_order_events_truncated AFTER TRUNCATE ON oe_order_events
    FOR EACH STATEMENT EXECUTE FUNCTION oe_projection_note_pruned();

-- The delete's candidates, oldest first (a partial index: open orders are not in it).
CREATE INDEX IF NOT EXISTS oe_orders_terminal_age_key ON oe_orders (updated_at_ms, order_id)
    WHERE status IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED');
-- The foreign key checks of a deleted event row (oe_orders.last_event_seq, oe_fills.event_seq) and of a deleted order
-- (oe_fills.order_id), and the delete of an order's fills.
CREATE INDEX IF NOT EXISTS oe_orders_last_event_key ON oe_orders (last_event_seq);
CREATE INDEX IF NOT EXISTS oe_fills_order_key ON oe_fills (order_id);
CREATE INDEX IF NOT EXISTS oe_fills_event_key ON oe_fills (event_seq);

-- The delete (see above). STRICT: a NULL argument deletes nothing. The cutoff is recorded in the same transaction as
-- the rows it deleted.
CREATE OR REPLACE FUNCTION gateway_history_prune_v1(before_ms BIGINT, max_orders INTEGER,
    OUT orders_deleted BIGINT, OUT fills_deleted BIGINT, OUT events_deleted BIGINT)
    LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    WITH doomed AS (
        DELETE FROM oe_orders
         WHERE order_id IN (SELECT order_id FROM oe_orders
                             WHERE status IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED') AND updated_at_ms < before_ms
                             ORDER BY updated_at_ms, order_id
                             LIMIT greatest(max_orders, 0))
           AND status IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED') AND updated_at_ms < before_ms
        RETURNING order_id
    ), fills AS (
        DELETE FROM oe_fills WHERE order_id IN (SELECT order_id FROM doomed) RETURNING 1
    ), events AS (
        DELETE FROM oe_order_events WHERE order_id IN (SELECT order_id FROM doomed) RETURNING 1
    )
    SELECT (SELECT count(*) FROM doomed), (SELECT count(*) FROM fills), (SELECT count(*) FROM events)
      INTO orders_deleted, fills_deleted, events_deleted;
    IF orders_deleted > 0 THEN
        INSERT INTO oe_projection_pruned (singleton, max_before_ms) VALUES (TRUE, before_ms)
            ON CONFLICT (singleton) DO UPDATE
            SET max_before_ms = GREATEST(oe_projection_pruned.max_before_ms, EXCLUDED.max_before_ms);
    END IF;
END
$$;

-- The definer functions resolve names in pg_catalog, then the schema this file is applied in (the projection's own,
-- never a name a caller supplies), pg_temp last.
DO $$
BEGIN
    EXECUTE format('ALTER FUNCTION oe_projection_note_pruned() SET search_path = pg_catalog, %I, pg_temp',
        current_schema());
    EXECUTE format('ALTER FUNCTION gateway_history_prune_v1(BIGINT, INTEGER) SET search_path = pg_catalog, %I, pg_temp',
        current_schema());
END
$$;

-- The pruner role: EXECUTE on the delete, nothing else. Roles belong to the server, so a concurrent create by another
-- database's schema step is not an error.
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
REVOKE ALL ON FUNCTION gateway_history_prune_v1(BIGINT, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION gateway_history_prune_v1(BIGINT, INTEGER) TO oe_history_pruner;
