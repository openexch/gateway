#include "ProjectionStore.h"

#include <vector>

#include "gateway/common/Crypto.h"

namespace openexchange { namespace gateway { namespace projector {

using pg::Exec;
using pg::Result;

namespace {

#include "gateway/projection_schema.inc"

// pg_advisory_xact_lock key of the schema step: the eight bytes "oeprojsc" as a big-endian int64.
constexpr const char *SCHEMA_LOCK_KEY = "8026945547655607139";

// The state row's columns a takeover and a renewal read back.
constexpr const char *STATE_RETURNING = " RETURNING epoch, applied_seq, applied_log_position, applied_order_id, halt_reason";

void readState(const Result &r, ProjectionState &state)
{
    PGresult *raw = r.get();
    state.epoch = PQgetisnull(raw, 0, 0) ? std::nullopt : std::optional<std::int64_t>(std::stoll(PQgetvalue(raw, 0, 0)));
    state.appliedSeq = std::stoll(PQgetvalue(raw, 0, 1));
    state.appliedLogPosition = std::stoll(PQgetvalue(raw, 0, 2));
    state.appliedOrderId = std::stoll(PQgetvalue(raw, 0, 3));
    state.haltReason = PQgetisnull(raw, 0, 4) ? std::nullopt : std::optional<std::string>(PQgetvalue(raw, 0, 4));
}

std::int64_t rowsOf(const Result &r)
{
    if (!r)
    {
        return 0;
    }
    const char *n = PQcmdTuples(r.get());
    return nullptr == n || '\0' == *n ? 0 : std::stoll(n);
}

void rollback(PgConn &conn, std::int64_t timeoutMs)
{
    if (!conn.ok())
    {
        return;
    }
    Result r;
    std::string ignored;
    if (Exec::OK != conn.exec("ROLLBACK", {}, timeoutMs, r, ignored, false))
    {
        conn.close();
    }
}

// A SQLSTATE that says "the database holds something else" (a relation or object already there, an undefined or
// mismatched column, an insufficient privilege): not a transient failure.
bool foreignState(const std::string &state)
{
    return 0 == state.rfind("42", 0);
}

}

const char *nameOf(SchemaVerdict v) noexcept
{
    switch (v)
    {
        case SchemaVerdict::APPLIED: return "applied";
        case SchemaVerdict::PRESENT: return "present";
        case SchemaVerdict::FOREIGN: return "foreign";
        case SchemaVerdict::FAILED: break;
    }
    return "failed";
}

const char *nameOf(Hold h) noexcept
{
    switch (h)
    {
        case Hold::HELD: return "held";
        case Hold::LOST: return "lost";
        case Hold::FAILED: break;
    }
    return "failed";
}

const char *projectionSchemaSql() noexcept
{
    return PROJECTION_SCHEMA_SQL;
}

const char *projectionSchemaSha256() noexcept
{
    return PROJECTION_SCHEMA_SHA256;
}

std::string newWriterToken()
{
    return crypto::hex(crypto::randomBytes(16));
}

bool connectProjection(PgConn &conn, const PgOptions &o, std::string &error)
{
    return conn.connect(o.url, o.statementTimeoutMs, error,
        { "SET idle_in_transaction_session_timeout = " + std::to_string(o.idleInTxTimeoutMs),
            "SET application_name = 'oe-projector'", "SET client_min_messages = warning" },
        0,
        { { "keepalives", "1" }, { "keepalives_idle", std::to_string(o.keepaliveIdleS) },
            { "keepalives_interval", std::to_string(o.keepaliveIntervalS) },
            { "keepalives_count", std::to_string(o.keepaliveCount) } });
}

SchemaVerdict migrate(PgConn &conn, std::int64_t timeoutMs, std::string &detail)
{
    Result r;
    std::string error;
    auto failed = [&](const char *step) {
        const bool foreign = foreignState(conn.lastSqlState());
        detail = std::string(step) + ": " + error;
        rollback(conn, timeoutMs);
        return foreign ? SchemaVerdict::FOREIGN : SchemaVerdict::FAILED;
    };
    if (Exec::OK != conn.exec("BEGIN", {}, timeoutMs, r, error, false))
    {
        return failed("BEGIN");
    }
    if (Exec::OK != conn.exec(std::string("SELECT pg_advisory_xact_lock(") + SCHEMA_LOCK_KEY + ")", {}, timeoutMs, r,
            error, false))
    {
        return failed("schema lock");
    }
    if (Exec::OK != conn.exec("CREATE TABLE IF NOT EXISTS oe_projection_schema (version INTEGER NOT NULL PRIMARY KEY, "
            "name TEXT NOT NULL, sha256 TEXT NOT NULL)", {}, timeoutMs, r, error, false))
    {
        return failed("oe_projection_schema");
    }
    if (Exec::OK != conn.exec("SELECT version, sha256 FROM oe_projection_schema ORDER BY version", {}, timeoutMs, r,
            error, false))
    {
        return failed("oe_projection_schema");
    }
    bool present = false;
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        const int version = std::stoi(PQgetvalue(r.get(), i, 0));
        const std::string sha = PQgetvalue(r.get(), i, 1);
        if (PROJECTION_SCHEMA_VERSION != version)
        {
            detail = "the database records history projection schema version " + std::to_string(version) +
                "; this binary carries version " + std::to_string(PROJECTION_SCHEMA_VERSION) + " only";
            rollback(conn, timeoutMs);
            return SchemaVerdict::FOREIGN;
        }
        if (sha != PROJECTION_SCHEMA_SHA256)
        {
            detail = "the database records history projection schema version " + std::to_string(version) +
                " with sha256 " + sha + "; this binary's sql/history_projection_v1.sql has " + PROJECTION_SCHEMA_SHA256;
            rollback(conn, timeoutMs);
            return SchemaVerdict::FOREIGN;
        }
        present = true;
    }
    if (!present)
    {
        if (Exec::OK != conn.exec(PROJECTION_SCHEMA_SQL, {}, timeoutMs, r, error, true))
        {
            return failed("sql/history_projection_v1.sql");
        }
        if (Exec::OK != conn.exec("INSERT INTO oe_projection_schema (version, name, sha256) VALUES ($1, "
                "'history_projection', $2)", { std::to_string(PROJECTION_SCHEMA_VERSION), PROJECTION_SCHEMA_SHA256 },
                timeoutMs, r, error, false))
        {
            return failed("oe_projection_schema");
        }
    }
    if (Exec::OK != conn.exec("COMMIT", {}, timeoutMs, r, error, false))
    {
        return failed("COMMIT");
    }
    detail = present ? "schema version 1 present" : "schema version 1 applied";
    return present ? SchemaVerdict::PRESENT : SchemaVerdict::APPLIED;
}

WriterLease::Take WriterLease::takeover(PgConn &conn, std::int64_t timeoutMs, ProjectionState &state,
    std::string &error)
{
    m_held = false;
    const std::string token = newWriterToken();
    Result r;
#if defined(PROJ_NC_NO_FENCE)
    // test build only: the takeover ignores the lease
    const std::string sql = "UPDATE oe_projection_state SET writer_token = $1, lease_until = clock_timestamp() + $2::bigint * "
        "interval '1 millisecond', updated_at = clock_timestamp() WHERE singleton";
#else
    const std::string sql = "UPDATE oe_projection_state SET writer_token = $1, lease_until = clock_timestamp() + $2::bigint * "
        "interval '1 millisecond', updated_at = clock_timestamp() WHERE singleton AND (lease_until IS NULL OR "
        "lease_until < clock_timestamp())";
#endif
    if (Exec::OK != conn.exec(sql + STATE_RETURNING, { token, std::to_string(m_leaseMs) }, timeoutMs, r, error, false))
    {
        return Take::FAILED;
    }
    if (1 != PQntuples(r.get()))
    {
        return Take::STANDBY;
    }
    readState(r, state);
    m_token = token;
    m_held = true;
    return Take::WRITER;
}

Hold WriterLease::fenceInTransaction(PgConn &conn, std::int64_t timeoutMs, const std::string &set,
    std::vector<std::string> params, ProjectionState *state, std::string &error)
{
    params.insert(params.begin(), { m_token, std::to_string(m_leaseMs) });
#if defined(PROJ_NC_NO_FENCE) || defined(PROJ_NC_SELECT_FENCE)
    // test builds only: the state update is not conditional on the token
    const std::string where = " WHERE singleton AND $1::text IS NOT NULL";
#else
    const std::string where = " WHERE writer_token = $1";
#endif
    Result r;
    if (Exec::OK != conn.exec("UPDATE oe_projection_state SET lease_until = clock_timestamp() + $2::bigint * interval "
            "'1 millisecond', updated_at = clock_timestamp()" + set + where + STATE_RETURNING, params, timeoutMs, r,
            error, false))
    {
        return Hold::FAILED;
    }
    if (1 != PQntuples(r.get()))
    {
        error = "the fence updated " + std::to_string(rowsOf(r)) + " rows: another copy holds the writer lease";
        return Hold::LOST;
    }
    if (nullptr != state)
    {
        readState(r, *state);
    }
    return Hold::HELD;
}

Hold WriterLease::renew(PgConn &conn, std::int64_t timeoutMs, ProjectionState &state, std::string &error)
{
    if (m_token.empty())
    {
        error = "no lease was ever taken";
        return Hold::LOST;
    }
    const Hold h = fenceInTransaction(conn, timeoutMs, "", {}, &state, error);
    m_held = Hold::HELD == h || (Hold::FAILED == h && m_held);
    if (Hold::LOST == h)
    {
        m_held = false;
    }
    return h;
}

Hold WriterLease::commitFenced(PgConn &conn, std::int64_t timeoutMs,
    const std::function<bool(PgConn &, std::string &)> &writes, const StateAdvance &advance, std::string &error,
    const std::function<void()> &beforeFence, const std::function<void()> &beforeCommit)
{
    Result r;
    if (Exec::OK != conn.exec("BEGIN", {}, timeoutMs, r, error, false))
    {
        return Hold::FAILED;
    }
#if defined(PROJ_NC_SELECT_FENCE)
    // test build only: the fence is a SELECT before the writes
    if (Exec::OK != conn.exec("SELECT writer_token = $1 FROM oe_projection_state", { m_token }, timeoutMs, r, error,
            false))
    {
        rollback(conn, timeoutMs);
        return Hold::FAILED;
    }
    if (1 != PQntuples(r.get()) || 't' != PQgetvalue(r.get(), 0, 0)[0])
    {
        rollback(conn, timeoutMs);
        m_held = false;
        error = "another copy holds the writer lease";
        return Hold::LOST;
    }
#endif
    if (writes && !writes(conn, error))
    {
        rollback(conn, timeoutMs);
        return Hold::FAILED;
    }
    if (beforeFence)
    {
        beforeFence();
    }
    std::string set;
    std::vector<std::string> params;
    auto column = [&](const char *name, const std::optional<std::int64_t> &value) {
        if (value)
        {
            params.push_back(std::to_string(*value));
            set += std::string(", ") + name + " = $" + std::to_string(params.size() + 2);
        }
    };
    column("epoch", advance.epoch);
    column("applied_seq", advance.appliedSeq);
    column("applied_log_position", advance.appliedLogPosition);
    column("applied_order_id", advance.appliedOrderId);
    const Hold h = fenceInTransaction(conn, timeoutMs, set, params, nullptr, error);
    if (Hold::HELD != h)
    {
        rollback(conn, timeoutMs);
        if (Hold::LOST == h)
        {
            m_held = false;
        }
        return h;
    }
    if (beforeCommit)
    {
        beforeCommit();
    }
    if (Exec::OK != conn.exec("COMMIT", {}, timeoutMs, r, error, false))
    {
        // the outcome is not known: the caller reads the state row again before anything else is applied
        error = "COMMIT: " + error;
        conn.close();
        return Hold::FAILED;
    }
    return Hold::HELD;
}

Hold WriterLease::rebuild(PgConn &conn, std::int64_t timeoutMs, std::int64_t lockTimeoutMs, std::string &error)
{
    Result r;
    if (Exec::OK != conn.exec("BEGIN", {}, timeoutMs, r, error, false))
    {
        return Hold::FAILED;
    }
    // the fence first: a copy that lost the lease truncates nothing
    const Hold h = fenceInTransaction(conn, timeoutMs, ", epoch = NULL, applied_seq = 0, applied_log_position = 0, "
        "applied_order_id = 0, head_seq = NULL, beat_at = NULL, halt_reason = NULL, halt_detail = NULL", {}, nullptr,
        error);
    if (Hold::HELD != h)
    {
        rollback(conn, timeoutMs);
        if (Hold::LOST == h)
        {
            m_held = false;
        }
        return h;
    }
    // TRUNCATE takes an ACCESS EXCLUSIVE lock: it waits behind every open history read, and every new read queues behind
    // it. The wait is bounded; past it the transaction rolls back and the command is run again later.
    if (Exec::OK != conn.exec("SET LOCAL lock_timeout = " + std::to_string(lockTimeoutMs), {}, timeoutMs, r, error,
            false))
    {
        rollback(conn, timeoutMs);
        return Hold::FAILED;
    }
    if (Exec::OK != conn.exec("TRUNCATE oe_fills, oe_orders, oe_order_events, oe_projection_source_memo, "
            "oe_projection_replay", {}, timeoutMs, r, error, false))
    {
        rollback(conn, timeoutMs);
        return Hold::FAILED;
    }
    if (Exec::OK != conn.exec("COMMIT", {}, timeoutMs, r, error, false))
    {
        error = "COMMIT: " + error;
        conn.close();
        return Hold::FAILED;
    }
    return Hold::HELD;
}

void WriterLease::release(PgConn &conn, std::int64_t timeoutMs)
{
    if (!m_held || !conn.ok())
    {
        return;
    }
    Result r;
    std::string ignored;
    conn.exec("UPDATE oe_projection_state SET lease_until = NULL, updated_at = clock_timestamp() WHERE writer_token = $1",
        { m_token }, timeoutMs, r, ignored, false);
    m_held = false;
}

}}}
