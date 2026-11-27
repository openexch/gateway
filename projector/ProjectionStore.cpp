#include "ProjectionStore.h"

#include <algorithm>
#include <iterator>
#include <vector>

#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/Crypto.h"

namespace openexchange { namespace gateway { namespace projector {

using pg::Exec;
using pg::Result;

namespace {

const ProjectionSchemaStep PROJECTION_SCHEMA_STEPS[] = {
#ifdef PROJ_NC_WALLCLOCK_COLUMN
#include "gateway/projection_schema_nc_wallclock_column.inc" // test build only: version 1 plus a wall-clock column
#else
#include "gateway/projection_schema.inc"
#endif
};

// pg_advisory_xact_lock key of the schema step: the eight bytes "oeprojsc" as a big-endian int64.
constexpr const char *SCHEMA_LOCK_KEY = "8026945547655607139";

// The state row's columns a takeover and a renewal read back.
constexpr const char *STATE_RETURNING = " RETURNING epoch, applied_seq, applied_log_position, applied_order_id, halt_reason, halt_detail";

void readState(const Result &r, ProjectionState &state)
{
    PGresult *raw = r.get();
    state.epoch = PQgetisnull(raw, 0, 0) ? std::nullopt : std::optional<std::int64_t>(std::stoll(PQgetvalue(raw, 0, 0)));
    state.appliedSeq = std::stoll(PQgetvalue(raw, 0, 1));
    state.appliedLogPosition = std::stoll(PQgetvalue(raw, 0, 2));
    state.appliedOrderId = std::stoll(PQgetvalue(raw, 0, 3));
    state.haltReason = PQgetisnull(raw, 0, 4) ? std::nullopt : std::optional<std::string>(PQgetvalue(raw, 0, 4));
    state.haltDetail = PQgetisnull(raw, 0, 5) ? std::nullopt : std::optional<std::string>(PQgetvalue(raw, 0, 5));
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
        case SchemaVerdict::UPGRADED: return "upgraded";
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

const std::vector<ProjectionSchemaStep> &projectionSchemaSteps()
{
    static const std::vector<ProjectionSchemaStep> steps(std::begin(PROJECTION_SCHEMA_STEPS),
        std::end(PROJECTION_SCHEMA_STEPS));
    return steps;
}

const char *projectionSchemaSha256() noexcept
{
    return std::end(PROJECTION_SCHEMA_STEPS)[-1].sha256;
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

SchemaVerdict migrate(PgConn &conn, std::int64_t timeoutMs, std::int64_t schemaTimeoutMs, std::string &detail)
{
    static_assert(std::size(PROJECTION_SCHEMA_STEPS) == PROJECTION_SCHEMA_VERSION, "one step per version");
    return migrateSteps(conn, timeoutMs, schemaTimeoutMs, projectionSchemaSteps(), detail);
}

SchemaVerdict migrateSteps(PgConn &conn, std::int64_t timeoutMs, std::int64_t schemaTimeoutMs,
    const std::vector<ProjectionSchemaStep> &steps, std::string &detail)
{
    const int carried = static_cast<int>(steps.size());
    Result r;
    std::string error;
    auto failed = [&](const std::string &step) {
        const bool foreign = foreignState(conn.lastSqlState());
        detail = step + ": " + error;
        rollback(conn, timeoutMs);
        return foreign ? SchemaVerdict::FOREIGN : SchemaVerdict::FAILED;
    };
    auto refuse = [&](const std::string &why) {
        detail = why;
        rollback(conn, timeoutMs);
        return SchemaVerdict::FOREIGN;
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
    // the record must be the binary's steps 1..n with their sha256, nothing else
    const int recorded = PQntuples(r.get());
    for (int i = 0; i < recorded; ++i)
    {
        const int version = std::stoi(PQgetvalue(r.get(), i, 0));
        const std::string sha = PQgetvalue(r.get(), i, 1);
        if (version > carried)
        {
            return refuse("the database records history projection schema version " + std::to_string(version) +
                "; this binary carries versions 1 to " + std::to_string(carried) + " only");
        }
        if (version != i + 1)
        {
            return refuse("the database records history projection schema version " + std::to_string(version) +
                " without version " + std::to_string(i + 1) + ": not a record this binary wrote");
        }
        const ProjectionSchemaStep &step = steps[static_cast<std::size_t>(i)];
        if (sha != step.sha256)
        {
            return refuse("the database records history projection schema version " + std::to_string(version) +
                " with sha256 " + sha + "; this binary's " + step.file + " has " + step.sha256);
        }
    }
    if (recorded < carried &&
        Exec::OK != conn.exec("SET LOCAL statement_timeout = " + std::to_string(schemaTimeoutMs), {}, timeoutMs, r,
            error, false))
    {
        return failed("SET LOCAL statement_timeout");
    }
    for (int i = recorded; i < carried; ++i)
    {
        const ProjectionSchemaStep &step = steps[static_cast<std::size_t>(i)];
        if (Exec::OK != conn.exec(step.sql, {}, schemaTimeoutMs, r, error, true))
        {
            return failed(step.file);
        }
        if (Exec::OK != conn.exec("INSERT INTO oe_projection_schema (version, name, sha256) VALUES ($1, $2, $3)",
                { std::to_string(step.version), step.name, step.sha256 }, timeoutMs, r, error, false))
        {
            return failed("oe_projection_schema");
        }
    }
    if (Exec::OK != conn.exec("COMMIT", {}, schemaTimeoutMs, r, error, false))
    {
        return failed("COMMIT");
    }
    const std::string newest = "schema version " + std::to_string(carried);
    if (recorded == carried)
    {
        detail = newest + " present";
        return SchemaVerdict::PRESENT;
    }
    if (0 == recorded)
    {
        detail = newest + " applied (versions 1 to " + std::to_string(carried) + ")";
        return SchemaVerdict::APPLIED;
    }
    detail = newest + " applied in place on version " + std::to_string(recorded);
    return SchemaVerdict::UPGRADED;
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
#ifdef PROJ_NC_BEAT_ALWAYS
    const std::string always = ", beat_at = clock_timestamp()"; // test build only: every fence beats
#else
    const std::string always;
#endif
    Result r;
    if (Exec::OK != conn.exec("UPDATE oe_projection_state SET lease_until = clock_timestamp() + $2::bigint * interval "
            "'1 millisecond', updated_at = clock_timestamp()" + always + set + where + STATE_RETURNING, params,
            timeoutMs, r, error, false))
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

namespace {

// the monotonic clock pj-duty stamps a probe's send time with (a host clock never reaches the database)
std::int64_t monotonicNs()
{
    return cluster_tools::runtime::Supervisor::monotonicNs();
}

// The beat statement (see ProjectionStore.h), after the fence of the same transaction. True with `written` when it
// moved beat_at.
bool writeBeat(PgConn &conn, std::int64_t timeoutMs, const std::string &token, const BeatCandidate &beat,
    bool &written, std::string &error)
{
    // the interval since the probe was sent, measured on this copy's monotonic clock as late as possible; the database
    // subtracts it from its own clock
    const std::int64_t sinceUs = std::max<std::int64_t>(0, (monotonicNs() - beat.sentAtNs) / 1000);
#if defined(PROJ_NC_BEAT_NO_EPOCH)
    // test build only: no genesis term (the probe's genesis is not compared, an EPOCH_MISMATCH halt does not stop it)
    const std::string sql = "UPDATE oe_projection_state SET beat_at = clock_timestamp() - $2::bigint * interval "
        "'1 microsecond', head_seq = $3::bigint WHERE writer_token = $1 AND $4::bigint IS NOT NULL AND applied_seq >= "
        "$3::bigint AND (halt_reason IS NULL OR halt_reason = 'EPOCH_MISMATCH') AND (beat_at IS NULL OR beat_at < "
        "clock_timestamp() - $2::bigint * interval '1 microsecond')";
#else
    const std::string sql = "UPDATE oe_projection_state SET beat_at = clock_timestamp() - $2::bigint * interval "
        "'1 microsecond', head_seq = $3::bigint WHERE writer_token = $1 AND epoch = $4::bigint AND applied_seq >= "
        "$3::bigint AND halt_reason IS NULL AND (beat_at IS NULL OR beat_at < clock_timestamp() - $2::bigint * "
        "interval '1 microsecond')";
#endif
    Result r;
    if (Exec::OK != conn.exec(sql, { token, std::to_string(sinceUs), std::to_string(beat.head),
            std::to_string(beat.genesis) }, timeoutMs, r, error, false))
    {
        return false;
    }
    written = 1 == rowsOf(r);
    return true;
}

}

Hold WriterLease::commitFenced(PgConn &conn, std::int64_t timeoutMs,
    const std::function<bool(PgConn &, std::string &)> &writes, const StateAdvance &advance, std::string &error,
    const std::function<void()> &beforeFence, const std::function<void()> &beforeCommit, bool *beatWritten)
{
    if (nullptr != beatWritten)
    {
        *beatWritten = false;
    }
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
    bool beat = false;
    if (advance.beat && !writeBeat(conn, timeoutMs, m_token, *advance.beat, beat, error))
    {
        rollback(conn, timeoutMs);
        return Hold::FAILED;
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
    if (nullptr != beatWritten)
    {
        *beatWritten = beat;
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
#ifdef PROJ_NC_REBUILD_KEEPS_BEAT
    // test build only: the rebuild keeps beat_at (the heartbeat view keeps its row through the rebuild)
    const char *reset = ", epoch = NULL, applied_seq = 0, applied_log_position = 0, applied_order_id = 0, head_seq = "
        "NULL, halt_reason = NULL, halt_detail = NULL";
#else
    const char *reset = ", epoch = NULL, applied_seq = 0, applied_log_position = 0, applied_order_id = 0, head_seq = "
        "NULL, beat_at = NULL, halt_reason = NULL, halt_detail = NULL";
#endif
    const Hold h = fenceInTransaction(conn, timeoutMs, reset, {}, nullptr, error);
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
