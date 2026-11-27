#ifndef GATEWAY_PROJECTOR_PROJECTION_STORE_H
#define GATEWAY_PROJECTOR_PROJECTION_STORE_H

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "gateway/common/PgConn.h"

// ProjectionStore - the projector's PostgreSQL statements, over one PgConn owned by the writer thread (pj-pg) or by the
// rebuild command. Nothing here decides money: the projection is history only.
//
// Schema: the steps sql/history_projection_v1.sql (the base) and sql/history_projection_v2.sql (retention: the
// indexes, oe_projection_pruned and gateway_history_prune_v1, applied on top of version 1), each embedded at configure
// time with its own sha256. migrate() reads oe_projection_schema (one row per recorded step: version, name, sha256),
// then applies the steps the database does not record yet, in order, in one transaction under a transaction-scoped
// advisory lock (N copies may start at once), and records each one: an empty database gets every step (APPLIED), a
// database that records version 1 gets version 2 in place (UPGRADED: no truncate, no replay), one that records them
// all is PRESENT. A recorded sha256 that differs from the binary's file of that version, a recorded version the binary
// does not carry, a record with a version missing below another, or a database whose relations collide with the
// files' (for example the gateway's reference tables) is FOREIGN: the process refuses to run on it (exit 78), it never
// retries into it. The steps' statements run under HISTORY_PROJECTOR_PG_SCHEMA_TIMEOUT_MS (SET LOCAL
// statement_timeout): an upgrade builds indexes over tables that already hold history, and holds their SHARE locks
// (writes wait, reads do not) until it commits.
//
// The single writer (one database, N projector copies on N hosts):
//   - takeover: UPDATE oe_projection_state SET writer_token = <new random token>, lease_until = clock_timestamp() +
//     lease WHERE lease_until IS NULL OR lease_until < clock_timestamp(). One row: this copy is the writer. Zero rows:
//     another copy holds an unexpired lease; this copy is a standby and polls again. Every time is the database's
//     clock: one clock for every copy.
//   - the fence: every write transaction of the writer, and every renewal, ends with UPDATE oe_projection_state ...
//     WHERE writer_token = <mine>, in the same transaction as its rows. Zero rows updated rolls the transaction back and
//     the copy becomes a standby. The check is that conditional UPDATE itself, never a SELECT followed by writes, so a
//     writer stopped past its lease and then resumed cannot commit (its first transaction updates zero rows), on any
//     connection and through a pooler.
//   - the state row is read back by every takeover and renewal (RETURNING), so a writer that lost its connection reads
//     the cursor again before anything is applied.
//   - release (a clean stop): lease_until = NULL WHERE writer_token = <mine>, so a standby takes over at its next poll.
// The beat (the heartbeat means complete as of): for a head probe sent at monotonic time t and answered with the core's
// head H and genesisId G, a write transaction whose cursor has reached H also runs, after its fence,
//   UPDATE oe_projection_state SET beat_at = clock_timestamp() - <now - t>, head_seq = H
//    WHERE writer_token = <mine> AND epoch = G AND applied_seq >= H AND halt_reason IS NULL AND beat_at is older
// so beat_at is "every event of this genesis is applied as of this instant": the interval comes from the copy's own
// monotonic clock and is applied by the database's clock, no host wall clock is trusted, and beat_at never moves back.
// A projection of another genesis (epoch <> G), one below the head, or a halted one never beats, however large its
// applied_seq. pj-duty hands a probe on only once its head is applied (FollowDuty.h); the statement checks it again
// against what the transaction committed.
// Test-only controls (never in a published build): PROJ_NC_BEAT_ALWAYS sets beat_at = clock_timestamp() at every fence
// (a beat without a fresh head); PROJ_NC_BEAT_NO_EPOCH leaves out the genesis terms of the beat (epoch = G, and an
// EPOCH_MISMATCH halt); PROJ_NC_WALLCLOCK_COLUMN applies the schema with a wall-clock column on oe_orders
// (projected_at DEFAULT now()), pinned by the sha256 of that text (CMakeLists.txt), so two projections differ.
//
// Rebuild (oe-projector --rebuild): with the lease held, one transaction resets the state row under the fence (epoch,
// cursor, head, beat_at and the halt cleared) and runs a single TRUNCATE naming every base table; from that commit the
// heartbeat view has no row, so the gateway answers 503 until the new projection beats. The truncate of oe_order_events
// also resets oe_projection_pruned.up_to_seq to 0 (its trigger, sql/history_projection_v2.sql).

namespace openexchange { namespace gateway { namespace projector {

struct PgOptions
{
    std::string url;
    std::int64_t statementTimeoutMs = 3000;
    std::int64_t idleInTxTimeoutMs = 10'000;
    std::int64_t keepaliveIdleS = 5;
    std::int64_t keepaliveIntervalS = 2;
    std::int64_t keepaliveCount = 3;
};

// Connects with the projector's session settings (statement_timeout, idle_in_transaction_session_timeout,
// application_name) and TCP keepalives. False with `error`.
bool connectProjection(PgConn &conn, const PgOptions &options, std::string &error);

enum class SchemaVerdict : std::uint8_t
{
    APPLIED,  // this call created the schema (every step)
    UPGRADED, // the database recorded some steps; this call applied the rest in place
    PRESENT,  // every step was recorded with the binary's sha256
    FOREIGN, // the database holds something else: refuse (78)
    FAILED   // the call failed (connection, timeout): retry
};
const char *nameOf(SchemaVerdict v) noexcept;

// One step of the schema, as embedded: version 1 is the base, every later one applies on top of the one before.
struct ProjectionSchemaStep
{
    int version;
    const char *name;   // the name oe_projection_schema records
    const char *file;   // the source file, for messages
    const char *sql;
    const char *sha256; // of the file
};

inline constexpr int PROJECTION_SCHEMA_VERSION = 2; // the newest step this binary carries
const std::vector<ProjectionSchemaStep> &projectionSchemaSteps();
const char *projectionSchemaSha256() noexcept; // the newest step's

// `timeoutMs` bounds the bookkeeping statements, `schemaTimeoutMs` each step's statements.
SchemaVerdict migrate(PgConn &conn, std::int64_t timeoutMs, std::int64_t schemaTimeoutMs, std::string &detail);
// The same rule over `steps` (versions 1..n): migrate() passes the binary's steps; a test passes fewer to act as an
// older binary (one carrying version 1 only refuses a database that records version 2, as that binary did).
SchemaVerdict migrateSteps(PgConn &conn, std::int64_t timeoutMs, std::int64_t schemaTimeoutMs,
    const std::vector<ProjectionSchemaStep> &steps, std::string &detail);

// The state row as a takeover or a renewal read it.
struct ProjectionState
{
    std::optional<std::int64_t> epoch; // nullopt: no genesis learned yet
    std::int64_t appliedSeq = 0;
    std::int64_t appliedLogPosition = 0;
    std::int64_t appliedOrderId = 0;
    std::optional<std::string> haltReason;
    std::optional<std::string> haltDetail;
};

// An answered head probe (see the beat above).
struct BeatCandidate
{
    std::int64_t head = 0;      // H: the newest eventSeq the core had emitted
    std::int64_t genesis = 0;   // G: the core's genesisId
    std::int64_t sentAtNs = 0;  // t: when the probe was sent (monotonic)
};

// What a fenced transaction moves the state row to (only the fields set are written).
struct StateAdvance
{
    std::optional<std::int64_t> epoch;
    std::optional<std::int64_t> appliedSeq;
    std::optional<std::int64_t> appliedLogPosition;
    std::optional<std::int64_t> appliedOrderId;
    std::optional<BeatCandidate> beat; // the beat statement runs after the fence, in the same transaction
};

enum class Hold : std::uint8_t
{
    HELD,   // the fence matched this copy's token: committed
    LOST,   // zero rows: another copy holds the lease; rolled back, this copy is a standby
    FAILED  // the statement or the connection failed (the outcome of a COMMIT may be unknown: read the state again)
};
const char *nameOf(Hold h) noexcept;

class WriterLease
{
public:
    explicit WriterLease(std::int64_t leaseMs) : m_leaseMs(leaseMs) {}

    enum class Take : std::uint8_t
    {
        WRITER,
        STANDBY,
        FAILED
    };

    // One takeover attempt with a fresh token. WRITER fills `state`.
    Take takeover(PgConn &conn, std::int64_t timeoutMs, ProjectionState &state, std::string &error);
    // The fence alone, in its own transaction (an idle writer every third of the lease; a writer whose connection was
    // rebuilt). HELD fills `state`.
    Hold renew(PgConn &conn, std::int64_t timeoutMs, ProjectionState &state, std::string &error);
    // One write transaction: BEGIN, `writes`, the fence with `advance`, COMMIT. `beforeFence` and `beforeCommit` (test
    // builds only) run between the writes and the fence, and between the fence and the COMMIT.
    // `beatWritten` (optional) is set true when the advance's beat moved beat_at in the committed transaction.
    Hold commitFenced(PgConn &conn, std::int64_t timeoutMs,
        const std::function<bool(PgConn &, std::string &error)> &writes, const StateAdvance &advance,
        std::string &error, const std::function<void()> &beforeFence = {},
        const std::function<void()> &beforeCommit = {}, bool *beatWritten = nullptr);
    // The rebuild transaction (see above). The TRUNCATE waits at most `lockTimeoutMs` for open reads of the base tables
    // (SET LOCAL lock_timeout); past it nothing changes and the call is FAILED.
    Hold rebuild(PgConn &conn, std::int64_t timeoutMs, std::int64_t lockTimeoutMs, std::string &error);
    // A clean stop: the lease is given up at once. Best effort (a failure leaves the lease to expire).
    void release(PgConn &conn, std::int64_t timeoutMs);

    bool held() const noexcept { return m_held; }
    void drop() noexcept { m_held = false; }
    const std::string &token() const noexcept { return m_token; }
    std::int64_t leaseMs() const noexcept { return m_leaseMs; }

private:
    Hold fenceInTransaction(PgConn &conn, std::int64_t timeoutMs, const std::string &set,
        std::vector<std::string> params, ProjectionState *state, std::string &error);

    std::int64_t m_leaseMs;
    std::string m_token;
    bool m_held = false;
};

// A random 128-bit token in hex.
std::string newWriterToken();

}}}

#endif
