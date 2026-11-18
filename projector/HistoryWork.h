#ifndef GATEWAY_PROJECTOR_HISTORY_WORK_H
#define GATEWAY_PROJECTOR_HISTORY_WORK_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "gateway/common/PgConn.h"

#include "FollowExchange.h"
#include "ProjectorApp.h"

// HistoryWork - the writer's batches (the FencedWork of pj-pg): what pj-duty queued, written in one transaction per
// batch, records in sequence order, ended by the fence (ProjectionStore.h) that also moves the cursor:
//   1. the event row with its 216 payload bytes;
//   2. the order from the record's full state, guarded by last_event_seq < the record's sequence;
//   3. for a FILL with tradeId > 0 and lastQty > 0, the fills row (maker = liquidity MAKER, executed_at_ms = updatedAtMs);
//   4. the source memo rows of the sources the batch read from, the replay rows started or ended, the halt, then the
//      fence: applied_seq, applied_log_position, applied_order_id (the greatest order id seen), the epoch of an empty
//      database. Zero rows at the fence rolls everything back;
//   5. after the fence, the beat of the newest probe the batch carries (the beat rule, ProjectionStore.h).
// A record at or below the cursor is compared with the stored bytes of its sequence (from the database, or from this
// batch when that sequence was applied by it): equal is a counted skip, different a payload conflict (the line carries
// the sequence, the source and both SHA-256 values; nothing of it is written).
// A batch ends at HISTORY_PROJECTOR_BATCH_EVENTS records or HISTORY_PROJECTOR_BATCH_MS, whichever comes first; the
// bookkeeping items (a replay started or ended, the epoch, a halt) end it at once.
// An uncertain commit (the COMMIT's answer lost) is resolved by reading the cursor again (onWriter): equal to the cursor
// before the batch, the batch is written again; equal to its last sequence, it committed; anything else, the state is
// read again from the database and pj-duty starts over from it (a new generation).
//
// Test-only controls (never in a published build): PROJ_NC_CURSOR_OWN_TX commits the cursor before the rows, in a
// transaction of its own (a crash between the two then leaves the cursor ahead of the rows); PROJ_NC_DROP_ONE
// acknowledges one record (journalSeq PROJ_NC_DROP_SEQ from the environment, default 2) without writing any of its
// rows, the cursor moving past it (a hole in oe_order_events).

namespace openexchange { namespace gateway { namespace projector {

// The statements (one PgConn, inside the caller's transaction).
namespace store {
bool loadMemo(PgConn &conn, std::int64_t timeoutMs, std::vector<MemoRow> &out, std::string &error);
bool loadReplays(PgConn &conn, std::int64_t timeoutMs, std::vector<ReplayRow> &out, std::string &error);
bool insertReplay(PgConn &conn, std::int64_t timeoutMs, const ReplayRow &row, std::string &error);
bool deleteReplay(PgConn &conn, std::int64_t timeoutMs, const ReplayRow &row, std::string &error);
bool storedPayloads(PgConn &conn, std::int64_t timeoutMs, const std::vector<std::int64_t> &seqs,
    std::map<std::int64_t, std::string> &out, std::string &error);
bool applyRecord(PgConn &conn, std::int64_t timeoutMs, const JournalRecord &record, std::string &error);
bool upsertMemo(PgConn &conn, std::int64_t timeoutMs, const MemoRow &row, std::string &error);
bool writeHalt(PgConn &conn, std::int64_t timeoutMs, const HaltState &halt, std::string &error);
}

// Counters pj-pg keeps for /health and /metrics (relaxed atomics, written by pj-pg only).
struct WriterStats
{
    std::atomic<std::uint64_t> recordsApplied{ 0 };
    std::atomic<std::uint64_t> skips{ 0 };            // compared and equal
    std::atomic<std::uint64_t> payloadConflicts{ 0 };
    std::atomic<std::uint64_t> replayRows{ 0 };       // replay rows recorded
    std::atomic<std::uint64_t> replayRowsForgotten{ 0 };
    std::atomic<std::uint64_t> resets{ 0 };           // generations started (takeovers, unresolved commits)
    std::atomic<std::uint64_t> uncertainCommits{ 0 }; // commits whose outcome was read back from the cursor
};

struct HistoryWorkSettings
{
    std::int64_t batchEvents = 256;
    std::int64_t batchMs = 50;
    std::int64_t statementTimeoutMs = 3000;
};

class HistoryWork final : public FencedWork
{
public:
    HistoryWork(FollowExchange &exchange, HistoryWorkSettings settings, WriterStats &stats) :
        m_exchange(exchange), m_settings(settings), m_stats(stats)
    {
    }

    void onWriter(const ProjectionState &state) override;
    void onStandby() override;
    bool due(std::int64_t nowNs) override;
    bool write(PgConn &conn, const std::string &token, StateAdvance &advance, std::string &error) override;
    void committed(const StateAdvance &advance) override;

    const ProjectionState &state() const noexcept { return m_state; }
    bool loaded() const noexcept { return m_loaded; }

private:
    struct Outcome
    {
        std::vector<CommittedProof> proofs;
        std::uint64_t compared = 0;
        std::uint64_t equal = 0;
        std::vector<Conflict> conflicts;
        std::vector<std::int64_t> replaysRecorded;
        std::vector<std::size_t> appliedFrom;
        std::vector<std::size_t> comparedFrom; // sources whose compares were verified equal
        std::vector<std::pair<std::size_t, std::int64_t>> comparedUpTo; // per source: the highest sequence verified
        std::optional<HaltState> halt;
        std::uint64_t applied = 0;
    };

    FollowExchange &m_exchange;
    HistoryWorkSettings m_settings;
    WriterStats &m_stats;

    bool m_loaded = false;   // a generation was handed to pj-duty
    bool m_needLoad = false; // the next transaction reads the memo and replay rows and starts a generation
    ProjectionState m_state; // as committed
    std::vector<Item> m_batch;
    std::int64_t m_batchStartNs = 0;
    std::int64_t m_batchLastSeq = 0; // the last APPLY of m_batch (0: none)
    std::vector<MemoRow> m_loadMemo;
    std::vector<ReplayRow> m_loadReplays;
    Outcome m_outcome; // of the transaction being written
};

}}}

#endif
