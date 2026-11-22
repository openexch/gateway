#ifndef GATEWAY_MARKETDATA_JOURNAL_FOLLOW_H
#define GATEWAY_MARKETDATA_JOURNAL_FOLLOW_H

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cluster_tools/archive/ArchiveMetrics.h"

#include "gateway/common/ProcessShell.h"
#include "gateway/common/Status.h"

#include "MarketDataSettings.h"
#include "TradePath.h"

// JournalFollow - the trade path's reader on md-duty: the ME journal (schema 4) through the archives of
// MD_ME_JOURNAL_ARCHIVES, one attached at a time in that failover order, with the cluster-tools archive follow library
// (one ChainFollower per archive, the bridge's walk, the ME journal profile of the frame gate). It starts once the
// market history writer has read md_state (TradePath.h: the forward rule, the halts, the two cursors) and hands every
// record it takes to the pending list (TradeLedger), from which md-pg commits. A full pending list pauses the read
// (the follower reads nothing and still probes); nothing is dropped.
//
// Sources. A source is left (its replay stopped, the next source in the list attached) when it faults (a connect,
// list or replay fault, or a stall verdict: never connected, image lost, archive gone, a stall, a stuck bounded
// replay, an early end), when it refuses a frame, or when it does not hold the next record (BELOW_RANGE, HOLE). A
// faulted source is asked again after MD_SOURCE_BACKOFF_MS; a source that refused a frame or misses the next record
// is marked and not asked again until a GAP re-scan. When every source is marked: SOURCE_SCHEMA if one refused a frame,
// JOURNAL_PREFIX_PURGED if every one begins above the next record, else GAP (every mark is dropped and every source
// read again from its chain start each MD_GAP_RESCAN_MS until one delivers the record).
//
// journal_fresh is the follower's own stall verdicts (FollowPolicy::stallVerdicts, checkedClean): the last source check
// of the attached follower passed within two MD_SOURCE_PROBE_MS and no stall verdict came after it, and the oldest
// pending record waited less than MD_HISTORY_STALE_MS. The head of a recording is never the measure (a dead recording's
// head does not move).
//
// Test-only controls (never in a published build): MD_NC_NO_SCHEMA_CHECK (a frame the gate refused for its schema is
// decoded as a trade anyway), MD_NC_NO_PARTIAL_FLAG (an empty history that starts above journalSeq 1 records 1 as its
// first record), and those of TradePath.cpp (MD_NC_NO_EPOCH_CHECK, MD_NC_NO_PREFIX_CHECK, MD_NC_NO_MARKET_CHECK,
// MD_NC_NO_LIVE_EMIT_CURSOR).

namespace openexchange { namespace gateway { namespace marketdata {

// What /health and /metrics show of the trade path (md-duty writes it; the admin thread reads).
struct TradeView
{
    bool started = false;              // md_state was read: the trade path runs
    std::int64_t next = 0;             // the next record to take (0: an empty history before its first record)
    std::int64_t liveEmit = 0;         // the live-emit cursor
    std::uint64_t liveEmitted = 0;     // trades handed to the live path
    std::optional<std::int64_t> epoch;
    MdHalt halt = MdHalt::NONE;        // the first by TradeHalts::primary
    std::string haltDetail;
    std::array<bool, static_cast<std::size_t>(MdHalt::COUNT)> halts{};
    std::optional<std::size_t> source;
    std::string phase = "IDLE";
    bool attached = false;
    bool fresh = false;
    std::vector<std::string> marks;    // per source: NONE, BELOW_RANGE, HOLE, SCHEMA, and the back-off left
    std::uint64_t switches = 0;
    std::uint64_t holes = 0;
    std::uint64_t belowRange = 0;
    std::uint64_t refusals = 0;
    std::uint64_t reReads = 0;
    std::uint64_t rescans = 0;
    std::uint64_t pausedFull = 0;      // duty cycles the read waited on a full pending list
};

class TradeViewBox
{
public:
    TradeView get() const
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_view;
    }
    void set(const TradeView &v)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_view = v;
    }

private:
    mutable std::mutex m_lock;
    TradeView m_view;
};

// Everything the trade path shares across threads, owned by main for the process's life.
struct TradeShared
{
    explicit TradeShared(std::size_t pendingCapacity) : ledger(pendingCapacity) {}
    TradeLedger ledger;
    TradeViewBox view;
    cluster_tools::archive::ArchiveMetrics archiveMetrics;
};

class JournalFollow
{
public:
    // `invoker` runs while an archive control call waits (the ME session's keepalive). `egressSchemaRefusals` reads the
    // book side's count of egress frames of another order schema (a nonzero count is SOURCE_SCHEMA).
    JournalFollow(const DutyContext &context, const MarketDataSettings &settings, TradeShared &shared,
        std::function<void()> invoker, std::function<std::uint64_t()> egressSchemaRefusals, LiveTrades *live);
    ~JournalFollow();

    int doWork(std::int64_t nowNs);
    void fillStatus(Status &status, std::int64_t nowNs) const;
    void close();

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

}}}

#endif
