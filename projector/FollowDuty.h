#ifndef GATEWAY_PROJECTOR_FOLLOW_DUTY_H
#define GATEWAY_PROJECTOR_FOLLOW_DUTY_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "gateway/common/ProcessShell.h"

#include "FollowExchange.h"
#include "FollowRules.h"
#include "ProjectorApp.h"
#include "ProjectorSettings.h"

// FollowDuty - pj-duty of oe-projector: while this copy is the writer (writerNow), the cluster session (the genesis
// and the core's head, RequestOrderEvents maxEvents = 0 every HISTORY_PROJECTOR_HEAD_PROBE_MS), the journal sources
// (one ChainFollower of the cluster-tools archive follow library per configured archive, one attached at a time) and
// the frame sink that turns records into the writer's items (FollowExchange). A standby holds none of them: the
// replays are stopped and the session closed when the lease is lost, and /health counts them from the real attach and
// detach (replaysOpen, sessionOpen).
//
// Reading: the expected epoch is the database's (the cluster's genesisId written into an empty database first; a record,
// a recording or a database of another genesis is refused, epoch 0 included). A replay is recorded in
// oe_projection_replay before any record of it is read past the first (the follower stays paused until pj-pg committed
// the row), so a restart or a standby that takes over stops it by its session id; on every takeover the rows left are
// stopped on their archives (an "unknown session" answer is counted, not an error) and forgotten. stopAllReplays is
// never used: it would end other readers' replays of the same recording.
//
// The rules (FollowRules.h): the forward rule, the source choice (readers avoid the leader; the member's reader replay
// budget), the per-source back-off, BEHIND, and the halts, none of them a latch. Each decision is logged with its
// reason and shown in /health.
//
// The member nodes: pj-node polls GET /health and GET /metrics of each source's node endpoint
// (HISTORY_PROJECTOR_JOURNAL_NODE_ENDPOINTS, every HISTORY_PROJECTOR_NODE_POLL_MS, a round bounded by
// HISTORY_PROJECTOR_NODE_POLL_TIMEOUT_MS) while this copy is the writer: the role the member reports, its
// readerReplayBudget and core_archive_replays_active. Nothing is read from a member's files.
//
// Test-only controls (never in a published build): PROJ_NC_NO_STOP_REPLAY (the recorded replays are not stopped at a
// start or a takeover), PROJ_NC_NO_EPOCH_CHECK (no epoch filter, no mismatch halt), PROJ_NC_EPOCH_FROM_FIRST_RECORD
// (an empty projection's followers keep no epoch filter), PROJ_NC_NO_MTU_CHECK (the configured MTU is not handed to
// the library), PROJ_NC_COMPARES_NOT_DELIVERY (verified compares do not count for BEHIND), PROJ_NC_HOLE_MARKS_ALL (a
// hole one source shows marks every source), PROJ_NC_START_AT_FIRST (an empty projection starts at the first record
// its source holds), and the FollowRules controls. PROJ_TC_SOURCE_FAULTS (test build only) injects per-source faults in
// the frame sink: records dropped (holed or wiped sources) and a delivery rate (a slow source); see FollowDuty.cpp.

namespace openexchange { namespace gateway { namespace projector {

// What one source's member node reported in the last round (pj-node writes, pj-duty reads).
struct NodeInfo
{
    NodeRole role = NodeRole::UNKNOWN;
    std::optional<std::int64_t> replaysActive;
    std::optional<std::int64_t> readerBudget;
    std::int64_t answeredAtNs = -1;
    std::string error;
};

class NodeTable
{
public:
    explicit NodeTable(std::size_t n) : m_nodes(n) {}
    std::vector<NodeInfo> snapshot() const
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_nodes;
    }
    void store(std::vector<NodeInfo> nodes)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_nodes = std::move(nodes);
    }
    void setActive(bool active) noexcept { m_active.store(active, std::memory_order_release); }
    bool active() const noexcept { return m_active.load(std::memory_order_acquire); }
    std::atomic<std::uint64_t> rounds{ 0 };
    std::atomic<std::uint64_t> failures{ 0 };

private:
    mutable std::mutex m_lock;
    std::vector<NodeInfo> m_nodes;
    std::atomic<bool> m_active{ false };
};

// pj-node: the member node polls (see above).
class NodePoller
{
public:
    NodePoller(const ProjectorSettings &settings, NodeTable &table) : m_settings(settings), m_table(table) {}
    ~NodePoller() { stop(); }
    void start();
    void stop();
    // One round on the caller's thread (the tests drive it).
    void roundOnce();

private:
    const ProjectorSettings &m_settings;
    NodeTable &m_table;
    std::mutex m_lock;
    std::condition_variable m_cv;
    bool m_stop = false;
    std::thread m_thread;
};

// The follow side's view for /health and /metrics (pj-duty writes it every status interval; the admin thread reads).
struct FollowView
{
    std::uint64_t generation = 0;
    std::optional<std::size_t> source;      // attached source index
    std::string endpoint;
    std::int64_t archiveId = -1;
    std::size_t chainIndex = 0;
    std::int64_t recordingId = -1;
    std::string phase = "IDLE";
    std::int64_t lastAttachFrom = -1;       // the replay position of the last attach
    bool lastAttachFromMemo = false;
    std::int64_t next = 0;                  // the next sequence pj-duty hands on
    std::int64_t appliedSeq = 0;
    std::optional<std::int64_t> dbEpoch;
    std::int64_t genesisId = 0;             // the cluster's, last read (0: none)
    std::int64_t headSeq = -1;              // the core's head at the last answered probe
    std::int64_t lastHeadProbeNs = -1;
    HaltState halt;
    std::vector<std::string> sourceStates;  // per source: "<mark>/<role>/<blocked ms>"
    std::uint64_t switches = 0;
    std::uint64_t holes = 0;
    std::uint64_t belowRange = 0;
    std::uint64_t conflicts = 0;
    std::uint64_t schemaRefusals = 0;
    std::uint64_t mtuRefusals = 0;
    std::uint64_t behind = 0;
    std::uint64_t leaderAvoided = 0;
    std::uint64_t budgetSkips = 0;
    std::uint64_t refusedReplays = 0;
    std::uint64_t stopRequestsAnswered = 0; // recorded replays whose stopReplay the archive answered OK (not proof of a stop)
    std::uint64_t orphanUnknown = 0;
    std::uint64_t epochZeroRecords = 0;
    std::uint64_t foreignEpochRecordings = 0;
    std::uint64_t reReads = 0;
    std::uint64_t replaysStarted = 0;
    std::uint64_t replayStops = 0;
};

class FollowViewBox
{
public:
    FollowView get() const
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_view;
    }
    void set(FollowView v)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_view = std::move(v);
    }

private:
    mutable std::mutex m_lock;
    FollowView m_view;
};

struct FollowParts
{
    FollowExchange *exchange = nullptr;
    NodeTable *nodes = nullptr;
    FollowViewBox *view = nullptr;
};

std::unique_ptr<DutyWork> makeFollowDuty(const DutyContext &context, const ProjectorSettings &settings,
    ProjectorShared &shared, const FollowParts &parts);

}}}

#endif
