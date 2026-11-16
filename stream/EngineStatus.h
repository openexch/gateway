#ifndef GATEWAY_STREAM_ENGINE_STATUS_H
#define GATEWAY_STREAM_ENGINE_STATUS_H

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "gateway/common/Settings.h"

// EngineStatus - the engine status feed of oe-stream, the pure part: what one poll of a node's own endpoint says, the
// derivation of each cluster's view from one round of polls, election detection across rounds, the `engine` and
// `election` frames, and the rate cap with change detection. No socket, no clock of its own (EnginePoller owns both).
//
// Source. Display telemetry, not replicated state: it comes over the network from each node's own endpoint (cluster-kit
// NodeEndpoint: GET /health and GET /metrics on the node's probe port), never from a disk and never inferred from
// another node. A follower's row is what that follower says about itself; a node that does not answer is `unreachable`
// from its own failed poll, whatever the leader says.
//
//   /health body (cluster-kit NodeReadiness.probe().detail, on 200 and on 503 alike): space separated key=value tokens,
//     role=<LEADER|FOLLOWER|CANDIDATE> recovery=<reason> source term elections applied commit checkpoint
//     observationAgeMs consensusAgeMs maxLagBytes maxCheckpointAgeMs. HTTP 200 = the node's duty cycle is live.
//   /metrics (Prometheus text): the unlabelled series named below with the cluster's prefix ("core_" or "match_").
//
// Fields and where each comes from (null = no source; a UI renders null as a dash):
//   node.id                  the configured member id (STREAM_*_NODE_ENDPOINTS), checked against <p>member_id
//   node.role                /health role (leader, follower, candidate); unreachable = this node's own poll failed
//   node.live                /health status 200
//   node.recovery            /health recovery (the node's own readiness reason, "ready" when ready)
//   node.term                /health term
//   node.commitPosition      /health commit (on a follower the larger of its local and the leader-notified commit)
//   node.appliedPosition     /health applied (the log position this node's service has applied)
//   node.behindBytes         the leader's commitPosition minus this node's appliedPosition, both as polled in the same
//                            round (log positions are bytes). Sampled from two processes, not at one instant, so a node
//                            polled after the leader can read negative; not clamped. Null while there is no leader
//   node.entriesBehind       null: no node reports a count of entries, only byte positions
//   node.snapshot.position   core_snapshot_last_log_position (-1 = never -> null); the matching engine exposes none
//   node.snapshot.atMs       the poll time minus <p>snapshot_age_seconds, fixed when the snapshot is first seen
//   node.snapshot.stateHash  <p>snapshot_state_hash: no engine exposes a snapshot state hash today, so null
//   node.version, node.buildCommit  null: neither engine exposes its version or build commit on its endpoint
//   cluster.leaderId/term    the live node (/health 200) that reports itself leader, with its term (two leaders: the
//                            higher term wins; the same term twice: null, shown as a conflict). A node that is not live
//                            keeps the role of its last evidence on its own row and is never the cluster's leader
//   cluster.snapshot         the highest snapshot position any node reports, with its time; agreement = "matched" when
//                            every node that reports a hash for that position reports the same one, "diverged" (with
//                            divergedNodes naming the nodes that differ from the majority) otherwise, null with fewer
//                            than two hashes at that position
//   cluster.lastElection     detected across rounds (below)
//   bridge.consumedJournalSeq   the core leader's core_feed_cursor (lastAppliedJournalSeq of the ME feed)
//   bridge.meJournalSeq         the matching-engine leader's match_engine_next_journal_seq minus one (0 = unbound -> null)
//   bridge.entriesBehind        meJournalSeq - consumedJournalSeq, sampled from two processes in one round, not at one
//                               instant (not clamped: it can read negative)
//
// Node answers. A node whose /health and /metrics both answer, and whose <p>member_id is its configured id, is shown; a
// node neither of which answers is `unreachable`; anything between (one answer, an unreadable body, another member id)
// is shown with role null and the reason in `error`, never with values that might be another node's.
//
// Elections. The model remembers the leader and term of the last round that had one. A round in which no node reports
// itself leader and some node reports itself in an election (role candidate, or recovery=election) is the `start` (the
// old leader's last polled commit, the candidates); a dead leader alone is not a start, its followers say so when their
// election begins. The first round with another leader or a higher term is the `end` (old and new leader and term); a
// start whose old leader comes back in its own term ends with the leader and term unchanged. The
// contract's duration (last committed entry of the old term to the first of the new one) has no source: durationMs is
// null; observedGapMs is the time between the last round that saw the old leader and the first that saw the new one, so
// it carries the poll interval as its resolution. "Order entry reopened" (orderEntryReopened) is null: whether a
// gateway's order entry is open again is that gateway's own session state, which oe-stream does not hold; the core's
// admission gate is replicated state an election does not change, so it is no source for it.

namespace openexchange { namespace gateway { namespace stream { namespace engine {

// The configured endpoint of one node (STREAM_CORE_NODE_ENDPOINTS, STREAM_ME_NODE_ENDPOINTS; Settings.h).
using NodeEndpointSpec = gateway::NodeEndpoint;

struct HealthBody
{
    std::optional<std::string> role; // LEADER, FOLLOWER, CANDIDATE as the node wrote it
    std::optional<std::string> recovery;
    std::optional<std::int64_t> term;
    std::optional<std::int64_t> elections;
    std::optional<std::int64_t> applied;
    std::optional<std::int64_t> commit;
};

// nullopt when the body carries no role= token (not a cluster-kit node probe).
std::optional<HealthBody> parseHealth(std::string_view body);

// The unlabelled series of a Prometheus exposition: name -> value text. Comments, labelled series and malformed lines
// are skipped.
std::map<std::string, std::string> parseMetrics(std::string_view body);
// An integer series ("123"); nullopt when absent or not an integer.
std::optional<std::int64_t> metricInt(const std::map<std::string, std::string> &metrics, const std::string &name);

// One node's answers in one round.
struct NodeSample
{
    bool healthAnswered = false; // an HTTP answer to GET /health (any status)
    int healthStatus = 0;
    std::string healthBody;
    bool metricsAnswered = false;
    int metricsStatus = 0;
    std::string metricsBody;
    std::string error; // why the node was not reached (connect refused, timeout, ...)
};

enum class Flavor : std::uint8_t
{
    CORE,
    MATCHING_ENGINE,
};

struct SnapshotView
{
    std::optional<std::int64_t> position;
    std::optional<std::int64_t> atMs;
    std::optional<std::string> stateHash;
    std::optional<std::string> agreement; // matched, diverged
};

struct NodeView
{
    std::int64_t id = 0;
    std::optional<std::string> role; // leader, follower, candidate, unreachable; null = answered but unreadable
    std::optional<bool> live;
    std::optional<std::string> recovery;
    std::optional<std::int64_t> term;
    std::optional<std::int64_t> commitPosition;
    std::optional<std::int64_t> appliedPosition;
    std::optional<std::int64_t> behindBytes;
    SnapshotView snapshot;
    std::optional<std::string> error;
    // the series the bridge line reads (not rendered per node)
    std::optional<std::int64_t> feedCursor;
    std::optional<std::int64_t> nextJournalSeq;
};

struct Election
{
    std::string phase; // start, end
    std::int64_t observedAtMs = 0;
    std::optional<std::int64_t> previousTerm;
    std::optional<std::int64_t> previousLeaderId;
    std::optional<std::int64_t> lastCommitPosition; // start: the old leader's last polled commit
    std::vector<std::int64_t> candidates;           // start: the nodes reporting candidate
    std::optional<std::int64_t> term;               // end
    std::optional<std::int64_t> leaderId;           // end
    std::optional<std::int64_t> observedGapMs;      // end
};

// One cluster's view, rebuilt from every complete round.
class ClusterModel
{
public:
    ClusterModel(Flavor flavor, std::vector<NodeEndpointSpec> nodes);

    // One sample per configured node, in configuration order. Returns the election events this round shows.
    std::vector<Election> ingest(const std::vector<NodeSample> &round, std::int64_t nowMs);

    Flavor flavor() const noexcept { return m_flavor; }
    const char *key() const noexcept;
    const std::vector<NodeEndpointSpec> &endpoints() const noexcept { return m_specs; }
    const std::vector<NodeView> &nodes() const noexcept { return m_nodes; }
    std::optional<std::int64_t> leaderId() const noexcept { return m_leaderId; }
    std::optional<std::int64_t> term() const noexcept { return m_term; }
    const SnapshotView &snapshot() const noexcept { return m_snapshot; }
    const std::vector<std::int64_t> &divergedNodes() const noexcept { return m_diverged; }
    const std::optional<Election> &lastElection() const noexcept { return m_lastElection; }
    const NodeView *leader() const noexcept;

    std::string json() const; // the cluster's block of the engine frame

private:
    NodeView readNode(const NodeEndpointSpec &spec, const NodeSample &sample, std::int64_t nowMs, const NodeView *before);
    void deriveSnapshot();

    Flavor m_flavor;
    std::string m_prefix;
    std::vector<NodeEndpointSpec> m_specs;
    std::vector<NodeView> m_nodes;
    std::optional<std::int64_t> m_leaderId;
    std::optional<std::int64_t> m_term;
    SnapshotView m_snapshot;
    std::vector<std::int64_t> m_diverged;
    std::optional<Election> m_lastElection;
    // election tracking across rounds
    std::optional<std::int64_t> m_knownLeader;
    std::optional<std::int64_t> m_knownTerm;
    std::optional<std::int64_t> m_knownLeaderCommit;
    std::int64_t m_knownLeaderSeenMs = 0;
    bool m_electionOpen = false;
};

// Snapshot agreement among nodes at one position: every hash reported for `position` equal -> "matched"; any differs
// -> "diverged" and `diverged` names the nodes whose hash differs from the most common one (ties: from the lowest id's);
// fewer than two hashes at that position -> nullopt.
std::optional<std::string> snapshotAgreement(const std::vector<NodeView> &nodes, std::int64_t position,
    std::vector<std::int64_t> &diverged);

// The whole frame: {"type":"engine","core":{...},"matchingEngine":{...},"bridge":{...}}.
std::string engineFrame(const ClusterModel &core, const ClusterModel &me);
std::string electionFrame(const char *clusterKey, const Election &e);

// The rate cap with change detection: a frame identical to the last one sent is never sent again; a changed frame is
// sent at once when the last send is at least `minIntervalMs` old, else held (the newest replaces an older held one) and
// sent by poll() once the interval has passed.
class FrameGate
{
public:
    explicit FrameGate(std::int64_t minIntervalMs) : m_minIntervalMs(minIntervalMs) {}
    std::optional<std::string> offer(std::string frame, std::int64_t nowMs);
    std::optional<std::string> poll(std::int64_t nowMs);
    // When poll() will send the held frame (nullopt: nothing held).
    std::optional<std::int64_t> heldDueMs() const noexcept;
    std::uint64_t suppressedUnchanged() const noexcept { return m_unchanged; }

private:
    std::int64_t m_minIntervalMs;
    std::optional<std::string> m_last;
    std::optional<std::string> m_held;
    std::int64_t m_lastSentMs = 0;
    bool m_sentOnce = false;
    std::uint64_t m_unchanged = 0;
};

}}}}

#endif
