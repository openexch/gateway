#ifndef GATEWAY_PROJECTOR_SETTINGS_H
#define GATEWAY_PROJECTOR_SETTINGS_H

#include <cstdint>
#include <string>
#include <vector>

#include "cluster_tools/archive/ArchiveSettings.h"
#include "cluster_tools/client/Credentials.h"
#include "cluster_tools/runtime/DirProbe.h"

#include "gateway/common/Environment.h"
#include "gateway/common/Settings.h"

// The settings of oe-projector, prefix HISTORY_PROJECTOR_, read with the rules of SettingsReader (every problem in one
// pass, exit 78 before anything starts; every setting in force is written at boot and shown in /health; a misspelt
// HISTORY_PROJECTOR_ name is refused). None is a limit in code: each size, bound, rate and timeout below is a setting
// with its default.
//
// Process (as the gateway binaries):
//   _DRIVER_MODE           required: embedded (dev and demo) or external (the host aeronmd at AERON_DIR)
//   AERON_DIR / _DRIVER_DIR, _DRIVER_TIMEOUT_MS 10000    by mode, as the gateway binaries
//   _INSTANCE_ID           required: a positive int64; the embedded driver dir suffix and the process counter key
//   _ADMIN_BIND            required: host:port (IPv4) of /health, /ready, /metrics
//   _IDLE backoff|busy, _DUTY_WATCHDOG_MS 10000 (above _ARCHIVE_TIMEOUT_MS), _STOP_TIMEOUT_MS 2000,
//   _STATUS_INTERVAL_MS 100,
//   _EMBEDDED_RETRY_MS 1000 (embedded only)
// The journal sources (the cluster-tools archive reader rules, core order journal profile):
//   _JOURNAL_ARCHIVES      required: archive control endpoints host:port,... in failover order
//   _JOURNAL_CONTROL_STREAM, _JOURNAL_STREAM, _JOURNAL_CHANNEL, _REPLAY_STREAM, _JOURNAL_MTU   required, never guessed;
//                          the MTU is checked against each recording before a replay
//   _LOCAL_HOST            required: the routable address replays are sent to (loopback refused with remote sources)
//   _JOURNAL_NODE_ENDPOINTS required: one entry per archive, in the same order: host:port of that core member's node
//                          endpoint (its /health and /metrics: the role it reports, its readerReplayBudget and
//                          core_archive_replays_active), or '-' for a source that is not a core member (a journal
//                          replica: never the leader, no budget read). Readers avoid the leader with it.
//   _NODE_POLL_MS 1000, _NODE_POLL_TIMEOUT_MS 500 (below the poll), _NODE_MAX_BODY_BYTES 1048576
//   _NODE_FRESH_MS 3000 (above the poll): a node answer older than this is unknown (role, budget, replay count)
//   _NODE_FIRST_ROUND_MS 3000: after a takeover no source is chosen before the first node round answered, at most
//                          this long (a source is never chosen blind: it may be the leader)
//   _ARCHIVE_PROBE_MS 10000   the follower's recording probe: a replay with no progress between two probes while its
//                          recording grew is a stall (the source is left)
// The cluster session (the genesis and the core's head):
//   _CLUSTER_MEMBERS       required: the core members, id=host:port,...
//   _CLUSTER_KEY_FILE      required: a file holding keyId:GATEWAY:secretHex (a GATEWAY key; another class is refused)
//   _CORE_INGRESS_CHANNEL  required: a full aeron:udp URI naming term-length and mtu
//   _CORE_RESPONSE_CHANNEL required: a full aeron:udp URI naming endpoint, term-length and mtu
//   _REQUEST_TIMEOUT_MS 3000 (a head probe's deadline), _RECONNECT_DELAY_MS 1000, _REGISTER_RESEND_MS 5000
//   _SESSION_MAX_INFLIGHT 64, _QUERY_RATE_COUNT 100, _QUERY_RATE_PERIOD_MS 1000, _QUERY_BURST 200   the session's
//                          own bounds (the head probe sends one query at a time)
// PostgreSQL:
//   _PG_URL                required: postgresql:// URL of the projection database (the projector's role)
//   _PG_STATEMENT_TIMEOUT_MS 3000, _PG_RETRY_MS 1000, _PG_IDLE_IN_TX_TIMEOUT_MS 10000 (set on the projector's session:
//   ends a transaction a hung writer left open), _PG_KEEPALIVE_IDLE_S 5, _PG_KEEPALIVE_INTERVAL_S 2,
//   _PG_KEEPALIVE_COUNT 3 (TCP keepalive of the projector's connection)
//   _REBUILD_LOCK_TIMEOUT_MS 1000   --rebuild: the longest wait for the base tables behind open history reads; past it
//                          the rebuild changes nothing and exits 75
// The writer:
//   _LEASE_MS 10000, _LEASE_POLL_MS 1000   the writer lease and the standby's poll
//   _BATCH_EVENTS 256, _BATCH_MS 50, _QUEUE_EVENTS 4096
//   _HEAD_PROBE_MS 1000, _HEAD_FRESH_MS 3000, _ARCHIVE_TIMEOUT_MS 5000, _RELIST_MS 1000, _SOURCE_BEHIND_MS 5000,
//   _SOURCE_BACKOFF_MS 1000, _SOURCE_BACKOFF_MAX_MS 30000, _GAP_RETRY_MS 5000, _READY_LAG_MS 5000
// Pairs: _HEAD_FRESH_MS and _READY_LAG_MS above _HEAD_PROBE_MS (else a healthy copy flaps not ready between two
// probes); _SOURCE_BACKOFF_MAX_MS at least _SOURCE_BACKOFF_MS; _LEASE_POLL_MS below _LEASE_MS (a standby that polls less
// often than a lease lasts adds a whole lease to every takeover); _PG_STATEMENT_TIMEOUT_MS below a third of _LEASE_MS (a renewal that
// runs to its timeout still returns before the writer's own step-down point, the lease less one renewal interval);
// _REBUILD_LOCK_TIMEOUT_MS below _PG_STATEMENT_TIMEOUT_MS. A WARN at boot, not a refusal: _PG_IDLE_IN_TX_TIMEOUT_MS
// above _LEASE_MS (the fence still stops a stopped writer, but a takeover can then wait the idle timeout instead of at
// most one lease); _DUTY_WATCHDOG_MS not above _ARCHIVE_TIMEOUT_MS (an archive control call that waits out its timeout
// on pj-duty can trip the watchdog, exit 70, before the source is left).

namespace openexchange { namespace gateway {

inline constexpr const char *PROJECTOR_PREFIX = "HISTORY_PROJECTOR";

struct ProjectorSettings
{
    CommonSettings common; // driver, instance id, admin bind, idle, watchdog, stop timeout, status interval
    cluster_tools::archive::ArchiveSettings archive;
    std::vector<std::optional<Bind>> nodeEndpoints; // per archive; nullopt: not a core member
    std::int64_t nodePollMs = 1000;
    std::int64_t nodePollTimeoutMs = 500;
    std::int64_t nodeMaxBodyBytes = 1'048'576;
    std::int64_t nodeFreshMs = 3000;
    std::int64_t nodeFirstRoundMs = 3000;
    std::int64_t archiveProbeMs = 10'000;
    std::string clusterMembers;
    std::string clusterKeyFile;
    std::string ingressChannel;
    std::string responseChannel;
    std::int64_t requestTimeoutMs = 3000;
    std::int64_t reconnectDelayMs = 1000;
    std::int64_t registerResendMs = 5000;
    std::int64_t sessionMaxInflight = 64;
    std::int64_t queryRateCount = 100;
    std::int64_t queryRatePeriodMs = 1000;
    std::int64_t queryBurst = 200;
    cluster_tools::client::CoreCredentials clusterCredentials;
    std::string pgUrl;
    std::int64_t pgStatementTimeoutMs = 3000;
    std::int64_t pgRetryMs = 1000;
    std::int64_t pgIdleInTxTimeoutMs = 10'000;
    std::int64_t pgKeepaliveIdleS = 5;
    std::int64_t pgKeepaliveIntervalS = 2;
    std::int64_t pgKeepaliveCount = 3;
    std::int64_t rebuildLockTimeoutMs = 1000;
    std::int64_t leaseMs = 10'000;
    std::int64_t leasePollMs = 1000;
    std::int64_t batchEvents = 256;
    std::int64_t batchMs = 50;
    std::int64_t queueEvents = 4096;
    std::int64_t headProbeMs = 1000;
    std::int64_t headFreshMs = 3000;
    std::int64_t archiveTimeoutMs = 5000;
    std::int64_t relistMs = 1000;
    std::int64_t sourceBehindMs = 5000;
    std::int64_t sourceBackoffMs = 1000;
    std::int64_t sourceBackoffMaxMs = 30'000;
    std::int64_t gapRetryMs = 5000;
    std::int64_t readyLagMs = 5000;
    std::vector<std::string> warnings; // accepted but worth an operator's look: written at boot as WARN lines
};

ParseResult<ProjectorSettings> parseProjectorSettings(const Environment &env,
    const cluster_tools::runtime::DirProbe &probe = cluster_tools::runtime::systemDirProbe());

}}

#endif
