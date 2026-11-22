#ifndef GATEWAY_MARKETDATA_SETTINGS_H
#define GATEWAY_MARKETDATA_SETTINGS_H

#include <cstdint>
#include <string>
#include <vector>

#include "cluster_tools/archive/ArchiveSettings.h"
#include "cluster_tools/client/ClientSettings.h"
#include "cluster_tools/client/Credentials.h"
#include "cluster_tools/runtime/DirProbe.h"

#include "gateway/common/Environment.h"
#include "gateway/common/Settings.h"

// The settings of oe-marketdata, read from the environment with the rules of SettingsReader. Every problem is found in
// one pass and printed as "missing setting: <KEY> ..." or "invalid setting: <KEY> ..."; the process exits 78 before
// anything starts (no bind, no driver, no Aeron object). Every size, bound and timeout is a setting with a default,
// never a limit in code. Two sizes are not settings: the number of book ladders (5, markets 1..5) and the levels a side
// (20) are constants of the cluster-tools book ladder.
//
// Required (no default):
//   MD_DRIVER_MODE           embedded (dev and demo) or external (prod, the host aeronmd)
//   AERON_DIR                external only (refused with embedded)
//   MD_DRIVER_DIR            embedded only (refused with external): the parent of <dir>/oe-marketdata-<instance id>
//   MD_INSTANCE_ID           a positive int64: the embedded driver dir suffix and the process counter key
//   MD_ME_CLUSTER_ADDRESSES  the ME members' hosts in member id order (h0,h1,...); never 0.0.0.0
//   MD_ME_PORT_BASE          the ME deployment's port base; member i's ingress is
//   CLUSTER_PORT_STRIDE      MD_ME_PORT_BASE + i * CLUSTER_PORT_STRIDE + 2 (the cluster-kit port rule; a stride of 0 is
//                            one member per address)
//   MD_ME_EGRESS_ENDPOINT    host:port of this process's egress (the ME must route to it; never 0.0.0.0)
//   ME_AUTH_CREDENTIAL       keyId:GATEWAY:secretHex; another class is refused
//   MD_ME_JOURNAL_ARCHIVES, MD_JOURNAL_CONTROL_STREAM, MD_JOURNAL_STREAM, MD_JOURNAL_CHANNEL, MD_REPLAY_STREAM,
//   MD_JOURNAL_MTU, MD_LOCAL_HOST   the ME journal's archives, with the bridge's meaning (the trade path; read and
//                            checked now, used by the journal follower)
//   MD_PUBLIC_BIND, MD_ADMIN_BIND   host:port; port 0 refused; no default port; the admin bind is IPv4 and never the
//                            public listener
//   MD_PG_URL, MD_PG_USER, MD_PG_PASSWORD_FILE   the market history database (the trade path)
//   MD_CORS_ORIGINS          a list of http(s) origins, or exactly `none`
//   MD_EXPECTED_MARKETS      the market ids this service serves, each 1..5, no repeats; readiness and the tape are
//                            judged against it
// With defaults:
//   MD_DRIVER_TIMEOUT_MS 10000 (3000..60000), MD_EGRESS_STALE_MS 10000, MD_BOOK_STALE_MS 15000,
//   MD_HISTORY_STALE_MS 5000, MD_SEND_STALL_MS 5000, MD_SOURCE_PROBE_MS 10000, MD_TRADE_FLUSH_MS 20,
//   MD_TICKERS_INTERVAL_MS 1000, MD_PENDING_TRADES 65536, MD_COMMIT_BATCH 512, MD_COMMIT_INTERVAL_MS 50,
//   MD_RECENT_TRADES 50, MD_CANDLE_HISTORY 200, MD_CANDLES_LIMIT_MAX 500, MD_WS_MAX_CONNECTIONS 4096,
//   MD_WS_QUEUE_BYTES 524288, MD_WS_UNWRITABLE_CLOSE_MS 30000, MD_WS_IDLE_MS 60000, MD_WS_MAX_FRAME 65536;
//   the trade path's: MD_ARCHIVE_TIMEOUT_MS 3000 (an archive control call, a replay that never connects, an endpoint
//   that never resolves; below MD_DUTY_WATCHDOG_MS: such a call blocks the duty thread), MD_SOURCE_BACKOFF_MS 1000 (a
//   journal source that faulted is asked again after this), MD_GAP_RESCAN_MS 5000 (a GAP re-reads every source from its
//   chain start this often), MD_PG_STATEMENT_TIMEOUT_MS 3000 (every market history statement and connect),
//   MD_PG_RETRY_MS 1000 (an unreachable market history is tried again after this);
//   the process shell's: MD_IO_THREADS 2, MD_IDLE backoff, MD_COMMAND_RING 4096, MD_DUTY_WATCHDOG_MS 5000,
//   MD_STOP_TIMEOUT_MS 2000, MD_STATUS_INTERVAL_MS 100, MD_EMBEDDED_RETRY_MS 1000 (embedded only);
//   the ME session's: MD_ME_RECONNECT_MS 1000 (a closed or failed session is replaced after this),
//   MD_ME_INGRESS_CHANNEL aeron:udp?term-length=64k|mtu=1408 (a full aeron:udp URI naming term-length and mtu; the
//   service offers no ingress but its keepalive).

namespace openexchange { namespace gateway { namespace marketdata {

inline constexpr int MARKET_ID_MIN = 1;
inline constexpr int MARKET_ID_MAX = 5; // the book ladders of the cluster-tools library (TICK_MARKETS)

struct MarketDataSettings
{
    CommonSettings common; // driver, instance id, binds, I/O threads, idle, watchdog, stop, status interval
    std::vector<std::string> meAddresses;
    std::int64_t mePortBase = 0;
    std::int64_t portStride = 0;
    std::string meIngress;        // id=host:port,... derived from the three keys above
    std::string meEgressEndpoint; // host:port
    std::string meIngressChannel = "aeron:udp?term-length=64k|mtu=1408";
    cluster_tools::client::CoreCredentials meCredentials;
    std::int64_t meReconnectMs = 1000;
    cluster_tools::archive::ArchiveSettings journal;
    std::string pgUrl;
    std::string pgUser;
    std::string pgPasswordFile;
    std::vector<std::string> corsOrigins; // empty = none
    std::vector<int> expectedMarkets;     // ascending, distinct

    std::int64_t egressStaleMs = 10'000;
    std::int64_t bookStaleMs = 15'000;
    std::int64_t historyStaleMs = 5000;
    std::int64_t sendStallMs = 5000;
    std::int64_t sourceProbeMs = 10'000;
    std::int64_t tradeFlushMs = 20;
    std::int64_t tickersIntervalMs = 1000;
    std::int64_t pendingTrades = 65'536;
    std::int64_t commitBatch = 512;
    std::int64_t commitIntervalMs = 50;
    std::int64_t recentTrades = 50;
    std::int64_t candleHistory = 200;
    std::int64_t candlesLimitMax = 500;
    std::int64_t wsMaxConnections = 4096;
    std::int64_t wsQueueBytes = 524'288;
    std::int64_t wsUnwritableCloseMs = 30'000;
    std::int64_t wsIdleMs = 60'000;
    std::int64_t wsMaxFrame = 65'536;
    std::int64_t archiveTimeoutMs = 3000;
    std::int64_t sourceBackoffMs = 1000;
    std::int64_t gapRescanMs = 5000;
    std::int64_t pgStatementTimeoutMs = 3000;
    std::int64_t pgRetryMs = 1000;
};

ParseResult<MarketDataSettings> parseMarketDataSettings(const Environment &env,
    const cluster_tools::runtime::DirProbe &probe = cluster_tools::runtime::systemDirProbe());

// The ME session settings these values make.
cluster_tools::client::ClientSettings meClientSettings(const MarketDataSettings &settings);

}}}

#endif
