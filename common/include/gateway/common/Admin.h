#ifndef GATEWAY_COMMON_ADMIN_H
#define GATEWAY_COMMON_ADMIN_H

#include <cstdint>
#include <string>
#include <vector>

#include "cluster_tools/runtime/HttpServer.h"

#include "gateway/common/Readiness.h"
#include "gateway/common/Settings.h"
#include "gateway/common/Status.h"

// The admin endpoints, served on the admin bind only (never on the public listener), GET only:
//   /health          200 while the media driver is connected and the duty heartbeat is younger than the watchdog
//                    bound, else 503 (status STARTING before the driver connects, STALLED after); JSON: process, version,
//                    build sha, cluster-tools sha, money schema, driver mode and dir, duty heartbeat age, session
//                    state, readiness with every condition, the treasury lane (oe-gateway), and every setting in force
//                    with where it came from (secrets redacted).
//   /ready           200 "ready" or 503 with JSON {"ready":false,"failing":[names]} (Readiness.h).
//   /ready/session   oe-gateway: connected and registered; oe-marketdata: me_session.
//   /ready/book      oe-marketdata only: me_session, book_fresh, clients_served.
//   /metrics         Prometheus text, prefix gateway_, stream_, projector_ or md_.
// oe-projector: /ready also names the copy's part ("role":"writer" or "standby"); /health carries the projector block
// its writer thread renders (FrontWork::healthJson) and no gateway or stream block.
// Pure: route() renders from an AdminView the admin thread assembles at request time, so the tests need no process.

namespace openexchange { namespace gateway {

struct AdminView
{
    Role role = Role::GATEWAY;
    std::string processName;   // oe-gateway, oe-stream
    std::string version;
    std::string build;         // this repository's commit
    std::string clusterTools;  // the cluster-tools commit the package was built from
    int moneySchemaVersion = 0;
    std::string moneySchemaSha256;

    std::string driverMode;    // embedded, external
    std::string driverDir;
    std::int64_t driverTimeoutMs = 0;
    bool driverReclaimed = false;

    std::int64_t nowNs = 0;
    std::int64_t heartbeatAgeMs = 0; // duty heartbeat age, measured at request time
    std::int64_t watchdogMs = 0;
    std::string idleMode = "backoff"; // <P>_IDLE in force: busy_spin, backoff or sleep
    std::int64_t conductorHeartbeatAgeMs = -1;
    std::uint64_t logDropped = 0;
    std::uint64_t adminServed = 0;

    Status status;
    GatewayReadyBounds gatewayBounds;
    StreamReadyBounds streamBounds;
    ProjectorReadyBounds projectorBounds;
    MarketDataReadyBounds marketDataBounds;
    std::string treasuryLane;  // oe-gateway: "off" or "on"
    const std::vector<SettingRecord> *settings = nullptr;
    std::string frontHealth;   // members the public side adds to /health (",\"auth\":{...}")
    std::string frontMetrics;  // Prometheus lines the public side adds to /metrics
};

cluster_tools::runtime::HttpResponse adminRoute(const cluster_tools::runtime::HttpRequest &request, const AdminView &view);

std::string renderHealth(const AdminView &view, int &status);
std::string renderMetrics(const AdminView &view);
std::string jsonEscape(const std::string &text);
// The metric prefix of a role: gateway_, stream_, projector_, md_.
const char *metricPrefix(Role role) noexcept;

}}

#endif
