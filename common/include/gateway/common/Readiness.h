#ifndef GATEWAY_COMMON_READINESS_H
#define GATEWAY_COMMON_READINESS_H

#include <cstdint>
#include <string>
#include <vector>

#include "gateway/common/Status.h"

// Readiness - the conditions of /ready and /ready/session, each with a stable name. A 503 lists every failing
// condition by name, in this order. Pure: the inputs are a Status snapshot, the clock and the settings.
//
// oe-gateway /ready, all of:
//   driver_connected    the Aeron client is connected to its media driver and the process counter registered (false
//                       through the driver wait and the embedded start retries)
//   storage_space       the session's connect attempts are not refused for storage space (a starved session reads not
//                       ready while the episode lasts and exits 70 once it lasted the runtime's bound)
//   session_connected   a session to a leader (no leader = not ready)
//   session_registered  the registration ack arrived
//   config_loaded       a config report is loaded
//   gate_open           the global admission gate is OPEN
//   keepalive_fresh     a successful keepalive offer no older than GATEWAY_READY_STALE_MS
//   timeout_streak      fewer than GATEWAY_READY_TIMEOUT_STREAK request deadlines missed in a row
//   public_listener     the public listener accepts connections
// oe-gateway /ready/session: driver_connected, session_connected and session_registered only (a proxy routes cancels and balance
// reads on it while the gate is closed).
// oe-stream /ready, all of:
//   driver_connected    as above
//   session_registered  the registration ack arrived
//   event_gap           no unresolved event gap
//   head_probe_fresh    the last head probe answered no longer ago than STREAM_READY_STALE_MS
//   ws_listener         the WebSocket listener accepts connections

namespace openexchange { namespace gateway {

namespace condition {
inline constexpr const char *DRIVER_CONNECTED = "driver_connected";
inline constexpr const char *STORAGE_SPACE = "storage_space";
inline constexpr const char *SESSION_CONNECTED = "session_connected";
inline constexpr const char *SESSION_REGISTERED = "session_registered";
inline constexpr const char *CONFIG_LOADED = "config_loaded";
inline constexpr const char *GATE_OPEN = "gate_open";
inline constexpr const char *KEEPALIVE_FRESH = "keepalive_fresh";
inline constexpr const char *TIMEOUT_STREAK = "timeout_streak";
inline constexpr const char *PUBLIC_LISTENER = "public_listener";
inline constexpr const char *EVENT_GAP = "event_gap";
inline constexpr const char *HEAD_PROBE_FRESH = "head_probe_fresh";
inline constexpr const char *WS_LISTENER = "ws_listener";
}

struct GatewayReadyBounds
{
    std::int64_t readyStaleMs = 3000;
    std::int64_t timeoutStreak = 5;
};

struct StreamReadyBounds
{
    std::int64_t readyStaleMs = 15'000;
};

struct Condition
{
    const char *name;
    bool ok;
};

// Every condition with its state, in the documented order.
std::vector<Condition> gatewayReadyConditions(const Status &s, std::int64_t nowNs, const GatewayReadyBounds &bounds);
std::vector<Condition> gatewaySessionConditions(const Status &s);
std::vector<Condition> streamReadyConditions(const Status &s, std::int64_t nowNs, const StreamReadyBounds &bounds);

// The names of the failing ones (empty = ready).
std::vector<std::string> failing(const std::vector<Condition> &conditions);

}}

#endif
