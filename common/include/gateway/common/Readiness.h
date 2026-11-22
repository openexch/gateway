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
// oe-projector /ready, all of (a standby reads not ready and says so):
//   driver_connected    as above
//   lease_held          this copy holds the writer lease
//   session_registered  the head-probe session's registration ack arrived
//   head_probe_fresh    a head probe answered no longer ago than HISTORY_PROJECTOR_HEAD_FRESH_MS
//   not_halted          no halt is set
//   beat_fresh          the last beat this copy committed is younger than HISTORY_PROJECTOR_READY_LAG_MS
// oe-marketdata /ready, all of:
//   driver_connected    as above
//   me_session          an ME leader session is open and a ClusterHeartbeat arrived within MD_EGRESS_STALE_MS
//   book_fresh          every market of MD_EXPECTED_MARKETS (never "the markets seen so far") has a snapshot of this ME
//                       session, an OK ladder, and either a book frame younger than MD_BOOK_STALE_MS or no level on
//                       either side with a fresh heartbeat (an emptied book does not turn stale by being quiet)
//   journal_attached    a journal source is attached (the follower is FOLLOWING)
//   journal_fresh       its stall verdicts are clean within two MD_SOURCE_PROBE_MS and no record read waits on the
//                       market history longer than MD_HISTORY_STALE_MS
//   history_writable    the market history writer committed (or found nothing to commit) since its last failure
//   no_halt             no halt of the trade path is set
//   clients_served      no frame waits for a connected client longer than MD_SEND_STALL_MS
// oe-marketdata /ready/book: me_session, book_fresh, clients_served. /ready/session: me_session.

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
inline constexpr const char *LEASE_HELD = "lease_held";
inline constexpr const char *NOT_HALTED = "not_halted";
inline constexpr const char *BEAT_FRESH = "beat_fresh";
inline constexpr const char *ME_SESSION = "me_session";
inline constexpr const char *BOOK_FRESH = "book_fresh";
inline constexpr const char *JOURNAL_ATTACHED = "journal_attached";
inline constexpr const char *JOURNAL_FRESH = "journal_fresh";
inline constexpr const char *HISTORY_WRITABLE = "history_writable";
inline constexpr const char *NO_HALT = "no_halt";
inline constexpr const char *CLIENTS_SERVED = "clients_served";
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

struct ProjectorReadyBounds
{
    std::int64_t headFreshMs = 3000;
    std::int64_t readyLagMs = 5000;
};

struct MarketDataReadyBounds
{
    std::int64_t egressStaleMs = 10'000; // MD_EGRESS_STALE_MS
    std::int64_t bookStaleMs = 15'000;   // MD_BOOK_STALE_MS
};

// The state of one market's book as readiness and the stale flag of the book frames see it (one rule for both).
enum class MdBookState : std::uint8_t
{
    NOT_EXPECTED,
    FRESH,
    NO_SNAPSHOT, // no snapshot in this ME session
    BROKEN,      // the ladder is not OK (a chain break, or emptied by an unversioned snapshot)
    STALE        // OK, but no book frame within MD_BOOK_STALE_MS while it has levels (or no fresh heartbeat while empty)
};
const char *nameOf(MdBookState state) noexcept;
bool mdHeartbeatFresh(std::int64_t lastHeartbeatNs, std::int64_t nowNs, const MarketDataReadyBounds &bounds) noexcept;
MdBookState mdBookState(const MdMarketStatus &market, std::int64_t lastHeartbeatNs, std::int64_t nowNs,
    const MarketDataReadyBounds &bounds) noexcept;

struct Condition
{
    const char *name;
    bool ok;
};

// Every condition with its state, in the documented order.
std::vector<Condition> gatewayReadyConditions(const Status &s, std::int64_t nowNs, const GatewayReadyBounds &bounds);
std::vector<Condition> gatewaySessionConditions(const Status &s);
std::vector<Condition> streamReadyConditions(const Status &s, std::int64_t nowNs, const StreamReadyBounds &bounds);
std::vector<Condition> projectorReadyConditions(const Status &s, std::int64_t nowNs, const ProjectorReadyBounds &bounds);
std::vector<Condition> marketDataReadyConditions(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds);
std::vector<Condition> marketDataBookConditions(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds);
std::vector<Condition> marketDataSessionConditions(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds);

// The names of the failing ones (empty = ready).
std::vector<std::string> failing(const std::vector<Condition> &conditions);

}}

#endif
