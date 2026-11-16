#ifndef GATEWAY_STREAM_WS_SERVER_H
#define GATEWAY_STREAM_WS_SERVER_H

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "gateway/common/Authenticator.h"
#include "gateway/common/DutyBridge.h"

#include "StreamCore.h"

// WsServer - oe-stream's public listener: WebSocket on Boost.Beast over one Asio reactor run by STREAM_IO_THREADS
// threads (st-io-<n>). The I/O threads accept, authenticate, parse client frames and write; they never touch the
// cluster session or any window or cache state (the duty thread's), and the duty thread never touches a socket.
//
// Path /ws/v2, nothing else; a query string is refused (400): a credential never comes from a URL. One credential, by
// one of three paths:
//   Sec-WebSocket-Protocol: bearer, <token>   (browsers; the answer names the "bearer" subprotocol)
//   Authorization: Bearer <token> | X-API-Key  (non-browser clients)
//   a first frame {"op":"auth","token":"..."} within STREAM_WS_AUTH_TIMEOUT_MS of the upgrade (a header-less client)
// Two credentials at once are 401; a header credential that fails is answered over HTTP (401, 429, 503) before any
// upgrade; a first-frame credential that fails closes 1008 (1013 when the users store cannot answer). The principal
// decides the user: a "userId" key anywhere in any client frame closes the connection 4003.
//
// Client frames: {"op":"subscribe","channels":[...],"resume":{"eventSeq":N}?}, {"op":"unsubscribe","channels":[...]},
// {"op":"ping"}; anything else (an unknown op, an unknown key, a bad channel) is an `error` frame, code VALIDATION. A
// frame above STREAM_WS_MAX_MESSAGE_BYTES closes 1009. Every accepted op goes to the duty thread through the bounded
// command ring (STREAM_COMMAND_RING); a full ring refuses the op (`error` OVERLOADED, or close 1013 for the open itself).
//
// Duty -> I/O: per connection a WsOutlet, a small lock-free handover ring the duty thread fills from the connection's
// bounded queue (StreamCore) and the connection's strand drains in order, one write at a time. The duty thread wakes a
// connection through the st-wake thread (an atomic notify, no lock and no syscall that can block on the duty thread);
// st-wake posts the write to the strand.
//
// Liveness: a ping every STREAM_WS_PING_MS while the connection is not writing; a connection that sent nothing (no frame,
// no pong) for STREAM_WS_IDLE_MS is closed 1001. A close the duty thread asks for (4001 at the queue bound) is sent after
// the write in progress; a peer that does not let that write finish within STREAM_WS_IDLE_MS loses its socket.

namespace openexchange { namespace gateway { namespace stream {

struct StreamCommand
{
    enum class Kind : std::uint8_t
    {
        NONE,
        OPEN,
        SUBSCRIBE,
        UNSUBSCRIBE,
        PING,
        GONE,
    };
    Kind kind = Kind::NONE;
    std::uint64_t connId = 0;
    std::int64_t userId = 0;
    std::shared_ptr<Outlet> outlet;
    unsigned channels = 0;
    bool hasResume = false;
    std::int64_t resumeFrom = 0;
    std::int64_t frameUserId = 0; // read only by the test-only control GW_NC_ROUTE_BY_FRAME_USER
};

using CommandRing = MpmcRing<StreamCommand>;

struct WsServerSettings
{
    std::string host;
    std::uint16_t port = 0;
    int ioThreads = 2;
    std::int64_t maxConnections = 2000;
    std::int64_t authTimeoutMs = 5000;
    std::int64_t pingMs = 15'000;
    std::int64_t idleMs = 60'000;
    std::int64_t maxMessageBytes = 65'536;
    std::size_t handoverDepth = 64; // frames handed to a writer at once (a batching step; the bound is StreamCore's)
    std::size_t maxHeaderBytes = 8192;
    std::int64_t sendBufferBytes = 0; // SO_SNDBUF of each socket; 0 = the OS default
    std::string threadPrefix = "st";
};

// Close codes counted by /metrics (stream_ws_closed_total{code}); any other code is counted as "other".
inline constexpr std::array<std::uint16_t, 9> COUNTED_CLOSE_CODES = { 1000, 1001, 1006, 1008, 1009, 1011, 1013, 4001, 4003 };

struct WsCounters
{
    std::atomic<std::int64_t> connections{ 0 };
    std::atomic<std::uint64_t> refused{ 0 };        // closed at accept above STREAM_MAX_CONNECTIONS
    std::atomic<std::uint64_t> httpRefused{ 0 };    // answered over HTTP without an upgrade
    std::atomic<std::uint64_t> authFailed{ 0 };
    std::atomic<std::uint64_t> opsRefused{ 0 };     // client ops refused because the command ring was full
    std::array<std::atomic<std::uint64_t>, COUNTED_CLOSE_CODES.size() + 1> closed{}; // by COUNTED_CLOSE_CODES, then other
};

class WsServer
{
public:
    WsServer(WsServerSettings settings, Authenticator &auth, CommandRing &commands);
    ~WsServer();
    WsServer(const WsServer &) = delete;
    WsServer &operator=(const WsServer &) = delete;

    bool bind(std::string &error);
    std::uint16_t boundPort() const noexcept;
    void start();
    bool stop(std::int64_t boundMs);
    bool accepting() const noexcept { return m_accepting.load(std::memory_order_acquire); }
    const WsCounters &counters() const noexcept;

    struct Impl;

private:
    std::unique_ptr<Impl> m_impl;
    std::atomic<bool> m_accepting{ false };
};

}}}

#endif
