#ifndef GATEWAY_MARKETDATA_MD_WS_SERVER_H
#define GATEWAY_MARKETDATA_MD_WS_SERVER_H

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "gateway/common/DutyBridge.h"

#include "BookCore.h"

// MdWsServer - oe-marketdata's public listener: a WebSocket on Boost.Beast over one Asio reactor run by MD_IO_THREADS
// threads (md-io-<n>). Public market data: no credential is asked for or read on this socket (the user streams keep
// theirs in oe-stream). The I/O threads accept, parse client frames and write; they never touch a book or a
// subscription (the duty thread's BookCore), and the duty thread never touches a socket.
//
// Path /ws, nothing else (404). Client frames, JSON: {"action":"subscribe","marketId":N}, {"action":"refresh",
// "marketId":N}, {"action":"ping"}. Anything else becomes an ERROR frame (BAD_REQUEST) sent in order by the duty thread.
// A frame above MD_WS_MAX_FRAME closes 1009. Every accepted action goes to the duty thread through the bounded command
// ring (MD_COMMAND_RING); a full ring drops the action and counts it (the open itself is closed 1013).
//
// Duty -> I/O: per connection an Outlet with a byte-bounded queue (MD_WS_QUEUE_BYTES). One write at a time on the
// connection's strand. When a frame would pass the bound the queue is dropped, nothing more is queued, and once the
// write in progress finishes the connection asks the duty thread for a resync: a fresh initial state. A connection
// whose write has not finished within MD_WS_UNWRITABLE_CLOSE_MS loses its socket. A connection that sent nothing (no
// frame, no pong to the server's pings) for MD_WS_IDLE_MS is closed. Above MD_WS_MAX_CONNECTIONS a new connection is
// closed at accept.

namespace openexchange { namespace gateway { namespace marketdata {

struct MdCommand
{
    enum class Kind : std::uint8_t
    {
        NONE,
        OPEN,
        SUBSCRIBE,
        REFRESH,
        PING,
        BAD,
        RESYNC,
        GONE,
    };
    Kind kind = Kind::NONE;
    std::uint64_t connId = 0;
    std::shared_ptr<Outlet> outlet;
    std::int64_t marketId = 0;
    std::string text; // BAD: what was wrong
};

using MdCommandRing = MpmcRing<MdCommand>;

struct MdWsSettings
{
    std::string host;
    std::uint16_t port = 0;
    int ioThreads = 2;
    std::int64_t maxConnections = 4096;
    std::int64_t queueBytes = 524'288;
    std::int64_t unwritableCloseMs = 30'000;
    std::int64_t idleMs = 60'000;
    std::int64_t maxFrame = 65'536;
    std::int64_t sendStallMs = 5000;
    std::size_t maxHeaderBytes = 8192;
};

struct MdWsCounters
{
    std::atomic<std::int64_t> connections{ 0 };         // md_ws_connections
    std::atomic<std::uint64_t> refused{ 0 };            // closed at accept above MD_WS_MAX_CONNECTIONS
    std::atomic<std::uint64_t> notFound{ 0 };           // an HTTP request that is not the /ws upgrade
    std::atomic<std::uint64_t> opsRefused{ 0 };         // client actions dropped because the command ring was full
    std::atomic<std::uint64_t> closedSlow{ 0 };         // md_ws_closed_slow_total
    std::atomic<std::uint64_t> queueDrops{ 0 };         // queues dropped at MD_WS_QUEUE_BYTES
    std::array<std::atomic<std::uint64_t>, static_cast<std::size_t>(FrameType::COUNT)> sent{}; // md_ws_frames_sent_total
    std::atomic<std::int64_t> pending{ 0 };             // frames queued and not yet written or dropped
    std::atomic<std::int64_t> waitingSinceNs{ -1 };     // the last write progress, or when pending left 0
};

class MdWsServer
{
public:
    MdWsServer(MdWsSettings settings, MdCommandRing &commands);
    ~MdWsServer();
    MdWsServer(const MdWsServer &) = delete;
    MdWsServer &operator=(const MdWsServer &) = delete;

    bool bind(std::string &error);
    std::uint16_t boundPort() const noexcept;
    void start();
    bool stop(std::int64_t boundMs);
    bool accepting() const noexcept { return m_accepting.load(std::memory_order_acquire); }
    const MdWsCounters &counters() const noexcept;
    // clients_served: no frame has waited for a connected client longer than MD_SEND_STALL_MS.
    bool clientsServed(std::int64_t nowNs) const noexcept;

    struct Impl;

private:
    std::unique_ptr<Impl> m_impl;
    std::atomic<bool> m_accepting{ false };
};

}}}

#endif
