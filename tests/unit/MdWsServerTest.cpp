// oe-marketdata's public listener, T1 in process: the real MdWsServer on a loopback port with a small duty loop in place
// of BookCore. A slow client (a bounded queue, a fresh state after it drains, closed after the unwritable time), a
// client that sends nothing (closed after the idle time), the other clients unaffected; clients_served against a sender
// that drops its frames (the MD_TC_SEND_DROPS test build); the per-source budgets of the public port.

#include <sys/socket.h>
#include <sys/time.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include "Check.h"

#include "MdWsServer.h"

using namespace openexchange::gateway;
using namespace openexchange::gateway::marketdata;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace {

std::int64_t monoNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

Frame textFrame(std::string text, FrameType type = FrameType::BOOK_DELTA)
{
    return Frame{ std::make_shared<const std::string>(std::move(text)), type };
}

// The listener and a duty loop that keeps the outlets: OPEN and GONE are recorded, a RESYNC is answered with one
// "STATE" frame, a LIMITED with one "LIMITED" frame, a PING with a PONG; every accepted client action is counted.
class Front
{
public:
    explicit Front(MdWsSettings s) : ring(4096), server(s, ring, nullptr, &inputMarkNs)
    {
        std::string error;
        if (!server.bind(error))
        {
            throw std::runtime_error(error);
        }
        server.start();
        duty = std::thread([this] { run(); });
    }
    ~Front()
    {
        stop = true;
        duty.join();
        server.stop(2000);
    }

    std::uint16_t port() const { return server.boundPort(); }

    // The connection ids open now, in open order.
    std::vector<std::uint64_t> open()
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<std::uint64_t> out;
        for (const auto &kv : outlets)
        {
            out.push_back(kv.first);
        }
        return out;
    }
    bool offer(std::uint64_t id, const Frame &f)
    {
        std::lock_guard<std::mutex> lock(mutex);
        const auto it = outlets.find(id);
        return outlets.end() != it && it->second->offer(f);
    }
    bool waitFor(const std::function<bool()> &cond, int ms)
    {
        const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
        while (std::chrono::steady_clock::now() < end)
        {
            if (cond())
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return cond();
    }

    MdCommandRing ring;
    std::atomic<std::int64_t> inputMarkNs{ -1 };
    MdWsServer server;
    std::atomic<int> resyncs{ 0 };
    std::atomic<int> gones{ 0 };
    std::atomic<int> limited{ 0 };
    std::atomic<int> actions{ 0 };
    std::mutex mutex;
    std::map<std::uint64_t, std::shared_ptr<Outlet>> outlets;

private:
    void run()
    {
        while (!stop)
        {
            MdCommand c;
            while (ring.tryPop(c))
            {
                std::lock_guard<std::mutex> lock(mutex);
                switch (c.kind)
                {
                    case MdCommand::Kind::OPEN: outlets[c.connId] = c.outlet; break;
                    case MdCommand::Kind::GONE:
                        outlets.erase(c.connId);
                        gones++;
                        break;
                    case MdCommand::Kind::RESYNC:
                        if (const auto it = outlets.find(c.connId); outlets.end() != it)
                        {
                            it->second->resyncBegins();
                            it->second->offer(textFrame("STATE", FrameType::BOOK_SNAPSHOT));
                        }
                        resyncs++;
                        break;
                    case MdCommand::Kind::LIMITED:
                        if (const auto it = outlets.find(c.connId); outlets.end() != it)
                        {
                            it->second->offer(textFrame("LIMITED", FrameType::ERROR));
                        }
                        limited++;
                        break;
                    case MdCommand::Kind::PING:
                        if (const auto it = outlets.find(c.connId); outlets.end() != it)
                        {
                            it->second->offer(textFrame("PONG", FrameType::PONG));
                        }
                        actions++;
                        break;
                    default: actions++; break;
                }
                c = MdCommand{};
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }

    std::atomic<bool> stop{ false };
    std::thread duty;
};

// A blocking client; `rcvbuf` > 0 shrinks its receive buffer (a slow reader fills the path sooner).
class Client
{
public:
    Client() : ws(ioc) {}

    // `timeoutS` 0: reads block until a frame or the socket's end (a reader thread that must keep answering pings).
    bool connect(std::uint16_t port, int rcvbuf = 0, const std::vector<std::pair<std::string, std::string>> &headers = {},
        int timeoutS = 2)
    {
        beast::error_code ec;
        ws.next_layer().open(tcp::v4(), ec);
        if (rcvbuf > 0)
        {
            ::setsockopt(ws.next_layer().native_handle(), SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        }
        ws.next_layer().connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port), ec);
        if (ec)
        {
            return false;
        }
        if (timeoutS > 0)
        {
            timeval tv{ timeoutS, 0 };
            ::setsockopt(ws.next_layer().native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        ws.set_option(websocket::stream_base::decorator([headers](websocket::request_type &req) {
            for (const auto &[k, v] : headers)
            {
                req.set(k, v);
            }
        }));
        websocket::response_type res;
        ws.handshake(res, "127.0.0.1", "/ws", ec);
        status = res.result_int();
        retryAfter = res.count(http::field::retry_after) ? std::string(res[http::field::retry_after]) : "";
        return !ec;
    }
    std::optional<std::string> next()
    {
        beast::flat_buffer b;
        beast::error_code ec;
        ws.read(b, ec);
        if (ec)
        {
            return std::nullopt;
        }
        return beast::buffers_to_string(b.data());
    }
    void send(const std::string &text)
    {
        beast::error_code ec;
        ws.write(net::buffer(text), ec);
    }

    net::io_context ioc;
    websocket::stream<tcp::socket> ws;
    unsigned status = 0;
    std::string retryAfter;
};

// One plain HTTP request; the status (0 when nothing came back).
unsigned httpStatus(std::uint16_t port, const std::string &target, const std::string &forwardedFor = "")
{
    net::io_context ioc;
    beast::tcp_stream s(ioc);
    beast::error_code ec;
    s.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port), ec);
    if (ec)
    {
        return 0;
    }
    http::request<http::empty_body> req(http::verb::get, target, 11);
    req.set(http::field::host, "127.0.0.1");
    if (!forwardedFor.empty())
    {
        req.set("X-Forwarded-For", forwardedFor);
    }
    s.expires_after(std::chrono::seconds(3));
    http::write(s, req, ec);
    beast::flat_buffer b;
    http::response<http::string_body> res;
    http::read(s, b, res, ec);
    return ec && ec != http::error::end_of_stream ? 0 : res.result_int();
}

MdWsSettings settings()
{
    MdWsSettings s;
    s.host = "127.0.0.1";
    s.port = 0;
    s.ioThreads = 2;
    s.connectRate = Rate{ 1000, 1000, "1000/s" };
    s.connectBurst = 1000;
    s.messageRate = Rate{ 1000, 1000, "1000/s" };
    s.messageBurst = 1000;
    return s;
}

}

// ---------------------------------------------------------------- slow and silent clients

TEST(md_ws_slow_client)
{
    MdWsSettings s = settings();
    s.queueBytes = 262'144;
    s.unwritableCloseMs = 2000;
    s.idleMs = 6000;
    Front front(s);
    Client normal;
    Client slow;
    CHECK(normal.connect(front.port(), 0, {}, 0));
    CHECK(slow.connect(front.port(), 4096));
    CHECK(front.waitFor([&] { return 2 == front.open().size(); }, 3000));
    const std::vector<std::uint64_t> ids = front.open();
    if (2 != ids.size())
    {
        return;
    }
    const std::uint64_t normalId = ids[0];
    const std::uint64_t slowId = ids[1];
    // the normal client reads everything; every frame offered to it must arrive, in order
    std::atomic<int> normalSeen{ 0 };
    std::atomic<bool> normalOrdered{ true };
    std::atomic<bool> reading{ true };
    std::thread reader([&] {
        int expect = 0;
        while (reading)
        {
            const auto f = normal.next();
            if (!f)
            {
                break;
            }
            if (0 == f->rfind("F", 0))
            {
                const int n = std::stoi(f->substr(1, f->find(' ') - 1));
                normalOrdered = normalOrdered && n == expect;
                expect = n + 1;
                normalSeen++;
            }
        }
    });
    // 32 KB frames, one per ms to each: the slow client's path fills, then its queue hits the bound
    const std::string pad(32 * 1024, 'x');
    int normalOffered = 0;
    bool normalRefused = false;
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    for (int n = 0; std::chrono::steady_clock::now() < end; ++n)
    {
        if (front.offer(normalId, textFrame("F" + std::to_string(n) + " " + pad)))
        {
            normalOffered++;
        }
        else
        {
            normalRefused = true;
        }
        front.offer(slowId, textFrame("S" + std::to_string(n) + " " + pad));
        if (front.server.counters().queueDrops.load() > 0 || front.server.counters().queueHighWater.load() > s.queueBytes)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto finish = [&] {
        reading = false;
        beast::error_code ignored;
        normal.ws.next_layer().shutdown(tcp::socket::shutdown_both, ignored);
        reader.join();
    };
    const std::int64_t high = front.server.counters().queueHighWater.load();
    CHECK_MSG(high <= s.queueBytes, "client queue grew past its setting: " + std::to_string(high) + " bytes held, " +
        "MD_WS_QUEUE_BYTES " + std::to_string(s.queueBytes));
    if (high > s.queueBytes)
    {
        finish(); // the rest of the case stands on the bound
        return;
    }
    CHECK_MSG(1 == front.server.counters().queueDrops.load(), "the slow client's queue was never dropped at its bound");
    // the slow client reads again: it drains and gets a fresh state (the resync), and is not closed
    bool sawState = false;
    for (int i = 0; i < 100'000 && !sawState; ++i)
    {
        const auto f = slow.next();
        if (!f)
        {
            break;
        }
        sawState = "STATE" == *f;
    }
    CHECK_MSG(sawState && 1 == front.resyncs.load(), "no fresh state after the slow client drained (resyncs " +
        std::to_string(front.resyncs.load()) + ")");
    CHECK(0 == front.server.counters().closedSlow.load());
    // the other client was not touched by any of it
    CHECK(front.waitFor([&] { return normalSeen.load() == normalOffered; }, 5000));
    CHECK_MSG(!normalRefused && normalOrdered && normalSeen.load() == normalOffered,
        "another client was affected: " + std::to_string(normalSeen.load()) + " of " + std::to_string(normalOffered) +
            " frames");

    // a client that stops reading for good: closed after MD_WS_UNWRITABLE_CLOSE_MS
    Client stuck;
    CHECK(stuck.connect(front.port(), 4096));
    CHECK(front.waitFor([&] { return 3 == front.open().size(); }, 3000));
    const std::uint64_t stuckId = front.open().back();
    const int gonesBefore = front.gones.load();
    const auto t0 = std::chrono::steady_clock::now();
    for (int n = 0; front.gones.load() == gonesBefore && std::chrono::steady_clock::now() - t0 < std::chrono::seconds(10);
         ++n)
    {
        front.offer(stuckId, textFrame("S" + std::to_string(n) + " " + pad));
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK_MSG(1 == front.server.counters().closedSlow.load() && front.gones.load() == gonesBefore + 1,
        "a client that never drained was not closed after the unwritable time");
    CHECK(front.server.clientsServed(monoNs()));

    // a client that sends nothing (no frame, no pong): closed after MD_WS_IDLE_MS
    Client silent;
    silent.connect(front.port());
    CHECK(front.waitFor([&] { return 3 == front.open().size(); }, 3000));
    const int beforeIdle = front.gones.load();
    const auto t1 = std::chrono::steady_clock::now();
    CHECK(front.waitFor([&] { return front.gones.load() == beforeIdle + 1; }, 12'000));
    const auto idleMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t1);
    CHECK_MSG(idleMs.count() >= 3000, "the silent client was closed after " + std::to_string(idleMs.count()) + " ms");
    // the reading client answers the pings and stays
    CHECK(2 == front.open().size());
    finish();
}

// ---------------------------------------------------------------- clients_served

// The normal build: frames reach the client, clients_served holds and the sent counter moves. The MD_TC_SEND_DROPS
// build (a sender that drops every frame): clients_served clears after MD_SEND_STALL_MS and the counter stays flat.
TEST(md_ws_clients_served)
{
    MdWsSettings s = settings();
    s.sendStallMs = 300;
    Front front(s);
    Client c;
    CHECK(c.connect(front.port()));
    CHECK(front.waitFor([&] { return 1 == front.open().size(); }, 3000));
    if (front.open().empty())
    {
        return;
    }
    const std::uint64_t id = front.open()[0];
    auto sentTotal = [&] {
        std::uint64_t n = 0;
        for (const auto &v : front.server.counters().sent)
        {
            n += v.load();
        }
        return n;
    };
    const std::uint64_t before = sentTotal();
    std::thread reader([&] {
        while (c.next())
        {
        }
    });
    // the ladder updates: an input with a subscriber is marked, then its frame is offered, every 20 ms for 1 s
    bool servedThroughout = true;
    for (int i = 0; i < 50; ++i)
    {
        std::int64_t none = -1;
        front.inputMarkNs.compare_exchange_strong(none, monoNs());
        front.offer(id, textFrame("{\"type\":\"BOOK_DELTA\",\"n\":" + std::to_string(i) + "}"));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        servedThroughout = servedThroughout && front.server.clientsServed(monoNs());
    }
    const bool served = front.server.clientsServed(monoNs());
    const std::uint64_t sent = sentTotal() - before;
#ifdef MD_TC_SEND_DROPS
    CHECK_MSG(!served, "ready while no frame reaches a client");
    CHECK_MSG(0 == sent, "frames counted as sent by a sender that dropped them: " + std::to_string(sent));
#else
    CHECK_MSG(served && servedThroughout, "clients_served false while frames reach the client");
    CHECK_MSG(sent >= 50, "md_ws_frames_sent_total did not move: " + std::to_string(sent));
    // the input stops: nothing is owed, clients_served holds
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    CHECK(front.server.clientsServed(monoNs()));
#endif
    beast::error_code ignored;
    c.ws.next_layer().shutdown(tcp::socket::shutdown_both, ignored);
    reader.join();
}

// ---------------------------------------------------------------- the public port's budgets

TEST(md_ws_rate_limits)
{
    MdWsSettings s = settings();
    s.connectRate = Rate{ 1, 1000, "1/s" };
    s.connectBurst = 2;
    s.messageRate = Rate{ 1, 1000, "1/s" };
    s.messageBurst = 3;
    s.trustedProxies = { "127.0.0.1" };
    Front front(s);
    const std::vector<std::pair<std::string, std::string>> a = { { "X-Forwarded-For", "203.0.113.7" } };
    const std::vector<std::pair<std::string, std::string>> b = { { "X-Forwarded-For", "203.0.113.8" } };
    // the connection budget per source, read behind the trusted proxy: two for 203.0.113.7, then 429 with Retry-After
    Client c1;
    Client c2;
    Client c3;
    CHECK(c1.connect(front.port(), 0, a));
    CHECK(c2.connect(front.port(), 0, a));
    CHECK_MSG(!c3.connect(front.port(), 0, a) && 429 == c3.status && !c3.retryAfter.empty(),
        "a source past MD_RATE_CONNECT was not refused (status " + std::to_string(c3.status) + ")");
    // another source has its own budget; every HTTP request on the port takes from it, not only the upgrade
    Client c4;
    CHECK(c4.connect(front.port(), 0, b));
    CHECK(404 == httpStatus(front.port(), "/nothing", "203.0.113.8"));
    CHECK_MSG(429 == httpStatus(front.port(), "/api/candles", "203.0.113.8"), "a REST request outside the budget");
    CHECK(2 == front.server.counters().limitedConnects.load());
    // the message budget: three frames pass, the fourth and fifth are refused, with one ERROR RATE_LIMITED for the run
    CHECK(front.waitFor([&] { return 3 == front.open().size(); }, 3000));
    for (int i = 0; i < 5; ++i)
    {
        c1.send("{\"action\":\"ping\"}");
    }
    CHECK(front.waitFor([&] { return 3 == front.actions.load() && 1 == front.limited.load(); }, 3000));
    CHECK_MSG(3 == front.actions.load() && 1 == front.limited.load() && 2 == front.server.counters().limitedMessages.load(),
        "client frames past MD_RATE_MESSAGE were acted on (" + std::to_string(front.actions.load()) + " actions, " +
            std::to_string(front.limited.load()) + " refusal runs)");
    int pongs = 0;
    bool limitedFrame = false;
    for (int i = 0; i < 4; ++i)
    {
        const auto f = c1.next();
        pongs += f && "PONG" == *f ? 1 : 0;
        limitedFrame = limitedFrame || (f && "LIMITED" == *f);
    }
    CHECK(3 == pongs && limitedFrame);
    // the budget is per source: c2 (same source) is refused too; after a refill a frame passes and ends the run
    c2.send("{\"action\":\"ping\"}");
    CHECK(front.waitFor([&] { return 2 == front.limited.load(); }, 3000));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    c1.send("{\"action\":\"ping\"}");
    CHECK(front.waitFor([&] { return 4 == front.actions.load(); }, 3000));
    c1.send("{\"action\":\"ping\"}");
    c1.send("{\"action\":\"ping\"}");
    CHECK(front.waitFor([&] { return 3 == front.limited.load(); }, 3000));

    // without a trusted proxy the header is never read: every request is the socket peer's
    MdWsSettings direct = settings();
    direct.connectRate = Rate{ 1, 60'000, "1/min" };
    direct.connectBurst = 1;
    Front plain(direct);
    Client d1;
    Client d2;
    CHECK(d1.connect(plain.port(), 0, a));
    CHECK_MSG(!d2.connect(plain.port(), 0, b) && 429 == d2.status, "the client-IP header was read from an untrusted peer");
}
