// T0 rows of oe-stream's WebSocket edge, the binding: the three credential paths, never a query parameter, one
// credential per connection, the first-frame timeout, a userId in a client frame closes 4003, and no frame of another
// user reaches a connection. The real WsServer on a loopback port, dev principals, and a duty thread driving StreamCore
// over the model core of StreamRig.h.

#include <sys/socket.h>
#include <sys/time.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>

#include "StreamRig.h"
#include "WsServer.h"

using namespace gwtest::stream_rig;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;
using openexchange::gateway::AuthMaterial;
using openexchange::gateway::Authenticator;
using openexchange::gateway::AuthenticatorSettings;
namespace auth_mode = openexchange::gateway::auth_mode;

namespace {

// The process around the listener: the command ring, a duty thread with StreamCore and the model core.
class Edge
{
public:
    Edge() :
        auth(authSettings(), AuthMaterial{}, nullptr, nullptr), ring(1024), core(settings(), fake, [this] { return config; })
    {
        config = std::make_shared<gs::CoreConfig>();
        st::WsServerSettings s;
        s.host = "127.0.0.1";
        s.port = 0;
        s.authTimeoutMs = 400;
        s.pingMs = 60'000; // no ping during a case: a quiet connection's read times out
        s.idleMs = 5000;
        s.maxMessageBytes = 4096;
        server = std::make_unique<st::WsServer>(s, auth, ring);
        std::string error;
        if (!server->bind(error))
        {
            throw std::runtime_error(error);
        }
        server->start();
        core.onRegistered(1, 1, nowNs());
        duty = std::thread([this] { run(); });
    }
    ~Edge()
    {
        stop = true;
        duty.join();
        server->stop(2000);
    }

    static AuthenticatorSettings authSettings()
    {
        AuthenticatorSettings a;
        a.modes = auth_mode::DEV;
        return a;
    }
    static std::int64_t nowNs()
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    template<typename F>
    void withCore(F &&f)
    {
        std::lock_guard<std::mutex> lock(mutex);
        f(fake);
    }

    std::uint16_t port() const { return server->boundPort(); }

    Authenticator auth;
    st::CommandRing ring;
    FakeCore fake;
    std::shared_ptr<const gs::CoreConfig> config;
    st::StreamCore core;
    std::unique_ptr<st::WsServer> server;

private:
    void run()
    {
        while (!stop)
        {
            {
                std::lock_guard<std::mutex> lock(mutex);
                const std::int64_t now = nowNs();
                st::StreamCommand c;
                while (ring.tryPop(c))
                {
                    switch (c.kind)
                    {
                        case st::StreamCommand::Kind::OPEN: core.open(c.connId, c.userId, c.outlet, now); break;
                        case st::StreamCommand::Kind::SUBSCRIBE:
                            core.subscribe(c.connId, c.channels,
                                c.hasResume ? std::optional<std::int64_t>(c.resumeFrom) : std::nullopt, now, c.frameUserId);
                            break;
                        case st::StreamCommand::Kind::UNSUBSCRIBE: core.unsubscribe(c.connId, c.channels); break;
                        case st::StreamCommand::Kind::PING: core.ping(c.connId); break;
                        case st::StreamCommand::Kind::GONE: core.gone(c.connId, now); break;
                        case st::StreamCommand::Kind::NONE: break;
                    }
                    c = st::StreamCommand{};
                }
                core.doWork(now);
                fake.answerAll();
                fake.deliver(core, now);
                core.doWork(now);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    std::mutex mutex;
    std::atomic<bool> stop{ false };
    std::thread duty;
};

class WsClient
{
public:
    WsClient() : ws(ioc) {}

    // false with the HTTP status when the upgrade was refused
    bool connect(std::uint16_t port, const std::string &target, const std::vector<std::pair<std::string, std::string>> &headers,
        unsigned &status, std::string &subprotocol)
    {
        tcp::resolver resolver(ioc);
        net::connect(ws.next_layer(), resolver.resolve("127.0.0.1", std::to_string(port)));
        timeval tv{ 2, 0 };
        setsockopt(ws.next_layer().native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ws.set_option(websocket::stream_base::decorator([headers](websocket::request_type &req) {
            for (const auto &[k, v] : headers)
            {
                req.set(k, v);
            }
        }));
        websocket::response_type res;
        beast::error_code ec;
        ws.handshake(res, "127.0.0.1", target, ec);
        status = res.result_int();
        if (auto it = res.find(http::field::sec_websocket_protocol); it != res.end())
        {
            subprotocol = std::string(it->value());
        }
        return !ec;
    }
    bool connect(std::uint16_t port, const std::vector<std::pair<std::string, std::string>> &headers = {})
    {
        unsigned status = 0;
        std::string sub;
        return connect(port, "/ws/v2", headers, status, sub);
    }

    void send(const std::string &text)
    {
        beast::error_code ec;
        ws.write(net::buffer(text), ec);
    }

    // the next text frame, or nullopt (closed or timed out; `closeCode` says which)
    std::optional<std::string> next()
    {
        beast::flat_buffer b;
        beast::error_code ec;
        ws.read(b, ec);
        if (ec)
        {
            if (ec == websocket::error::closed)
            {
                closeCode = ws.reason().code;
            }
            return std::nullopt;
        }
        return beast::buffers_to_string(b.data());
    }

    // frames until `type` arrives (or nothing more comes)
    std::optional<json::object> until(const std::string &type, int max = 50)
    {
        for (int i = 0; i < max; ++i)
        {
            auto f = next();
            if (!f)
            {
                return std::nullopt;
            }
            json::object o = json::parse(*f).as_object();
            frames.push_back(o);
            if (o.at("type").as_string() == type)
            {
                return o;
            }
        }
        return std::nullopt;
    }

    net::io_context ioc;
    websocket::stream<tcp::socket> ws;
    std::uint16_t closeCode = 0;
    std::vector<json::object> frames;
};

}

TEST(ws_first_frame_auth)
{
    Edge edge;
    WsClient c;
    CHECK(c.connect(edge.port()));
    c.send(R"({"op":"auth","token":"dev:501"})");
    c.send(R"({"op":"subscribe","channels":["orders"]})");
    CHECK_MSG(c.until("subscribed").has_value(), "first-frame auth: the subscribe is answered");
    CHECK_MSG(c.until("snapshot").has_value(), "first-frame auth: the snapshot follows");
}

TEST(ws_subprotocol_and_header_credentials)
{
    Edge edge;
    {
        WsClient c;
        unsigned status = 0;
        std::string sub;
        CHECK(c.connect(edge.port(), "/ws/v2", { { "Sec-WebSocket-Protocol", "bearer, dev:502" } }, status, sub));
        CHECK_MSG("bearer" == sub, "the answer names the bearer subprotocol: '" + sub + "'");
        c.send(R"({"op":"subscribe","channels":["fills"]})");
        CHECK(c.until("subscribed").has_value());
    }
    {
        WsClient c;
        CHECK(c.connect(edge.port(), { { "Authorization", "Bearer dev:503" } }));
        c.send(R"({"op":"ping"})");
        CHECK(c.until("pong").has_value());
    }
}

TEST(ws_no_token_from_url_and_one_credential)
{
    Edge edge;
    {
        WsClient c;
        unsigned status = 0;
        std::string sub;
        CHECK_MSG(!c.connect(edge.port(), "/ws/v2?token=dev:504", {}, status, sub) && 400 == status,
            "token accepted from URL (status " + std::to_string(status) + ")");
    }
    {
        WsClient c;
        unsigned status = 0;
        std::string sub;
        CHECK_MSG(!c.connect(edge.port(), "/ws/v2", { { "Sec-WebSocket-Protocol", "bearer, dev:505" },
                      { "Authorization", "Bearer dev:505" } }, status, sub) && 401 == status,
            "two credentials refused (status " + std::to_string(status) + ")");
    }
    {
        WsClient c;
        unsigned status = 0;
        std::string sub;
        CHECK_MSG(!c.connect(edge.port(), "/ws/v2", { { "Authorization", "Bearer nope" } }, status, sub) && 401 == status,
            "a bad header credential is 401 before any upgrade");
    }
    {
        WsClient c;
        CHECK(c.connect(edge.port()));
        c.next(); // nothing is sent: the first frame never comes
        CHECK_MSG(1008 == c.closeCode, "the first-frame auth timeout closes 1008, got " + std::to_string(c.closeCode));
    }
}

TEST(ws_user_in_a_frame_closes_4003)
{
    Edge edge;
    WsClient c;
    CHECK(c.connect(edge.port(), { { "Authorization", "Bearer dev:506" } }));
    c.send(R"({"op":"subscribe","channels":["orders"],"userId":507})");
    while (c.next().has_value())
    {
    }
    CHECK_MSG(4003 == c.closeCode, "a userId in a client frame closes 4003, got " + std::to_string(c.closeCode));
}

TEST(ws_no_foreign_event)
{
    Edge edge;
    WsClient a;
    WsClient b;
    CHECK(a.connect(edge.port(), { { "Authorization", "Bearer dev:601" } }));
    CHECK(b.connect(edge.port(), { { "Authorization", "Bearer dev:602" } }));
    b.send(R"({"op":"subscribe","channels":["orders"]})");
    CHECK(b.until("snapshot").has_value());
    // a frame that names another user: closed (4003); under the control it rebinds the connection to that user
    a.send(R"({"op":"subscribe","channels":["orders"],"userId":602})");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::set<std::int64_t> mine;
    std::set<std::int64_t> theirs;
    edge.withCore([&](FakeCore &core) {
        for (int i = 0; i < 5; ++i)
        {
            mine.insert(core.create(601));
            theirs.insert(core.create(602));
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    int foreign = 0;
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);
    while (std::chrono::steady_clock::now() < end)
    {
        auto f = a.next();
        if (!f)
        {
            break;
        }
        const json::object o = json::parse(*f).as_object();
        if (o.at("type").as_string() == "order")
        {
            const std::int64_t id = std::stoll(std::string(o.at("order").at("orderId").as_string()));
            foreign += 0 == mine.count(id) ? 1 : 0;
        }
        if (o.at("type").as_string() == "snapshot")
        {
            foreign += o.at("orders").as_array().size() > 0 ? 1 : 0;
        }
    }
    CHECK_MSG(0 == foreign, "foreign event delivered (" + std::to_string(foreign) + " frames of user 602 on user 601's "
        "connection)");
    int own = 0;
    for (int i = 0; i < 20 && own < 5; ++i)
    {
        auto o = b.until("order", 5);
        if (!o)
        {
            break;
        }
        const std::int64_t id = std::stoll(std::string(o->at("order").at("orderId").as_string()));
        CHECK_MSG(theirs.count(id) == 1, "foreign event delivered to user 602");
        own++;
    }
    CHECK_MSG(5 == own, "user 602 gets its own events: " + std::to_string(own));
}
