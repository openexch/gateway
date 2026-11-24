// T0 rows of oe-stream's WebSocket edge, the binding: the three credential paths, never a query parameter, one
// credential per connection, the first-frame timeout, a userId in a client frame closes 4003, and no frame of another
// user reaches a connection; the anonymous engine-only connection: its handshake, its refusals, its cap and counters. The real WsServer on a loopback port, dev principals, and a duty thread driving StreamCore
// over the model core of StreamRig.h.

#include <sys/socket.h>
#include <sys/time.h>

#include <atomic>
#include <chrono>
#include <functional>
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
    explicit Edge(std::int64_t anonMaxConnections = 500) :
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
        s.anonMaxConnections = anonMaxConnections;
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

    void engine(const std::string &frame, bool election = false)
    {
        std::lock_guard<std::mutex> lock(mutex);
        const auto text = std::make_shared<const std::string>(frame);
        if (election)
        {
            core.onEngineEvent(text);
        }
        else
        {
            core.onEngineStatus(text);
        }
    }

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
                        case st::StreamCommand::Kind::OPEN:
                            if (c.anonymous)
                            {
                                core.openAnonymous(c.connId, c.outlet, now);
                            }
                            else
                            {
                                core.open(c.connId, c.userId, c.outlet, now);
                            }
                            break;
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

// stream_ws_frames_sent_total: every finished write to a client is counted (the observation's work counter of
// oe-stream)
TEST(ws_frames_sent_counted)
{
    Edge edge;
    const std::uint64_t before = edge.server->counters().framesSent.load();
    WsClient c;
    CHECK(c.connect(edge.port(), { { "Authorization", "Bearer dev:701" } }));
    for (int i = 0; i < 3; ++i)
    {
        c.send(R"({"op":"ping"})");
        CHECK(c.until("pong").has_value());
    }
    c.send(R"({"op":"subscribe","channels":["orders"]})");
    CHECK(c.until("snapshot").has_value());
    const std::uint64_t received = c.frames.size();
    const std::uint64_t sent = edge.server->counters().framesSent.load() - before;
    CHECK_MSG(received >= 5 && sent >= received,
        "frames sent not counted: " + std::to_string(sent) + " counted, " + std::to_string(received) + " received");
}

namespace {

constexpr const char *ANON_SUBSCRIBE = R"({"op":"subscribe","channels":["engine"]})";

// the next frame as an object (nullopt when closed or timed out)
std::optional<json::object> nextObject(WsClient &c)
{
    auto f = c.next();
    if (!f)
    {
        return std::nullopt;
    }
    return json::parse(*f).as_object();
}

bool waitFor(const std::function<bool()> &done, int ms = 2000)
{
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < end)
    {
        if (done())
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return done();
}

}

// A connection with no credential whose first frame subscribes to `engine` opens anonymously: `subscribed` [engine],
// then the newest engine frame, then every later engine and election frame; no frame of any user. Every other op that
// would reach user data is an error VALIDATION (counted as an anonymous refusal, never as an auth failure).
TEST(ws_anonymous_engine_subscription)
{
    Edge edge;
    edge.engine(R"({"type":"engine","v":1})");
    const std::uint64_t authFailedBefore = edge.server->counters().authFailed.load();
    WsClient a;
    CHECK(a.connect(edge.port()));
    a.send(ANON_SUBSCRIBE);
    auto f = nextObject(a);
    CHECK_MSG(f && "subscribed" == f->at("type").as_string() && 1 == f->at("channels").as_array().size() &&
            "engine" == f->at("channels").as_array()[0].as_string(),
        "the anonymous first frame is answered subscribed [engine]");
    f = nextObject(a);
    CHECK_MSG(f && "engine" == f->at("type").as_string() && 1 == f->at("v").as_int64(), "the newest engine frame follows");
    CHECK(waitFor([&] { return 1 == edge.server->counters().anonConnections.load(); }));

    // a signed-in user's traffic never reaches it; engine and election frames do
    WsClient u;
    CHECK(u.connect(edge.port(), { { "Authorization", "Bearer dev:801" } }));
    u.send(R"({"op":"subscribe","channels":["orders","fills","engine"]})");
    auto us = u.until("subscribed");
    CHECK_MSG(us && 3 == us->at("channels").as_array().size(), "a signed-in connection may name engine too");
    CHECK(u.until("snapshot").has_value());
    edge.withCore([](FakeCore &core) { core.create(801); });
    CHECK(u.until("order").has_value());
    edge.engine(R"({"type":"election","phase":"start"})", true);
    edge.engine(R"({"type":"engine","v":2})");
    f = nextObject(a);
    CHECK_MSG(f && "election" == f->at("type").as_string(), "an election frame reaches the anonymous connection");
    f = nextObject(a);
    CHECK_MSG(f && "engine" == f->at("type").as_string() && 2 == f->at("v").as_int64(), "a new engine frame follows");
    CHECK(u.until("election").has_value());

    // refusals: user channels, resume, auth
    for (const char *op : { R"({"op":"subscribe","channels":["orders"]})", R"({"op":"subscribe","channels":["fills"]})",
             R"({"op":"subscribe","channels":["balances","engine"]})",
             R"({"op":"subscribe","channels":["engine"],"resume":{"eventSeq":0}})", R"({"op":"auth","token":"dev:802"})" })
    {
        a.send(op);
        f = nextObject(a);
        CHECK_MSG(f && "error" == f->at("type").as_string() && "VALIDATION" == f->at("code").as_string(),
            std::string("refused with error VALIDATION: ") + op);
    }
    CHECK_MSG(5 == edge.server->counters().anonRefusedOps.load(),
        "anonymous refusals counted: " + std::to_string(edge.server->counters().anonRefusedOps.load()));
    // ping and unsubscribe are taken; nothing of a user arrived in between
    a.send(R"({"op":"ping"})");
    f = nextObject(a);
    CHECK_MSG(f && "pong" == f->at("type").as_string() && f->at("eventSeq").is_null(), "ping answered, no eventSeq");
    a.send(R"({"op":"unsubscribe","channels":["engine"]})");
    f = nextObject(a);
    CHECK_MSG(f && "subscribed" == f->at("type").as_string() && f->at("channels").as_array().empty(),
        "unsubscribe answered subscribed []");
    edge.engine(R"({"type":"engine","v":3})");
    a.send(R"({"op":"ping"})");
    f = nextObject(a);
    CHECK_MSG(f && "pong" == f->at("type").as_string(), "no engine frame after unsubscribe");
    CHECK_MSG(edge.server->counters().authFailed.load() == authFailedBefore, "an anonymous connection counted as an "
        "auth failure");
    // a userId key still closes 4003
    a.send(R"({"op":"ping","userId":801})");
    while (a.next().has_value())
    {
    }
    CHECK_MSG(4003 == a.closeCode, "a userId on an anonymous connection closes 4003, got " + std::to_string(a.closeCode));
    CHECK(waitFor([&] { return 0 == edge.server->counters().anonConnections.load(); }));
}

// The anonymous cap: above STREAM_ANON_MAX_CONNECTIONS the first frame is closed 1013 and counted (capacity), not as
// an auth failure; a closed anonymous connection frees its slot; a signed-in connection is not held to the cap.
TEST(ws_anonymous_cap)
{
    Edge edge(2);
    std::vector<std::unique_ptr<WsClient>> held;
    for (int i = 0; i < 2; ++i)
    {
        held.push_back(std::make_unique<WsClient>());
        CHECK(held.back()->connect(edge.port()));
        held.back()->send(ANON_SUBSCRIBE);
        CHECK(held.back()->until("subscribed").has_value());
    }
    CHECK(2 == edge.server->counters().anonConnections.load());
    {
        WsClient over;
        CHECK(over.connect(edge.port()));
        over.send(ANON_SUBSCRIBE);
        while (over.next().has_value())
        {
        }
        CHECK_MSG(1013 == over.closeCode, "above the anonymous cap: 1013, got " + std::to_string(over.closeCode));
    }
    CHECK(1 == edge.server->counters().anonRefusedCapacity.load());
    CHECK(0 == edge.server->counters().authFailed.load());
    {
        WsClient signedIn;
        CHECK(signedIn.connect(edge.port(), { { "Authorization", "Bearer dev:811" } }));
        signedIn.send(R"({"op":"ping"})");
        CHECK_MSG(signedIn.until("pong").has_value(), "a signed-in connection is not held to the anonymous cap");
    }
    held[0]->ws.close(websocket::close_code::normal);
    held.erase(held.begin());
    CHECK_MSG(waitFor([&] { return 1 == edge.server->counters().anonConnections.load(); }),
        "a closed anonymous connection frees its slot: " + std::to_string(edge.server->counters().anonConnections.load()));
    WsClient again;
    CHECK(again.connect(edge.port()));
    again.send(ANON_SUBSCRIBE);
    CHECK_MSG(again.until("subscribed").has_value(), "a freed slot is taken again");
}

// Without a credential every other first frame still closes 1008 and counts as an auth failure (a subscribe to a user
// channel, engine with a resume, an extra key).
TEST(ws_first_frame_other_than_engine_still_1008)
{
    Edge edge;
    int n = 0;
    for (const char *first : { R"({"op":"subscribe","channels":["orders"]})",
             R"({"op":"subscribe","channels":["engine","orders"]})",
             R"({"op":"subscribe","channels":["engine"],"resume":{"eventSeq":1}})", R"({"op":"ping"})" })
    {
        WsClient c;
        CHECK(c.connect(edge.port()));
        c.send(first);
        while (c.next().has_value())
        {
        }
        n++;
        CHECK_MSG(1008 == c.closeCode, std::string("first frame closes 1008: ") + first + ", got " +
            std::to_string(c.closeCode));
    }
    CHECK(waitFor([&] { return static_cast<std::uint64_t>(n) == edge.server->counters().authFailed.load(); }));
    CHECK(0 == edge.server->counters().anonConnections.load() && 0 == edge.server->counters().anonRefusedOps.load());
}

// An anonymous connection's ring ops are bounded: subscribe and unsubscribe above the burst are refused RATE_LIMITED
// (an anonymous refusal); its pings never reach the command ring and are always answered.
TEST(ws_anonymous_op_rate)
{
    Edge edge;
    WsClient a;
    CHECK(a.connect(edge.port()));
    a.send(ANON_SUBSCRIBE);
    CHECK(a.until("subscribed").has_value());
    int taken = 0;
    int limited = 0;
    for (int i = 0; i < 8; ++i)
    {
        a.send(0 == i % 2 ? R"({"op":"unsubscribe","channels":["engine"]})" : ANON_SUBSCRIBE);
    }
    for (int i = 0; i < 8; ++i)
    {
        auto f = nextObject(a);
        if (!f)
        {
            break;
        }
        const std::string type(f->at("type").as_string());
        taken += "subscribed" == type ? 1 : 0;
        limited += "error" == type && "RATE_LIMITED" == f->at("code").as_string() ? 1 : 0;
    }
    CHECK_MSG(taken >= 4 && taken <= 5 && 8 == taken + limited, "ops above the burst are refused: " +
        std::to_string(taken) + " taken, " + std::to_string(limited) + " refused");
    CHECK(static_cast<std::uint64_t>(limited) == edge.server->counters().anonRefusedOps.load());
    const std::uint64_t refusedBefore = edge.server->counters().opsRefused.load();
    for (int i = 0; i < 20; ++i)
    {
        a.send(R"({"op":"ping"})");
    }
    int pongs = 0;
    for (int i = 0; i < 20; ++i)
    {
        auto f = nextObject(a);
        pongs += f && "pong" == f->at("type").as_string() && f->at("eventSeq").is_null() ? 1 : 0;
    }
    CHECK_MSG(20 == pongs, "every ping of an anonymous connection is answered: " + std::to_string(pongs));
    CHECK(refusedBefore == edge.server->counters().opsRefused.load());
}

// A close the server starts while it handles a client frame (4003 a userId, 1013 the anonymous cap, 1008 a bad first
// frame) releases the connection, and an anonymous slot, once the close handshake is done: well before
// STREAM_WS_IDLE_MS (5000 here), while the peer keeps its socket open.
TEST(ws_server_close_frees_the_slot)
{
    Edge edge(1);
    const auto held = [&] { return edge.server->counters().connections.load(); };
    const auto quickly = [&](const std::function<bool()> &done) { return waitFor(done, 1500); };
    // 4003 on an anonymous connection
    WsClient a;
    CHECK(a.connect(edge.port()));
    a.send(ANON_SUBSCRIBE);
    CHECK(a.until("subscribed").has_value());
    CHECK(1 == edge.server->counters().anonConnections.load());
    a.send(R"({"op":"ping","userId":5})");
    while (a.next().has_value())
    {
    }
    CHECK(4003 == a.closeCode);
    CHECK_MSG(quickly([&] { return 0 == edge.server->counters().anonConnections.load() && 0 == held(); }),
        "a server close held its connection (4003): " + std::to_string(held()) + " held, " +
            std::to_string(edge.server->counters().anonConnections.load()) + " anonymous");
    // 1013 above the anonymous cap (1 here)
    WsClient first;
    CHECK(first.connect(edge.port()));
    first.send(ANON_SUBSCRIBE);
    CHECK(first.until("subscribed").has_value());
    WsClient over;
    CHECK(over.connect(edge.port()));
    over.send(ANON_SUBSCRIBE);
    while (over.next().has_value())
    {
    }
    CHECK(1013 == over.closeCode);
    CHECK_MSG(quickly([&] { return 1 == held(); }), "a server close held its connection (1013): " +
        std::to_string(held()) + " held");
    // 1008 a bad first frame
    WsClient bad;
    CHECK(bad.connect(edge.port()));
    bad.send(R"({"op":"ping"})");
    while (bad.next().has_value())
    {
    }
    CHECK(1008 == bad.closeCode);
    CHECK_MSG(quickly([&] { return 1 == held(); }), "a server close held its connection (1008): " +
        std::to_string(held()) + " held");
}
