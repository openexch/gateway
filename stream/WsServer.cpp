#include "WsServer.h"

#include <pthread.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <vector>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/json.hpp>

#include "gateway/common/Api.h"
#include "gateway/common/RateLimit.h"

namespace openexchange { namespace gateway { namespace stream {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
namespace json = boost::json;
using tcp = net::ip::tcp;

namespace {

std::int64_t monoNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::size_t closeSlot(std::uint16_t code)
{
    for (std::size_t i = 0; i < COUNTED_CLOSE_CODES.size(); ++i)
    {
        if (COUNTED_CLOSE_CODES[i] == code)
        {
            return i;
        }
    }
    return COUNTED_CLOSE_CODES.size();
}

std::string trim(std::string_view s)
{
    while (!s.empty() && (' ' == s.front() || '\t' == s.front()))
    {
        s.remove_prefix(1);
    }
    while (!s.empty() && (' ' == s.back() || '\t' == s.back()))
    {
        s.remove_suffix(1);
    }
    return std::string(s);
}

class WsConn;

// The waker: the duty thread pushes a connection here (lock-free) and notifies; this thread posts the write to the
// connection's strand. The ring holds one entry per connection at most (the outlet's kick flag), so it is sized by
// STREAM_MAX_CONNECTIONS and a push cannot fail while the bound holds.
class Waker
{
public:
    explicit Waker(std::size_t capacity) : m_ring(capacity) {}

    void start(const std::string &prefix)
    {
        m_thread = std::thread([this, prefix] {
            const std::string name = prefix + "-wake";
            pthread_setname_np(pthread_self(), name.c_str());
            run();
        });
    }
    bool stop(std::int64_t boundMs);
    bool push(std::weak_ptr<WsConn> conn)
    {
        if (!m_ring.tryPush(std::move(conn)))
        {
            return false;
        }
        m_signal.fetch_add(1, std::memory_order_release);
        m_signal.notify_one();
        return true;
    }

private:
    void run();
    MpmcRing<std::weak_ptr<WsConn>> m_ring;
    std::atomic<std::uint32_t> m_signal{ 0 };
    std::atomic<bool> m_stop{ false };
    std::atomic<bool> m_finished{ false };
    std::thread m_thread;
};

// The duty thread's view of one connection (StreamCore's Outlet) and the I/O side's handover.
class WsOutlet final : public Outlet
{
public:
    WsOutlet(std::size_t depth, Waker &waker) : m_handover(depth), m_waker(waker) {}

    // ---- duty side
    bool offer(const Text &frame) override
    {
        Text copy = frame;
        if (!m_handover.tryPush(std::move(copy)))
        {
            return false;
        }
        m_unwrittenMessages.fetch_add(1, std::memory_order_relaxed);
        m_unwrittenBytes.fetch_add(static_cast<std::int64_t>(frame->size()), std::memory_order_relaxed);
        kick();
        return true;
    }
    std::int64_t unwrittenMessages() const override { return m_unwrittenMessages.load(std::memory_order_relaxed); }
    std::int64_t unwrittenBytes() const override { return m_unwrittenBytes.load(std::memory_order_relaxed); }
    void close(std::uint16_t code, const char *) override
    {
        m_closeCode.store(code, std::memory_order_relaxed);
        m_closeRequested.store(true, std::memory_order_release);
        kick();
    }
    bool closed() const override { return m_gone.load(std::memory_order_acquire); }

    // ---- I/O side
    bool take(Text &frame) { return m_handover.tryPop(frame); }
    void written(std::size_t bytes)
    {
        m_unwrittenMessages.fetch_sub(1, std::memory_order_relaxed);
        m_unwrittenBytes.fetch_sub(static_cast<std::int64_t>(bytes), std::memory_order_relaxed);
    }
    bool ioPush(Text frame) // a frame of the I/O side's own (an error answer), behind what is queued
    {
        const std::size_t size = frame->size();
        if (!m_handover.tryPush(std::move(frame)))
        {
            return false;
        }
        m_unwrittenMessages.fetch_add(1, std::memory_order_relaxed);
        m_unwrittenBytes.fetch_add(static_cast<std::int64_t>(size), std::memory_order_relaxed);
        return true;
    }
    void clearKick() { m_kickPending.store(false, std::memory_order_release); }
    bool closeRequested() const { return m_closeRequested.load(std::memory_order_acquire); }
    std::uint16_t closeCode() const { return m_closeCode.load(std::memory_order_relaxed); }
    void markGone() { m_gone.store(true, std::memory_order_release); }

    std::weak_ptr<WsConn> conn;

private:
    void kick()
    {
        if (!m_kickPending.exchange(true, std::memory_order_acq_rel))
        {
            if (!m_waker.push(conn))
            {
                m_kickPending.store(false, std::memory_order_release); // cannot happen within the bound; retried next offer
            }
        }
    }

    MpmcRing<Text> m_handover;
    Waker &m_waker;
    std::atomic<std::int64_t> m_unwrittenMessages{ 0 };
    std::atomic<std::int64_t> m_unwrittenBytes{ 0 };
    std::atomic<bool> m_kickPending{ false };
    std::atomic<bool> m_closeRequested{ false };
    std::atomic<std::uint16_t> m_closeCode{ 0 };
    std::atomic<bool> m_gone{ false };
};

}

struct WsServer::Impl
{
    Impl(WsServerSettings s, Authenticator &a, CommandRing &c) :
        settings(std::move(s)), auth(a), commands(c), acceptor(net::make_strand(ioc)),
        waker(static_cast<std::size_t>(settings.maxConnections) + 16)
    {
    }

    void accept();

    WsServerSettings settings;
    Authenticator &auth;
    CommandRing &commands;
    net::io_context ioc;
    tcp::acceptor acceptor;
    Waker waker;
    WsCounters counters;
    std::atomic<std::uint64_t> nextConnId{ 1 };
    std::uint16_t port = 0;
    std::vector<std::thread> threads;
    std::mutex mutex;
    std::condition_variable cv;
    int running = 0;
};

namespace {

bool Waker::stop(std::int64_t boundMs)
{
    if (!m_thread.joinable())
    {
        return true;
    }
    m_stop.store(true, std::memory_order_release);
    m_signal.fetch_add(1, std::memory_order_release);
    m_signal.notify_all();
    const std::int64_t end = monoNs() + boundMs * 1'000'000LL;
    while (!m_finished.load(std::memory_order_acquire) && monoNs() < end)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!m_finished.load(std::memory_order_acquire))
    {
        m_thread.detach();
        return false;
    }
    m_thread.join();
    return true;
}

class WsConn : public std::enable_shared_from_this<WsConn>
{
public:
    WsConn(tcp::socket socket, WsServer::Impl &impl) :
        m_ws(std::move(socket)), m_impl(impl), m_timer(m_ws.get_executor()), m_id(impl.nextConnId.fetch_add(1))
    {
    }

    ~WsConn()
    {
        m_impl.counters.connections.fetch_sub(1, std::memory_order_relaxed);
        if (m_anonymous)
        {
            m_impl.counters.anonConnections.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    void start()
    {
        net::dispatch(m_ws.get_executor(), [self = shared_from_this()] { self->readRequest(); });
    }

    void onKick()
    {
        if (!m_outlet)
        {
            return;
        }
        m_outlet->clearKick();
        if (m_outlet->closeRequested() && !m_closing)
        {
            m_closeAfterWrite = true;
            m_pendingCloseCode = m_outlet->closeCode();
            m_pendingCloseReason = "RESYNC";
            m_closeRequestedNs = monoNs();
        }
        if (!m_writing)
        {
            writeNext();
        }
    }

    net::any_io_executor executor() { return m_ws.get_executor(); }

private:
    // ---------------------------------------------------------------- the upgrade and the header credential

    void readRequest()
    {
        m_parser.emplace();
        m_parser->header_limit(static_cast<std::uint32_t>(m_impl.settings.maxHeaderBytes));
        m_parser->body_limit(0);
        beast::get_lowest_layer(m_ws).expires_after(std::chrono::milliseconds(m_impl.settings.authTimeoutMs));
        http::async_read(beast::get_lowest_layer(m_ws), m_buffer, *m_parser,
            [self = shared_from_this()](beast::error_code ec, std::size_t) { self->onRequest(ec); });
    }

    void onRequest(beast::error_code ec)
    {
        if (ec)
        {
            finish();
            return;
        }
        m_request = m_parser->release();
        const std::string_view target(m_request.target().data(), m_request.target().size());
        const std::size_t q = target.find('?');
        const std::string_view path = target.substr(0, q);
        if ("/ws/v2" != path)
        {
            httpError(404, api::code::NOT_FOUND, "not found", "the user stream is at /ws/v2");
            return;
        }
        if (std::string_view::npos != q)
        {
            httpError(400, api::code::VALIDATION, "query parameters are not accepted",
                "a credential never travels in a URL: use the bearer subprotocol, a header or the first frame");
            return;
        }
        if (!websocket::is_upgrade(m_request))
        {
            httpError(426, api::code::VALIDATION, "a WebSocket upgrade is required", std::nullopt);
            return;
        }
        // the credential paths: subprotocol, header, or none (the first frame)
        int given = 0;
        if (auto it = m_request.find(http::field::sec_websocket_protocol); it != m_request.end())
        {
            std::vector<std::string> protocols;
            std::string_view v(it->value().data(), it->value().size());
            while (!v.empty())
            {
                const std::size_t comma = v.find(',');
                protocols.push_back(trim(v.substr(0, comma)));
                v = std::string_view::npos == comma ? std::string_view() : v.substr(comma + 1);
            }
            if (protocols.size() != 2 || "bearer" != protocols[0] || protocols[1].empty())
            {
                httpError(401, api::code::UNAUTHORIZED, "unauthorized",
                    "the only subprotocol is \"bearer, <token>\"");
                return;
            }
            m_subprotocol = true;
            m_authorization = "Bearer " + protocols[1];
            given++;
        }
        if (auto it = m_request.find(http::field::authorization); it != m_request.end())
        {
            m_authorization = std::string(it->value().data(), it->value().size());
            given++;
        }
        if (auto it = m_request.find("X-API-Key"); it != m_request.end())
        {
            m_apiKey = std::string(it->value().data(), it->value().size());
            given++;
        }
        if (m_request.count(http::field::authorization) > 1 || m_request.count("X-API-Key") > 1)
        {
            given = 2;
        }
        if (given > 1)
        {
            httpError(401, api::code::UNAUTHORIZED, "unauthorized", "one credential per connection");
            return;
        }
        if (0 == given)
        {
            acceptUpgrade(); // the credential comes in the first frame
            return;
        }
        resolve([self = shared_from_this()](AuthResult result) {
            net::post(self->m_ws.get_executor(), [self, result = std::move(result)] {
                if (AuthOutcome::OK != result.outcome)
                {
                    self->m_impl.counters.authFailed.fetch_add(1, std::memory_order_relaxed);
                    switch (result.outcome)
                    {
                        case AuthOutcome::RATE_LIMITED:
                            self->httpError(429, api::code::RATE_LIMITED, "too many requests", result.detail);
                            return;
                        case AuthOutcome::UNAVAILABLE:
                            self->httpError(503, api::code::AUTH_UNAVAILABLE, "the users store is unavailable", result.detail);
                            return;
                        case AuthOutcome::BUSY:
                            self->httpError(503, api::code::AUTH_BUSY, "the users store is busy", result.detail);
                            return;
                        default:
                            self->httpError(401, api::code::UNAUTHORIZED, "unauthorized", result.detail);
                            return;
                    }
                }
                self->m_userId = result.principal.userId;
                self->acceptUpgrade();
            });
        });
    }

    void resolve(Authenticator::Done done)
    {
        CredentialHeaders h;
        if (!m_authorization.empty())
        {
            h.authorization = std::string_view(m_authorization);
        }
        if (!m_apiKey.empty())
        {
            h.apiKey = std::string_view(m_apiKey);
        }
        m_impl.auth.resolve(h, std::move(done));
    }

    void httpError(unsigned status, const char *code, std::string text, std::optional<std::string> detail)
    {
        m_impl.counters.httpRefused.fetch_add(1, std::memory_order_relaxed);
        ApiResponse r = api::error(status, code, std::move(text), std::move(detail));
        auto response = std::make_shared<http::response<http::string_body>>(static_cast<http::status>(status), 11);
        response->set(http::field::content_type, "application/json");
        response->set(http::field::cache_control, "no-store");
        for (const auto &[name, value] : r.headers)
        {
            response->insert(beast::string_view(name.data(), name.size()), beast::string_view(value.data(), value.size()));
        }
        response->body() = std::move(r.body);
        response->keep_alive(false);
        response->prepare_payload();
        beast::get_lowest_layer(m_ws).expires_after(std::chrono::milliseconds(m_impl.settings.authTimeoutMs));
        http::async_write(beast::get_lowest_layer(m_ws), *response,
            [self = shared_from_this(), response](beast::error_code, std::size_t) {
                beast::error_code ignored;
                beast::get_lowest_layer(self->m_ws).socket().shutdown(tcp::socket::shutdown_send, ignored);
                self->finish();
            });
    }

    void acceptUpgrade()
    {
        beast::get_lowest_layer(m_ws).expires_never();
        websocket::stream_base::timeout t{};
        t.handshake_timeout = std::chrono::milliseconds(m_impl.settings.authTimeoutMs);
        t.idle_timeout = websocket::stream_base::none(); // this connection's own ping and idle rules (timer)
        t.keep_alive_pings = false;
        m_ws.set_option(t);
        m_ws.read_message_max(static_cast<std::size_t>(m_impl.settings.maxMessageBytes));
        const bool sub = m_subprotocol;
        m_ws.set_option(websocket::stream_base::decorator([sub](websocket::response_type &res) {
            if (sub)
            {
                res.set(http::field::sec_websocket_protocol, "bearer");
            }
            res.set(http::field::cache_control, "no-store");
        }));
        m_ws.control_callback([this](websocket::frame_type, beast::string_view) { m_lastActivityNs = monoNs(); });
        m_ws.async_accept(m_request, [self = shared_from_this()](beast::error_code ec) { self->onAccepted(ec); });
    }

    void onAccepted(beast::error_code ec)
    {
        if (ec)
        {
            finish();
            return;
        }
        m_ws.text(true);
        m_ws.auto_fragment(false); // one message, one frame (a snapshot is written whole)
        m_upgraded = true;
        m_lastActivityNs = monoNs();
        m_outlet = std::make_shared<WsOutlet>(m_impl.settings.handoverDepth, m_impl.waker);
        m_outlet->conn = weak_from_this();
        if (m_userId > 0)
        {
            open();
        }
        else
        {
            m_authDeadlineNs = monoNs() + m_impl.settings.authTimeoutMs * 1'000'000LL;
        }
        armTimer();
        read();
    }

    // ---------------------------------------------------------------- after the upgrade

    void open()
    {
        StreamCommand c;
        c.kind = StreamCommand::Kind::OPEN;
        c.connId = m_id;
        c.userId = m_userId;
        c.anonymous = m_anonymous;
        c.outlet = m_outlet;
        if (!m_impl.commands.tryPush(std::move(c)))
        {
            m_impl.counters.opsRefused.fetch_add(1, std::memory_order_relaxed);
            closeWith(1013, "OVERLOADED");
            return;
        }
        m_opened = true;
    }

    void read()
    {
        m_ws.async_read(m_readBuffer, [self = shared_from_this()](beast::error_code ec, std::size_t) { self->onRead(ec); });
    }

    void onRead(beast::error_code ec)
    {
        if (ec)
        {
            if (ec == websocket::error::message_too_big)
            {
                countClose(1009);
            }
            else if (ec == websocket::error::closed)
            {
                countClose(m_ws.reason().code);
            }
            else if (!m_closing)
            {
                countClose(1006);
            }
            finish();
            return;
        }
        m_lastActivityNs = monoNs();
        const std::string frame = beast::buffers_to_string(m_readBuffer.data());
        m_readBuffer.consume(m_readBuffer.size());
        if (!m_ws.got_text())
        {
            closeWith(1003, "text frames only");
            return;
        }
        onFrame(frame);
        if (!m_closing)
        {
            read();
        }
    }

    void onFrame(const std::string &text)
    {
        boost::system::error_code ec;
        const json::value v = json::parse(text, ec);
        if (ec || !v.is_object())
        {
            sendError(api::code::VALIDATION, "a client frame is one JSON object");
            return;
        }
        std::string where;
        if (api::namesAUser(v, where))
        {
#ifdef GW_NC_ROUTE_BY_FRAME_USER
            // test-only control: the frame's user is passed on to the router instead of closing
            m_frameUserId = 0;
            if (const auto *o = v.if_object(); nullptr != o)
            {
                if (const json::value *u = o->if_contains("userId"); nullptr != u && u->is_int64())
                {
                    m_frameUserId = u->as_int64();
                }
            }
            if (0 == m_frameUserId)
#endif
            {
                closeWith(4003, "a client frame names a user");
                return;
            }
        }
        const json::object &o = v.as_object();
        const json::value *op = o.if_contains("op");
        if (nullptr == op || !op->is_string())
        {
            sendError(api::code::VALIDATION, "a client frame names its op");
            return;
        }
        const std::string_view name(op->as_string().data(), op->as_string().size());
        if (!m_opened && 0 == m_userId && !m_anonymous)
        {
            firstFrame(o, name);
            return;
        }
        if (!m_opened)
        {
            return; // the open was refused; the close is under way
        }
        if (m_anonymous && "auth" == name)
        {
            refuseAnonymous("a signed-out connection cannot sign in; open a new connection with a credential");
            return;
        }
        if ("ping" == name)
        {
            if (!onlyKeys(o, { "op" }))
            {
                return;
            }
            if (m_anonymous)
            {
                // answered here: an anonymous connection follows no event stream (its pong never has an eventSeq), and
                // its pings never reach the command ring the signed-in connections share
                ioFrame(std::make_shared<const std::string>("{\"type\":\"pong\",\"eventSeq\":null}"));
                return;
            }
            StreamCommand c;
            c.kind = StreamCommand::Kind::PING;
            c.connId = m_id;
            pushOp(std::move(c));
            return;
        }
        if ("subscribe" == name || "unsubscribe" == name)
        {
            const bool sub = "subscribe" == name;
#ifdef GW_NC_ROUTE_BY_FRAME_USER
            // test-only control: the frame's userId reached here (a published build closed it 4003 above)
            if (!onlyKeys(o, sub ? std::initializer_list<std::string_view>{ "op", "channels", "resume", "userId" }
                                 : std::initializer_list<std::string_view>{ "op", "channels", "userId" }))
#else
            if (!onlyKeys(o, sub ? std::initializer_list<std::string_view>{ "op", "channels", "resume" }
                                 : std::initializer_list<std::string_view>{ "op", "channels" }))
#endif
            {
                return;
            }
            const json::value *ch = o.if_contains("channels");
            unsigned channels = 0;
            if (nullptr == ch || !ch->is_array() || ch->as_array().empty())
            {
                sendError(api::code::VALIDATION, "channels is a non-empty list of orders, fills, balances, engine");
                return;
            }
            for (const json::value &c : ch->as_array())
            {
                const unsigned bit = channelBit(c);
                if (0 == bit)
                {
                    sendError(api::code::VALIDATION, "unknown channel (orders, fills, balances, engine)");
                    return;
                }
                channels |= bit;
            }
            if (m_anonymous && sub && (0 != (channels & ~channel::ENGINE) || nullptr != o.if_contains("resume")))
            {
                refuseAnonymous("a signed-out connection receives the engine channel only (no orders, fills, balances "
                    "or resume)");
                return;
            }
            if (m_anonymous && !m_anonOps->take(monoNs() / 1'000'000).ok)
            {
                // the ops an anonymous connection may put on the shared command ring
                m_impl.counters.anonRefusedOps.fetch_add(1, std::memory_order_relaxed);
                sendError(api::code::RATE_LIMITED, "too many subscribe and unsubscribe ops on a signed-out connection");
                return;
            }
            StreamCommand c;
            c.kind = sub ? StreamCommand::Kind::SUBSCRIBE : StreamCommand::Kind::UNSUBSCRIBE;
            c.connId = m_id;
            c.channels = channels;
            c.frameUserId = m_frameUserId;
            if (const json::value *r = o.if_contains("resume"); sub && nullptr != r)
            {
                const json::object *ro = r->if_object();
                const json::value *seq = nullptr == ro ? nullptr : ro->if_contains("eventSeq");
                if (nullptr == ro || ro->size() != 1 || nullptr == seq || !(seq->is_int64() || seq->is_uint64()) ||
                    (seq->is_int64() && seq->as_int64() < 0) ||
                    (seq->is_uint64() && seq->as_uint64() > static_cast<std::uint64_t>(INT64_MAX)))
                {
                    sendError(api::code::VALIDATION, "resume is {\"eventSeq\": a non-negative integer}");
                    return;
                }
                c.hasResume = true;
                c.resumeFrom = seq->is_int64() ? seq->as_int64() : static_cast<std::int64_t>(seq->as_uint64());
            }
            pushOp(std::move(c));
            return;
        }
        if ("auth" == name)
        {
            sendError(api::code::VALIDATION, "this connection is already authenticated");
            return;
        }
        sendError(api::code::VALIDATION, "unknown op (subscribe, unsubscribe, ping)");
    }

    bool onlyKeys(const json::object &o, std::initializer_list<std::string_view> allowed)
    {
        for (const auto &kv : o)
        {
            bool ok = false;
            for (const std::string_view a : allowed)
            {
                ok = ok || a == std::string_view(kv.key().data(), kv.key().size());
            }
            if (!ok)
            {
                sendError(api::code::VALIDATION, "unknown field " + std::string(kv.key()));
                return false;
            }
        }
        return true;
    }

    static unsigned channelBit(const json::value &v)
    {
        const json::string *s = v.if_string();
        return nullptr == s ? 0u : "orders" == *s ? channel::ORDERS : "fills" == *s ? channel::FILLS
            : "balances" == *s ? channel::BALANCES : "engine" == *s ? channel::ENGINE : 0u;
    }

    // {"op":"subscribe","channels":["engine"]}: the engine channel alone (named once or more), no other key
    static bool anonymousEngineSubscribe(const json::object &o, std::string_view name)
    {
        const json::value *ch = o.if_contains("channels");
        if ("subscribe" != name || o.size() != 2 || nullptr == ch || !ch->is_array() || ch->as_array().empty())
        {
            return false;
        }
        for (const json::value &c : ch->as_array())
        {
            if (channel::ENGINE != channelBit(c))
            {
                return false;
            }
        }
        return true;
    }

    // A connection with no user: the engine status only, inside its own share of the connections.
    void openAnonymous()
    {
        const std::int64_t held = m_impl.counters.anonConnections.fetch_add(1, std::memory_order_relaxed) + 1;
        if (held > m_impl.settings.anonMaxConnections)
        {
            m_impl.counters.anonConnections.fetch_sub(1, std::memory_order_relaxed);
            m_impl.counters.anonRefusedCapacity.fetch_add(1, std::memory_order_relaxed);
            closeWith(1013, "ANON_CAPACITY");
            return;
        }
        m_anonymous = true; // counted until the connection is destroyed
        m_authDeadlineNs = -1;
        m_anonOps.emplace(m_impl.settings.anonOpsPerSecond, 1000, m_impl.settings.anonOpsBurst);
        open();
    }

    void refuseAnonymous(const std::string &text)
    {
        m_impl.counters.anonRefusedOps.fetch_add(1, std::memory_order_relaxed);
        sendError(api::code::VALIDATION, text);
    }

    void firstFrame(const json::object &o, std::string_view name)
    {
        if (m_authPending)
        {
            return; // one credential: frames before its answer are dropped
        }
        if (anonymousEngineSubscribe(o, name))
        {
            openAnonymous();
            return;
        }
        const json::value *token = o.if_contains("token");
        if ("auth" != name || o.size() != 2 || nullptr == token || !token->is_string() || token->as_string().empty())
        {
            m_impl.counters.authFailed.fetch_add(1, std::memory_order_relaxed);
            closeWith(1008, "the first frame is {\"op\":\"auth\",\"token\":\"...\"}");
            return;
        }
        m_authPending = true;
        m_authorization = "Bearer " + std::string(token->as_string().data(), token->as_string().size());
        resolve([self = shared_from_this()](AuthResult result) {
            net::post(self->m_ws.get_executor(), [self, result = std::move(result)] {
                self->m_authPending = false;
                if (self->m_closing)
                {
                    return;
                }
                if (AuthOutcome::OK != result.outcome)
                {
                    self->m_impl.counters.authFailed.fetch_add(1, std::memory_order_relaxed);
                    const bool later = AuthOutcome::UNAVAILABLE == result.outcome || AuthOutcome::BUSY == result.outcome;
                    self->closeWith(later ? 1013 : 1008, later ? "AUTH_UNAVAILABLE" : "UNAUTHORIZED");
                    return;
                }
                self->m_userId = result.principal.userId;
                self->m_authDeadlineNs = -1;
                self->open();
            });
        });
    }

    void pushOp(StreamCommand c)
    {
        if (!m_impl.commands.tryPush(std::move(c)))
        {
            m_impl.counters.opsRefused.fetch_add(1, std::memory_order_relaxed);
            sendError(api::code::OVERLOADED, "the stream is overloaded; send the op again");
        }
    }

    void sendError(const char *code, const std::string &text)
    {
        if (!m_outlet || m_closing)
        {
            return;
        }
        ioFrame(std::make_shared<const std::string>("{\"type\":\"error\",\"code\":\"" + std::string(code) +
            "\",\"error\":" + api::quote(text) + "}"));
    }

    // a frame of the I/O side's own, behind what is queued (dropped when the handover is full: the duty side's bound,
    // which counts it as unwritten, closes such a connection 4001)
    void ioFrame(Text frame)
    {
        if (!m_outlet || m_closing)
        {
            return;
        }
        if (m_outlet->ioPush(std::move(frame)) && !m_writing)
        {
            writeNext();
        }
    }

    // ---------------------------------------------------------------- writes, in handover order

    void writeNext()
    {
        if (m_closing)
        {
            return;
        }
        Text frame;
        if (!m_outlet->take(frame))
        {
            m_writing = false;
            if (m_closeAfterWrite && !m_pinging)
            {
                m_closeAfterWrite = false;
                closeWith(m_pendingCloseCode, m_pendingCloseReason.c_str());
            }
            return;
        }
        if (m_closeAfterWrite)
        {
            // the duty thread asked for the close: what it queued after the bound is not sent
            m_outlet->written(frame->size());
            writeNext();
            return;
        }
        m_writing = true;
        m_ws.async_write(net::buffer(*frame), [self = shared_from_this(), frame](beast::error_code ec, std::size_t) {
            self->m_outlet->written(frame->size());
            if (ec)
            {
                self->m_writing = false;
                if (!self->m_closing)
                {
                    self->countClose(1006);
                }
                self->hardClose();
                return;
            }
            self->m_impl.counters.framesSent.fetch_add(1, std::memory_order_relaxed);
            self->writeNext();
        });
    }

    // ---------------------------------------------------------------- liveness and closing

    void armTimer()
    {
        // the timer's step: fine enough for the shortest of the three bounds it checks (a step, not a bound)
        const std::int64_t stepMs = std::max<std::int64_t>(1, std::min({ m_impl.settings.pingMs,
            m_impl.settings.authTimeoutMs, m_impl.settings.idleMs, std::int64_t{ 2000 } }) / 2);
        m_timer.expires_after(std::chrono::milliseconds(stepMs));
        m_timer.async_wait([self = shared_from_this()](beast::error_code ec) {
            if (!ec)
            {
                self->onTimer();
            }
        });
    }

    void onTimer()
    {
        if (m_finished)
        {
            return;
        }
        const std::int64_t now = monoNs();
        if (m_closing || m_closeAfterWrite)
        {
            const std::int64_t since = m_closing ? m_closingNs : m_closeRequestedNs;
            if (now - since > m_impl.settings.idleMs * 1'000'000LL)
            {
                hardClose(); // the peer does not let the close (or the write before it) finish
                return;
            }
            armTimer();
            return;
        }
        if (m_authDeadlineNs >= 0 && !m_opened && now > m_authDeadlineNs)
        {
            m_impl.counters.authFailed.fetch_add(1, std::memory_order_relaxed);
            closeWith(1008, "AUTH_TIMEOUT");
            armTimer();
            return;
        }
        if (now - m_lastActivityNs > m_impl.settings.idleMs * 1'000'000LL)
        {
            closeWith(1001, "idle");
            armTimer();
            return;
        }
        if (now - m_lastPingNs >= m_impl.settings.pingMs * 1'000'000LL && !m_writing && !m_pinging)
        {
            m_lastPingNs = now;
            m_pinging = true;
            m_ws.async_ping({}, [self = shared_from_this()](beast::error_code) {
                self->m_pinging = false;
                if (self->m_closeAfterWrite && !self->m_writing)
                {
                    self->writeNext(); // a close asked for during the ping
                }
            });
        }
        armTimer();
    }

    void closeWith(std::uint16_t code, const char *reason)
    {
        if (m_closing)
        {
            return;
        }
        if (m_writing || m_pinging)
        {
            m_closeAfterWrite = true; // after the write (or ping) in progress; the timer bounds the wait
            m_pendingCloseCode = code;
            m_pendingCloseReason = reason;
            m_closeRequestedNs = monoNs();
            return;
        }
        m_closing = true;
        m_closingNs = monoNs();
        countClose(code);
        m_ws.async_close(websocket::close_reason(static_cast<websocket::close_code>(code), reason), [self = shared_from_this()](beast::error_code ec) {
            if (ec)
            {
                self->hardClose();
                return;
            }
#ifndef GW_NC_CLOSE_WAITS_FOR_READ
            // the close handshake is done: the connection (and an anonymous slot) is released now. No read may be
            // running (a close asked for while a client frame was handled stops the read loop), so this cannot wait
            // for one; when a read is running it also finishes, and finish() runs once
            self->finish();
#endif
            // test-only control GW_NC_CLOSE_WAITS_FOR_READ: only a running read loop finishes the connection, so a
            // close started from a client frame holds its slot until the timer's STREAM_WS_IDLE_MS
        });
    }

    void hardClose()
    {
        beast::error_code ignored;
        beast::get_lowest_layer(m_ws).socket().shutdown(tcp::socket::shutdown_both, ignored);
        beast::get_lowest_layer(m_ws).socket().close(ignored);
        finish();
    }

    void countClose(std::uint16_t code)
    {
        if (!m_closeCounted)
        {
            m_closeCounted = true;
            m_impl.counters.closed[closeSlot(code)].fetch_add(1, std::memory_order_relaxed);
        }
    }

    void finish()
    {
        if (m_finished)
        {
            return;
        }
        m_finished = true;
        m_timer.cancel();
        if (m_outlet)
        {
            m_outlet->markGone();
        }
        if (m_opened)
        {
            StreamCommand c;
            c.kind = StreamCommand::Kind::GONE;
            c.connId = m_id;
            m_impl.commands.tryPush(std::move(c)); // a full ring is fine: the duty thread also sweeps closed outlets
        }
    }

    websocket::stream<beast::tcp_stream> m_ws;
    WsServer::Impl &m_impl;
    net::steady_timer m_timer;
    std::uint64_t m_id;
    beast::flat_buffer m_buffer;
    beast::flat_buffer m_readBuffer;
    std::optional<http::request_parser<http::empty_body>> m_parser;
    http::request<http::empty_body> m_request;
    std::shared_ptr<WsOutlet> m_outlet;
    std::string m_authorization;
    std::string m_apiKey;
    bool m_subprotocol = false;
    std::int64_t m_userId = 0;
    std::int64_t m_frameUserId = 0;
    bool m_anonymous = false; // counted in anonConnections
    std::optional<TokenBucketMs> m_anonOps; // subscribe and unsubscribe ops of an anonymous connection
    bool m_upgraded = false;
    bool m_opened = false;
    bool m_authPending = false;
    std::int64_t m_authDeadlineNs = -1;
    bool m_writing = false;
    bool m_pinging = false;
    bool m_closing = false;
    bool m_closeAfterWrite = false;
    std::uint16_t m_pendingCloseCode = 1000;
    std::string m_pendingCloseReason;
    std::int64_t m_closeRequestedNs = 0;
    std::int64_t m_closingNs = 0;
    bool m_closeCounted = false;
    bool m_finished = false;
    std::int64_t m_lastActivityNs = 0;
    std::int64_t m_lastPingNs = 0;
};

void Waker::run()
{
    std::weak_ptr<WsConn> conn;
    for (;;)
    {
        const std::uint32_t seen = m_signal.load(std::memory_order_acquire);
        bool any = false;
        while (m_ring.tryPop(conn))
        {
            any = true;
            if (auto c = conn.lock())
            {
                net::post(c->executor(), [c] { c->onKick(); });
            }
        }
        if (m_stop.load(std::memory_order_acquire))
        {
            break;
        }
        if (!any)
        {
            m_signal.wait(seen, std::memory_order_acquire);
        }
    }
    m_finished.store(true, std::memory_order_release);
}

}

void WsServer::Impl::accept()
{
    acceptor.async_accept(net::make_strand(ioc), [this](beast::error_code ec, tcp::socket socket) {
        if (ec)
        {
            if (ec == net::error::operation_aborted || !acceptor.is_open())
            {
                return;
            }
            accept();
            return;
        }
        if (counters.connections.load(std::memory_order_relaxed) >= settings.maxConnections)
        {
            counters.refused.fetch_add(1, std::memory_order_relaxed);
            beast::error_code ignored;
            socket.close(ignored);
        }
        else
        {
            counters.connections.fetch_add(1, std::memory_order_relaxed);
            if (settings.sendBufferBytes > 0)
            {
                beast::error_code ignored;
                socket.set_option(net::socket_base::send_buffer_size(static_cast<int>(
                    std::min<std::int64_t>(settings.sendBufferBytes, INT32_MAX))), ignored);
            }
            std::make_shared<WsConn>(std::move(socket), *this)->start();
        }
        accept();
    });
}

WsServer::WsServer(WsServerSettings settings, Authenticator &auth, CommandRing &commands) :
    m_impl(std::make_unique<Impl>(std::move(settings), auth, commands))
{
}

WsServer::~WsServer()
{
    stop(2000);
}

const WsCounters &WsServer::counters() const noexcept
{
    return m_impl->counters;
}

std::uint16_t WsServer::boundPort() const noexcept
{
    return m_impl->port;
}

bool WsServer::bind(std::string &error)
{
    beast::error_code ec;
    const net::ip::address address =
        net::ip::make_address("localhost" == m_impl->settings.host ? "127.0.0.1" : m_impl->settings.host, ec);
    if (ec)
    {
        error = "'" + m_impl->settings.host + "' is not an IP address";
        return false;
    }
    const tcp::endpoint endpoint(address, m_impl->settings.port);
    m_impl->acceptor.open(endpoint.protocol(), ec);
    if (!ec)
    {
        m_impl->acceptor.set_option(net::socket_base::reuse_address(true), ec);
    }
    if (!ec)
    {
        m_impl->acceptor.bind(endpoint, ec);
    }
    if (!ec)
    {
        m_impl->acceptor.listen(net::socket_base::max_listen_connections, ec);
    }
    if (ec)
    {
        error = "cannot listen on " + m_impl->settings.host + ":" + std::to_string(m_impl->settings.port) + ": " +
            ec.message();
        return false;
    }
    m_impl->port = m_impl->acceptor.local_endpoint().port();
    return true;
}

void WsServer::start()
{
    m_impl->waker.start(m_impl->settings.threadPrefix);
    m_impl->accept();
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    for (int i = 0; i < m_impl->settings.ioThreads; ++i)
    {
        m_impl->running++;
        m_impl->threads.emplace_back([this, i] {
            char name[16];
            std::snprintf(name, sizeof(name), "%s-io-%d", m_impl->settings.threadPrefix.c_str(), i);
            pthread_setname_np(pthread_self(), name);
            m_impl->ioc.run();
            std::lock_guard<std::mutex> l(m_impl->mutex);
            m_impl->running--;
            m_impl->cv.notify_all();
        });
    }
    m_accepting.store(true, std::memory_order_release);
}

bool WsServer::stop(std::int64_t boundMs)
{
    m_accepting.store(false, std::memory_order_release);
    net::post(m_impl->acceptor.get_executor(), [impl = m_impl.get()] {
        beast::error_code ignored;
        impl->acceptor.close(ignored);
    });
    m_impl->ioc.stop();
    bool ok = true;
    std::vector<std::thread> threads;
    {
        std::unique_lock<std::mutex> lock(m_impl->mutex);
        threads.swap(m_impl->threads);
        if (!m_impl->cv.wait_for(lock, std::chrono::milliseconds(boundMs), [this] { return 0 == m_impl->running; }))
        {
            for (std::thread &t : threads)
            {
                t.detach();
            }
            threads.clear();
            ok = false;
        }
    }
    for (std::thread &t : threads)
    {
        t.join();
    }
    return m_impl->waker.stop(boundMs) && ok;
}

}}}
