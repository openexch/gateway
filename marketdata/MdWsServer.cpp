#include "MdWsServer.h"

#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <mutex>
#include <optional>
#include <thread>
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

#include "CandleRoute.h"

namespace openexchange { namespace gateway { namespace marketdata {

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

std::int64_t monoMs()
{
    return monoNs() / 1'000'000LL;
}

// The client frame -> a command (or a BAD command naming what was wrong). Runs on an I/O thread.
MdCommand parseClientFrame(std::uint64_t connId, const std::string &text)
{
    MdCommand c;
    c.connId = connId;
    c.kind = MdCommand::Kind::BAD;
    boost::system::error_code ec;
    const json::value v = json::parse(text, ec);
    if (ec || !v.is_object())
    {
        c.text = "a client frame is a JSON object with an action";
        return c;
    }
    const json::object &o = v.get_object();
    const json::value *action = o.if_contains("action");
    if (nullptr == action || !action->is_string())
    {
        c.text = "action is required: subscribe, refresh or ping";
        return c;
    }
    const json::string &a = action->get_string();
    if ("ping" == a)
    {
        c.kind = MdCommand::Kind::PING;
        return c;
    }
    if ("subscribe" != a && "refresh" != a)
    {
        c.text = "unknown action '" + std::string(a.data(), a.size()) + "': subscribe, refresh or ping";
        return c;
    }
    const json::value *market = o.if_contains("marketId");
    if (nullptr == market || !(market->is_int64() || market->is_uint64()))
    {
        c.text = std::string(a.data(), a.size()) + " needs marketId, a JSON integer";
        return c;
    }
    const std::int64_t id = market->is_int64() ? market->get_int64() : static_cast<std::int64_t>(
        std::min<std::uint64_t>(market->get_uint64(), static_cast<std::uint64_t>(INT64_MAX)));
    c.kind = "subscribe" == a ? MdCommand::Kind::SUBSCRIBE : MdCommand::Kind::REFRESH;
    c.marketId = id;
    return c;
}

class WsSession;

// The duty thread's view of one connection: a byte-bounded queue; the connection's strand drains it.
class WsOutlet final : public Outlet
{
public:
    WsOutlet(std::int64_t queueBytes, MdWsCounters &counters) : m_queueBytes(queueBytes), m_counters(counters) {}

    void attach(const std::shared_ptr<WsSession> &session) { m_session = session; }

    // ---- duty side
    bool offer(const Frame &frame) override;
    void resyncBegins() override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_resyncPending = false;
        m_resyncAsked = false;
    }

    // ---- strand side
    // The next frame to write, or none (writing ends; `askResync` says the duty thread is to send a fresh state).
    bool next(Frame &out, bool &askResync)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        askResync = false;
        if (m_queue.empty())
        {
            m_writing = false;
            if (m_resyncPending && !m_resyncAsked && !m_closed)
            {
                m_resyncAsked = true;
                askResync = true;
            }
            return false;
        }
        out = std::move(m_queue.front());
        m_queue.pop_front();
        m_bytes -= static_cast<std::int64_t>(out.text->size());
        return true;
    }
    void close()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_closed = true;
        release(static_cast<std::int64_t>(m_queue.size()));
        m_queue.clear();
        m_bytes = 0;
    }
    void release(std::int64_t frames)
    {
        if (frames > 0)
        {
            m_counters.pending.fetch_sub(frames, std::memory_order_acq_rel);
            m_counters.waitingSinceNs.store(monoNs(), std::memory_order_release);
        }
    }

private:
    const std::int64_t m_queueBytes;
    MdWsCounters &m_counters;
    std::weak_ptr<WsSession> m_session;
    std::mutex m_mutex;
    std::deque<Frame> m_queue;
    std::int64_t m_bytes = 0;
    bool m_writing = false;
    bool m_resyncPending = false;
    bool m_resyncAsked = false;
    bool m_closed = false;
};

}

struct MdWsServer::Impl
{
    Impl(MdWsSettings s, MdCommandRing &c, CandleRoute *r, std::atomic<std::int64_t> *mark) :
        settings(std::move(s)),
        commands(c),
        candles(r),
        inputMarkNs(mark),
        sources(settings.trustedProxies),
        connectBuckets(settings.connectRate, settings.connectBurst),
        messageBuckets(settings.messageRate, settings.messageBurst),
        acceptor(net::make_strand(ioc))
    {
    }

    void accept();

    // A write finished at `nowNs`: the source inputs before it are served.
    void served(std::int64_t nowNs)
    {
        if (nullptr == inputMarkNs)
        {
            return;
        }
        std::int64_t mark = inputMarkNs->load(std::memory_order_acquire);
        if (mark >= 0 && mark <= nowNs)
        {
            inputMarkNs->compare_exchange_strong(mark, -1, std::memory_order_acq_rel);
        }
    }

    MdWsSettings settings;
    MdCommandRing &commands;
    CandleRoute *candles;
    std::atomic<std::int64_t> *inputMarkNs;
    SourceAddress sources;
    KeyedRateLimiter connectBuckets;
    KeyedRateLimiter messageBuckets;
    net::io_context ioc;
    tcp::acceptor acceptor;
    std::uint16_t port = 0;
    MdWsCounters counters;
    std::atomic<std::uint64_t> nextConnId{ 1 };
    std::mutex mutex;
    std::condition_variable cv;
    int running = 0;
    std::vector<std::thread> threads;
};

namespace {

class WsSession : public std::enable_shared_from_this<WsSession>
{
public:
    WsSession(tcp::socket socket, MdWsServer::Impl &impl) :
        m_impl(impl), m_ws(std::move(socket)), m_timer(m_ws.get_executor())
    {
    }

    void run()
    {
        m_parser.emplace();
        m_parser->header_limit(static_cast<std::uint32_t>(m_impl.settings.maxHeaderBytes));
        m_parser->body_limit(0);
        beast::get_lowest_layer(m_ws).expires_after(std::chrono::milliseconds(m_impl.settings.idleMs));
        http::async_read(beast::get_lowest_layer(m_ws), m_buffer, *m_parser,
            beast::bind_front_handler(&WsSession::onRequest, shared_from_this()));
    }

    void kick()
    {
        net::dispatch(m_ws.get_executor(), [self = shared_from_this()] { self->writeNext(); });
    }

private:
    void onRequest(beast::error_code ec, std::size_t)
    {
        if (ec)
        {
            return;
        }
        const auto &req = m_parser->get();
        if (!takeConnectBudget(req))
        {
            return;
        }
        if (!websocket::is_upgrade(req) && nullptr != m_impl.candles && CandleRoute::owns(req.target()))
        {
            serveRest(req);
            return;
        }
        if (!websocket::is_upgrade(req) || "/ws" != req.target())
        {
            m_impl.counters.notFound.fetch_add(1, std::memory_order_relaxed);
            auto res = std::make_shared<http::response<http::string_body>>(http::status::not_found, req.version());
            res->set(http::field::content_type, "text/plain");
            res->keep_alive(false);
            res->body() = "not found\n";
            res->prepare_payload();
            http::async_write(beast::get_lowest_layer(m_ws), *res,
                [self = shared_from_this(), res](beast::error_code, std::size_t) {
                    beast::error_code ignored;
                    beast::get_lowest_layer(self->m_ws).socket().shutdown(tcp::socket::shutdown_both, ignored);
                });
            return;
        }
        beast::get_lowest_layer(m_ws).expires_never();
        websocket::stream_base::timeout t;
        t.handshake_timeout = std::chrono::milliseconds(m_impl.settings.idleMs);
        t.idle_timeout = std::chrono::milliseconds(m_impl.settings.idleMs);
        t.keep_alive_pings = true;
        m_ws.set_option(t);
        m_ws.read_message_max(static_cast<std::size_t>(m_impl.settings.maxFrame));
        m_ws.async_accept(req, beast::bind_front_handler(&WsSession::onAccept, shared_from_this()));
    }

    // The source of the request and one token of MD_RATE_CONNECT; refused: 429 RATE_LIMITED and the connection closes.
    bool takeConnectBudget(const http::request<http::empty_body> &req)
    {
        beast::error_code ec;
        const tcp::endpoint peer = beast::get_lowest_layer(m_ws).socket().remote_endpoint(ec);
        std::vector<std::string_view> forwarded;
        if (m_impl.sources.enabled())
        {
            for (auto range = req.equal_range(m_impl.settings.clientIpHeader); range.first != range.second; ++range.first)
            {
                forwarded.emplace_back(range.first->value().data(), range.first->value().size());
            }
        }
        m_source = budgetKey(m_impl.sources.resolve(ec ? net::ip::address() : peer.address(), forwarded),
            m_impl.settings.sourceV6PrefixBits);
        const TakeResult t = m_impl.connectBuckets.take(m_source, monoMs());
        if (t.ok)
        {
            return true;
        }
        m_impl.counters.limitedConnects.fetch_add(1, std::memory_order_relaxed);
        api::ErrorSpec spec;
        spec.status = 429;
        spec.code = api::code::RATE_LIMITED;
        spec.text = "too many requests";
        spec.detail = "the source's request budget (MD_RATE_CONNECT) is spent";
        spec.retryAfterMs = t.retryAfterMs;
        ApiResponse a = api::error(spec);
        RestAnswer answer;
        answer.status = a.status;
        answer.body = std::move(a.body);
        answer.headers = std::move(a.headers);
        writeRest(req.version(), answer);
        return false;
    }

    // GET /api/candles and its preflight: CandleRoute answers (after a read on md-rest), the answer is written on this
    // connection's strand, and the connection closes.
    void serveRest(const http::request<http::empty_body> &req)
    {
        std::optional<std::string> origin;
        if (const auto it = req.find(http::field::origin); req.end() != it)
        {
            origin = std::string(it->value());
        }
        const unsigned version = req.version();
        m_impl.candles->handle(std::string(req.method_string()), std::string(req.target()), origin,
            [self = shared_from_this(), version](RestAnswer answer) {
                auto shared = std::make_shared<RestAnswer>(std::move(answer));
                net::post(self->m_ws.get_executor(), [self, version, shared] { self->writeRest(version, *shared); });
            });
    }

    void writeRest(unsigned version, const RestAnswer &answer)
    {
        auto res = std::make_shared<http::response<http::string_body>>(static_cast<http::status>(answer.status), version);
        if (!answer.body.empty())
        {
            res->set(http::field::content_type, "application/json");
        }
        res->set(http::field::cache_control, "no-store");
        for (const auto &h : answer.headers)
        {
            res->set(h.first, h.second);
        }
        res->keep_alive(false);
        res->body() = answer.body;
        res->prepare_payload();
        beast::get_lowest_layer(m_ws).expires_after(std::chrono::milliseconds(m_impl.settings.idleMs));
        http::async_write(beast::get_lowest_layer(m_ws), *res, [self = shared_from_this(), res](beast::error_code, std::size_t) {
            beast::error_code ignored;
            beast::get_lowest_layer(self->m_ws).socket().shutdown(tcp::socket::shutdown_both, ignored);
        });
    }

    void onAccept(beast::error_code ec)
    {
        if (ec)
        {
            return;
        }
        m_connId = m_impl.nextConnId.fetch_add(1, std::memory_order_relaxed);
        m_outlet = std::make_shared<WsOutlet>(m_impl.settings.queueBytes, m_impl.counters);
        m_outlet->attach(shared_from_this());
        m_open = true;
        m_impl.counters.connections.fetch_add(1, std::memory_order_relaxed);
        MdCommand open;
        open.kind = MdCommand::Kind::OPEN;
        open.connId = m_connId;
        open.outlet = m_outlet;
        if (!m_impl.commands.tryPush(std::move(open)))
        {
            m_impl.counters.opsRefused.fetch_add(1, std::memory_order_relaxed);
            closeWith(websocket::close_code::try_again_later);
            return;
        }
        armUnwritableCheck();
        read();
    }

    void read()
    {
        m_ws.async_read(m_in, beast::bind_front_handler(&WsSession::onRead, shared_from_this()));
    }

    void onRead(beast::error_code ec, std::size_t)
    {
        if (ec)
        {
            finish();
            return;
        }
        const std::string text = beast::buffers_to_string(m_in.data());
        m_in.consume(m_in.size());
        if (!m_impl.messageBuckets.take(m_source, monoMs()).ok)
        {
            // refused: not acted on; the first refusal of a run is answered with one ERROR RATE_LIMITED
            m_impl.counters.limitedMessages.fetch_add(1, std::memory_order_relaxed);
            if (!m_limited)
            {
                m_limited = true;
                MdCommand limited;
                limited.kind = MdCommand::Kind::LIMITED;
                limited.connId = m_connId;
                limited.text = "the source's client frame budget (MD_RATE_MESSAGE) is spent: frames are dropped until "
                    "it refills";
                if (!m_impl.commands.tryPush(std::move(limited)))
                {
                    m_impl.counters.opsRefused.fetch_add(1, std::memory_order_relaxed);
                }
            }
            read();
            return;
        }
        m_limited = false;
        MdCommand c = parseClientFrame(m_connId, text);
        if (!m_impl.commands.tryPush(std::move(c)))
        {
            m_impl.counters.opsRefused.fetch_add(1, std::memory_order_relaxed);
        }
        read();
    }

    void writeNext()
    {
        if (!m_open || m_writing)
        {
            return;
        }
        bool askResync = false;
        if (!m_outlet->next(m_current, askResync))
        {
            if (askResync)
            {
                MdCommand c;
                c.kind = MdCommand::Kind::RESYNC;
                c.connId = m_connId;
                if (!m_impl.commands.tryPush(std::move(c)))
                {
                    m_impl.counters.opsRefused.fetch_add(1, std::memory_order_relaxed);
                }
            }
            return;
        }
#ifdef MD_TC_SEND_DROPS
        // test build only (never published): a sender that drops every frame instead of writing it
        do
        {
            m_outlet->release(1);
            m_current = Frame{};
        } while (m_outlet->next(m_current, askResync));
        return;
#endif
        m_writing = true;
        m_writeStartNs = monoNs();
        m_ws.text(true);
        m_ws.async_write(net::buffer(*m_current.text),
            beast::bind_front_handler(&WsSession::onWrite, shared_from_this()));
    }

    void onWrite(beast::error_code ec, std::size_t)
    {
        m_writing = false;
        m_outlet->release(1);
        if (ec)
        {
            finish();
            return;
        }
        m_impl.counters.sent[static_cast<std::size_t>(m_current.type)].fetch_add(1, std::memory_order_relaxed);
        m_impl.served(monoNs());
        m_current = Frame{};
        writeNext();
    }

    // A write that has not finished within MD_WS_UNWRITABLE_CLOSE_MS closes the connection.
    void armUnwritableCheck()
    {
        const std::int64_t periodMs = std::max<std::int64_t>(1, std::min<std::int64_t>(1000,
            m_impl.settings.unwritableCloseMs / 4));
        m_timer.expires_after(std::chrono::milliseconds(periodMs));
        m_timer.async_wait([self = shared_from_this()](beast::error_code ec) {
            if (ec || !self->m_open)
            {
                return;
            }
            if (self->m_writing && monoNs() - self->m_writeStartNs >= self->m_impl.settings.unwritableCloseMs * 1'000'000LL)
            {
                self->m_impl.counters.closedSlow.fetch_add(1, std::memory_order_relaxed);
                beast::error_code ignored;
                beast::get_lowest_layer(self->m_ws).socket().close(ignored);
                return;
            }
            self->armUnwritableCheck();
        });
    }

    void closeWith(websocket::close_code code)
    {
        m_ws.async_close(code, [self = shared_from_this()](beast::error_code) { self->finish(); });
    }

    void finish()
    {
        if (!m_open)
        {
            return;
        }
        m_open = false;
        m_timer.cancel();
        m_outlet->close(); // the queued frames are released; a write in progress is released by its handler
        m_impl.counters.connections.fetch_sub(1, std::memory_order_relaxed);
        MdCommand gone;
        gone.kind = MdCommand::Kind::GONE;
        gone.connId = m_connId;
        // GONE must reach the duty thread or the subscription would outlive the socket: retry on this strand
        pushGone(std::move(gone));
    }

    void pushGone(MdCommand gone)
    {
        if (m_impl.commands.tryPush(std::move(gone)))
        {
            return;
        }
        auto retry = std::make_shared<net::steady_timer>(m_ws.get_executor(), std::chrono::milliseconds(1));
        retry->async_wait([self = shared_from_this(), retry, id = m_connId](beast::error_code) {
            MdCommand again;
            again.kind = MdCommand::Kind::GONE;
            again.connId = id;
            self->pushGone(std::move(again));
        });
    }

    MdWsServer::Impl &m_impl;
    websocket::stream<beast::tcp_stream> m_ws;
    net::steady_timer m_timer;
    beast::flat_buffer m_buffer;
    beast::flat_buffer m_in;
    std::optional<http::request_parser<http::empty_body>> m_parser;
    std::shared_ptr<WsOutlet> m_outlet;
    std::uint64_t m_connId = 0;
    std::string m_source; // the budget key of this connection's source address
    bool m_limited = false; // inside a run of client frames refused by MD_RATE_MESSAGE
    bool m_open = false;
    bool m_writing = false;
    std::int64_t m_writeStartNs = 0;
    Frame m_current;
};

bool WsOutlet::offer(const Frame &frame)
{
    std::shared_ptr<WsSession> session;
    bool queued = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_closed || m_resyncPending)
        {
            return false;
        }
        const std::int64_t size = static_cast<std::int64_t>(frame.text->size());
#ifdef MD_NC_NO_QUEUE_BOUND
        const bool over = false; // control build only (never published): the queue has no bound
#else
        const bool over = m_bytes + size > m_queueBytes;
#endif
        if (over)
        {
            // the bound: the queue goes, nothing more is queued until the fresh initial state of a resync
            m_counters.queueDrops.fetch_add(1, std::memory_order_relaxed);
            release(static_cast<std::int64_t>(m_queue.size()));
            m_queue.clear();
            m_bytes = 0;
            m_resyncPending = true;
            m_resyncAsked = false;
            session = m_writing ? nullptr : m_session.lock();
            if (nullptr != session)
            {
                m_writing = true; // the strand reports the empty queue and asks for the resync
            }
        }
        else
        {
            if (0 == m_counters.pending.fetch_add(1, std::memory_order_acq_rel))
            {
                m_counters.waitingSinceNs.store(monoNs(), std::memory_order_release);
            }
            m_queue.push_back(frame);
            m_bytes += size;
            queued = true;
            if (m_bytes > m_counters.queueHighWater.load(std::memory_order_relaxed))
            {
                m_counters.queueHighWater.store(m_bytes, std::memory_order_relaxed); // offers come from the duty thread
            }
            if (!m_writing)
            {
                m_writing = true; // a running write drains the queue itself; otherwise the strand is kicked
                session = m_session.lock();
            }
        }
    }
    if (nullptr != session)
    {
        session->kick();
    }
    return queued;
}

}

void MdWsServer::Impl::accept()
{
    acceptor.async_accept(net::make_strand(ioc), [this](beast::error_code ec, tcp::socket socket) {
        if (ec == net::error::operation_aborted || !acceptor.is_open())
        {
            return;
        }
        if (!ec)
        {
            if (counters.connections.load(std::memory_order_relaxed) >= settings.maxConnections)
            {
                counters.refused.fetch_add(1, std::memory_order_relaxed);
                beast::error_code ignored;
                socket.close(ignored);
            }
            else
            {
                std::make_shared<WsSession>(std::move(socket), *this)->run();
            }
        }
        accept();
    });
}

MdWsServer::MdWsServer(MdWsSettings settings, MdCommandRing &commands, CandleRoute *candles,
    std::atomic<std::int64_t> *inputMarkNs) :
    m_impl(std::make_unique<Impl>(std::move(settings), commands, candles, inputMarkNs))
{
}

MdWsServer::~MdWsServer()
{
    stop(2000);
}

bool MdWsServer::bind(std::string &error)
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

std::uint16_t MdWsServer::boundPort() const noexcept
{
    return m_impl->port;
}

const MdWsCounters &MdWsServer::counters() const noexcept
{
    return m_impl->counters;
}

bool MdWsServer::clientsServed(std::int64_t nowNs) const noexcept
{
#ifdef MD_NC_NO_CLIENTS_SERVED
    (void)nowNs;
    return true; // control build only (never published): the condition is not judged
#else
    const MdWsCounters &c = m_impl->counters;
    const std::int64_t stallNs = m_impl->settings.sendStallMs * 1'000'000LL;
    if (c.connections.load(std::memory_order_acquire) <= 0)
    {
        return true;
    }
    if (nullptr != m_impl->inputMarkNs)
    {
        const std::int64_t mark = m_impl->inputMarkNs->load(std::memory_order_acquire);
        if (mark >= 0 && nowNs - mark >= stallNs)
        {
            return false; // a source input with a subscriber and no finished write since
        }
    }
    if (c.pending.load(std::memory_order_acquire) <= 0)
    {
        return true;
    }
    const std::int64_t since = c.waitingSinceNs.load(std::memory_order_acquire);
    return since < 0 || nowNs - since < stallNs;
#endif
}

void MdWsServer::start()
{
    m_impl->accept();
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    for (int i = 0; i < m_impl->settings.ioThreads; ++i)
    {
        m_impl->running++;
        m_impl->threads.emplace_back([this, i] {
            char name[16];
            std::snprintf(name, sizeof(name), "md-io-%d", i);
            pthread_setname_np(pthread_self(), name);
            m_impl->ioc.run();
            std::lock_guard<std::mutex> l(m_impl->mutex);
            m_impl->running--;
            m_impl->cv.notify_all();
        });
    }
    m_accepting.store(true, std::memory_order_release);
}

bool MdWsServer::stop(std::int64_t boundMs)
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
    return ok;
}

}}}
