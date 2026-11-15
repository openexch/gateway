#include "gateway/common/HttpServer.h"

#include <pthread.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

namespace openexchange { namespace gateway {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

namespace {

std::int64_t monoNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

const char *reasonText(unsigned status)
{
    return http::obsolete_reason(static_cast<http::status>(status)).data();
}

}

struct HttpServer::Impl
{
    Impl(HttpServer &owner, HttpServerSettings s, Handler h) :
        server(owner), settings(std::move(s)), handler(std::move(h)), acceptor(net::make_strand(ioc))
    {
    }

    void accept();

    HttpServer &server;
    HttpServerSettings settings;
    Handler handler;
    net::io_context ioc;
    tcp::acceptor acceptor;
    std::vector<std::thread> threads;
    std::mutex mutex;
    std::condition_variable cv;
    int running = 0;
};

namespace {

class Session;

class Exchange final : public HttpExchange
{
public:
    Exchange(std::shared_ptr<Session> session, http::request<http::string_body> request, net::ip::address peer) :
        m_session(std::move(session)), m_request(std::move(request)), m_peer(std::move(peer)), m_receivedNs(monoNs())
    {
        const std::string_view t(m_request.target().data(), m_request.target().size());
        const std::size_t q = t.find('?');
        m_path = t.substr(0, q);
        m_query = std::string_view::npos == q ? std::string_view() : t.substr(q + 1);
    }

    std::string_view method() const override
    {
        return std::string_view(m_request.method_string().data(), m_request.method_string().size());
    }
    std::string_view target() const override
    {
        return std::string_view(m_request.target().data(), m_request.target().size());
    }
    std::string_view path() const override { return m_path; }
    std::string_view query() const override { return m_query; }
    std::optional<std::string_view> header(std::string_view name) const override
    {
        const auto it = m_request.find(beast::string_view(name.data(), name.size()));
        if (it == m_request.end())
        {
            return std::nullopt;
        }
        return std::string_view(it->value().data(), it->value().size());
    }
    std::vector<std::string_view> headers(std::string_view name) const override
    {
        std::vector<std::string_view> out;
        const auto range = m_request.equal_range(beast::string_view(name.data(), name.size()));
        for (auto it = range.first; it != range.second; ++it)
        {
            out.emplace_back(it->value().data(), it->value().size());
        }
        return out;
    }
    std::size_t headerCount(std::string_view name) const override
    {
        return m_request.count(beast::string_view(name.data(), name.size()));
    }
    std::string_view body() const override { return m_request.body(); }
    const net::ip::address &peer() const override { return m_peer; }
    std::int64_t receivedNs() const override { return m_receivedNs; }
    void respond(ApiResponse response) override;
    void runOnIo(std::function<void()> work) override;

    bool keepAlive() const { return m_request.keep_alive(); }
    unsigned version() const { return m_request.version(); }

private:
    std::shared_ptr<Session> m_session;
    http::request<http::string_body> m_request;
    net::ip::address m_peer;
    std::int64_t m_receivedNs;
    std::string_view m_path;
    std::string_view m_query;
    std::atomic<bool> m_answered{ false };
};

class Session : public std::enable_shared_from_this<Session>
{
public:
    Session(tcp::socket socket, HttpServer::Impl &impl, std::atomic<std::int64_t> &connections,
        std::atomic<std::uint64_t> &requests) :
        m_stream(std::move(socket)), m_impl(impl), m_connections(connections), m_requests(requests)
    {
        boost::system::error_code ec;
        const auto endpoint = m_stream.socket().remote_endpoint(ec);
        if (!ec)
        {
            m_peer = endpoint.address();
        }
    }

    ~Session() { m_connections.fetch_sub(1, std::memory_order_relaxed); }

    void start()
    {
        net::dispatch(m_stream.get_executor(), [self = shared_from_this()] { self->read(); });
    }

    void deliver(std::shared_ptr<http::response<http::string_body>> response, bool keepAlive)
    {
        net::post(m_stream.get_executor(), [self = shared_from_this(), response, keepAlive] {
            self->write(response, keepAlive);
        });
    }

    template<typename F>
    void post(F &&f)
    {
        net::post(m_stream.get_executor(), std::forward<F>(f));
    }

private:
    void read()
    {
        m_parser.emplace();
        m_parser->body_limit(m_impl.settings.maxBodyBytes);
        m_parser->header_limit(static_cast<std::uint32_t>(m_impl.settings.maxHeaderBytes));
        m_stream.expires_after(std::chrono::milliseconds(m_impl.settings.idleTimeoutMs));
        http::async_read(m_stream, m_buffer, *m_parser,
            [self = shared_from_this()](beast::error_code ec, std::size_t) { self->onRead(ec); });
    }

    void onRead(beast::error_code ec)
    {
        if (ec == http::error::body_limit || ec == http::error::header_limit)
        {
            const bool body = ec == http::error::body_limit;
            auto r = std::make_shared<http::response<http::string_body>>(
                body ? http::status::payload_too_large : http::status::request_header_fields_too_large, 11);
            r->set(http::field::content_type, "application/json");
            r->set(http::field::cache_control, "no-store");
            r->body() = std::string("{\"error\":\"") + (body ? "request body too large" : "request headers too large") +
                "\",\"code\":\"VALIDATION\",\"reason\":null,\"detail\":\"" +
                (body ? "the body is above the gateway's body bound" : "the header block is above the gateway's bound") +
                "\",\"keyFinal\":false}";
            r->keep_alive(false);
            r->prepare_payload();
            write(r, false);
            return;
        }
        if (ec)
        {
            close();
            return;
        }
        m_requests.fetch_add(1, std::memory_order_relaxed);
        auto exchange = std::make_shared<Exchange>(shared_from_this(), m_parser->release(), m_peer);
        m_stream.expires_never(); // the handler owns the time now; the write sets the bound again
        m_impl.handler(exchange);
    }

    void write(std::shared_ptr<http::response<http::string_body>> response, bool keepAlive)
    {
        m_stream.expires_after(std::chrono::milliseconds(m_impl.settings.idleTimeoutMs));
        http::async_write(m_stream, *response,
            [self = shared_from_this(), response, keepAlive](beast::error_code ec, std::size_t) {
                if (ec || !keepAlive)
                {
                    self->close();
                    return;
                }
                self->read();
            });
    }

    void close()
    {
        beast::error_code ignored;
        m_stream.socket().shutdown(tcp::socket::shutdown_send, ignored);
        m_stream.close();
    }

    beast::tcp_stream m_stream;
    beast::flat_buffer m_buffer;
    std::optional<http::request_parser<http::string_body>> m_parser;
    HttpServer::Impl &m_impl;
    std::atomic<std::int64_t> &m_connections;
    std::atomic<std::uint64_t> &m_requests;
    net::ip::address m_peer;
};

void Exchange::respond(ApiResponse r)
{
    if (m_answered.exchange(true))
    {
        return;
    }
    auto response = std::make_shared<http::response<http::string_body>>(static_cast<http::status>(r.status), version());
    response->reason(reasonText(r.status));
    if (!r.body.empty())
    {
        response->set(http::field::content_type, "application/json");
    }
    response->set(http::field::cache_control, "no-store");
    for (const auto &[name, value] : r.headers)
    {
        response->insert(beast::string_view(name.data(), name.size()), beast::string_view(value.data(), value.size()));
    }
    response->body() = std::move(r.body);
    response->keep_alive(keepAlive());
    response->prepare_payload();
    m_session->deliver(response, keepAlive());
}

void Exchange::runOnIo(std::function<void()> work)
{
    m_session->post([work = std::move(work)] { work(); });
}

}

void HttpServer::Impl::accept()
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
        if (server.m_connections.load(std::memory_order_relaxed) >= settings.maxConnections)
        {
            server.m_refused.fetch_add(1, std::memory_order_relaxed);
            beast::error_code ignored;
            socket.close(ignored);
        }
        else
        {
            server.m_connections.fetch_add(1, std::memory_order_relaxed);
            std::make_shared<Session>(std::move(socket), *this, server.m_connections, server.m_requests)->start();
        }
        accept();
    });
}

HttpServer::HttpServer(HttpServerSettings settings, Handler handler) :
    m_impl(std::make_unique<Impl>(*this, std::move(settings), std::move(handler)))
{
}

HttpServer::~HttpServer()
{
    stop(2000);
}

bool HttpServer::bind(std::string &error)
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
    m_port = m_impl->acceptor.local_endpoint().port();
    return true;
}

void HttpServer::start()
{
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

bool HttpServer::stop(std::int64_t boundMs)
{
    m_accepting.store(false, std::memory_order_release);
    net::post(m_impl->acceptor.get_executor(), [impl = m_impl.get()] {
        beast::error_code ignored;
        impl->acceptor.close(ignored);
    });
    m_impl->ioc.stop();
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
            return false;
        }
    }
    for (std::thread &t : threads)
    {
        t.join();
    }
    return true;
}

}}
