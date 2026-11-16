#ifndef GATEWAY_COMMON_HTTP_SERVER_H
#define GATEWAY_COMMON_HTTP_SERVER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/ip/address.hpp>

// HttpServer - the public HTTP/1.1 listener of oe-gateway on Boost.Beast over one Asio reactor run by
// GATEWAY_IO_THREADS threads (<prefix>-io-<n>). The I/O threads parse, call the handler and write; they never call
// the cluster, never hash a password and never wait on PostgreSQL: a handler hands slow work to its pool and answers
// later, from any thread, through HttpExchange::respond (posted back to the connection's strand).
//
// Bounds (settings, none fixed in code): the body (GATEWAY_MAX_BODY_BYTES: above it 413), the header block
// (GATEWAY_MAX_HEADER_BYTES: above it 431), the connections held (GATEWAY_MAX_CONNECTIONS: a connection above the
// bound is closed at accept and counted), and the idle time of a connection between and inside requests
// (GATEWAY_HTTP_IDLE_TIMEOUT_MS: a slow or silent client is closed). One request at a time per connection; keep-alive
// as the client asks. Every response carries Cache-Control: no-store.

namespace openexchange { namespace gateway {

struct ApiResponse
{
    unsigned status = 200;
    std::string body;                                        // JSON (or empty for 204)
    std::vector<std::pair<std::string, std::string>> headers; // extra headers (Retry-After, CORS)
};

class HttpExchange
{
public:
    virtual ~HttpExchange() = default;
    virtual std::string_view method() const = 0;
    virtual std::string_view target() const = 0;  // as received: path and query
    virtual std::string_view path() const = 0;    // up to '?'
    virtual std::string_view query() const = 0;   // after '?' ("" when none)
    virtual std::optional<std::string_view> header(std::string_view name) const = 0; // the first occurrence
    virtual std::vector<std::string_view> headers(std::string_view name) const = 0;  // every occurrence, in order
    virtual std::size_t headerCount(std::string_view name) const = 0;
    virtual std::string_view body() const = 0;
    virtual const boost::asio::ip::address &peer() const = 0;
    virtual std::int64_t receivedNs() const = 0;
    // Exactly once, from any thread; later calls are ignored.
    virtual void respond(ApiResponse response) = 0;
    // Runs `work` on the connection's strand (an I/O thread). Used only by the test-only control that puts hashing
    // on the I/O thread; nothing slow is ever posted here otherwise.
    virtual void runOnIo(std::function<void()> work) = 0;
};

struct HttpServerSettings
{
    std::string host;
    std::uint16_t port = 0;
    int ioThreads = 2;
    std::size_t maxBodyBytes = 65'536;
    std::size_t maxHeaderBytes = 8192;
    std::int64_t idleTimeoutMs = 60'000;
    std::int64_t maxConnections = 2000;
    std::string threadPrefix = "gw";
};

class HttpServer
{
public:
    using Handler = std::function<void(std::shared_ptr<HttpExchange>)>;

    HttpServer(HttpServerSettings settings, Handler handler);
    ~HttpServer();
    HttpServer(const HttpServer &) = delete;
    HttpServer &operator=(const HttpServer &) = delete;

    // Bind and listen (port 0 = ephemeral). False with `error` (the caller exits 78).
    bool bind(std::string &error);
    std::uint16_t boundPort() const noexcept { return m_port; }
    // Starts the I/O threads; from here the listener accepts.
    void start();
    // Closes the listener and every connection, joins the I/O threads inside `boundMs`; false when one did not stop.
    bool stop(std::int64_t boundMs);

    bool accepting() const noexcept { return m_accepting.load(std::memory_order_acquire); }
    std::int64_t connections() const noexcept { return m_connections.load(std::memory_order_relaxed); }
    std::uint64_t refusedConnections() const noexcept { return m_refused.load(std::memory_order_relaxed); }
    std::uint64_t requests() const noexcept { return m_requests.load(std::memory_order_relaxed); }

    struct Impl;

private:
    std::unique_ptr<Impl> m_impl;
    std::uint16_t m_port = 0;
    std::atomic<bool> m_accepting{ false };
    std::atomic<std::int64_t> m_connections{ 0 };
    std::atomic<std::uint64_t> m_refused{ 0 };
    std::atomic<std::uint64_t> m_requests{ 0 };
};

}}

#endif
