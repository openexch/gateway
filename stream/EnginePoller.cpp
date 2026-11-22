#include "EnginePoller.h"

#include <pthread.h>

#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <system_error>
#include <utility>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include "gateway/common/FrontLog.h"

namespace openexchange { namespace gateway { namespace stream {

namespace net = boost::asio;
namespace http = boost::beast::http;
using tcp = net::ip::tcp;
using Clock = std::chrono::steady_clock;

// One name lookup, shared by its lookup thread and the round that waits for it. The thread holds only this (and copies
// of the name), never the poller: a round that stops waiting, or a poller that is destroyed, leaves it nothing to free.
struct Lookup
{
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    std::vector<tcp::endpoint> endpoints;
    std::string error;
};

namespace node_error = engine::node_error;

struct NodeAddress
{
    engine::NodeEndpointSpec spec;
    bool literal = false;                 // an IP literal: parsed once, never looked up
    std::vector<tcp::endpoint> endpoints; // the address the requests use (empty: not known yet)
    std::shared_ptr<Lookup> lookup;       // a lookup still running (at most one per node)
    std::string error;                    // why the address is not known this round
    const char *code = nullptr;           // its node_error code
};

namespace {

std::shared_ptr<Lookup> startLookup(const std::string &host, std::uint16_t port)
{
    auto job = std::make_shared<Lookup>();
    try
    {
        std::thread([job, host, port] {
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            addrinfo *found = nullptr;
            const int rc = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &found);
            std::vector<tcp::endpoint> endpoints;
            std::string error;
            if (0 != rc)
            {
                error = "resolve " + host + ": " + ::gai_strerror(rc);
            }
            else
            {
                for (const addrinfo *a = found; nullptr != a; a = a->ai_next)
                {
                    if ((AF_INET == a->ai_family || AF_INET6 == a->ai_family) &&
                        a->ai_addrlen <= sizeof(sockaddr_storage))
                    {
                        tcp::endpoint e;
                        std::memcpy(e.data(), a->ai_addr, a->ai_addrlen);
                        e.resize(a->ai_addrlen);
                        endpoints.push_back(e);
                    }
                }
                ::freeaddrinfo(found);
                if (endpoints.empty())
                {
                    error = "resolve " + host + ": no IPv4 or IPv6 address";
                }
            }
            {
                std::lock_guard<std::mutex> lock(job->mutex);
                job->endpoints = std::move(endpoints);
                job->error = std::move(error);
                job->done = true;
            }
            job->cv.notify_all();
        }).detach();
    }
    catch (const std::system_error &e)
    {
        std::lock_guard<std::mutex> lock(job->mutex);
        job->error = std::string("resolve ") + host + ": no lookup thread (" + e.what() + ")";
        job->done = true;
    }
    return job;
}

std::unique_ptr<NodeAddress> makeAddress(const engine::NodeEndpointSpec &spec)
{
    auto a = std::make_unique<NodeAddress>();
    a->spec = spec;
    boost::system::error_code ec;
    const net::ip::address ip = net::ip::make_address(spec.host, ec);
    if (!ec)
    {
        a->literal = true;
        a->endpoints.emplace_back(ip, spec.port);
    }
    return a;
}

// Makes the node's address known for this round, waiting for a lookup no longer than `deadline`.
void prepare(NodeAddress &a, Clock::time_point deadline, std::int64_t timeoutMs)
{
    a.error.clear();
    a.code = nullptr;
    if (a.literal || !a.endpoints.empty())
    {
        return;
    }
    if (!a.lookup)
    {
        a.lookup = startLookup(a.spec.host, a.spec.port);
    }
    std::unique_lock<std::mutex> lock(a.lookup->mutex);
    if (!a.lookup->cv.wait_until(lock, deadline, [&a] { return a.lookup->done; }))
    {
        a.error = "name lookup of " + a.spec.host + " still running after " + std::to_string(timeoutMs) + " ms";
        a.code = node_error::TIMEOUT;
        return; // the same lookup is waited for again next round; no second one is started
    }
    a.endpoints = std::move(a.lookup->endpoints);
    a.error = a.lookup->error;
    a.code = a.error.empty() ? nullptr : node_error::UNREACHABLE;
    lock.unlock();
    a.lookup.reset();
}

// The node_error code of a request that got no answer.
const char *failureCode(bool timedOut, const boost::system::error_code &ec)
{
    if (timedOut || net::error::timed_out == ec)
    {
        return node_error::TIMEOUT;
    }
    if (net::error::connection_refused == ec)
    {
        return node_error::REFUSED;
    }
    if (ec.category() == http::make_error_code(http::error::body_limit).category())
    {
        return node_error::BAD_ANSWER; // over the body or header bound, or not HTTP
    }
    return node_error::UNREACHABLE;
}

std::string hostHeader(const engine::NodeEndpointSpec &node)
{
    const bool v6 = node.host.find(':') != std::string::npos;
    return (v6 ? "[" + node.host + "]" : node.host) + ":" + std::to_string(node.port);
}

// One GET to a known address: connect, write, read, under one deadline timer that cancels whatever is pending.
class Fetch : public std::enable_shared_from_this<Fetch>
{
public:
    Fetch(net::io_context &ioc, const NodeAddress &node, std::string target, Clock::time_point deadline,
        std::int64_t timeoutMs, std::int64_t maxBodyBytes, HttpAnswer &answer) :
        m_socket(ioc), m_timer(ioc), m_endpoints(node.endpoints), m_host(hostHeader(node.spec)),
        m_target(std::move(target)), m_deadline(deadline), m_timeoutMs(timeoutMs), m_answer(answer)
    {
        m_parser.body_limit(static_cast<std::uint64_t>(maxBodyBytes));
        m_parser.header_limit(16 * 1024); // a node's probe answer carries a few headers (a parsing step, not a setting)
    }

    void run()
    {
        auto self = shared_from_this();
        m_timer.expires_at(m_deadline);
        m_timer.async_wait([self](const boost::system::error_code &ec) {
            if (!ec && !self->m_done)
            {
                self->m_timedOut = true;
                boost::system::error_code ignored;
                self->m_socket.close(ignored);
            }
        });
        net::async_connect(m_socket, m_endpoints, [self](const boost::system::error_code &ec2, const tcp::endpoint &) {
            if (ec2)
            {
                return self->finish("connect", ec2);
            }
            self->m_request = http::request<http::empty_body>(http::verb::get, self->m_target, 11);
            self->m_request.set(http::field::host, self->m_host);
            self->m_request.set(http::field::connection, "close");
            http::async_write(self->m_socket, self->m_request, [self](const boost::system::error_code &ec3, std::size_t) {
                if (ec3)
                {
                    return self->finish("write", ec3);
                }
                http::async_read(self->m_socket, self->m_buffer, self->m_parser,
                    [self](const boost::system::error_code &ec4, std::size_t n) {
                        if (ec4)
                        {
                            return self->finish("read", ec4);
                        }
                        self->m_answer.answered = true;
                        self->m_answer.status = static_cast<int>(self->m_parser.get().result_int());
                        self->m_answer.body = std::move(self->m_parser.get().body());
                        self->m_answer.bytes = n;
                        self->finish(nullptr, {});
                    });
            });
        });
    }

private:
    void finish(const char *step, const boost::system::error_code &ec)
    {
        if (m_done)
        {
            return;
        }
        m_done = true;
        m_timer.cancel();
        boost::system::error_code ignored;
        m_socket.shutdown(tcp::socket::shutdown_both, ignored);
        m_socket.close(ignored);
        if (nullptr != step)
        {
            m_answer.answered = false;
            m_answer.error = m_timedOut ? "no answer within " + std::to_string(m_timeoutMs) + " ms"
                                        : std::string(step) + ": " + ec.message();
            m_answer.code = failureCode(m_timedOut, ec);
        }
    }

    tcp::socket m_socket;
    net::steady_timer m_timer;
    std::vector<tcp::endpoint> m_endpoints;
    std::string m_host;
    std::string m_target;
    Clock::time_point m_deadline;
    std::int64_t m_timeoutMs;
    HttpAnswer &m_answer;
    http::request<http::empty_body> m_request;
    boost::beast::flat_buffer m_buffer;
    http::response_parser<http::string_body> m_parser;
    bool m_done = false;
    bool m_timedOut = false;
};

// Every request of one round: the addresses first (lookups waited for until the deadline), then every GET at once,
// all under the same deadline.
std::vector<HttpAnswer> fetchAll(const std::vector<std::pair<NodeAddress *, std::string>> &requests,
    std::int64_t timeoutMs, std::int64_t maxBodyBytes)
{
    std::vector<HttpAnswer> answers(requests.size());
    if (requests.empty())
    {
        return answers;
    }
    const Clock::time_point deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    std::vector<NodeAddress *> prepared; // a node has two requests: its address is made known once per round
    for (const auto &[address, target] : requests)
    {
        if (std::find(prepared.begin(), prepared.end(), address) == prepared.end())
        {
            prepared.push_back(address);
            prepare(*address, deadline, timeoutMs);
        }
    }
    net::io_context ioc;
    for (std::size_t i = 0; i < requests.size(); ++i)
    {
        NodeAddress &a = *requests[i].first;
        if (a.endpoints.empty())
        {
            answers[i].error = a.error.empty() ? "no address for " + a.spec.host : a.error;
            answers[i].code = nullptr != a.code ? a.code : node_error::UNREACHABLE;
            continue;
        }
        std::make_shared<Fetch>(ioc, a, requests[i].second, deadline, timeoutMs, maxBodyBytes, answers[i])->run();
    }
    ioc.run(); // returns when every fetch finished: the round's deadline bounds each one
    return answers;
}

std::int64_t wallMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::int64_t steadyMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
}

}

std::vector<HttpAnswer> httpGetAll(const std::vector<std::pair<engine::NodeEndpointSpec, std::string>> &requests,
    std::int64_t timeoutMs, std::int64_t maxBodyBytes)
{
    std::vector<std::unique_ptr<NodeAddress>> addresses;
    std::vector<std::pair<NodeAddress *, std::string>> plan;
    for (const auto &[node, target] : requests)
    {
        addresses.push_back(makeAddress(node));
        plan.emplace_back(addresses.back().get(), target);
    }
    return fetchAll(plan, timeoutMs, maxBodyBytes);
}

EnginePoller::EnginePoller(EnginePollerSettings settings, EngineRing &out, EngineStats &stats) :
    m_settings(std::move(settings)), m_out(out), m_stats(stats),
    m_core(engine::Flavor::CORE, m_settings.coreNodes), m_me(engine::Flavor::MATCHING_ENGINE, m_settings.meNodes),
    m_gate(m_settings.frameMinMs)
{
    for (const auto *nodes : { &m_settings.coreNodes, &m_settings.meNodes })
    {
        for (const engine::NodeEndpointSpec &n : *nodes)
        {
            m_addresses.push_back(makeAddress(n));
        }
    }
    m_loggedDetail.resize(m_addresses.size());
}

EnginePoller::~EnginePoller()
{
    stop(2000);
}

void EnginePoller::start()
{
    // the frame before any poll: every configured node with null values, so a connection gets an engine frame at once
    if (auto first = m_gate.offer(engine::engineFrame(m_core, m_me), steadyMs()))
    {
        m_stats.lastFrameBytes.store(static_cast<std::int64_t>(first->size()), std::memory_order_relaxed);
        m_stats.frames.fetch_add(1, std::memory_order_relaxed);
        hand(EngineOut{ EngineOut::Kind::STATUS, std::make_shared<const std::string>(std::move(*first)) });
    }
    if (m_settings.coreNodes.empty() && m_settings.meNodes.empty())
    {
        return; // nothing to poll: the first frame (all null) stays the only one
    }
    m_thread = std::thread([this] {
        const std::string name = m_settings.threadPrefix + "-engine";
        pthread_setname_np(pthread_self(), name.c_str());
        run();
    });
}

bool EnginePoller::stop(std::int64_t boundMs)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_stop = true;
    }
    m_cv.notify_all();
    if (!m_thread.joinable())
    {
        return true;
    }
    // never detached: the thread uses this object. A round ends by its deadline (a name lookup still running is left to
    // its own thread), so the join is bounded by the request timeout.
    const auto started = Clock::now();
    m_thread.join();
    return Clock::now() - started <= std::chrono::milliseconds(boundMs + m_settings.timeoutMs);
}

void EnginePoller::run()
{
    for (;;)
    {
        const auto started = Clock::now();
        roundOnce(wallMs(), steadyMs());
        m_stats.lastRoundMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started).count(),
            std::memory_order_relaxed);
        const auto nextRound = started + std::chrono::milliseconds(m_settings.pollMs);
        for (;;)
        {
            // wake for the next round, or earlier when the rate cap lets a held frame out
            auto wakeAt = nextRound;
            if (const std::optional<std::int64_t> due = m_gate.heldDueMs())
            {
                wakeAt = std::min(wakeAt, Clock::time_point(std::chrono::milliseconds(*due)));
            }
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                if (m_cv.wait_until(lock, wakeAt, [this] { return m_stop; }))
                {
                    return;
                }
            }
            if (Clock::now() >= nextRound)
            {
                break;
            }
            releaseHeld(steadyMs());
        }
    }
}

void EnginePoller::releaseHeld(std::int64_t nowSteadyMs)
{
    if (auto frame = m_gate.poll(nowSteadyMs))
    {
        m_stats.lastFrameBytes.store(static_cast<std::int64_t>(frame->size()), std::memory_order_relaxed);
        m_stats.frames.fetch_add(1, std::memory_order_relaxed);
        hand(EngineOut{ EngineOut::Kind::STATUS, std::make_shared<const std::string>(std::move(*frame)) });
    }
    flush();
}

void EnginePoller::roundOnce(std::int64_t nowWallMs, std::int64_t nowSteadyMs)
{
    std::vector<std::pair<NodeAddress *, std::string>> requests;
    for (const std::unique_ptr<NodeAddress> &a : m_addresses)
    {
        requests.emplace_back(a.get(), "/health");
        requests.emplace_back(a.get(), "/metrics");
    }
    const std::vector<HttpAnswer> answers = fetchAll(requests, m_settings.timeoutMs, m_settings.maxBodyBytes);
    for (std::size_t i = 0; i < m_addresses.size(); ++i)
    {
        NodeAddress &a = *m_addresses[i];
        if (!a.literal && !answers[2 * i].answered && !answers[2 * i + 1].answered)
        {
            a.endpoints.clear(); // no answer at that address: look the name up again next round (the node may have moved)
        }
    }
    m_stats.rounds.fetch_add(1, std::memory_order_relaxed);
    m_stats.requests.fetch_add(answers.size(), std::memory_order_relaxed);
    std::size_t at = 0;
    auto samples = [&](std::size_t count) {
        std::vector<engine::NodeSample> round(count);
        for (engine::NodeSample &s : round)
        {
            const HttpAnswer &h = answers[at++];
            const HttpAnswer &m = answers[at++];
            for (const HttpAnswer *a : { &h, &m })
            {
                m_stats.bytesRead.fetch_add(a->bytes, std::memory_order_relaxed);
                if (!a->answered)
                {
                    m_stats.requestFailures.fetch_add(1, std::memory_order_relaxed);
                }
            }
            s.healthAnswered = h.answered;
            s.healthStatus = h.status;
            s.healthBody = h.body;
            s.metricsAnswered = m.answered;
            s.metricsStatus = m.status;
            s.metricsBody = m.body;
            const HttpAnswer &failed = !h.error.empty() ? h : m;
            s.error = failed.error;
            s.failure = failed.code;
        }
        return round;
    };
    const std::vector<engine::NodeSample> coreRound = samples(m_settings.coreNodes.size());
    const std::vector<engine::NodeSample> meRound = samples(m_settings.meNodes.size());
    for (auto &[model, round] : { std::pair<engine::ClusterModel *, const std::vector<engine::NodeSample> *>{ &m_core, &coreRound },
             std::pair<engine::ClusterModel *, const std::vector<engine::NodeSample> *>{ &m_me, &meRound } })
    {
        for (const engine::Election &e : model->ingest(*round, nowWallMs))
        {
            m_stats.electionEvents.fetch_add(1, std::memory_order_relaxed);
            hand(EngineOut{ EngineOut::Kind::EVENT,
                std::make_shared<const std::string>(engine::electionFrame(model->key(), e)) });
        }
    }
    logReasons();
    const std::uint64_t unchangedBefore = m_gate.suppressedUnchanged();
    if (auto frame = m_gate.offer(engine::engineFrame(m_core, m_me), nowSteadyMs))
    {
        m_stats.lastFrameBytes.store(static_cast<std::int64_t>(frame->size()), std::memory_order_relaxed);
        m_stats.frames.fetch_add(1, std::memory_order_relaxed);
        hand(EngineOut{ EngineOut::Kind::STATUS, std::make_shared<const std::string>(std::move(*frame)) });
    }
    m_stats.unchanged.fetch_add(m_gate.suppressedUnchanged() - unchangedBefore, std::memory_order_relaxed);
    flush();
}

void EnginePoller::logReasons()
{
    std::size_t at = 0;
    for (const engine::ClusterModel *model : { &m_core, &m_me })
    {
        for (const engine::NodeView &n : model->nodes())
        {
            if (at >= m_loggedDetail.size())
            {
                return;
            }
            std::string &last = m_loggedDetail[at++];
            const std::string now = n.errorDetail.value_or("");
            if (now == last)
            {
                continue;
            }
            if (now.empty())
            {
                frontLog(FrontLevel::INFO, "engine status: %s node %lld answers again", model->key(),
                    static_cast<long long>(n.id));
            }
            else
            {
                frontLog(FrontLevel::WARN, "engine status: %s node %lld %s: %s", model->key(),
                    static_cast<long long>(n.id), n.error.value_or("").c_str(), now.c_str());
            }
            last = now;
        }
    }
}

void EnginePoller::hand(EngineOut out)
{
    if (EngineOut::Kind::STATUS == out.kind)
    {
        for (auto it = m_pending.begin(); it != m_pending.end(); ++it)
        {
            if (EngineOut::Kind::STATUS == it->kind)
            {
                m_pending.erase(it); // a newer engine frame replaces one the ring could not take yet
                break;
            }
        }
    }
    m_pending.push_back(std::move(out));
    flush();
}

void EnginePoller::flush()
{
    while (!m_pending.empty())
    {
        EngineOut next = m_pending.front();
        if (!m_out.tryPush(std::move(next)))
        {
            return; // the duty thread takes them every cycle; the rest go with the next round
        }
        m_pending.pop_front();
    }
}

}}}
