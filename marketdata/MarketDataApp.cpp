#include "MarketDataApp.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/Environment.h"
#include "gateway/common/ProcessShell.h"

#include "BookCore.h"
#include "MarketDataSettings.h"
#include "MdWsServer.h"

// oe-marketdata - the public market data of the exchange: the order books from the ME's egress over one GATEWAY-class
// ME session of its own, served on a public WebSocket with no auth. Stateless on the book side (a restart starts with
// empty ladders that heal on the ME's next snapshot). Threads: main (the supervisor), md-duty (the ME session and
// BookCore: every ladder and subscription), md-io-<n> (the public sockets, client frames, writes), md-admin. Exit
// codes: ExitCodes.h. The service sends the ME no ingress but the session keepalive.

namespace openexchange { namespace gateway { namespace marketdata {

namespace {

namespace cl = cluster_tools::client;
namespace at = cluster_tools::client::aeron_transport;
namespace rt = cluster_tools::runtime;

using MeClient = cl::ClusterClient<at::AeronTransport>;

// Commands taken from the I/O side per duty cycle and egress fragments polled per cycle: batching steps, not capacities.
constexpr int COMMANDS_PER_CYCLE = 512;
constexpr int EGRESS_PER_CYCLE = 256;

std::int64_t wallClockMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

BookCoreSettings coreSettings(const MarketDataSettings &s)
{
    BookCoreSettings c;
    c.expectedMarkets = s.expectedMarkets;
    c.bounds.egressStaleMs = s.egressStaleMs;
    c.bounds.bookStaleMs = s.bookStaleMs;
    return c;
}

// The duty work: the ME session, BookCore and the command ring from the I/O side.
class MarketDataDuty final : public DutyWork
{
public:
    MarketDataDuty(const DutyContext &context, const MarketDataSettings &s, MdCommandRing &commands, MdStats &stats) :
        m_settings(s),
        m_commands(commands),
        m_log(context.log),
        m_transport(context.aeron),
        m_client(m_transport, meClientSettings(s)),
        m_core(coreSettings(s), stats, &wallClockMs)
    {
    }

    int doWork() override
    {
        const std::int64_t now = rt::Supervisor::monotonicNs();
        int work = 0;
        MdCommand c;
        while (work < COMMANDS_PER_CYCLE && m_commands.tryPop(c))
        {
            work++;
            switch (c.kind)
            {
                case MdCommand::Kind::OPEN: m_core.open(c.connId, std::move(c.outlet)); break;
                case MdCommand::Kind::SUBSCRIBE: m_core.subscribe(c.connId, c.marketId, now); break;
                case MdCommand::Kind::REFRESH: m_core.refresh(c.connId, c.marketId, now); break;
                case MdCommand::Kind::PING: m_core.ping(c.connId); break;
                case MdCommand::Kind::BAD: m_core.badRequest(c.connId, c.text); break;
                case MdCommand::Kind::RESYNC: m_core.resync(c.connId, now); break;
                case MdCommand::Kind::GONE: m_core.gone(c.connId); break;
                case MdCommand::Kind::NONE: break;
            }
            c = MdCommand{};
        }
        work += supervise(now);
        work += m_client.doWork();
        if (Link::UP == m_link)
        {
            work += m_client.poll(
                [this, now](const cl::EgressMessage &m) {
                    m_lastEgressNs = now;
                    m_haveEgress = true;
                    m_core.onEgress(m.payload, m.length, m.leadershipTermId, m_client.leadershipTermId(), now);
                },
                EGRESS_PER_CYCLE);
        }
        work += m_core.doWork(now);
        return work;
    }

    void fillStatus(Status &status) const override
    {
        m_core.fillStatus(status);
        status.connected = Link::UP == m_link;
        status.sessionEpoch = static_cast<std::int64_t>(m_client.sessionEpoch());
        status.leaderMemberId = Link::UP == m_link ? m_client.leaderMemberId() : -1;
    }

    int exitCode() const override { return 0; }
    void close() override { m_client.close(); }

private:
    enum class Link : std::uint8_t
    {
        DOWN,
        CONNECTING,
        UP
    };

    void log(rt::LogLevel level, const std::string &text)
    {
        if (nullptr != m_log)
        {
            m_log->push(level, rt::Supervisor::monotonicNs(), "md-duty: %s", text.c_str());
        }
    }

    void down(std::int64_t now, const std::string &why)
    {
        if (Link::UP == m_link)
        {
            m_core.onSessionEnd(now);
        }
        log(rt::LogLevel::WARN, "ME session down (" + why + "): reconnecting in " + std::to_string(m_settings.meReconnectMs) +
            " ms");
        m_link = Link::DOWN;
        m_nextConnectNs = now + m_settings.meReconnectMs * 1'000'000LL;
        m_haveEgress = false;
    }

    int supervise(std::int64_t now)
    {
        const cl::ClientState st = m_client.state();
        switch (m_link)
        {
            case Link::DOWN:
                if (now - m_nextConnectNs < 0)
                {
                    return 0;
                }
                if (!m_client.connect())
                {
                    down(now, "connect refused: " + m_client.lastDetail());
                    return 1;
                }
                m_link = Link::CONNECTING;
                return 1;
            case Link::CONNECTING:
                if (cl::ClientState::CONNECTED == st)
                {
                    m_link = Link::UP;
                    m_term = m_client.leadershipTermId();
                    m_haveEgress = false;
                    m_core.onSessionUp(now);
                    log(rt::LogLevel::INFO, "ME session " + std::to_string(m_client.clusterSessionId()) + " open, term " +
                        std::to_string(m_term) + ", leader " + std::to_string(m_client.leaderMemberId()));
                    return 1;
                }
                if (cl::ClientState::FAILED == st || cl::ClientState::CLOSED == st || cl::ClientState::IDLE == st)
                {
                    down(now, "connect failed: " + m_client.lastDetail());
                    return 1;
                }
                return 0;
            case Link::UP:
                break;
        }
        if (cl::ClientState::CLOSED == st || cl::ClientState::FAILED == st || cl::ClientState::IDLE == st)
        {
            down(now, std::string("closed: ") + cl::nameOf(m_client.closeReason()) + ": " + m_client.lastDetail());
            return 1;
        }
        if (m_client.leadershipTermId() != m_term)
        {
            // a new ME leader: the ladders reset and heal on its next snapshot
            log(rt::LogLevel::INFO, "ME new leader term " + std::to_string(m_client.leadershipTermId()) + " (was " +
                std::to_string(m_term) + "): every book waits for the next snapshot");
            m_term = m_client.leadershipTermId();
            m_core.onSessionEnd(now);
            m_core.onSessionUp(now);
            return 1;
        }
        if (m_haveEgress && now - m_lastEgressNs >= m_settings.egressStaleMs * 1'000'000LL)
        {
            m_client.close();
            down(now, "no egress for " + std::to_string(m_settings.egressStaleMs) + " ms");
            return 1;
        }
        return 0;
    }

    const MarketDataSettings &m_settings;
    MdCommandRing &m_commands;
    rt::LogRing *m_log;
    at::AeronTransport m_transport;
    MeClient m_client;
    BookCore m_core;
    Link m_link = Link::DOWN;
    std::int64_t m_nextConnectNs = 0;
    std::int64_t m_term = -1;
    std::int64_t m_lastEgressNs = 0;
    bool m_haveEgress = false;
};

// The public side: the WebSocket listener.
class MarketDataFront final : public FrontWork
{
public:
    MarketDataFront(const MarketDataSettings &s, MdCommandRing &commands, const MdStats &stats) :
        m_settings(s), m_stats(stats)
    {
        MdWsSettings ws;
        ws.host = s.common.publicBind.host;
        ws.port = s.common.publicBind.port;
        ws.ioThreads = static_cast<int>(s.common.ioThreads);
        ws.maxConnections = s.wsMaxConnections;
        ws.queueBytes = s.wsQueueBytes;
        ws.unwritableCloseMs = s.wsUnwritableCloseMs;
        ws.idleMs = s.wsIdleMs;
        ws.maxFrame = s.wsMaxFrame;
        ws.sendStallMs = s.sendStallMs;
        m_server = std::make_unique<MdWsServer>(ws, commands);
    }
    ~MarketDataFront() override { stop(m_settings.common.stopTimeoutMs); }

    bool bind(std::string &error) override { return m_server->bind(error); }
    void start() override { m_server->start(); }
    bool stop(std::int64_t boundMs) override { return m_server->stop(boundMs); }
    bool accepting() const override { return m_server->accepting(); }

    void fillStatus(Status &status) const override
    {
        status.wsListenerAccepting = m_server->accepting();
        status.mdClientsServed = m_server->clientsServed(rt::Supervisor::monotonicNs());
    }

    std::string healthJson() const override
    {
        const MdWsCounters &w = m_server->counters();
        std::string markets;
        for (const int id : m_settings.expectedMarkets)
        {
            markets += (markets.empty() ? "" : ",") + std::to_string(id);
        }
        return ",\"ws\":{\"accepting\":" + std::string(m_server->accepting() ? "true" : "false") + ",\"connections\":" +
            std::to_string(w.connections.load()) + ",\"refusedConnections\":" + std::to_string(w.refused.load()) +
            "},\"expectedMarkets\":[" + markets + "]";
    }

    std::string metrics(const std::string &p) const override
    {
        const MdWsCounters &w = m_server->counters();
        const MdStats &s = m_stats;
        std::string out;
        auto line = [&](const char *type, const std::string &name, const std::string &help, const std::string &value) {
            out += "# HELP " + p + name + " " + help + "\n# TYPE " + p + name + " " + type + "\n" + p + name + " " + value + "\n";
        };
        out += "# HELP " + p + "book_frames_total ME book frames the ladders took, by template.\n# TYPE " + p +
            "book_frames_total counter\n" + p + "book_frames_total{template=\"22\"} " + std::to_string(s.snapshots.load()) +
            "\n" + p + "book_frames_total{template=\"25\"} " + std::to_string(s.deltas.load()) + "\n";
        line("counter", "book_breaks_total", "Chain breaks of a ladder (the book is stale until the next snapshot).",
            std::to_string(s.breaks.load()));
        line("gauge", "books_stale", "Expected markets whose book is not fresh.", std::to_string(s.booksStale.load()));
        const std::int64_t last = s.lastEgressNs.load();
        line("gauge", "egress_age_ms", "Age of the last ME egress message (-1 = none yet).",
            last < 0 ? std::string("-1") : std::to_string((rt::Supervisor::monotonicNs() - last) / 1'000'000LL));
        out += "# HELP " + p + "egress_dropped_templates_total ME egress frames dropped unread after the message header, "
            "by template.\n# TYPE " + p + "egress_dropped_templates_total counter\n";
        for (std::size_t i = 0; i <= MdStats::DROPPED_TEMPLATES.size(); ++i)
        {
            out += p + "egress_dropped_templates_total{template=\"" +
                (i < MdStats::DROPPED_TEMPLATES.size() ? std::to_string(MdStats::DROPPED_TEMPLATES[i]) : std::string("other")) +
                "\"} " + std::to_string(s.dropped[i].load()) + "\n";
        }
        line("counter", "egress_schema_refused_total", "Order-schema frames of another version, refused undecoded.",
            std::to_string(s.schemaRefused.load()));
        line("counter", "egress_stale_term_total", "Book frames of an ended ME leader term, dropped.",
            std::to_string(s.staleTerm.load()));
        line("counter", "foreign_market_frames_total", "Book frames of a market outside MD_EXPECTED_MARKETS, dropped.",
            std::to_string(s.foreignMarket.load()));
        line("counter", "book_malformed_total", "Book frames without a readable market, dropped.",
            std::to_string(s.malformedNoMarket.load()));
        line("counter", "me_heartbeats_total", "ClusterHeartbeat frames of the ME.", std::to_string(s.heartbeats.load()));
        line("counter", "me_session_ends_total", "ME session ends and leader changes (every ladder reset).",
            std::to_string(s.sessionsEnded.load()));
        line("gauge", "ws_connections", "WebSocket connections held.", std::to_string(w.connections.load()));
        line("counter", "ws_refused_total", "Connections closed at accept above MD_WS_MAX_CONNECTIONS.",
            std::to_string(w.refused.load()));
        line("counter", "ws_ops_refused_total", "Client actions dropped because the command ring was full.",
            std::to_string(w.opsRefused.load()));
        line("counter", "ws_resyncs_total", "Fresh initial states sent after a queue bound.", std::to_string(s.resyncs.load()));
        line("counter", "ws_queue_drops_total", "Client queues dropped at MD_WS_QUEUE_BYTES.",
            std::to_string(w.queueDrops.load()));
        line("counter", "ws_closed_slow_total", "Connections closed after MD_WS_UNWRITABLE_CLOSE_MS without a finished "
            "write.", std::to_string(w.closedSlow.load()));
        out += "# HELP " + p + "ws_frames_sent_total Frames written to clients, by type.\n# TYPE " + p +
            "ws_frames_sent_total counter\n";
        for (std::size_t i = 0; i < static_cast<std::size_t>(FrameType::COUNT); ++i)
        {
            out += p + "ws_frames_sent_total{type=\"" + nameOf(static_cast<FrameType>(i)) + "\"} " +
                std::to_string(w.sent[i].load()) + "\n";
        }
        return out;
    }

private:
    const MarketDataSettings &m_settings;
    const MdStats &m_stats;
    std::unique_ptr<MdWsServer> m_server;
};

}

int marketDataMain()
{
    const SystemEnvironment env;
    ParseResult<MarketDataSettings> parsed = parseMarketDataSettings(env);
    if (parsed.errors.empty())
    {
        // the session client's own checks, in the same pass as the settings
        if (const std::string e = cl::validate(meClientSettings(parsed.settings)); !e.empty())
        {
            parsed.errors.push_back("invalid setting: MD_ME_CLUSTER_ADDRESSES: the ME session refuses these settings: " + e);
        }
    }
    if (!parsed.ok())
    {
        return refuseConfiguration("oe-marketdata", parsed.errors);
    }
    const MarketDataSettings &s = parsed.settings;
    ProcessIdentity identity;
    identity.role = Role::MARKETDATA;
    identity.processName = "oe-marketdata";
    identity.threadPrefix = "md";
    identity.dirPrefix = "oe-marketdata-";
    identity.instanceKey = "MD_INSTANCE_ID";
    identity.adminBindKey = "MD_ADMIN_BIND";
    identity.frontBindKey = "MD_PUBLIC_BIND";
    ProcessInputs inputs;
    inputs.common = &s.common;
    inputs.records = &parsed.records;
    inputs.marketDataBounds.egressStaleMs = s.egressStaleMs;
    inputs.marketDataBounds.bookStaleMs = s.bookStaleMs;
    // live until the process exits (runProcess does not return)
    MdCommandRing commands(static_cast<std::size_t>(s.common.commandRing));
    MdStats stats;
    inputs.makeDuty = [&s, &commands, &stats](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return std::make_unique<MarketDataDuty>(context, s, commands, stats);
    };
    inputs.makeFront = [&s, &commands, &stats]() -> std::unique_ptr<FrontWork> {
        return std::make_unique<MarketDataFront>(s, commands, stats);
    };
    runProcess(identity, inputs);
}

}}}
