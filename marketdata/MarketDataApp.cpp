#include "MarketDataApp.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/Admin.h"
#include "gateway/common/Environment.h"
#include "gateway/common/ProcessShell.h"

#include "BookCore.h"
#include "CandleRoute.h"
#include "HistoryWriter.h"
#include "JournalFollow.h"
#include "MarketDataSettings.h"
#include "MdWsServer.h"
#include "TradeCore.h"

// oe-marketdata - the public market data of the exchange: the order books from the ME's egress over one GATEWAY-class
// ME session of its own, served on a public WebSocket with no auth, and the trade path: the ME journal's trades read
// through the members' archives (JournalFollow) into the market history database (HistoryWriter). Stateless on the
// book side (a restart starts with empty ladders that heal on the ME's next snapshot); the trade path's only cursor is
// md_state.last_journal_seq. The tape, the 1m candles and the tickers are memory state (TradeCore), started from the
// relations and moved by the live trades; GET /api/candles answers from the relations only (CandleRoute). Threads: main
// (the supervisor), md-duty (the ME session, BookCore: every ladder and subscription, TradeCore and the journal
// follower), md-pg (the market history writer), md-rest (the candle route's reads), md-io-<n> (the public sockets,
// client frames, writes), md-admin. Exit codes: ExitCodes.h (75 also: the market history writer lock is held, or the market
// history has another writer). The service sends the ME no ingress but the session keepalive.

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

std::int64_t monoNs()
{
    return rt::Supervisor::monotonicNs();
}

TradeCoreSettings tradeCoreSettings(const MarketDataSettings &s)
{
    TradeCoreSettings t;
    t.expectedMarkets = s.expectedMarkets;
    t.recentTrades = static_cast<std::size_t>(s.recentTrades);
    t.candleHistory = static_cast<std::size_t>(s.candleHistory);
    t.flushMs = s.tradeFlushMs;
    t.tickersIntervalMs = s.tickersIntervalMs;
    return t;
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
    MarketDataDuty(const DutyContext &context, const MarketDataSettings &s, MdCommandRing &commands, MdStats &stats,
        TradeShared &trades) :
        m_settings(s),
        m_commands(commands),
        m_log(context.log),
        m_trades(trades),
        m_transport(context.aeron),
        m_client(m_transport, meClientSettings(s)),
        m_core(coreSettings(s), stats, &wallClockMs),
        m_tradeCore(tradeCoreSettings(s), &wallClockMs, &monoNs),
        m_follow(context, s, trades, [this] { m_client.doWork(); },
            [&stats] { return stats.schemaRefused.load(std::memory_order_relaxed); }, &m_tradeCore)
    {
        m_core.attachTrades(&m_tradeCore);
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
                case MdCommand::Kind::LIMITED: m_core.rateLimited(c.connId, c.text); break;
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
        work += m_follow.doWork(now);
        return work;
    }

    void fillStatus(Status &status) const override
    {
        m_core.fillStatus(status);
        status.connected = Link::UP == m_link;
        status.sessionEpoch = static_cast<std::int64_t>(m_client.sessionEpoch());
        status.leaderMemberId = Link::UP == m_link ? m_client.leaderMemberId() : -1;
        m_follow.fillStatus(status, rt::Supervisor::monotonicNs());
    }

    int exitCode() const override { return m_trades.ledger.fatalExit.load(std::memory_order_acquire); }
    void close() override
    {
        m_follow.close();
        m_client.close();
    }

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
    TradeShared &m_trades;
    at::AeronTransport m_transport;
    MeClient m_client;
    BookCore m_core;
    TradeCore m_tradeCore;
    JournalFollow m_follow;
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
    MarketDataFront(const MarketDataSettings &s, MdCommandRing &commands, MdStats &stats, TradeShared &trades,
        HistoryWriter &writer) :
        m_settings(s), m_stats(stats), m_trades(trades), m_writer(writer)
    {
        CandleRouteSettings rest;
        rest.pg.url = s.pgUrl;
        rest.pg.user = s.pgUser;
        rest.pg.passwordFile = s.pgPasswordFile;
        rest.pg.statementTimeoutMs = s.pgStatementTimeoutMs;
        rest.corsOrigins = s.corsOrigins;
        rest.expectedMarkets = s.expectedMarkets;
        rest.limitMax = s.candlesLimitMax;
        rest.queue = static_cast<std::size_t>(s.candlesQueue);
        m_candles = std::make_unique<CandleRoute>(rest);
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
        ws.connectRate = s.connectRate;
        ws.connectBurst = s.connectBurst;
        ws.messageRate = s.messageRate;
        ws.messageBurst = s.messageBurst;
        ws.trustedProxies = s.trustedProxies;
        ws.clientIpHeader = s.clientIpHeader;
        ws.sourceV6PrefixBits = static_cast<int>(s.sourceV6PrefixBits);
        m_server = std::make_unique<MdWsServer>(ws, commands, m_candles.get(), &stats.inputMarkNs);
    }
    ~MarketDataFront() override { stop(m_settings.common.stopTimeoutMs); }

    bool bind(std::string &error) override { return m_server->bind(error); }
    void start() override
    {
        m_writer.start();
        m_candles->start();
        m_server->start();
    }
    bool stop(std::int64_t boundMs) override
    {
        const bool ws = m_server->stop(boundMs);
        const bool rest = m_candles->stop(boundMs);
        return m_writer.stop(boundMs) && ws && rest;
    }
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
            "},\"expectedMarkets\":[" + markets + "]" + tradesJson();
    }

    // the trade path: the halt by name, the two cursors, the sources and the market history writer
    std::string tradesJson() const
    {
        const TradeView v = m_trades.view.get();
        const TradeLedger &l = m_trades.ledger;
        auto q = [](const std::string &t) { return "\"" + jsonEscape(t) + "\""; };
        auto n = [](std::uint64_t x) { return std::to_string(x); };
        std::string halts;
        for (std::size_t i = 1; i < v.halts.size(); ++i)
        {
            if (v.halts[i])
            {
                halts += (halts.empty() ? "" : ",") + q(nameOf(static_cast<MdHalt>(i)));
            }
        }
        std::string marks;
        for (std::size_t i = 0; i < v.marks.size(); ++i)
        {
            marks += (i ? "," : "") + q(v.marks[i]);
        }
        const std::int64_t oldest = l.oldestReadAtNs();
        return ",\"trades\":{\"started\":" + std::string(v.started ? "true" : "false") + ",\"halt\":" +
            (MdHalt::NONE == v.halt ? std::string("null") : q(nameOf(v.halt))) + ",\"haltDetail\":" +
            (MdHalt::NONE == v.halt ? std::string("null") : q(v.haltDetail)) + ",\"halts\":[" + halts + "]" +
            ",\"epoch\":" + (v.epoch ? std::to_string(*v.epoch) : std::string("null")) + ",\"committedSeq\":" +
            std::to_string(l.committedSeq()) + ",\"readSeq\":" + std::to_string(v.next > 0 ? v.next - 1 : 0) +
            ",\"liveEmitSeq\":" + std::to_string(v.liveEmit) + ",\"liveEmitted\":" + n(v.liveEmitted) +
            ",\"firstSeq\":" + std::to_string(l.firstSeq.load()) + ",\"partial\":" + (l.partial.load() ? "true" : "false") +
            ",\"pending\":" + std::to_string(l.pending()) + ",\"pendingAgeMs\":" +
            (oldest < 0 ? std::string("0") : std::to_string((rt::Supervisor::monotonicNs() - oldest) / 1'000'000LL)) +
            ",\"source\":" + (v.source ? std::to_string(*v.source) : std::string("null")) + ",\"phase\":" + q(v.phase) +
            ",\"attached\":" + (v.attached ? "true" : "false") + ",\"fresh\":" + (v.fresh ? "true" : "false") +
            ",\"sources\":[" + marks + "],\"counts\":{\"switches\":" + n(v.switches) + ",\"holes\":" + n(v.holes) +
            ",\"belowRange\":" + n(v.belowRange) + ",\"refusals\":" + n(v.refusals) + ",\"reReads\":" + n(v.reReads) +
            ",\"rescans\":" + n(v.rescans) + ",\"pausedFull\":" + n(v.pausedFull) + "}" +
            ",\"history\":{\"connected\":" + (l.connected.load() ? "true" : "false") + ",\"writable\":" +
            (l.writable.load() ? "true" : "false") + ",\"commits\":" + n(l.commits.load()) + ",\"failures\":" +
            n(l.failures.load()) + ",\"recoveries\":" + n(l.recoveries.load()) + ",\"lastError\":" + q(l.lastError()) +
            "}}";
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
        line("gauge", "ws_queue_high_water_bytes", "The most bytes one connection held queued (bounded by "
            "MD_WS_QUEUE_BYTES).", std::to_string(w.queueHighWater.load()));
        out += "# HELP " + p + "rate_limited_total Public requests and client frames refused by a source budget, by "
            "budget.\n# TYPE " + p + "rate_limited_total counter\n" + p + "rate_limited_total{budget=\"connect\"} " +
            std::to_string(w.limitedConnects.load()) + "\n" + p + "rate_limited_total{budget=\"message\"} " +
            std::to_string(w.limitedMessages.load()) + "\n";
        line("counter", "ws_closed_slow_total", "Connections closed after MD_WS_UNWRITABLE_CLOSE_MS without a finished "
            "write.", std::to_string(w.closedSlow.load()));
        // the trade path. journal_seq is the committed cursor md_state.last_journal_seq and nothing else (the console's
        // journal retention reads it): the read position and the live-emit cursor run ahead of it
        {
            const TradeView v = m_trades.view.get();
            const TradeLedger &l = m_trades.ledger;
            line("gauge", "journal_seq", "md_state.last_journal_seq: every ME journal record up to it is stored.",
                std::to_string(l.committedSeq()));
            line("counter", "trades_committed_total", "Trades committed to the market history.",
                std::to_string(l.tradesCommitted.load()));
            line("gauge", "pending_trades", "Journal records read and not yet committed.", std::to_string(l.pending()));
            const std::int64_t oldest = l.oldestReadAtNs();
            line("gauge", "history_age_ms", "Age of the oldest record read and not yet committed (0 = none).",
                oldest < 0 ? std::string("0") : std::to_string((rt::Supervisor::monotonicNs() - oldest) / 1'000'000LL));
            line("gauge", "history_partial", "1 when the market history starts above the journal's first record.",
                l.partial.load() ? "1" : "0");
            line("counter", "history_recoveries_total", "Times the market history became writable again after a failure.",
                std::to_string(l.recoveries.load()));
            out += "# HELP " + p + "halt The trade path's halts, by kind (1 = set).\n# TYPE " + p + "halt gauge\n";
            for (std::size_t i = 1; i < v.halts.size(); ++i)
            {
                out += p + "halt{kind=\"" + nameOf(static_cast<MdHalt>(i)) + "\"} " + (v.halts[i] ? "1" : "0") + "\n";
            }
            out += "# HELP " + p + "source_stalls_total Stall verdicts of the journal followers, by reason.\n# TYPE " + p +
                "source_stalls_total counter\n";
            for (std::size_t i = 0; i < static_cast<std::size_t>(cluster_tools::archive::StallReason::COUNT); ++i)
            {
                const auto reason = static_cast<cluster_tools::archive::StallReason>(i);
                out += p + "source_stalls_total{reason=\"" + cluster_tools::archive::stallLabel(reason) + "\"} " +
                    std::to_string(m_trades.archiveMetrics.stalls(reason)) + "\n";
            }
            line("counter", "live_trades_total", "Trades handed to the live path (each once, never under a halt).",
                std::to_string(v.liveEmitted));
        }
        out += "# HELP " + p + "requests_total Public HTTP requests answered, by route and status.\n# TYPE " + p +
            "requests_total counter\n";
        for (const auto &kv : m_candles->requests())
        {
            out += p + "requests_total{route=\"" + kv.first.first + "\",code=\"" + std::to_string(kv.first.second) +
                "\"} " + std::to_string(kv.second) + "\n";
        }
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
    TradeShared &m_trades;
    HistoryWriter &m_writer;
    std::unique_ptr<CandleRoute> m_candles;
    std::unique_ptr<MdWsServer> m_server;
};

HistoryWriterSettings writerSettings(const MarketDataSettings &s)
{
    HistoryWriterSettings w;
    w.pg.url = s.pgUrl;
    w.pg.user = s.pgUser;
    w.pg.passwordFile = s.pgPasswordFile;
    w.pg.statementTimeoutMs = s.pgStatementTimeoutMs;
    w.retryMs = s.pgRetryMs;
    w.commitBatch = s.commitBatch;
    w.commitIntervalMs = s.commitIntervalMs;
    w.markets = s.expectedMarkets;
    w.recentTrades = s.recentTrades;
    w.candleHistory = s.candleHistory;
    return w;
}

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
    // the market history first: the legacy guard (78), the writer lock (75) and the schema (78) are settled before any
    // listener binds; an unreachable database only delays the trade path (the book path does not wait for it)
    TradeShared trades(static_cast<std::size_t>(s.pendingTrades));
    HistoryWriter writer(writerSettings(s), trades.ledger);
    writer.arrivalClockMs = &wallClockMs;
    if (const int code = writer.startCheck(); 0 != code)
    {
        std::fprintf(stderr, "oe-marketdata: refused to start (exit %d)\n", code);
        std::fflush(stderr);
        std::_Exit(code);
    }
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
    inputs.makeDuty = [&s, &commands, &stats, &trades](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return std::make_unique<MarketDataDuty>(context, s, commands, stats, trades);
    };
    inputs.makeFront = [&s, &commands, &stats, &trades, &writer]() -> std::unique_ptr<FrontWork> {
        return std::make_unique<MarketDataFront>(s, commands, stats, trades, writer);
    };
    runProcess(identity, inputs);
}

}}}
