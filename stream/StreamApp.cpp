#include "StreamApp.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/AuthMaterial.h"
#include "gateway/common/Authenticator.h"
#include "gateway/common/CoreSession.h"
#include "gateway/common/Environment.h"
#include "gateway/common/PgUserStore.h"
#include "gateway/common/Settings.h"
#include "gateway/common/WorkPool.h"

#include "EnginePoller.h"
#include "StreamCore.h"
#include "WsServer.h"

// oe-stream - the users' own order, fill and balance streams over WebSocket, from the core's egress through one
// GATEWAY cluster session of its own (acks, orders, control, and balances when STREAM_BALANCE_STREAM is on), so that
// fan-out never runs on the order-entry duty thread. Stateless, N copies with no coordination and no link to oe-gateway:
// what it knows comes from the core over that session, and the engine status feed from each node's own endpoint over
// HTTP (EnginePoller; never a disk). Threads: main (the supervisor), st-duty (the session, StreamCore: every window, gap,
// cache and queue), st-io-<n> (the WebSocket sockets, authentication, client frames), st-wake (posts the duty thread's
// writes to the sockets' strands), st-pg-auth-<n> (demo token lookups), st-engine (the node polls), st-admin. Exit codes:
// ExitCodes.h. This binary cannot encode an order or a money command (GW_NO_ORDER_ENTRY, tests/link).

namespace openexchange { namespace gateway {

namespace {

namespace cl = cluster_tools::client;
namespace at = cluster_tools::client::aeron_transport;
namespace st = stream;

using AeronClient = cl::ClusterClient<at::AeronTransport>;

session::SessionSettings streamSessionSettings(const StreamSettings &s)
{
    session::SessionSettings out;
    out.sessionClass = cl::SessionClass::GATEWAY;
    out.credentialClass = s.common.credentials.sessionClass;
    out.channels = cl::core_channel::ACKS | cl::core_channel::ORDERS | cl::core_channel::CONTROL |
        (s.balanceStream ? cl::core_channel::BALANCES : 0u);
    out.identity = s.common.instanceId;
    // the stream holds no request of a client: its own queries only (the core session's table bound)
    out.maxInflight = std::max<std::int64_t>(s.queryBurst * 4, 64);
    out.requestTimeoutMs = s.requestTimeoutMs;
    out.queryRateCount = s.queryRate.count;
    out.queryRatePeriodMs = s.queryRate.periodMs;
    out.queryBurst = s.queryBurst;
    out.reconnectDelayMs = s.common.reconnectDelayMs;
    out.registerResendMs = s.common.registerResendMs;
    out.readConfig = true; // the asset codes of the balance frames
    out.correlationIdBase = 0x5354'0000'0000'0000LL;
    // out.encoder stays the default: reads only (this binary does not link the writes)
    return out;
}

st::StreamCoreSettings coreSettings(const StreamSettings &s)
{
    st::StreamCoreSettings c;
    c.eventWindow = s.eventWindow;
    c.replayPage = s.replayPage;
    c.gapBufferEvents = s.gapBufferEvents;
    c.queryBurst = s.queryBurst;
    c.userLingerMs = s.userLingerMs;
    c.headProbeMs = s.headProbeMs;
    c.heartbeatMs = s.heartbeatMs;
    c.wsQueueMessages = s.wsQueueMessages;
    c.wsQueueBytes = s.wsQueueBytes;
    c.balanceStream = s.balanceStream;
    return c;
}

// Commands taken from the I/O side per duty cycle: a batching step, not a capacity (the rest wait for the next cycle).
constexpr int COMMANDS_PER_CYCLE = 512;

class SessionPort final : public st::QueryPort
{
public:
    explicit SessionPort(session::CoreSession<AeronClient> &s) : m_session(s) {}
    session::SubmitResult submit(session::Command command) override { return m_session.submit(std::move(command)); }

private:
    session::CoreSession<AeronClient> &m_session;
};

// The duty work of oe-stream: the core session, StreamCore, and the command ring from the I/O side.
class StreamDuty final : public DutyWork, private session::SessionListener
{
public:
    StreamDuty(const DutyContext &context, const StreamSettings &s, st::CommandRing &commands, st::StreamStats &stats,
        st::EngineRing &engine) :
        m_commands(commands),
        m_engine(engine),
        m_transport(context.aeron),
        m_client(m_transport, sessionClientSettings(s.common, "oe-stream")),
        m_session(m_client, streamSessionSettings(s),
            [this](const session::Outcome &o) { m_core.onOutcome(o, m_client.nanoTime()); }, context.log),
        m_port(m_session),
        m_core(coreSettings(s), m_port, [this] { return m_session.configSnapshot(); }, context.log, &stats)
    {
        m_session.setListener(this);
        if (!m_session.start())
        {
            m_startRefused = true;
            if (nullptr != context.log)
            {
                context.log->push(cluster_tools::runtime::LogLevel::CRITICAL, cluster_tools::runtime::Supervisor::monotonicNs(),
                    "session: the cluster client refused to start: %s (exit 78)", m_client.lastDetail().c_str());
            }
        }
    }

    int doWork() override
    {
        const std::int64_t now = m_client.nanoTime();
        int work = 0;
        st::StreamCommand c;
        while (work < COMMANDS_PER_CYCLE && m_commands.tryPop(c))
        {
            work++;
            switch (c.kind)
            {
                case st::StreamCommand::Kind::OPEN:
                    m_core.open(c.connId, c.userId, std::move(c.outlet), now);
                    break;
                case st::StreamCommand::Kind::SUBSCRIBE:
                    m_core.subscribe(c.connId, c.channels,
                        c.hasResume ? std::optional<std::int64_t>(c.resumeFrom) : std::nullopt, now, c.frameUserId);
                    break;
                case st::StreamCommand::Kind::UNSUBSCRIBE:
                    m_core.unsubscribe(c.connId, c.channels);
                    break;
                case st::StreamCommand::Kind::PING:
                    m_core.ping(c.connId);
                    break;
                case st::StreamCommand::Kind::GONE:
                    m_core.gone(c.connId, now);
                    break;
                case st::StreamCommand::Kind::NONE:
                    break;
            }
            c = st::StreamCommand{};
        }
        st::EngineOut e;
        while (m_engine.tryPop(e))
        {
            work++;
            if (st::EngineOut::Kind::STATUS == e.kind)
            {
                m_core.onEngineStatus(std::move(e.text));
            }
            else if (st::EngineOut::Kind::EVENT == e.kind)
            {
                m_core.onEngineEvent(e.text);
            }
            e = st::EngineOut{};
        }
        work += m_session.doWork();
        work += m_core.doWork(m_client.nanoTime());
        return work;
    }

    void fillStatus(Status &status) const override
    {
        m_session.fillStatus(status);
        status.gapOpen = m_core.gapOpen();
        status.lastHeadProbeNs = m_core.lastHeadProbeNs();
    }

    int exitCode() const override { return m_startRefused ? 78 : m_session.exitCode(); }
    void close() override { m_session.close(); }

private:
    void onRegistered(std::uint64_t epoch, std::int64_t term) override
    {
        m_core.onRegistered(epoch, term, m_client.nanoTime());
    }
    void onLiveOrderEvent(const wire::OrderEvent &e) override { m_core.onLiveOrderEvent(e, m_client.nanoTime()); }
    void onLiveBalance(const wire::BalanceUpdate &b) override { m_core.onLiveBalance(b, m_client.nanoTime()); }
    void onAdmission(const wire::AdmissionState &a) override { m_core.onAdmission(a); }

    st::CommandRing &m_commands;
    st::EngineRing &m_engine;
    at::AeronTransport m_transport;
    AeronClient m_client;
    session::CoreSession<AeronClient> m_session;
    SessionPort m_port;
    st::StreamCore m_core;
    bool m_startRefused = false;
};

// oe-stream's public side: the WebSocket listener, the auth pool (demo) and the waker.
class StreamFront final : public FrontWork
{
public:
    StreamFront(const StreamSettings &s, AuthMaterial material, st::CommandRing &commands, const st::StreamStats &stats,
        st::EngineRing &engine, st::EngineStats &engineStats) :
        m_settings(s), m_stats(stats), m_engineStats(engineStats)
    {
        st::EnginePollerSettings es;
        es.coreNodes = s.coreNodeEndpoints;
        es.meNodes = s.meNodeEndpoints;
        es.pollMs = s.enginePollMs;
        es.timeoutMs = s.enginePollTimeoutMs;
        es.frameMinMs = s.engineFrameMinMs;
        es.maxBodyBytes = s.engineMaxBodyBytes;
        m_poller = std::make_unique<st::EnginePoller>(es, engine, engineStats);
        const CommonSettings &c = s.common;
        if (0 != (c.authModes & auth_mode::DEMO))
        {
            PgSettings pg;
            pg.url = *c.pgUrl;
            pg.workers = static_cast<int>(s.pgAuthPool);
            pg.statementTimeoutMs = s.pgStatementTimeoutMs;
            pg.migrate = false; // oe-stream only verifies the users schema; oe-gateway applies it
            pg.retryMs = s.pgRetryMs;
            m_store = std::make_unique<PgUserStore>(pg);
            m_storePool = std::make_unique<WorkPool>("st-pg-auth", static_cast<int>(s.pgAuthPool),
                static_cast<std::size_t>(s.pgQueue));
        }
        AuthenticatorSettings as;
        as.modes = c.authModes;
        as.tokenCacheTtlMs = c.tokenCacheTtlMs;
        as.tokenStaleMs = c.tokenStaleMs;
        as.unknownLookupInflight = std::max<std::int64_t>(1, s.pgQueue / 2);
        m_auth = std::make_unique<Authenticator>(as, std::move(material), m_store.get(), m_storePool.get());
        st::WsServerSettings ws;
        ws.host = c.publicBind.host;
        ws.port = c.publicBind.port;
        ws.ioThreads = static_cast<int>(c.ioThreads);
        ws.maxConnections = c.maxConnections;
        ws.authTimeoutMs = s.wsAuthTimeoutMs;
        ws.pingMs = s.wsPingMs;
        ws.idleMs = s.wsIdleMs;
        ws.maxMessageBytes = s.wsMaxMessageBytes;
        ws.handoverDepth = static_cast<std::size_t>(std::min<std::int64_t>(s.wsQueueMessages, 64));
        ws.sendBufferBytes = s.wsSendBufferBytes;
        m_server = std::make_unique<st::WsServer>(ws, *m_auth, commands);
    }
    ~StreamFront() override { stop(m_settings.common.stopTimeoutMs); }

    bool bind(std::string &error) override { return m_server->bind(error); }
    void start() override
    {
        if (m_storePool)
        {
            m_storePool->start();
        }
        m_poller->start();
        m_server->start();
    }
    bool stop(std::int64_t boundMs) override
    {
        bool ok = m_server->stop(boundMs);
        ok = m_poller->stop(boundMs) && ok;
        if (m_storePool)
        {
            ok = m_storePool->stop(boundMs) && ok;
        }
        return ok;
    }
    bool accepting() const override { return m_server->accepting(); }

    std::string healthJson() const override
    {
        const st::WsCounters &w = m_server->counters();
        return ",\"ws\":{\"accepting\":" + std::string(m_server->accepting() ? "true" : "false") + ",\"connections\":" +
            std::to_string(w.connections.load()) + ",\"refusedConnections\":" + std::to_string(w.refused.load()) +
            "},\"window\":{\"firstSeq\":" + std::to_string(m_stats.windowFirstSeq.load()) + ",\"watermark\":" +
            std::to_string(m_stats.watermark.load()) + "},\"users\":{\"known\":" + std::to_string(m_stats.users.load()) +
            ",\"cached\":" + std::to_string(m_stats.usersCached.load()) + "},\"engine\":{\"coreNodes\":" +
            std::to_string(m_settings.coreNodeEndpoints.size()) + ",\"meNodes\":" +
            std::to_string(m_settings.meNodeEndpoints.size()) + ",\"rounds\":" + std::to_string(m_engineStats.rounds.load()) +
            ",\"lastRoundMs\":" + std::to_string(m_engineStats.lastRoundMs.load()) + "}";
    }

    std::string metrics(const std::string &p) const override
    {
        const st::WsCounters &w = m_server->counters();
        std::string out;
        auto line = [&](const char *type, const std::string &name, const std::string &help, const std::string &value) {
            out += "# HELP " + p + name + " " + help + "\n# TYPE " + p + name + " " + type + "\n" + p + name + " " + value + "\n";
        };
        line("counter", "event_gap_total", "Event gaps opened (a hole in the live stream, a new registration, a head probe "
            "ahead of the watermark).", std::to_string(m_stats.gaps.load()));
        line("counter", "event_replay_total", "Order events taken from a replay or backfill page.",
            std::to_string(m_stats.replayedEvents.load()));
        out += "# HELP " + p + "resync_total Resync frames sent, by reason.\n# TYPE " + p + "resync_total counter\n";
        for (std::size_t r = 0; r < st::RESYNC_REASONS; ++r)
        {
            out += p + "resync_total{reason=\"" + st::nameOf(static_cast<st::ResyncReason>(r)) + "\"} " +
                std::to_string(m_stats.resyncs[r].load()) + "\n";
        }
        line("gauge", "window_first_seq", "The oldest eventSeq the event window holds.",
            std::to_string(m_stats.windowFirstSeq.load()));
        line("gauge", "watermark", "Every order event up to this eventSeq was received and routed (-1 = not known yet).",
            std::to_string(m_stats.watermark.load()));
        line("gauge", "ws_connections", "WebSocket connections held.", std::to_string(w.connections.load()));
        line("counter", "ws_refused_total", "Connections closed at accept above STREAM_MAX_CONNECTIONS.",
            std::to_string(w.refused.load()));
        line("counter", "ws_auth_failed_total", "Connections whose credential was refused or missing.",
            std::to_string(w.authFailed.load()));
        line("counter", "ws_ops_refused_total", "Client ops refused because the command ring was full.",
            std::to_string(w.opsRefused.load()));
        line("counter", "ws_frames_sent_total", "Frames written to clients (a finished WebSocket write).",
            std::to_string(w.framesSent.load()));
        out += "# HELP " + p + "ws_closed_total Connections closed, by close code.\n# TYPE " + p + "ws_closed_total counter\n";
        for (std::size_t i = 0; i <= st::COUNTED_CLOSE_CODES.size(); ++i)
        {
            out += p + "ws_closed_total{code=\"" +
                (i < st::COUNTED_CLOSE_CODES.size() ? std::to_string(st::COUNTED_CLOSE_CODES[i]) : std::string("other")) +
                "\"} " + std::to_string(w.closed[i].load()) + "\n";
        }
        line("gauge", "ws_queue_high_water", "The most messages one connection held queued (the duty side plus unwritten).",
            std::to_string(m_stats.queueHighWater.load()));
        line("counter", "ws_closed_at_bound_total", "Connections closed 4001 at their outbound queue bound.",
            std::to_string(m_stats.closedAtBound.load()));
        line("gauge", "users_cached", "Users with an order or balance cache.", std::to_string(m_stats.usersCached.load()));
        line("counter", "snapshot_queries_total", "RequestOrderSnapshot pages submitted.",
            std::to_string(m_stats.snapshotQueries.load()));
        line("counter", "balance_queries_total", "RequestUserBalances submitted.", std::to_string(m_stats.balanceQueries.load()));
        line("counter", "event_queries_total", "RequestOrderEvents submitted (replays, backfills, head probes).",
            std::to_string(m_stats.eventQueries.load()));
        line("counter", "cache_serves_total", "Snapshots answered from a per-user cache (no core query).",
            std::to_string(m_stats.cacheServes.load()));
        line("counter", "window_resumes_total", "Client resumes served from the event window.",
            std::to_string(m_stats.windowResumes.load()));
        line("counter", "backfills_total", "Backfill rounds completed into the event window.",
            std::to_string(m_stats.backfills.load()));
        line("counter", "stale_balance_lines_total", "Balance lines discarded because a balance read was in flight.",
            std::to_string(m_stats.staleBalanceLines.load()));
        line("counter", "query_failures_total", "Stream queries that ended without a reply (asked again).",
            std::to_string(m_stats.queryFailures.load()));
        const st::EngineStats &e = m_engineStats;
        line("counter", "engine_rounds_total", "Engine status poll rounds (every configured node's /health and /metrics).",
            std::to_string(e.rounds.load()));
        line("counter", "engine_requests_total", "HTTP requests to node endpoints.", std::to_string(e.requests.load()));
        line("counter", "engine_request_failures_total", "Node endpoint requests with no HTTP answer (refused, timeout, "
            "too large).", std::to_string(e.requestFailures.load()));
        line("counter", "engine_read_bytes_total", "Bytes read from node endpoints.", std::to_string(e.bytesRead.load()));
        line("counter", "engine_frames_total", "Engine frames handed to the connections.", std::to_string(e.frames.load()));
        line("counter", "engine_election_events_total", "Election frames handed to the connections.",
            std::to_string(e.electionEvents.load()));
        line("counter", "engine_unchanged_total", "Rounds whose engine frame equalled the last one sent (not sent).",
            std::to_string(e.unchanged.load()));
        line("gauge", "engine_last_frame_bytes", "Size of the last engine frame.", std::to_string(e.lastFrameBytes.load()));
        line("gauge", "engine_last_round_ms", "Duration of the last poll round.", std::to_string(e.lastRoundMs.load()));
        return out;
    }

private:
    const StreamSettings &m_settings;
    const st::StreamStats &m_stats;
    st::EngineStats &m_engineStats;
    std::unique_ptr<st::EnginePoller> m_poller;
    std::unique_ptr<PgUserStore> m_store;
    std::unique_ptr<WorkPool> m_storePool;
    std::unique_ptr<Authenticator> m_auth;
    std::unique_ptr<st::WsServer> m_server;
};

// The session's own checks on the settings as parsed, in the same pass as every other problem.
void addSessionProblems(ParseResult<StreamSettings> &parsed)
{
    std::istringstream lines(session::validate(streamSessionSettings(parsed.settings)));
    for (std::string line; std::getline(lines, line);)
    {
        const char *key = std::string::npos != line.find("requestTimeoutMs") ? "STREAM_REQUEST_TIMEOUT_MS"
            : std::string::npos != line.find("query rate") ? "STREAM_QUERY_RATE"
            : std::string::npos != line.find("queryBurst") ? "STREAM_QUERY_BURST"
            : std::string::npos != line.find("reconnectDelayMs") ? "STREAM_RECONNECT_DELAY_MS"
            : std::string::npos != line.find("registerResendMs") ? "STREAM_REGISTER_RESEND_MS"
            : std::string::npos != line.find("identity") ? "STREAM_INSTANCE_ID" : nullptr;
        bool named = false;
        for (const std::string &e : parsed.errors)
        {
            named = named || (nullptr != key && 0 == e.rfind(key, 0));
        }
        if (!named)
        {
            parsed.errors.push_back(nullptr == key ? line : std::string(key) + ": " + line);
        }
    }
}

}

int streamMain(const ProcessHooks &hooks)
{
    const SystemEnvironment env;
    ParseResult<StreamSettings> parsed = parseStreamSettings(env);
    addSessionProblems(parsed);
    // the credential files of the enabled modes, read now so a missing or bad file is listed in the same pass (78)
    AuthMaterial material = loadAuthMaterial(parsed.settings.common, parsed.errors);
    if (!parsed.ok())
    {
        return refuseConfiguration("oe-stream", parsed.errors);
    }
    const StreamSettings &s = parsed.settings;
    ProcessIdentity identity;
    identity.role = Role::STREAM;
    identity.processName = "oe-stream";
    identity.threadPrefix = "st";
    identity.dirPrefix = "oe-stream-";
    identity.instanceKey = "STREAM_INSTANCE_ID";
    identity.adminBindKey = "STREAM_ADMIN_BIND";
    identity.frontBindKey = "STREAM_WS_BIND";
    ProcessInputs inputs;
    inputs.common = &s.common;
    inputs.records = &parsed.records;
    inputs.streamBounds.readyStaleMs = s.readyStaleMs;
    inputs.hooks = hooks;
    // live until the process exits (runProcess does not return)
    st::CommandRing commands(static_cast<std::size_t>(s.common.commandRing));
    st::StreamStats stats;
    st::EngineRing engine(st::ENGINE_RING_CELLS);
    st::EngineStats engineStats;
    inputs.makeDuty = [&s, &commands, &stats, &engine](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return std::make_unique<StreamDuty>(context, s, commands, stats, engine);
    };
    inputs.makeFront = [&s, &material, &commands, &stats, &engine, &engineStats]() -> std::unique_ptr<FrontWork> {
        return std::make_unique<StreamFront>(s, std::move(material), commands, stats, engine, engineStats);
    };
    runProcess(identity, inputs);
}

}}
