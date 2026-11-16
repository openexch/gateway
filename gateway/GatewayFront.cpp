#include "GatewayFront.h"

#include "gateway/common/FrontLog.h"

namespace openexchange { namespace gateway {

GatewayFront::GatewayFront(const GatewaySettings &g, AuthMaterial material, DutyBridge &bridge) :
    m_settings(g), m_bridge(bridge)
{
    const CommonSettings &c = g.common;
    const bool demo = 0 != (c.authModes & auth_mode::DEMO);
    if (demo)
    {
        PgSettings pg;
        pg.url = *c.pgUrl;
        pg.workers = static_cast<int>(g.pgAuthPool);
        pg.statementTimeoutMs = g.pgStatementTimeoutMs;
        pg.migrate = true;
        pg.retryMs = g.pgRetryMs;
        m_store = std::make_unique<PgUserStore>(pg);
        m_storePool = std::make_unique<WorkPool>("gw-pg-auth", static_cast<int>(g.pgAuthPool),
            static_cast<std::size_t>(g.pgQueue));
        m_hashPool = std::make_unique<WorkPool>("gw-hash", static_cast<int>(g.hashThreads),
            static_cast<std::size_t>(g.hashQueue));
    }
    if (c.pgUrl)
    {
        // history reads: their own connections and queue, so a history storm never takes one of the auth pool's
        PgHistorySettings hs;
        hs.url = *c.pgUrl;
        hs.workers = static_cast<int>(g.pgHistoryPool);
        hs.statementTimeoutMs = g.pgStatementTimeoutMs;
        hs.retryMs = g.pgRetryMs;
        m_history = std::make_unique<PgHistoryStore>(hs);
        m_historyPool = std::make_unique<WorkPool>("gw-pg-hist", static_cast<int>(g.pgHistoryPool),
            static_cast<std::size_t>(g.pgQueue));
    }
    else
    {
        frontLog(FrontLevel::INFO, "history reads off: GATEWAY_PG_URL is unset (history routes answer 503 "
            "HISTORY_UNAVAILABLE)");
    }
    AuthenticatorSettings as;
    as.modes = c.authModes;
    as.tokenCacheTtlMs = c.tokenCacheTtlMs;
    as.tokenStaleMs = c.tokenStaleMs;
    as.unknownLookupInflight = g.tokenLookupInflight;
    m_auth = std::make_unique<Authenticator>(as, std::move(material), m_store.get(), m_storePool.get());
    ApiDeps deps;
    deps.settings = &g;
    deps.auth = m_auth.get();
    deps.store = m_store.get();
    deps.storePool = m_storePool.get();
    deps.hashPool = m_hashPool.get();
    deps.history = m_history.get();
    deps.historyPool = m_historyPool.get();
    deps.askCore = [this](session::Command command, std::function<void(const session::Outcome &)> done) {
        return m_bridge.submit(std::move(command), std::move(done));
    };
    if (g.allowTreasuryLane && g.treasuryCredentials)
    {
        // Deposit and Withdraw reach the duty thread through the same ring; it hands them to the TREASURY session
        deps.askTreasury = [this](session::Command command, std::function<void(const session::Outcome &)> done) {
            return m_bridge.submit(std::move(command), std::move(done));
        };
    }
    deps.config = [this] { return m_bridge.config(); };
    deps.registered = [this] { return m_bridge.registered(); };
    m_api = std::make_unique<GatewayApi>(std::move(deps));
    HttpServerSettings hs;
    hs.host = c.publicBind.host;
    hs.port = c.publicBind.port;
    hs.ioThreads = static_cast<int>(c.ioThreads);
    hs.maxBodyBytes = static_cast<std::size_t>(g.maxBodyBytes);
    hs.maxHeaderBytes = static_cast<std::size_t>(g.maxHeaderBytes);
    hs.idleTimeoutMs = g.httpIdleTimeoutMs;
    hs.maxConnections = c.maxConnections;
    hs.threadPrefix = "gw";
    m_server = std::make_unique<HttpServer>(hs, [this](std::shared_ptr<HttpExchange> ex) { m_api->handle(std::move(ex)); });
}

GatewayFront::~GatewayFront()
{
    stop(m_settings.common.stopTimeoutMs);
}

bool GatewayFront::bind(std::string &error)
{
    return m_server->bind(error);
}

void GatewayFront::start()
{
    if (m_hashPool)
    {
        m_hashPool->start();
    }
    if (m_storePool)
    {
        m_storePool->start();
    }
    if (m_historyPool)
    {
        m_historyPool->start();
    }
    m_bridge.startCompletions();
    m_server->start();
}

bool GatewayFront::stop(std::int64_t boundMs)
{
    bool ok = m_server->stop(boundMs);
    if (m_hashPool)
    {
        ok = m_hashPool->stop(boundMs) && ok;
    }
    if (m_storePool)
    {
        ok = m_storePool->stop(boundMs) && ok;
    }
    if (m_historyPool)
    {
        ok = m_historyPool->stop(boundMs) && ok;
    }
    ok = m_bridge.stopCompletions(boundMs) && ok;
    return ok;
}

std::string GatewayFront::healthJson() const
{
    return ",\"http\":{\"accepting\":" + std::string(m_server->accepting() ? "true" : "false") + ",\"connections\":" +
        std::to_string(m_server->connections()) + ",\"refusedConnections\":" +
        std::to_string(m_server->refusedConnections()) + ",\"commandRing\":" + std::to_string(m_bridge.commandsQueued()) +
        "}" + m_api->healthJson();
}

std::string GatewayFront::metrics(const std::string &p) const
{
    std::string out = m_api->metrics(p);
    out += "# HELP " + p + "http_connections Public connections held.\n# TYPE " + p + "http_connections gauge\n" + p +
        "http_connections " + std::to_string(m_server->connections()) + "\n";
    out += "# HELP " + p + "http_connections_refused_total Connections closed at accept above GATEWAY_MAX_CONNECTIONS.\n"
        "# TYPE " + p + "http_connections_refused_total counter\n" + p + "http_connections_refused_total " +
        std::to_string(m_server->refusedConnections()) + "\n";
    out += "# HELP " + p + "command_ring_refused_total Commands refused because the command ring was full.\n"
        "# TYPE " + p + "command_ring_refused_total counter\n" + p + "command_ring_refused_total " +
        std::to_string(m_bridge.refused()) + "\n";
    return out;
}

}}
