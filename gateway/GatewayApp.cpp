#include "GatewayApp.h"

#include <memory>
#include <unordered_map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/AuthMaterial.h"
#include "gateway/common/CoreSession.h"
#include "gateway/common/DutyBridge.h"
#include "gateway/common/Environment.h"
#include "gateway/common/FrontLog.h"
#include "gateway/common/OrderEntry.h"
#include "gateway/common/Settings.h"

#include "GatewayFront.h"

// oe-gateway - the REST gateway in front of the core: one GATEWAY cluster session (plus one TREASURY session when the
// demo lane is on), stateless, N copies with no coordination: settings, both media driver modes, the supervisor, the
// duty thread with the session wrapper(s) (registration, the config cache, the request table), the admin endpoints,
// and the public REST listener (GatewayFront, GatewayApi: auth, orders, balances, markets, status, the treasury lane).
// The REST side reaches the sessions only through the DutyBridge. Exit codes: ExitCodes.h.

namespace openexchange { namespace gateway {

namespace {

namespace cl = cluster_tools::client;
namespace at = cluster_tools::client::aeron_transport;

using AeronClient = cl::ClusterClient<at::AeronTransport>;

session::SessionSettings gatewaySessionSettings(const GatewaySettings &g)
{
    session::SessionSettings s;
    s.sessionClass = cl::SessionClass::GATEWAY;
    s.credentialClass = g.common.credentials.sessionClass;
    s.channels = cl::core_channel::ACKS | cl::core_channel::CONTROL;
    s.identity = g.common.instanceId;
    s.maxInflight = g.maxInflight;
    s.requestTimeoutMs = g.requestTimeoutMs;
    s.queryRateCount = g.queryRate.count;
    s.queryRatePeriodMs = g.queryRate.periodMs;
    s.queryBurst = g.queryBurst;
    s.reconnectDelayMs = g.common.reconnectDelayMs;
    s.registerResendMs = g.common.registerResendMs;
    s.readConfig = true;
    s.encoder = session::encodeCommand; // the writes: orders, and the treasury lane's money commands
    return s;
}

// Commands taken from the bridge per duty cycle: a batching step, not a capacity (the rest wait for the next cycle).
constexpr int BRIDGE_DRAIN_PER_CYCLE = 256;

// The TREASURY session of the demo lane: the same request rules, the treasury credential, ACKS only (its replies are
// ORIGIN acks), no config read (RequestCoreConfig is not a TREASURY template), its own correlation id range.
session::SessionSettings treasurySessionSettings(const GatewaySettings &g)
{
    session::SessionSettings s = gatewaySessionSettings(g);
    s.sessionClass = cl::SessionClass::TREASURY;
    s.credentialClass = g.treasuryCredentials ? g.treasuryCredentials->sessionClass : cl::SessionClass::TREASURY;
    s.channels = cl::core_channel::ACKS;
    s.readConfig = false;
    s.correlationIdBase = 0x5452'0000'0000'0000LL;
    return s;
}

CommonSettings treasuryCommon(const GatewaySettings &g)
{
    CommonSettings c = g.common;
    if (g.treasuryCredentials)
    {
        c.credentials = *g.treasuryCredentials;
    }
    return c;
}

bool isMoneyCommand(const session::Command &c)
{
    return std::holds_alternative<session::Deposit>(c) || std::holds_alternative<session::Withdraw>(c);
}

// The duty work of oe-gateway: the GATEWAY session, and the TREASURY session when the demo lane is on, fed by the REST
// side through the DutyBridge (Deposit and Withdraw go to the TREASURY session, everything else to the GATEWAY
// session). The GATEWAY session registers, reads the config and keeps the readiness conditions current; every outcome
// goes back through the bridge. /ready follows the GATEWAY session only: the lane is a demo feature.
class GatewayDuty final : public DutyWork
{
public:
    GatewayDuty(const DutyContext &context, const GatewaySettings &g, DutyBridge &bridge) :
        m_bridge(bridge),
        m_transport(context.aeron),
        m_client(m_transport, sessionClientSettings(g.common, "oe-gateway")),
        m_session(m_client, gatewaySessionSettings(g), [this](const session::Outcome &o) { onOutcome(m_tickets, o); },
            context.log)
    {
        if (!m_session.start())
        {
            refuseStart(context, m_client.lastDetail());
        }
        if (g.allowTreasuryLane && g.treasuryCredentials)
        {
            m_treasury = std::make_unique<Lane>(context, g, *this);
            if (!m_treasury->session.start())
            {
                refuseStart(context, "treasury: " + m_treasury->client.lastDetail());
            }
            if (nullptr != context.log)
            {
                context.log->push(cluster_tools::runtime::LogLevel::INFO, cluster_tools::runtime::Supervisor::monotonicNs(),
                    "treasury_lane: on (a TREASURY session for /balances/deposit, /balances/withdraw and register "
                    "funding; demo only)");
            }
        }
    }

    int doWork() override
    {
        const int taken = m_bridge.drain(
            [this](DutyBridge::Request &&r) {
                const bool money = isMoneyCommand(r.command);
                session::SubmitResult res;
                if (money && !m_treasury)
                {
                    res.refusal = session::OutcomeKind::INTERNAL; // the routes never send one with the lane off
                }
                else if (money)
                {
                    res = m_treasury->session.submit(std::move(r.command));
                }
                else
                {
                    res = m_session.submit(std::move(r.command));
                }
                if (res.accepted)
                {
                    (money ? m_treasury->tickets : m_tickets).emplace(res.ticket, r.id);
                    return;
                }
                session::Outcome refused;
                refused.kind = res.refusal;
                m_bridge.complete(r.id, std::move(refused));
            },
            BRIDGE_DRAIN_PER_CYCLE);
        int work = taken + m_session.doWork();
        if (m_treasury)
        {
            work += m_treasury->session.doWork();
        }
        m_bridge.publishConfig(m_session.configSnapshot());
        m_bridge.publishRegistered(m_session.registrar().isRegistered());
        return work;
    }
    void fillStatus(Status &status) const override
    {
        m_session.fillStatus(status);
        if (m_treasury)
        {
            // readiness follows the GATEWAY session; the decoder and reply counters cover both sessions
            const session::CoreSession<AeronClient> &t = m_treasury->session;
            status.decodeRefused += t.decoder().counters().refusedTotal();
            for (std::size_t k = 0; k < wire::IGNORED_TEMPLATES.size(); ++k)
            {
                status.wireIgnored[k] += t.decoder().counters().ignored[k];
            }
            status.lateReplies += t.counters().lateReplies;
            status.outcomeUnknown += t.counters().outcomeUnknown;
            status.reoffers += t.counters().reoffers;
        }
    }
    int exitCode() const override
    {
        if (m_startRefused)
        {
            return 78;
        }
        if (0 != m_session.exitCode())
        {
            return m_session.exitCode();
        }
        return m_treasury ? m_treasury->session.exitCode() : 0;
    }
    void close() override
    {
        m_session.close();
        if (m_treasury)
        {
            m_treasury->session.close();
        }
    }

private:
    using Tickets = std::unordered_map<std::uint64_t, std::uint64_t>; // session ticket -> bridge id

    struct Lane
    {
        Lane(const DutyContext &context, const GatewaySettings &g, GatewayDuty &owner) :
            transport(context.aeron),
            client(transport, sessionClientSettings(treasuryCommon(g), "oe-gateway treasury")),
            session(client, treasurySessionSettings(g), [this, &owner](const session::Outcome &o) { owner.onOutcome(tickets, o); },
                context.log)
        {
        }
        Tickets tickets;
        at::AeronTransport transport;
        AeronClient client;
        session::CoreSession<AeronClient> session;
    };

    void refuseStart(const DutyContext &context, const std::string &detail)
    {
        m_startRefused = true;
        if (nullptr != context.log)
        {
            context.log->push(cluster_tools::runtime::LogLevel::CRITICAL, cluster_tools::runtime::Supervisor::monotonicNs(),
                "session: the cluster client refused to start: %s (exit 78)", detail.c_str());
        }
    }

    void onOutcome(Tickets &tickets, const session::Outcome &o)
    {
        const auto it = tickets.find(o.ticket);
        if (it == tickets.end())
        {
            return; // the session's own request (registration, config read)
        }
        const std::uint64_t id = it->second;
        tickets.erase(it);
        m_bridge.complete(id, o);
    }

    DutyBridge &m_bridge;
    Tickets m_tickets;
    at::AeronTransport m_transport;
    AeronClient m_client;
    session::CoreSession<AeronClient> m_session;
    std::unique_ptr<Lane> m_treasury;
    bool m_startRefused = false;
};

// The session's own checks (the client's 1 h bound on its timeouts, the token bucket) on the settings as parsed, in the
// same pass as every other problem. A check whose input setting already has a problem is left out: that setting holds
// its default here, and its problem is listed once, by the reader.
void addSessionProblems(ParseResult<GatewaySettings> &parsed)
{
    static const std::vector<std::pair<const char *, const char *>> inputs = {
        { "maxInflight", "GATEWAY_MAX_INFLIGHT" },
        { "requestTimeoutMs", "GATEWAY_REQUEST_TIMEOUT_MS" },
        { "query rate", "GATEWAY_QUERY_RATE" },
        { "queryBurst", "GATEWAY_QUERY_BURST" },
        { "reconnectDelayMs", "GATEWAY_RECONNECT_DELAY_MS" },
        { "registerResendMs", "GATEWAY_REGISTER_RESEND_MS" },
        { "identity", "GATEWAY_INSTANCE_ID" },
    };
    std::istringstream lines(session::validate(gatewaySessionSettings(parsed.settings)));
    std::vector<std::string> found;
    for (std::string line; std::getline(lines, line);)
    {
        const char *key = nullptr;
        for (const auto &[field, name] : inputs)
        {
            if (std::string::npos != line.find(field))
            {
                key = name;
                break;
            }
        }
        bool named = false;
        for (const std::string &e : parsed.errors)
        {
            named = named || (nullptr != key && 0 == e.rfind(key, 0));
        }
        if (!named)
        {
            found.push_back(nullptr == key ? line : std::string(key) + ": " + line);
        }
    }
    parsed.errors.insert(parsed.errors.end(), found.begin(), found.end());
}

}

int gatewayMain(const ProcessHooks &hooks)
{
    const SystemEnvironment env;
    ParseResult<GatewaySettings> parsed = parseGatewaySettings(env);
    addSessionProblems(parsed);
    // the credential files of the enabled modes, in the same pass: a missing or bad file is 78 with the rest
    AuthMaterial material = loadAuthMaterial(parsed.settings.common, parsed.errors);
    if (!parsed.ok())
    {
        return refuseConfiguration("oe-gateway", parsed.errors);
    }
    const GatewaySettings &g = parsed.settings;
    ProcessIdentity identity;
    identity.role = Role::GATEWAY;
    identity.processName = "oe-gateway";
    identity.threadPrefix = "gw";
    identity.dirPrefix = "oe-gateway-";
    identity.instanceKey = "GATEWAY_INSTANCE_ID";
    identity.adminBindKey = "GATEWAY_ADMIN_BIND";
    ProcessInputs inputs;
    inputs.common = &g.common;
    inputs.records = &parsed.records;
    inputs.gatewayBounds.readyStaleMs = g.readyStaleMs;
    inputs.gatewayBounds.timeoutStreak = g.readyTimeoutStreak;
    inputs.treasuryLane = g.allowTreasuryLane ? "on" : "off";
    inputs.hooks = hooks;
    setFrontLogProcess("oe-gateway");
    // lives until the process exits (runProcess does not return)
    // with the lane on, two sessions hold up to maxInflight requests each: the completion ring holds both
    DutyBridge bridge(static_cast<std::size_t>(g.common.commandRing),
        static_cast<std::size_t>(g.maxInflight) * (g.allowTreasuryLane ? 2u : 1u), "gw");
    inputs.makeDuty = [&g, &bridge](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return std::make_unique<GatewayDuty>(context, g, bridge);
    };
    inputs.makeFront = [&g, &bridge, &material]() -> std::unique_ptr<FrontWork> {
        return std::make_unique<GatewayFront>(g, std::move(material), bridge);
    };
    runProcess(identity, inputs);
}

}}
