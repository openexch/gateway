#include "GatewayApp.h"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/CoreSession.h"
#include "gateway/common/Environment.h"
#include "gateway/common/Settings.h"

// oe-gateway - the REST gateway in front of the core: one GATEWAY cluster session (plus one TREASURY session when the
// demo lane is on), stateless, N copies with no coordination. This build is the process shell and the core session:
// settings, both media driver modes, the supervisor, the duty thread with the session wrapper (registration, the
// config cache, the request table), and the admin endpoints; the REST routes plug in later. Exit codes: ExitCodes.h.

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
    return s;
}

// The duty work of oe-gateway: the GATEWAY session. Requests come from the REST layer (later); until then the session
// registers, reads the config and keeps the readiness conditions current.
class GatewayDuty final : public DutyWork
{
public:
    GatewayDuty(const DutyContext &context, const GatewaySettings &g) :
        m_transport(context.aeron),
        m_client(m_transport, sessionClientSettings(g.common, "oe-gateway")),
        m_session(m_client, gatewaySessionSettings(g), [](const session::Outcome &) {}, context.log)
    {
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

    int doWork() override { return m_session.doWork(); }
    void fillStatus(Status &status) const override { m_session.fillStatus(status); }
    int exitCode() const override { return m_startRefused ? 78 : m_session.exitCode(); }
    void close() override { m_session.close(); }

private:
    at::AeronTransport m_transport;
    AeronClient m_client;
    session::CoreSession<AeronClient> m_session;
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
    inputs.treasuryLane = g.allowTreasuryLane ? "on (no session yet)" : "off";
    inputs.hooks = hooks;
    inputs.makeDuty = [&g](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return std::make_unique<GatewayDuty>(context, g);
    };
    runProcess(identity, inputs);
}

}}
