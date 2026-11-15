#include "GatewayApp.h"

#include "gateway/common/Environment.h"
#include "gateway/common/Settings.h"

// oe-gateway - the REST gateway in front of the core: one GATEWAY cluster session (plus one TREASURY session when the
// demo lane is on), stateless, N copies with no coordination. This build is the process shell: settings, both media
// driver modes, the supervisor, the duty thread and the admin endpoints; the session wrapper and the REST routes plug
// in later. Exit codes: ExitCodes.h.

namespace openexchange { namespace gateway {

int gatewayMain(const ProcessHooks &hooks)
{
    const SystemEnvironment env;
    const ParseResult<GatewaySettings> parsed = parseGatewaySettings(env);
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
    runProcess(identity, inputs);
}

}}
