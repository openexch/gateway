#include "StreamApp.h"

#include "gateway/common/Environment.h"
#include "gateway/common/Settings.h"

// oe-stream - the users' own order, fill and balance streams over WebSocket, from the core's egress through one
// GATEWAY cluster session of its own, so that fan-out never runs on the order-entry duty thread. Stateless, N copies
// with no coordination and no link to oe-gateway. This build is the process shell: settings, both media driver modes,
// the supervisor, the duty thread and the admin endpoints; the session, the event window and the WebSocket router
// plug in later. Exit codes: ExitCodes.h.

namespace openexchange { namespace gateway {

int streamMain(const ProcessHooks &hooks)
{
    const SystemEnvironment env;
    const ParseResult<StreamSettings> parsed = parseStreamSettings(env);
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
    ProcessInputs inputs;
    inputs.common = &s.common;
    inputs.records = &parsed.records;
    inputs.streamBounds.readyStaleMs = s.readyStaleMs;
    inputs.hooks = hooks;
    runProcess(identity, inputs);
}

}}
