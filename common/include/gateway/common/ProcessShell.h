#ifndef GATEWAY_COMMON_PROCESS_SHELL_H
#define GATEWAY_COMMON_PROCESS_SHELL_H

#include <functional>
#include <string>
#include <vector>

#include "gateway/common/Readiness.h"
#include "gateway/common/Settings.h"

// ProcessShell - the process both binaries are, around their duty work, built from the cluster-tools runtime:
//   1. every setting in force is written to stderr (the boot record; /health shows the same list);
//   2. the admin listener binds (a bind failure is 78), before any driver or Aeron object, and the admin thread
//      (<prefix>-admin, Admin.h) serves from then on: through the driver wait and the embedded start retries /health
//      and /ready answer 503 with the condition driver_connected false;
//   3. the media driver per <P>_DRIVER_MODE: embedded starts the C media driver of the pinned Aeron in this process at
//      <dir>/<process>-<instance id> under an instance lock (a live driver or a held lock is 78; a start starved of
//      storage space is retried <P>_EMBEDDED_RETRY_MS apart and is 70 after the runtime's starvation bound); external
//      uses the host aeronmd at AERON_DIR (a directory that does not exist yet is no driver: step 4, 70);
//   4. the Aeron client connects with <P>_DRIVER_TIMEOUT_MS (no driver answering = 70); from then on a dead or stale
//      driver is a fatal conductor error (70, both modes), and main checks the client-conductor heartbeat at every
//      wake (ConductorWatch, 70); the process counter "<process> duty iterations" (type 1201, key = the instance id)
//      is registered with the driver (no answer inside the driver timeout = 70) and the duty thread sets it; from here
//      driver_connected holds;
//   5. the Supervisor runs the duty thread (<prefix>-duty) and main's loop: SIGTERM / SIGINT stop, the duty watchdog
//      (<P>_DUTY_WATCHDOG_MS, 70), the log drain; the duty thread publishes its Status every <P>_STATUS_INTERVAL_MS;
//   6. the stop path: the duty returns, the admin listener shuts down, every join and close is bounded by
//      <P>_STOP_TIMEOUT_MS (a bound hit is 70), then the process exits with the duty's code (0 on a clean stop). An
//      admin thread that does not stop inside the bound is detached and the process exits 70 at once, tearing down
//      nothing that thread can still reach.
// No cluster session is opened here: the session wrapper plugs into the duty loop.

namespace openexchange { namespace gateway {

struct ProcessIdentity
{
    Role role = Role::GATEWAY;
    const char *processName = "oe-gateway";   // log lines, exit lines, the Aeron client name
    const char *threadPrefix = "gw";          // gw-duty, gw-admin
    const char *dirPrefix = "oe-gateway-";    // the embedded driver dir <parent>/<dirPrefix><instance id>
    const char *instanceKey = "GATEWAY_INSTANCE_ID";
    const char *adminBindKey = "GATEWAY_ADMIN_BIND";
};

// Test builds only (never set by the production binaries).
struct ProcessHooks
{
    std::function<void()> onDutyIteration; // runs on the duty thread at the top of every iteration
};

struct ProcessInputs
{
    const CommonSettings *common = nullptr;
    const std::vector<SettingRecord> *records = nullptr;
    GatewayReadyBounds gatewayBounds;
    StreamReadyBounds streamBounds;
    std::string treasuryLane = "off";
    ProcessHooks hooks;
};

// Runs the process and exits it (std::_Exit with the code); does not return.
[[noreturn]] void runProcess(const ProcessIdentity &identity, const ProcessInputs &inputs);

// Prints every problem and the refusal line; returns 78.
int refuseConfiguration(const char *processName, const std::vector<std::string> &errors);

}}

#endif
