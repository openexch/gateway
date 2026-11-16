#ifndef GATEWAY_COMMON_PROCESS_SHELL_H
#define GATEWAY_COMMON_PROCESS_SHELL_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "gateway/common/Readiness.h"
#include "gateway/common/Settings.h"
#include "gateway/common/Status.h"

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
// The cluster session plugs into the duty loop through ProcessInputs::makeDuty: built on the duty thread once the
// Aeron client exists, driven every iteration, its Status fields published with the shell's, destroyed on the duty
// thread before the Aeron client closes. A duty whose exitCode() turns nonzero stops the process with that code.

namespace aeron {
class Aeron;
}

namespace cluster_tools { namespace runtime {
class LogRing;
}}

namespace openexchange { namespace gateway {

// The work of the duty thread (the core session and what hangs off it). Called on the duty thread only.
class DutyWork
{
public:
    virtual ~DutyWork() = default;
    virtual int doWork() = 0;                    // one iteration; the work done (0 = idle)
    virtual void fillStatus(Status &status) const = 0;
    virtual int exitCode() const = 0;            // 0 = keep running; else stop the process with this code
    virtual void close() = 0;                    // the stop path, before destruction
};

struct DutyContext
{
    std::shared_ptr<aeron::Aeron> aeron;
    cluster_tools::runtime::LogRing *log = nullptr;
};

struct ProcessIdentity
{
    Role role = Role::GATEWAY;
    const char *processName = "oe-gateway";   // log lines, exit lines, the Aeron client name
    const char *threadPrefix = "gw";          // gw-duty, gw-admin
    const char *dirPrefix = "oe-gateway-";    // the embedded driver dir <parent>/<dirPrefix><instance id>
    const char *instanceKey = "GATEWAY_INSTANCE_ID";
    const char *adminBindKey = "GATEWAY_ADMIN_BIND";
    const char *frontBindKey = "GATEWAY_HTTP_BIND";
};

// The public side of a binary (oe-gateway: the REST listener and its worker pools; oe-stream: the WebSocket listener),
// run by the shell around the duty
// thread: created and bound right after the admin bind (a bind failure is 78, naming ProcessIdentity::frontBindKey),
// started once the driver is connected (its threads start just before the duty thread), stopped on the stop path
// before the admin listener closes (bounded by <P>_STOP_TIMEOUT_MS; a thread that does not stop is 70). The admin thread
// reads accepting(), healthJson() and metrics() at request time, so all three are thread-safe.
class FrontWork
{
public:
    virtual ~FrontWork() = default;
    virtual bool bind(std::string &error) = 0;
    virtual void start() = 0;
    virtual bool stop(std::int64_t boundMs) = 0;
    virtual bool accepting() const = 0;
    virtual std::string healthJson() const = 0;                    // members appended to /health: ,"name":value...
    virtual std::string metrics(const std::string &prefix) const = 0; // Prometheus text, each name starting with prefix
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
    std::function<std::unique_ptr<DutyWork>(const DutyContext &)> makeDuty; // empty = no duty work (no session)
    std::function<std::unique_ptr<FrontWork>()> makeFront;                  // empty = no public listener in this build
};

// Runs the process and exits it (std::_Exit with the code); does not return.
[[noreturn]] void runProcess(const ProcessIdentity &identity, const ProcessInputs &inputs);

// Prints every problem and the refusal line; returns 78.
int refuseConfiguration(const char *processName, const std::vector<std::string> &errors);

}}

#endif
