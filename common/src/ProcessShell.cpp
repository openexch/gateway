#include "gateway/common/ProcessShell.h"

#include <pthread.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstring>
#include <stdexcept>
#include <string>
#include <cstdio>
#include <exception>
#include <memory>
#include <thread>

#include <Aeron.h>

#include "cluster_tools/build_info.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/client/aeron/FatalErrors.h"
#include "cluster_tools/runtime/ConductorWatch.h"
#include "cluster_tools/runtime/EmbeddedDriver.h"
#include "cluster_tools/runtime/HttpServer.h"
#include "cluster_tools/runtime/IdleStrategy.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/StatusBoardT.h"
#include "cluster_tools/runtime/StorageStarvation.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/build_info.h"
#include "gateway/common/Admin.h"
#include "gateway/common/ExitCodes.h"

namespace openexchange { namespace gateway {

namespace rt = cluster_tools::runtime;
namespace at = cluster_tools::client::aeron_transport;

namespace {

// The Aeron counter type of the process counter (outside Aeron's own range, below 1000, and the cluster-tools test
// builds' 1101).
constexpr std::int32_t PROCESS_COUNTER_TYPE_ID = 1201;

rt::NonBlockingLogWriter g_stderr(2);

void writeRecord(const rt::LogRecord &r)
{
    g_stderr(r);
}

std::int64_t nowNs()
{
    return rt::Supervisor::monotonicNs();
}

[[noreturn]] void finish(int code)
{
    std::fflush(stderr);
    std::_Exit(code);
}

// The verdict on a failed embedded start: storage space is retried (70 once the starvation lasts the runtime's bound),
// anything else is host configuration (78).
enum class StartVerdict
{
    REFUSE_78,
    RETRY_STORAGE,
    EXIT_70_STORAGE
};

StartVerdict startVerdict(bool failedForStorage, const std::string &why, std::int64_t now, rt::StorageStarvation &episode)
{
    if (!failedForStorage)
    {
        episode.onRelieved(now);
        return StartVerdict::REFUSE_78;
    }
    episode.onStarved(now, why);
    return episode.expired(now) ? StartVerdict::EXIT_70_STORAGE : StartVerdict::RETRY_STORAGE;
}

}

int refuseConfiguration(const char *processName, const std::vector<std::string> &errors)
{
    for (const std::string &error : errors)
    {
        std::fprintf(stderr, "%s config: %s\n", processName, error.c_str());
    }
    std::fprintf(stderr, "%s: refusing to start: %zu configuration problem%s (exit 78)\n", processName, errors.size(),
        1 == errors.size() ? "" : "s");
    std::fflush(stderr);
    return exit_code::CONFIG;
}

void runProcess(const ProcessIdentity &id, const ProcessInputs &in)
{
    const CommonSettings &c = *in.common;
    rt::Supervisor::setProcessName(id.processName);

    std::fprintf(stderr, "%s %s build %s cluster-tools %s money v%d (%s) aeron %s; instance %" PRId64 "; driver %s (%s, "
        "timeout %" PRId64 " ms); admin %s\n",
        id.processName, GW_VERSION, GW_SOURCE_SHA, GW_CLUSTER_TOOLS_SHA, cluster_tools::build_info::MONEY_SCHEMA_VERSION,
        cluster_tools::build_info::MONEY_SCHEMA_SHA256, cluster_tools::build_info::AERON_VERSION, c.instanceId,
        driverModeName(c.driverMode), DriverMode::EXTERNAL == c.driverMode ? c.aeronDir.c_str() : c.driverDir.c_str(),
        c.driverTimeoutMs, c.adminBind.text.c_str());
    for (const SettingRecord &r : *in.records)
    {
        std::fprintf(stderr, "%s setting %s=%s (%s)\n", id.processName, r.name.c_str(), r.value.c_str(), r.source.c_str());
    }

    rt::LogRing log;
    rt::StatusBoardT<Status> board;
    std::atomic<std::int64_t> lastBeatNs{ nowNs() };
    rt::SupervisorSettings supervisorSettings;
    supervisorSettings.watchdogNs = c.dutyWatchdogMs * 1'000'000LL;
    supervisorSettings.stalledNs = c.dutyWatchdogMs * 1'000'000LL;
    supervisorSettings.joinBoundNs = c.stopTimeoutMs * 1'000'000LL;
    supervisorSettings.sighupPolicy = rt::SighupPolicy::COUNT_ONLY;
    supervisorSettings.processName = id.processName;
    supervisorSettings.threadPrefix = id.threadPrefix;
    rt::Supervisor supervisor(board, log, supervisorSettings, &rt::Supervisor::exitProcess, &writeRecord);

    // Filled in below. The admin thread starts right after the bind and serves through the driver wait: it touches
    // `watch` only after reading driverConnected true (release-stored once the client, the process counter and the
    // watch all exist), and reads nothing else that this thread writes later.
    std::unique_ptr<rt::EmbeddedDriver> driver;
    std::unique_ptr<rt::ConductorWatch> watch;
    std::atomic<bool> driverConnected{ false };
    std::atomic<bool> driverReclaimed{ false };
    AdminView base;
    base.role = id.role;
    base.processName = id.processName;
    base.version = GW_VERSION;
    base.build = GW_SOURCE_SHA;
    base.clusterTools = GW_CLUSTER_TOOLS_SHA;
    base.moneySchemaVersion = cluster_tools::build_info::MONEY_SCHEMA_VERSION;
    base.moneySchemaSha256 = cluster_tools::build_info::MONEY_SCHEMA_SHA256;
    base.driverMode = driverModeName(c.driverMode);
    base.driverTimeoutMs = c.driverTimeoutMs;
    base.watchdogMs = c.dutyWatchdogMs;
    base.gatewayBounds = in.gatewayBounds;
    base.streamBounds = in.streamBounds;
    base.projectorBounds = in.projectorBounds;
    base.marketDataBounds = in.marketDataBounds;
    base.treasuryLane = in.treasuryLane;
    base.settings = in.records;

    // the driver dir per mode, known before anything starts
    std::string aeronDir = c.aeronDir;
    rt::EmbeddedDriverOptions embeddedOptions;
    if (DriverMode::EMBEDDED == c.driverMode)
    {
        embeddedOptions.processName = id.processName;
        embeddedOptions.dirPrefix = id.dirPrefix;
        embeddedOptions.instanceKey = id.instanceKey;
        aeronDir = rt::EmbeddedDriver::dirFor(c.driverDir, c.instanceId, embeddedOptions);
    }
    base.driverDir = aeronDir;

    std::unique_ptr<FrontWork> front = in.makeFront ? in.makeFront() : nullptr;
    rt::HttpServer admin(
        [&](const rt::HttpRequest &request) {
            AdminView view = base;
            view.nowNs = nowNs();
            view.heartbeatAgeMs = (view.nowNs - lastBeatNs.load(std::memory_order_acquire)) / 1'000'000LL;
            view.status = board.read();
            const bool connected = driverConnected.load(std::memory_order_acquire);
            view.status.driverConnected = connected;
            view.driverReclaimed = driverReclaimed.load(std::memory_order_acquire);
            view.conductorHeartbeatAgeMs = connected ? watch->ageMs() : -1;
            view.logDropped = log.dropped();
            if (front)
            {
                if (Role::GATEWAY == id.role)
                {
                    view.status.publicListenerAccepting = front->accepting();
                }
                else if (Role::STREAM == id.role)
                {
                    view.status.wsListenerAccepting = front->accepting();
                }
                front->fillStatus(view.status);
                view.frontHealth = front->healthJson();
                view.frontMetrics = front->metrics(metricPrefix(id.role));
            }
            return adminRoute(request, view);
        },
        id.adminBindKey);
    std::string error;
    if (!admin.bind("localhost" == c.adminBind.host ? "127.0.0.1" : c.adminBind.host, c.adminBind.port, error))
    {
        std::fprintf(stderr, "%s: %s: %s (exit 78)\n", id.processName, id.adminBindKey, error.c_str());
        finish(exit_code::CONFIG);
    }
    std::fprintf(stderr, "%s: admin listening on %s:%u\n", id.processName, c.adminBind.host.c_str(),
        static_cast<unsigned>(admin.boundPort()));
    if (front && !front->bind(error))
    {
        std::fprintf(stderr, "%s: %s: %s (exit 78)\n", id.processName, id.frontBindKey, error.c_str());
        finish(exit_code::CONFIG);
    }

    // the admin thread, serving from here on (/health and /ready read 503 with driver_connected false until the
    // driver is connected)
    std::thread adminThread([&] {
        char name[16];
        std::snprintf(name, sizeof(name), "%s-admin", id.threadPrefix);
        pthread_setname_np(pthread_self(), name);
        try
        {
            admin.run();
        }
        catch (const std::exception &e)
        {
            log.push(rt::LogLevel::CRITICAL, nowNs(), "%s-admin: exception escaped: %s", id.threadPrefix, e.what());
            supervisor.httpFatal.store(exit_code::SOFTWARE, std::memory_order_release);
            rt::Supervisor::wake();
        }
        catch (...)
        {
            log.push(rt::LogLevel::CRITICAL, nowNs(), "%s-admin: unknown exception escaped", id.threadPrefix);
            supervisor.httpFatal.store(exit_code::SOFTWARE, std::memory_order_release);
            rt::Supervisor::wake();
        }
    });

    // the driver per mode
    if (DriverMode::EMBEDDED == c.driverMode)
    {
        const std::string &dir = aeronDir;
        rt::StorageStarvation startStorage;
        for (;;)
        {
            if (in.hooks.ignoreDriverLoss)
            {
                // test build only: the driver's own termination is ignored
                driver = std::make_unique<rt::EmbeddedDriver>(embeddedOptions, [](const std::string &) {});
            }
            else
            {
                driver = std::make_unique<rt::EmbeddedDriver>(embeddedOptions,
                    [](const std::string &why) { rt::Supervisor::exitProcess(exit_code::SOFTWARE, why); });
            }
            if (driver->start(dir, false, error))
            {
                break;
            }
            const std::int64_t now = nowNs();
            const bool onset = !startStorage.active();
            const StartVerdict verdict = startVerdict(driver->startFailedForStorage(), error, now, startStorage);
            if (StartVerdict::REFUSE_78 == verdict)
            {
                std::fprintf(stderr, "%s: embedded media driver: %s (exit 78)\n", id.processName, error.c_str());
                finish(exit_code::CONFIG);
            }
            if (StartVerdict::EXIT_70_STORAGE == verdict)
            {
                std::fprintf(stderr, "%s stopped: the embedded media driver could not allocate its files for insufficient "
                    "storage space on every attempt for %lld s (the driver dir's tmpfs is too small or full): %s (exit 70)\n",
                    id.processName, static_cast<long long>(startStorage.heldNs(now) / 1'000'000'000LL), error.c_str());
                finish(exit_code::SOFTWARE);
            }
            if (onset)
            {
                std::fprintf(stderr, "%s: embedded media driver starved of storage space at start (retried %" PRId64
                    " ms apart, exit 70 if it lasts): %s\n", id.processName, c.embeddedRetryMs, error.c_str());
            }
            driver.reset();
            sigset_t stop;
            sigemptyset(&stop);
            sigaddset(&stop, SIGTERM);
            sigaddset(&stop, SIGINT);
            const timespec retry{ static_cast<time_t>(c.embeddedRetryMs / 1000),
                static_cast<long>((c.embeddedRetryMs % 1000) * 1'000'000L) };
            if (const int sig = ::sigtimedwait(&stop, nullptr, &retry); SIGTERM == sig || SIGINT == sig)
            {
                std::fprintf(stderr, "%s: stopped by signal %d while the embedded driver was starved (exit 0)\n",
                    id.processName, sig);
                finish(exit_code::CLEAN);
            }
        }
        driverReclaimed.store(driver->reclaimed(), std::memory_order_release);
        std::fprintf(stderr, "%s: embedded media driver at %s%s\n", id.processName, dir.c_str(),
            driver->reclaimed() ? " (a stale dir of this instance was reused and cleaned)" : "");
    }
    // a fatal conductor error is _Exit(70), its line classified as the supervisor's conductor exits are (the process
    // name, the driver timeout, F3b); test build only: counted and ignored
    const std::int64_t driverTimeoutMs = c.driverTimeoutMs;
    auto monitor = in.hooks.ignoreDriverLoss
        ? std::make_shared<at::AeronFatalMonitor>([](const std::string &) {})
        : std::make_shared<at::AeronFatalMonitor>([driverTimeoutMs](const std::string &message) {
              rt::Supervisor::exitProcess(exit_code::SOFTWARE, "Aeron client fatal error: " + message +
                  "; the driver timeout is " + std::to_string(driverTimeoutMs) + " ms (F3b)");
          });
    std::shared_ptr<aeron::Aeron> aeron;
    try
    {
        at::AeronClientOptions options;
        options.aeronDir = aeronDir;
        options.clientName = id.processName;
        options.driverTimeoutMs = c.driverTimeoutMs;
        aeron = at::connectAeron(options, monitor);
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "%s: no media driver answered at %s within %" PRId64 " ms: %s (exit 70)\n", id.processName,
            aeronDir.c_str(), c.driverTimeoutMs, e.what());
        finish(exit_code::SOFTWARE);
    }
    std::fprintf(stderr, "%s: Aeron client connected to the %s media driver at %s\n", id.processName,
        driverModeName(c.driverMode), aeronDir.c_str());

    // The process counter: "<process> duty iterations" keyed by the instance id, set by the duty thread. Adding it
    // also registers this client with the media driver, which allocates the client heartbeat counter only for a
    // client that holds a resource: without one an idle process (no session yet) reads as reclaimed to the
    // conductor watch and exits 70 after the driver timeout.
    std::shared_ptr<aeron::Counter> processCounter;
    try
    {
        std::uint8_t key[sizeof(std::int64_t)];
        std::memcpy(key, &c.instanceId, sizeof(key));
        const std::int64_t registration = aeron->addCounter(PROCESS_COUNTER_TYPE_ID, key, sizeof(key),
            std::string(id.processName) + " duty iterations instance " + std::to_string(c.instanceId));
        const std::int64_t deadline = nowNs() + c.driverTimeoutMs * 1'000'000LL;
        while (nullptr == (processCounter = aeron->findCounter(registration)))
        {
            if (nowNs() >= deadline)
            {
                throw std::runtime_error("the media driver did not answer the counter registration within " +
                    std::to_string(c.driverTimeoutMs) + " ms");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "%s: the process counter could not be registered: %s (exit 70)\n", id.processName, e.what());
        finish(exit_code::SOFTWARE);
    }

    watch = std::make_unique<rt::ConductorWatch>(aeron, c.driverTimeoutMs);
    if (!in.hooks.ignoreDriverLoss)
    {
        supervisor.setLivenessCheck([&](std::int64_t, std::string &why) { return watch->check(why); });
    }
    driverConnected.store(true, std::memory_order_release);
    if (front)
    {
        front->start();
    }

    const bool busy = IdleMode::BUSY == c.idle;
    const std::int64_t publishIntervalNs = c.statusIntervalMs * 1'000'000LL;
    const int code = supervisor.run(
        [&]() -> int {
            rt::IdleStrategy idle(rt::FORWARDER_IDLE);
            Status status;
            std::int64_t lastPublishNs = 0;
            std::unique_ptr<DutyWork> duty;
            if (in.makeDuty)
            {
                DutyContext context;
                context.aeron = aeron;
                context.monitor = monitor;
                context.log = &log;
                duty = in.makeDuty(context);
            }
            log.push(rt::LogLevel::INFO, nowNs(), "%s-duty: running%s", id.threadPrefix,
                duty ? "" : " (no cluster session in this build)");
            int dutyExit = exit_code::CLEAN;
            while (!supervisor.stopRequested.load(std::memory_order_acquire))
            {
                const std::int64_t start = nowNs();
                board.beat();
                lastBeatNs.store(start, std::memory_order_release);
                if (in.hooks.onDutyIteration)
                {
                    in.hooks.onDutyIteration();
                }
                const int work = duty ? duty->doWork() : 0;
                if (duty && 0 != duty->exitCode())
                {
                    dutyExit = duty->exitCode();
                    log.push(rt::LogLevel::CRITICAL, nowNs(), "%s-duty: stopping the process (exit %d)", id.threadPrefix,
                        dutyExit);
                    break;
                }
                status.dutyIterations++;
                processCounter->setOrdered(static_cast<std::int64_t>(status.dutyIterations));
                if (start - lastPublishNs >= publishIntervalNs)
                {
                    status.aeronErrors = monitor->errorCount();
                    if (duty)
                    {
                        duty->fillStatus(status);
                    }
                    if (board.tryPublish(status))
                    {
                        lastPublishNs = start;
                    }
                }
                const std::int64_t cycleUs = (nowNs() - start) / 1000;
                if (cycleUs > status.dutyMaxCycleUs)
                {
                    status.dutyMaxCycleUs = cycleUs;
                }
                if (!busy)
                {
                    idle.idle(work);
                }
            }
            if (duty)
            {
                duty->close();
                duty.reset(); // on this thread, before the Aeron client closes
            }
            log.push(rt::LogLevel::INFO, nowNs(), "%s-duty: stopped", id.threadPrefix);
            return dutyExit;
        },
        nullptr);

    // the stop path, every step bounded
    int exitCode = code;
    if (front && !front->stop(c.stopTimeoutMs))
    {
        std::fprintf(stderr, "%s: the public listener or a worker pool did not stop within %" PRId64 " ms (exit 70)\n",
            id.processName, c.stopTimeoutMs);
        std::fprintf(stderr, "%s: stopped (exit %d)\n", id.processName, exit_code::SOFTWARE);
        finish(exit_code::SOFTWARE);
    }
    admin.shutdown();
    const std::int64_t boundNs = c.stopTimeoutMs * 1'000'000LL;
    const std::int64_t stopStart = nowNs();
    while (!admin.finished() && nowNs() - stopStart < boundNs)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (admin.finished())
    {
        adminThread.join();
    }
    else
    {
        std::fprintf(stderr, "%s: %s-admin did not stop within %" PRId64 " ms (exit 70)\n", id.processName, id.threadPrefix,
            c.stopTimeoutMs);
        // The detached thread may still be inside a request reading the watch and the Aeron client: nothing it can
        // reach is torn down. The process exits here; the embedded driver dir is reclaimed by the next start.
        adminThread.detach();
        std::fprintf(stderr, "%s: stopped (exit %d)\n", id.processName, exit_code::SOFTWARE);
        finish(exit_code::SOFTWARE);
    }
    monitor->beginDeliberateClose();
    watch.reset();
    processCounter.reset();
    aeron.reset();
    if (driver && !driver->close(c.stopTimeoutMs))
    {
        std::fprintf(stderr, "%s: the embedded driver did not close within %" PRId64 " ms (exit 70)\n", id.processName,
            c.stopTimeoutMs);
        exitCode = exit_code::SOFTWARE;
    }
    std::fprintf(stderr, "%s: stopped (exit %d)\n", id.processName, exitCode);
    finish(exitCode);
}

}}
