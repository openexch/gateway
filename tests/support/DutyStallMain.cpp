// Test build only, never published: the production main of oe-gateway (or oe-stream with GW_TC_STREAM) plus a hook
// that halts the duty thread once SIGUSR2 arrives (the duty stops beating while main, the admin thread and the media
// driver keep running). The process must then read /health 503 and exit 70 on the duty watchdog.
#include <pthread.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

#include "cluster_tools/runtime/Supervisor.h"

#if defined(GW_TC_STREAM)
#include "StreamApp.h"
#else
#include "GatewayApp.h"
#endif

namespace {
std::atomic<bool> g_stall{ false };
}

int main()
{
    cluster_tools::runtime::Supervisor::blockSignals();
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    sigset_t usr2;
    sigemptyset(&usr2);
    sigaddset(&usr2, SIGUSR2);
    pthread_sigmask(SIG_BLOCK, &usr2, nullptr);
    std::fprintf(stderr, "TEST BUILD: SIGUSR2 halts the duty thread\n");
    std::thread waiter([usr2] {
        int sig = 0;
        if (0 == sigwait(&usr2, &sig))
        {
            std::fprintf(stderr, "TEST BUILD: SIGUSR2: halting the duty thread\n");
            g_stall.store(true, std::memory_order_release);
        }
    });
    waiter.detach();
    openexchange::gateway::ProcessHooks hooks;
    hooks.onDutyIteration = [] {
        while (g_stall.load(std::memory_order_acquire))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    };
#if defined(GW_TC_STREAM)
    return openexchange::gateway::streamMain(hooks);
#else
    return openexchange::gateway::gatewayMain(hooks);
#endif
}
