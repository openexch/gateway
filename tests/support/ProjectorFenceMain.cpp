// Test build only, never published: the production main of oe-projector plus a synthetic writer that commits one
// probe row per batch under the fence, so the single-writer rows can see a double write. Each batch inserts
// (seq, token) into oe_projection_fence_probe, where seq is the state row's applied_seq + 1 as this copy last read or
// committed it, and moves applied_seq to seq in the same transaction. The probe table has no unique key: two writers
// that both commit show as two rows of one seq.
//
// SIGUSR2: the next batch stops the whole process (SIGSTOP) after its writes and before its fence, the point where a
// writer stopped past its lease is most dangerous; SIGRTMIN + 3: after its fence and before its COMMIT, where it holds the
// state row's lock. The test resumes it with SIGCONT.
//
// Built three ways: with the shipped fence (oe-projector-tc-fence), and as the controls PROJ_NC_NO_FENCE (the takeover
// ignores the lease, the state update ignores the token) and PROJ_NC_SELECT_FENCE (the token is checked by a SELECT
// before the writes), which compile projector/ProjectionStore.cpp with that rule switched off.
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

#include "cluster_tools/runtime/Supervisor.h"

#include "ProjectorApp.h"

namespace {

using namespace openexchange::gateway;

std::atomic<bool> g_holdBeforeFence{ false };  // SIGUSR2
std::atomic<bool> g_holdBeforeCommit{ false }; // SIGRTMIN + 3 (SIGUSR1 is the supervisor's)

void holdHere(std::atomic<bool> &flag, const char *where, const sigset_t &cont)
{
    if (flag.exchange(false, std::memory_order_acq_rel))
    {
        std::fprintf(stderr, "TEST BUILD: stopping inside a batch, %s\n", where);
        std::fflush(stderr);
        ::kill(::getpid(), SIGSTOP);
        int sig = 0;
        sigwait(&cont, &sig);
        std::fprintf(stderr, "TEST BUILD: resumed inside the batch\n");
    }
}

class FenceProbeWork final : public FencedWork
{
public:
    explicit FenceProbeWork(std::int64_t intervalMs) : m_intervalNs(intervalMs * 1'000'000LL) {}

    void onWriter(const projector::ProjectionState &state) override
    {
        m_next = state.appliedSeq + 1;
        m_epoch = state.epoch.has_value();
        m_writer = true;
        std::fprintf(stderr, "TEST BUILD: probe writer starts at seq %lld\n", static_cast<long long>(m_next));
    }
    void onStandby() override { m_writer = false; }
    bool due(std::int64_t nowNs) override
    {
        if (!m_writer || nowNs - m_lastNs < m_intervalNs)
        {
            return false;
        }
        m_lastNs = nowNs;
        return true;
    }
    bool write(PgConn &conn, const std::string &token, projector::StateAdvance &advance, std::string &error) override
    {
        pg::Result r;
        if (pg::Exec::OK != conn.exec("CREATE TABLE IF NOT EXISTS oe_projection_fence_probe (seq BIGINT NOT NULL, "
                "token TEXT NOT NULL)", {}, 5000, r, error, false))
        {
            return false;
        }
        if (pg::Exec::OK != conn.exec("INSERT INTO oe_projection_fence_probe (seq, token) VALUES ($1, $2)",
                { std::to_string(m_next), token }, 5000, r, error, false))
        {
            return false;
        }
        advance.appliedSeq = m_next;
        if (!m_epoch)
        {
            advance.epoch = 42; // a cursor above 0 needs a genesis: this probe writer names one
        }
        return true;
    }
    void committed(const projector::StateAdvance &advance) override
    {
        m_next = *advance.appliedSeq + 1;
        m_epoch = true;
    }

private:
    std::int64_t m_intervalNs;
    std::int64_t m_lastNs = 0;
    std::int64_t m_next = 1;
    bool m_writer = false;
    bool m_epoch = false;
};

}

int main(int argc, char **argv)
{
    cluster_tools::runtime::Supervisor::blockSignals();
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    sigset_t usr;
    sigemptyset(&usr);
    sigaddset(&usr, SIGRTMIN + 3);
    sigaddset(&usr, SIGUSR2);
    pthread_sigmask(SIG_BLOCK, &usr, nullptr);
    // SIGCONT blocked in every thread (it still resumes a stopped process): the batch that stops itself waits for it,
    // so it cannot run past its stop point even when the stop lands late
    sigset_t cont;
    sigemptyset(&cont);
    sigaddset(&cont, SIGCONT);
    pthread_sigmask(SIG_BLOCK, &cont, nullptr);
    std::fprintf(stderr, "TEST BUILD: a probe row per batch; SIGUSR2 (SIGRTMIN + 3) stops the process inside the next batch "
        "before its fence (before its COMMIT)\n");
    std::thread waiter([usr] {
        for (;;)
        {
            int sig = 0;
            if (0 == sigwait(&usr, &sig))
            {
                (SIGUSR2 == sig ? g_holdBeforeFence : g_holdBeforeCommit).store(true, std::memory_order_release);
            }
        }
    });
    waiter.detach();
    static ProjectorHooks hooks;
    hooks.makeWork = [] { return std::make_unique<FenceProbeWork>(50); };
    hooks.beforeFence = [cont] { holdHere(g_holdBeforeFence, "before its fence", cont); };
    hooks.beforeCommit = [cont] { holdHere(g_holdBeforeCommit, "after its fence, before its COMMIT", cont); };
    return projectorMain(argc, argv, hooks);
}
