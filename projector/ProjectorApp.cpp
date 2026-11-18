#include "ProjectorApp.h"

#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/Admin.h"
#include "gateway/common/Environment.h"
#include "gateway/common/ExitCodes.h"

#include "ProjectorSettings.h"

namespace openexchange { namespace gateway {

namespace {

namespace rt = cluster_tools::runtime;
using projector::Hold;
using projector::ProjectionState;
using projector::SchemaVerdict;
using projector::StateAdvance;
using projector::WriterLease;

std::int64_t nowNs()
{
    return rt::Supervisor::monotonicNs();
}

projector::PgOptions pgOptions(const ProjectorSettings &s)
{
    projector::PgOptions o;
    o.url = s.pgUrl;
    o.statementTimeoutMs = s.pgStatementTimeoutMs;
    o.idleInTxTimeoutMs = s.pgIdleInTxTimeoutMs;
    o.keepaliveIdleS = s.pgKeepaliveIdleS;
    o.keepaliveIntervalS = s.pgKeepaliveIntervalS;
    o.keepaliveCount = s.pgKeepaliveCount;
    return o;
}

std::string q(const std::string &text)
{
    return "\"" + jsonEscape(text) + "\"";
}

// pj-pg: the schema, the lease and the fenced write transactions. The process shell runs it as the binary's "front":
// started once the driver is connected, stopped (bounded) on the stop path, read by the admin thread.
class ProjectionWriter final : public FrontWork
{
public:
    ProjectionWriter(const ProjectorSettings &s, ProjectorShared &shared, const ProjectorHooks &hooks) :
        m_settings(s), m_shared(shared), m_hooks(hooks), m_lease(s.leaseMs),
        m_work(hooks.makeWork ? hooks.makeWork() : nullptr)
    {
    }
    ~ProjectionWriter() override { stop(m_settings.common.stopTimeoutMs); }

    bool bind(std::string &) override { return true; } // no listener
    void start() override
    {
        m_thread = std::thread([this] { run(); });
    }
    bool stop(std::int64_t boundMs) override
    {
        {
            std::lock_guard<std::mutex> lock(m_lock);
            m_stop = true;
        }
        m_wake.notify_all();
        if (!m_thread.joinable())
        {
            return true;
        }
        const std::int64_t deadline = nowNs() + boundMs * 1'000'000LL;
        while (!m_finished.load(std::memory_order_acquire) && nowNs() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (!m_finished.load(std::memory_order_acquire))
        {
            m_thread.detach(); // the process exits 70 next; nothing this thread reaches is torn down before
            return false;
        }
        m_thread.join();
        return true;
    }
    bool accepting() const override { return true; }

    void fillStatus(Status &status) const override
    {
        std::lock_guard<std::mutex> lock(m_viewLock);
        status.leaseHeld = leaseHeldNow(nowNs());
        status.halted = m_view.state.haltReason.has_value();
        status.lastBeatNs = -1; // the beat is written by the head-probe rule, which this version does not run
    }

    std::string healthJson() const override
    {
        const std::int64_t now = nowNs();
        std::lock_guard<std::mutex> lock(m_viewLock);
        const View &v = m_view;
        const ProjectionState &st = v.state;
        std::string out = ",\"projector\":{";
        // "starting" until the first takeover attempt answered (a copy that has not asked is not known to be a standby)
        out += "\"role\":" + q(leaseHeldNow(now) ? "writer" : v.decided ? "standby" : "starting");
        out += ",\"pg\":{\"connected\":" + std::string(v.pgConnected ? "true" : "false") + ",\"failures\":" +
            std::to_string(v.pgFailures) + ",\"lastError\":" + q(v.lastPgError) + "}";
        out += ",\"schema\":{\"version\":" + std::to_string(projector::PROJECTION_SCHEMA_VERSION) + ",\"sha256\":" +
            q(projector::projectionSchemaSha256()) + ",\"state\":" + q(v.schema) + "}";
        out += ",\"lease\":{\"held\":" + std::string(leaseHeldNow(now) ? "true" : "false") + ",\"leaseMs\":" +
            std::to_string(m_settings.leaseMs) + ",\"confirmedAgeMs\":" +
            (v.lastConfirmNs > 0 ? std::to_string((now - v.lastConfirmNs) / 1'000'000LL) : std::string("null")) +
            ",\"token\":" + (v.tokenPrefix.empty() ? std::string("null") : q(v.tokenPrefix)) + ",\"takeovers\":" +
            std::to_string(v.takeovers) + ",\"lost\":" + std::to_string(v.leaseLost) + "}";
        out += ",\"cursor\":{\"epoch\":" + (st.epoch ? std::to_string(*st.epoch) : std::string("null")) +
            ",\"appliedSeq\":" + std::to_string(st.appliedSeq) + ",\"appliedLogPosition\":" +
            std::to_string(st.appliedLogPosition) + ",\"appliedOrderId\":" + std::to_string(st.appliedOrderId) + "}";
        out += ",\"halt\":{\"reason\":" + (st.haltReason ? q(*st.haltReason) : std::string("null")) + "}";
        out += ",\"follow\":{\"replaysOpen\":" + std::to_string(m_shared.replaysOpen.load()) + ",\"sessionOpen\":" +
            (m_shared.sessionOpen.load() ? "true" : "false") + "}";
        out += ",\"batches\":" + std::to_string(v.batches) + "}";
        return out;
    }

    std::string metrics(const std::string &p) const override
    {
        const std::int64_t now = nowNs();
        std::lock_guard<std::mutex> lock(m_viewLock);
        const View &v = m_view;
        std::string out;
        auto line = [&](const char *type, const std::string &name, const std::string &help, const std::string &value) {
            out += "# HELP " + p + name + " " + help + "\n# TYPE " + p + name + " " + type + "\n" + p + name + " " +
                value + "\n";
        };
        line("gauge", "lease_held", "1 while this copy holds the writer lease (0 = standby).",
            leaseHeldNow(now) ? "1" : "0");
        line("counter", "takeovers_total", "Writer leases this copy took.", std::to_string(v.takeovers));
        line("counter", "lease_lost_total", "Writer leases this copy lost (a fence or a renewal updated zero rows, or "
            "the lease went unconfirmed for the lease less one renewal interval).", std::to_string(v.leaseLost));
        line("gauge", "pg_connected", "1 while the projection database connection is up.", v.pgConnected ? "1" : "0");
        line("counter", "pg_failures_total", "Failed connects and statements on the projection database.",
            std::to_string(v.pgFailures));
        line("gauge", "schema_ready", "1 once the projection schema is applied or verified.",
            "applied" == v.schema || "present" == v.schema ? "1" : "0");
        line("counter", "batches_total", "Fenced write transactions committed.", std::to_string(v.batches));
        line("gauge", "applied_seq", "The cursor: every journalSeq up to it is applied (as last read or committed).",
            std::to_string(v.state.appliedSeq));
        line("gauge", "replays_open", "Replays this copy holds.", std::to_string(m_shared.replaysOpen.load()));
        return out;
    }

private:
    struct View
    {
        bool pgConnected = false;
        std::uint64_t pgFailures = 0;
        std::string lastPgError;
        std::string schema = "pending";
        bool decided = false; // a takeover attempt answered (writer or standby)
        bool leaseHeld = false;
        std::int64_t lastConfirmNs = 0;
        std::string tokenPrefix;
        std::uint64_t takeovers = 0;
        std::uint64_t leaseLost = 0;
        std::uint64_t batches = 0;
        ProjectionState state;
    };

    // the lease counts as held while its last confirmation is younger than the lease less one renewal interval (the
    // database counts the lease from the fence, which is later than the confirmation recorded here); pj-pg drops it
    // past that, at the latest one wait interval later, or once its statement returns
    bool leaseHeldNow(std::int64_t now) const
    {
        return m_view.leaseHeld && now - m_view.lastConfirmNs < writerHoldNs(m_settings.leaseMs);
    }

    // pj-pg confirmed the lease by a statement it sent at `sentNs`: pj-duty may hold replays and the session until the
    // step-down point measured from then
    void confirmed(std::int64_t sentNs)
    {
        m_shared.writerUntilNs.store(sentNs + writerHoldNs(m_settings.leaseMs), std::memory_order_release);
    }

    template<typename F>
    void update(F f)
    {
        std::lock_guard<std::mutex> lock(m_viewLock);
        f(m_view);
    }

    // waits up to `ms` (or until stop); true when stopping
    bool waitMs(std::int64_t ms)
    {
        std::unique_lock<std::mutex> lock(m_lock);
        m_wake.wait_for(lock, std::chrono::milliseconds(std::max<std::int64_t>(1, ms)), [this] { return m_stop; });
        return m_stop;
    }

    bool stopping()
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_stop;
    }

    void pgFailed(const std::string &what, const std::string &error)
    {
        update([&](View &v) {
            v.pgFailures++;
            v.lastPgError = what + ": " + error;
            v.pgConnected = m_conn.ok();
        });
        std::uint64_t held = 0;
        if (m_warn.take(m_settings.pgRetryMs * 1'000'000LL, held))
        {
            std::fprintf(stderr, "oe-projector: WARN PostgreSQL: %s: %s (%llu more held back)\n", what.c_str(),
                error.c_str(), static_cast<unsigned long long>(held));
        }
    }

    void becomeWriter(const ProjectionState &state, std::int64_t now)
    {
        update([&](View &v) {
            v.decided = true;
            v.leaseHeld = true;
            v.lastConfirmNs = now;
            v.tokenPrefix = m_lease.token().substr(0, 8);
            v.takeovers++;
            v.state = state;
        });
        confirmed(now);
        m_shared.writer.store(true, std::memory_order_release);
        std::fprintf(stderr, "oe-projector: writer: lease taken (token %.8s..., %lld ms) at applied_seq %lld, epoch %s\n",
            m_lease.token().c_str(), static_cast<long long>(m_settings.leaseMs),
            static_cast<long long>(state.appliedSeq), state.epoch ? std::to_string(*state.epoch).c_str() : "none");
        m_standbyLogged = false;
        if (m_work)
        {
            m_work->onWriter(state);
        }
    }

    void loseLease(const std::string &why, std::int64_t now)
    {
        m_lease.drop();
        m_shared.writer.store(false, std::memory_order_release);
        update([&](View &v) {
            v.leaseHeld = false;
            v.leaseLost++;
        });
        std::fprintf(stderr, "oe-projector: CRITICAL writer lease lost: %s; replays and the session are dropped, this "
            "copy is a standby\n", why.c_str());
        if (m_work)
        {
            m_work->onStandby();
        }
        m_nextPollNs = now + m_settings.leasePollMs * 1'000'000LL;
    }

    void run()
    {
        pthread_setname_np(pthread_self(), "pj-pg");
        const projector::PgOptions options = pgOptions(m_settings);
        const std::int64_t t = m_settings.pgStatementTimeoutMs;
        const std::int64_t leaseNs = m_settings.leaseMs * 1'000'000LL;
        const std::int64_t renewNs = std::max<std::int64_t>(1, leaseNs / 3);
        // the step-down point: a confirmation is the time its statement was sent, which is no later than the database's
        // clock at the fence; the copy gives the lease up one renewal interval before the database could hand it on
        const std::int64_t holdNs = writerHoldNs(m_settings.leaseMs);
        std::int64_t retryAt = 0;
        bool schemaReady = false;
        bool reassert = false;
        std::int64_t lastConfirmNs = 0;
        while (!stopping())
        {
            const std::int64_t now = nowNs();
            if (m_lease.held() && now - lastConfirmNs >= holdNs)
            {
                loseLease("not confirmed within the lease less one renewal interval (" +
                    std::to_string(holdNs / 1'000'000LL) + " of " + std::to_string(m_settings.leaseMs) +
                    " ms): PostgreSQL unreachable or too slow", now);
            }
            if (!m_conn.ok())
            {
                if (now < retryAt)
                {
                    waitMs((retryAt - now) / 1'000'000LL + 1);
                    continue;
                }
                std::string error;
                if (!projector::connectProjection(m_conn, options, error))
                {
                    pgFailed("connect", error);
                    retryAt = now + m_settings.pgRetryMs * 1'000'000LL;
                    continue;
                }
                update([](View &v) { v.pgConnected = true; });
                reassert = m_lease.held(); // read the state row again before anything is applied
            }
            if (!schemaReady)
            {
                std::string detail;
                const SchemaVerdict verdict = projector::migrate(m_conn, t, detail);
                if (SchemaVerdict::FOREIGN == verdict)
                {
                    std::fprintf(stderr, "oe-projector: CRITICAL the projection database holds something else: %s "
                        "(exit 78)\n", detail.c_str());
                    update([](View &v) { v.schema = "foreign"; });
                    m_shared.fatalExit.store(exit_code::CONFIG, std::memory_order_release);
                    break;
                }
                if (SchemaVerdict::FAILED == verdict)
                {
                    pgFailed("schema", detail);
                    m_conn.close();
                    retryAt = now + m_settings.pgRetryMs * 1'000'000LL;
                    continue;
                }
                schemaReady = true;
                update([&](View &v) { v.schema = projector::nameOf(verdict); });
                std::fprintf(stderr, "oe-projector: projection schema: %s (sha256 %s)\n", detail.c_str(),
                    projector::projectionSchemaSha256());
            }
            std::string error;
            ProjectionState state;
            if (!m_lease.held())
            {
                if (now < m_nextPollNs)
                {
                    waitMs((m_nextPollNs - now) / 1'000'000LL + 1);
                    continue;
                }
                switch (m_lease.takeover(m_conn, t, state, error))
                {
                    case WriterLease::Take::WRITER:
                        lastConfirmNs = now;
                        reassert = false;
                        becomeWriter(state, now);
                        break;
                    case WriterLease::Take::STANDBY:
                        update([](View &v) { v.decided = true; });
                        m_nextPollNs = now + m_settings.leasePollMs * 1'000'000LL;
                        if (!m_standbyLogged)
                        {
                            std::fprintf(stderr, "oe-projector: standby: another copy holds the writer lease (polling "
                                "every %lld ms)\n", static_cast<long long>(m_settings.leasePollMs));
                            m_standbyLogged = true;
                        }
                        break;
                    case WriterLease::Take::FAILED:
                        pgFailed("takeover", error);
                        m_conn.close();
                        retryAt = now + m_settings.pgRetryMs * 1'000'000LL;
                        break;
                }
                continue;
            }
            Hold h = Hold::HELD;
            const char *step = "renew";
            StateAdvance advance;
            bool committed = false;
            if (reassert)
            {
                h = m_lease.renew(m_conn, t, state, error);
                if (Hold::HELD == h)
                {
                    reassert = false;
                    update([&](View &v) { v.state = state; });
                    if (m_work)
                    {
                        m_work->onWriter(state);
                    }
                }
            }
            else if (m_work && m_work->due(now))
            {
                step = "batch";
                h = m_lease.commitFenced(m_conn, t,
                    [&](PgConn &c, std::string &e) { return m_work->write(c, m_lease.token(), advance, e); }, advance,
                    error, m_hooks.beforeFence, m_hooks.beforeCommit);
                committed = Hold::HELD == h;
            }
            else if (now - lastConfirmNs >= renewNs)
            {
                h = m_lease.renew(m_conn, t, state, error);
                if (Hold::HELD == h)
                {
                    update([&](View &v) { v.state = state; });
                }
            }
            else
            {
                const std::int64_t untilRenew = lastConfirmNs + renewNs - now;
                waitMs(std::min<std::int64_t>(untilRenew, m_settings.batchMs * 1'000'000LL) / 1'000'000LL + 1);
                continue;
            }
            const std::int64_t after = nowNs();
            switch (h)
            {
                case Hold::HELD:
                    // confirmed as of `now`, when the statement was sent: the database counted the lease from its
                    // fence, which ran no earlier, so a slow COMMIT shortens what is left of the lease here too
                    lastConfirmNs = now;
                    confirmed(now);
                    update([&](View &v) {
                        v.lastConfirmNs = now;
                        if (committed)
                        {
                            v.batches++;
                            if (advance.epoch) { v.state.epoch = advance.epoch; }
                            if (advance.appliedSeq) { v.state.appliedSeq = *advance.appliedSeq; }
                            if (advance.appliedLogPosition) { v.state.appliedLogPosition = *advance.appliedLogPosition; }
                            if (advance.appliedOrderId) { v.state.appliedOrderId = *advance.appliedOrderId; }
                        }
                    });
                    if (committed && m_work)
                    {
                        m_work->committed(advance);
                    }
                    break;
                case Hold::LOST:
                    loseLease(std::string(step) + ": " + error, after);
                    break;
                case Hold::FAILED:
                    pgFailed(step, error);
                    m_conn.close();
                    reassert = true;
                    retryAt = after + m_settings.pgRetryMs * 1'000'000LL;
                    break;
            }
        }
        if (m_lease.held() && m_conn.ok())
        {
            m_lease.release(m_conn, t);
            std::fprintf(stderr, "oe-projector: writer lease released\n");
        }
        m_shared.writer.store(false, std::memory_order_release);
        m_conn.close();
        m_finished.store(true, std::memory_order_release);
    }

    const ProjectorSettings &m_settings;
    ProjectorShared &m_shared;
    const ProjectorHooks &m_hooks;
    WriterLease m_lease;
    std::unique_ptr<FencedWork> m_work;
    PgConn m_conn;
    pg::WarnLimiter m_warn;
    std::int64_t m_nextPollNs = 0;
    bool m_standbyLogged = false;

    std::thread m_thread;
    std::atomic<bool> m_finished{ false };
    std::mutex m_lock;
    std::condition_variable m_wake;
    bool m_stop = false;

    mutable std::mutex m_viewLock;
    View m_view;
};

// pj-duty: attaches to the journal sources and the head-probe session only while this copy is the writer, and only
// inside the step-down point of pj-pg's last confirmation (writerNow), so a pj-pg blocked in a slow statement cannot
// keep them past the lease. This version holds neither (no replay, no session); the transitions are where they start
// and stop. On a clean stop the process shell joins pj-duty (DutyWork::close) before it stops pj-pg, so the lease is
// released after the replays and the session are dropped.
class ProjectorDuty final : public DutyWork
{
public:
    ProjectorDuty(const DutyContext &context, ProjectorShared &shared) : m_log(context.log), m_shared(shared) {}

    int doWork() override
    {
        const bool writer = writerNow(m_shared, nowNs());
        if (writer == m_attached)
        {
            return 0;
        }
        m_attached = writer;
        if (nullptr != m_log)
        {
            m_log->push(rt::LogLevel::INFO, nowNs(), "pj-duty: %s", writer
                    ? "writer: the journal sources and the head-probe session may be attached"
                    : "standby: no replay and no session are held");
        }
        return 1;
    }

    void fillStatus(Status &) const override {}
    int exitCode() const override { return m_shared.fatalExit.load(std::memory_order_acquire); }
    void close() override { m_shared.writer.store(false, std::memory_order_release); }

private:
    rt::LogRing *m_log;
    ProjectorShared &m_shared;
    bool m_attached = false;
};

// Sleeps `ms` unless SIGTERM or SIGINT arrives (the signals are blocked in every thread by main): true when signalled.
bool interrupted(std::int64_t ms)
{
    sigset_t stop;
    sigemptyset(&stop);
    sigaddset(&stop, SIGTERM);
    sigaddset(&stop, SIGINT);
    const timespec wait{ static_cast<time_t>(ms / 1000), static_cast<long>((ms % 1000) * 1'000'000L) };
    const int sig = ::sigtimedwait(&stop, nullptr, &wait);
    return SIGTERM == sig || SIGINT == sig;
}

int runRebuild(const ProjectorSettings &s)
{
    const char *me = "oe-projector --rebuild";
    const projector::PgOptions options = pgOptions(s);
    const std::int64_t t = s.pgStatementTimeoutMs;
    PgConn conn;
    WriterLease lease(s.leaseMs);
    bool schemaReady = false;
    bool waitingLogged = false;
    bool unreachableLogged = false;
    std::fprintf(stderr, "%s: waiting for the writer lease (lease %lld ms, polled every %lld ms)\n", me,
        static_cast<long long>(s.leaseMs), static_cast<long long>(s.leasePollMs));
    for (;;)
    {
        std::string error;
        if (!conn.ok() && !projector::connectProjection(conn, options, error))
        {
            if (!unreachableLogged)
            {
                std::fprintf(stderr, "%s: PostgreSQL unreachable: %s (retried every %lld ms)\n", me, error.c_str(),
                    static_cast<long long>(s.pgRetryMs));
                unreachableLogged = true;
            }
            if (interrupted(s.pgRetryMs))
            {
                std::fprintf(stderr, "%s: stopped by a signal before the rebuild: nothing changed (exit 75)\n", me);
                return exit_code::TEMPFAIL;
            }
            continue;
        }
        if (!schemaReady)
        {
            const SchemaVerdict verdict = projector::migrate(conn, t, error);
            if (SchemaVerdict::FOREIGN == verdict)
            {
                std::fprintf(stderr, "%s: the projection database holds something else: %s (exit 78)\n", me,
                    error.c_str());
                return exit_code::CONFIG;
            }
            if (SchemaVerdict::FAILED == verdict)
            {
                conn.close();
                if (interrupted(s.pgRetryMs))
                {
                    return exit_code::TEMPFAIL;
                }
                continue;
            }
            schemaReady = true;
        }
        ProjectionState state;
        const WriterLease::Take take = lease.takeover(conn, t, state, error);
        if (WriterLease::Take::WRITER == take)
        {
            std::fprintf(stderr, "%s: lease taken at applied_seq %lld, epoch %s\n", me,
                static_cast<long long>(state.appliedSeq),
                state.epoch ? std::to_string(*state.epoch).c_str() : "none");
            break;
        }
        if (WriterLease::Take::FAILED == take)
        {
            conn.close();
        }
        else if (!waitingLogged)
        {
            std::fprintf(stderr, "%s: another copy holds the writer lease: waiting until it is released or expires "
                "(stop the running writer to rebuild now)\n", me);
            waitingLogged = true;
        }
        if (interrupted(WriterLease::Take::FAILED == take ? s.pgRetryMs : s.leasePollMs))
        {
            std::fprintf(stderr, "%s: stopped by a signal before the rebuild: nothing changed (exit 75)\n", me);
            return exit_code::TEMPFAIL;
        }
    }
    std::string error;
    const Hold h = lease.rebuild(conn, t, s.rebuildLockTimeoutMs, error);
    if (Hold::HELD != h)
    {
        lease.release(conn, t); // best effort: the service copies need not wait out the lease
        std::fprintf(stderr, "%s: the rebuild did not commit (%s): %s (exit 75; it is safe to run again)\n", me,
            projector::nameOf(h), error.c_str());
        return exit_code::TEMPFAIL;
    }
    lease.release(conn, t);
    std::fprintf(stderr, "%s: every base table truncated, the state row reset (epoch, applied_seq, beat_at, halt); "
        "history answers 503 until the projection beats again; lease released (exit 0)\n", me);
    return exit_code::CLEAN;
}

}

bool writerNow(const ProjectorShared &shared, std::int64_t nowNs)
{
    return shared.writer.load(std::memory_order_acquire) &&
        nowNs < shared.writerUntilNs.load(std::memory_order_acquire);
}

std::int64_t writerHoldNs(std::int64_t leaseMs)
{
    const std::int64_t leaseNs = leaseMs * 1'000'000LL;
    return leaseNs - std::max<std::int64_t>(1, leaseNs / 3);
}

int projectorMain(int argc, char **argv, const ProjectorHooks &hooks)
{
    bool rebuild = false;
    for (int i = 1; i < argc; ++i)
    {
        if (0 == std::strcmp("--rebuild", argv[i]))
        {
            rebuild = true;
        }
        else
        {
            std::fprintf(stderr, "oe-projector: unknown argument '%s'\nusage: oe-projector [--rebuild] (settings from "
                "the environment, HISTORY_PROJECTOR_*)\n", argv[i]);
            return EXIT_USAGE;
        }
    }
    const SystemEnvironment env;
    static ParseResult<ProjectorSettings> parsed = parseProjectorSettings(env);
    if (!parsed.ok())
    {
        return refuseConfiguration("oe-projector", parsed.errors);
    }
    const ProjectorSettings &s = parsed.settings;
    for (const std::string &w : s.warnings)
    {
        std::fprintf(stderr, "oe-projector: WARN config: %s\n", w.c_str());
    }
    if (rebuild)
    {
        return runRebuild(s);
    }
    ProcessIdentity identity;
    identity.role = Role::PROJECTOR;
    identity.processName = "oe-projector";
    identity.threadPrefix = "pj";
    identity.dirPrefix = "oe-projector-";
    identity.instanceKey = "HISTORY_PROJECTOR_INSTANCE_ID";
    identity.adminBindKey = "HISTORY_PROJECTOR_ADMIN_BIND";
    identity.frontBindKey = "HISTORY_PROJECTOR_PG_URL";
    ProcessInputs inputs;
    inputs.common = &s.common;
    inputs.records = &parsed.records;
    inputs.projectorBounds.headFreshMs = s.headFreshMs;
    inputs.projectorBounds.readyLagMs = s.readyLagMs;
    inputs.hooks = hooks.process;
    // live until the process exits (runProcess does not return)
    static ProjectorShared shared;
    inputs.makeDuty = [](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return std::make_unique<ProjectorDuty>(context, shared);
    };
    inputs.makeFront = [&s, &hooks]() -> std::unique_ptr<FrontWork> {
        return std::make_unique<ProjectionWriter>(s, shared, hooks);
    };
    runProcess(identity, inputs);
}

}}
