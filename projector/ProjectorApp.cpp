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

#include "FollowDuty.h"
#include "HistoryWork.h"
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
    ProjectionWriter(const ProjectorSettings &s, ProjectorShared &shared, const ProjectorHooks &hooks,
        const projector::FollowParts &parts, projector::WriterStats &stats) :
        m_settings(s), m_shared(shared), m_hooks(hooks), m_lease(s.leaseMs), m_parts(parts), m_stats(stats),
        m_work(hooks.makeWork ? hooks.makeWork() : makeHistoryWork(s, parts, stats)), m_poller(s, *parts.nodes)
    {
    }

    static std::unique_ptr<FencedWork> makeHistoryWork(const ProjectorSettings &s, const projector::FollowParts &parts,
        projector::WriterStats &stats)
    {
        projector::HistoryWorkSettings w;
        w.batchEvents = s.batchEvents;
        w.batchMs = s.batchMs;
        w.statementTimeoutMs = s.pgStatementTimeoutMs;
        return std::make_unique<projector::HistoryWork>(*parts.exchange, w, stats);
    }
    ~ProjectionWriter() override { stop(m_settings.common.stopTimeoutMs); }

    bool bind(std::string &) override { return true; } // no listener
    void start() override
    {
        m_poller.start();
        m_thread = std::thread([this] { run(); });
    }
    bool stop(std::int64_t boundMs) override
    {
        {
            std::lock_guard<std::mutex> lock(m_lock);
            m_stop = true;
        }
        m_wake.notify_all();
        m_poller.stop();
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
        status.halted = projector::Halt::NONE != m_parts.view->get().halt.halt;
        // the beat counts only while this copy holds the lease: a standby is not ready whatever it beat before
        status.lastBeatNs = leaseHeldNow(nowNs()) ? m_shared.lastBeatNs.load(std::memory_order_acquire) : -1;
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
        const projector::FollowView f = m_parts.view->get();
        out += ",\"halt\":{\"reason\":" + (projector::Halt::NONE != f.halt.halt ? q(projector::nameOf(f.halt.halt))
                : st.haltReason ? q(*st.haltReason) : std::string("null")) + ",\"detail\":" +
            (projector::Halt::NONE != f.halt.halt ? q(f.halt.detail) : std::string("null")) + "}";
        auto n = [](std::uint64_t x) { return std::to_string(x); };
        auto a = [](const std::atomic<std::uint64_t> &x) { return std::to_string(x.load(std::memory_order_relaxed)); };
        out += ",\"follow\":{\"replaysOpen\":" + std::to_string(m_shared.replaysOpen.load()) + ",\"sessionOpen\":" +
            (m_shared.sessionOpen.load() ? "true" : "false");
        out += ",\"generation\":" + n(f.generation) + ",\"source\":" +
            (f.source ? std::to_string(*f.source) : std::string("null")) + ",\"endpoint\":" +
            (f.endpoint.empty() ? std::string("null") : q(f.endpoint)) + ",\"archiveId\":" +
            std::to_string(f.archiveId) + ",\"phase\":" + q(f.phase) + ",\"chainIndex\":" + n(f.chainIndex) +
            ",\"recordingId\":" + std::to_string(f.recordingId) + ",\"lastAttachFrom\":" +
            std::to_string(f.lastAttachFrom) + ",\"lastAttachFromMemo\":" + (f.lastAttachFromMemo ? "true" : "false");
        out += ",\"next\":" + std::to_string(f.next) + ",\"appliedSeq\":" + std::to_string(f.appliedSeq) +
            ",\"epoch\":" + (f.dbEpoch ? std::to_string(*f.dbEpoch) : std::string("null")) + ",\"genesisId\":" +
            std::to_string(f.genesisId) + ",\"headSeq\":" + std::to_string(f.headSeq) + ",\"lagEvents\":" +
            (f.headSeq >= 0 ? std::to_string(std::max<std::int64_t>(0, f.headSeq - f.appliedSeq)) : std::string("null"));
        {
            // lagMs: how far behind the instant the projection is complete as of (the age of its last beat)
            const std::int64_t beatNs = m_shared.lastBeatNs.load(std::memory_order_acquire);
            out += ",\"lagMs\":" + (beatNs >= 0 ? std::to_string((now - beatNs) / 1'000'000LL) : std::string("null")) +
                ",\"beats\":" + std::to_string(m_shared.beats.load()) + ",\"beatsOffered\":" + n(f.beatsOffered) +
                ",\"beatsRefused\":" + n(f.beatsRefused) + ",\"beatsPending\":" + std::to_string(f.beatsPending) +
                ",\"beatsDropped\":" + n(f.beatsDropped);
        }
        out += ",\"sources\":[";
        for (std::size_t i = 0; i < f.sourceStates.size(); ++i)
        {
            out += (i ? "," : "") + q(f.sourceStates[i]);
        }
        out += "]";
        out += ",\"counts\":{\"switches\":" + n(f.switches) + ",\"holes\":" + n(f.holes) + ",\"belowRange\":" +
            n(f.belowRange) + ",\"payloadConflicts\":" + a(m_stats.payloadConflicts) + ",\"schemaRefusals\":" +
            n(f.schemaRefusals) + ",\"mtuRefusals\":" + n(f.mtuRefusals) + ",\"behind\":" + n(f.behind) +
            ",\"leaderAvoided\":" + n(f.leaderAvoided) + ",\"budgetSkips\":" + n(f.budgetSkips) +
            ",\"refusedReplays\":" + n(f.refusedReplays) + ",\"stopRequestsAnswered\":" + n(f.stopRequestsAnswered) +
            ",\"orphanUnknown\":" + n(f.orphanUnknown) + ",\"epochZeroRecords\":" + n(f.epochZeroRecords) +
            ",\"foreignEpochRecordings\":" + n(f.foreignEpochRecordings) + ",\"reReads\":" + n(f.reReads) +
            ",\"replaysStarted\":" + n(f.replaysStarted) + ",\"replayStops\":" + n(f.replayStops) +
            ",\"recordsApplied\":" + a(m_stats.recordsApplied) + ",\"skips\":" + a(m_stats.skips) +
            ",\"replayRows\":" + a(m_stats.replayRows) + ",\"resets\":" + a(m_stats.resets) +
            ",\"uncertainCommits\":" + a(m_stats.uncertainCommits) + ",\"nodeRounds\":" +
            a(m_parts.nodes->rounds) + ",\"nodeFailures\":" + a(m_parts.nodes->failures) + "}}";
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
        line("gauge", "session_open", "1 while this copy holds the cluster session.", m_shared.sessionOpen.load() ? "1" : "0");
        const projector::FollowView f = m_parts.view->get();
        line("gauge", "head_seq", "The core's head at the last answered probe (-1: none).", std::to_string(f.headSeq));
        line("gauge", "halted", "1 while a halt is set.", projector::Halt::NONE != f.halt.halt ? "1" : "0");
        {
            const std::int64_t beatNs = m_shared.lastBeatNs.load(std::memory_order_acquire);
            line("gauge", "lag_ms", "Age of the last beat this copy committed: history is complete as of that long ago "
                "(-1: none).", beatNs >= 0 ? std::to_string((now - beatNs) / 1'000'000LL) : std::string("-1"));
            line("counter", "beats_total", "Beats this copy committed.", std::to_string(m_shared.beats.load()));
            line("gauge", "lag_events", "The core's head at the last answered probe less the cursor (-1: no probe).",
                f.headSeq >= 0 ? std::to_string(std::max<std::int64_t>(0, f.headSeq - f.appliedSeq)) : std::string("-1"));
        }
        line("counter", "records_applied_total", "Journal records applied.",
            std::to_string(m_stats.recordsApplied.load(std::memory_order_relaxed)));
        line("counter", "skips_total", "Records at or below the cursor compared equal with the stored bytes.",
            std::to_string(m_stats.skips.load(std::memory_order_relaxed)));
        line("counter", "payload_conflicts_total", "Records that differ from the stored bytes of their sequence.",
            std::to_string(m_stats.payloadConflicts.load(std::memory_order_relaxed)));
        line("counter", "source_switches_total", "Sources left.", std::to_string(f.switches));
        line("counter", "holes_total", "Holes seen on memo-free reads.", std::to_string(f.holes));
        line("counter", "below_range_total", "Memo-free reads that began above the cursor.", std::to_string(f.belowRange));
        line("counter", "foreign_epoch_recordings_total", "Recordings of another epoch, not walked.",
            std::to_string(f.foreignEpochRecordings));
        line("counter", "refused_replays_total", "Replay requests a source archive refused (its replay cap).",
            std::to_string(f.refusedReplays));
        line("counter", "orphan_stop_requests_answered_total", "Recorded replays of an earlier writer whose stopReplay the "
            "archive answered OK (it also answers OK for a session it no longer knows).",
            std::to_string(f.stopRequestsAnswered));
        line("counter", "orphan_unknown_total", "Recorded replays the archive no longer knew.", std::to_string(f.orphanUnknown));
        line("counter", "behind_total", "Sources left for BEHIND.", std::to_string(f.behind));
        line("counter", "leader_avoided_total", "Switches away from the leader's archive.", std::to_string(f.leaderAvoided));
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
        m_shared.lastBeatNs.store(-1, std::memory_order_release); // a takeover beats anew before it reads ready
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
                bool beat = false;
                h = m_lease.commitFenced(m_conn, t,
                    [&](PgConn &c, std::string &e) { return m_work->write(c, m_lease.token(), advance, e); }, advance,
                    error, m_hooks.beforeFence, m_hooks.beforeCommit, &beat);
                committed = Hold::HELD == h;
                if (committed && beat && advance.beat)
                {
                    m_shared.lastBeatNs.store(advance.beat->sentAtNs, std::memory_order_release);
                    m_shared.beats.fetch_add(1, std::memory_order_relaxed);
                }
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
        // a clean stop: what pj-duty queued on its way out (the replay rows it forgot when it stopped them; pj-duty
        // closes before this thread is stopped) is written in one last fenced batch, then the lease is released
        if (m_work && m_lease.held() && m_conn.ok() && !reassert && nowNs() - lastConfirmNs < holdNs &&
            m_work->due(nowNs()))
        {
            StateAdvance advance;
            std::string error;
            const Hold h = m_lease.commitFenced(m_conn, t,
                [&](PgConn &c, std::string &e) { return m_work->write(c, m_lease.token(), advance, e); }, advance, error,
                m_hooks.beforeFence, m_hooks.beforeCommit);
            if (Hold::HELD == h)
            {
                m_work->committed(advance);
            }
            else
            {
                std::fprintf(stderr, "oe-projector: the last batch at stop was not written (%s): the next writer stops "
                    "the replays still recorded\n", error.c_str());
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
    projector::FollowParts m_parts;
    projector::WriterStats &m_stats;
    std::unique_ptr<FencedWork> m_work;
    projector::NodePoller m_poller;
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
    static projector::FollowExchange exchange(static_cast<std::size_t>(s.queueEvents));
    static projector::NodeTable nodes(s.archive.archives.size());
    static projector::FollowViewBox view;
    static projector::WriterStats stats;
    static const projector::FollowParts parts{ &exchange, &nodes, &view };
    inputs.makeDuty = [&s](const DutyContext &context) -> std::unique_ptr<DutyWork> {
        return projector::makeFollowDuty(context, s, shared, parts);
    };
    inputs.makeFront = [&s, &hooks]() -> std::unique_ptr<FrontWork> {
        return std::make_unique<ProjectionWriter>(s, shared, hooks, parts, stats);
    };
    runProcess(identity, inputs);
}

}}
