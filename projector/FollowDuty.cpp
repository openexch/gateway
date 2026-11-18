#include "FollowDuty.h"

#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <map>

#include "cluster_tools/archive/aeron/ChainFollower.h"
#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/client/aeron/FatalErrors.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/CoreSession.h"
#include "gateway/common/ExitCodes.h"

#include "EngineStatus.h"
#include "EnginePoller.h"

namespace openexchange { namespace gateway { namespace projector {

namespace {

namespace cl = cluster_tools::client;
namespace at = cluster_tools::client::aeron_transport;
namespace rt = cluster_tools::runtime;
namespace aa = cluster_tools::archive::aeron_archive;

using AeronClient = cl::ClusterClient<at::AeronTransport>;
using ar::Clock;
using ar::TimePoint;

constexpr std::int64_t MS = 1'000'000LL;

std::int64_t monoNs()
{
    return rt::Supervisor::monotonicNs();
}

// The readerReplayBudget= token of a core member's /health detail line.
std::optional<std::int64_t> budgetOf(const std::string &body)
{
    const std::string token = "readerReplayBudget=";
    const std::size_t at = body.find(token);
    if (std::string::npos == at)
    {
        return std::nullopt;
    }
    std::int64_t v = 0;
    std::size_t i = at + token.size();
    bool any = false;
    while (i < body.size() && body[i] >= '0' && body[i] <= '9')
    {
        v = v * 10 + (body[i] - '0');
        ++i;
        any = true;
    }
    return any ? std::optional<std::int64_t>(v) : std::nullopt;
}

session::SessionSettings projectorSessionSettings(const ProjectorSettings &s)
{
    session::SessionSettings out;
    out.sessionClass = cl::SessionClass::GATEWAY;
    out.credentialClass = s.clusterCredentials.sessionClass;
    out.channels = cl::core_channel::ACKS | cl::core_channel::CONTROL;
    out.identity = s.common.instanceId;
    out.maxInflight = s.sessionMaxInflight; // the head probe only: one at a time
    out.requestTimeoutMs = s.requestTimeoutMs;
    out.queryRateCount = s.queryRateCount;
    out.queryRatePeriodMs = s.queryRatePeriodMs;
    out.queryBurst = s.queryBurst;
    out.reconnectDelayMs = s.reconnectDelayMs;
    out.registerResendMs = s.registerResendMs;
    out.readConfig = true;         // CoreConfigReport carries the genesisId
    out.correlationIdBase = 0x504A'0000'0000'0000LL;
    // out.encoder stays the default: reads only (this binary does not link the writes)
    return out;
}

cl::ClientSettings projectorClientSettings(const ProjectorSettings &s)
{
    cl::ClientSettings c;
    c.ingressEndpoints = s.clusterMembers;
    c.ingressChannel = s.ingressChannel;
    c.responseChannel = s.responseChannel;
    c.credentials = s.clusterCredentials.encode();
    c.clientName = "oe-projector identity=" + std::to_string(s.common.instanceId);
    return c;
}

// The session and its transport, alive only while this copy is the writer.
struct SessionBundle
{
    SessionBundle(const DutyContext &context, const ProjectorSettings &s, session::CoreSession<AeronClient>::Sink sink) :
        transport(context.aeron), client(transport, projectorClientSettings(s)),
        session(client, projectorSessionSettings(s), std::move(sink), context.log)
    {
    }
    at::AeronTransport transport;
    AeronClient client;
    session::CoreSession<AeronClient> session;
};

struct SourceState
{
    SourceView view;
    std::unique_ptr<Backoff> backoff;
};

struct OpenReplay
{
    std::size_t source = 0;
    ReplayRow row;
    bool recorded = false;
};

struct Orphan
{
    ReplayRow row;
    std::int64_t retryAtNs = 0;
    std::unique_ptr<aa::ArchiveSource> port; // the control session it is stopped through (connected step by step)
    std::int64_t deadlineNs = 0;
};

#ifdef PROJ_TC_SOURCE_FAULTS
// Test build only (never published): faults the frame sink injects before the forward rule, per configured source
// index, from PROJ_TC_SOURCE_FAULTS="<index>:drop=<seq>,<seq>;<index>:below=<seq>;<index>:rate=<records a second>".
// drop: those records are consumed and never forwarded (a source that does not hold them, a holed recording);
// below: every record under that sequence likewise (a member wiped and rejoined, its recordings starting above 1);
// rate: the source delivers at most that many records a second, the rest are refused and redelivered (a slow source).
class SourceFaults
{
public:
    enum class Verdict
    {
        PASS,
        DROP,
        DELAY
    };

    SourceFaults()
    {
        const char *text = std::getenv("PROJ_TC_SOURCE_FAULTS");
        std::string spec = nullptr == text ? std::string() : std::string(text);
        std::size_t at = 0;
        while (at < spec.size())
        {
            std::size_t end = spec.find(';', at);
            end = std::string::npos == end ? spec.size() : end;
            const std::string item = spec.substr(at, end - at);
            at = end + 1;
            const std::size_t colon = item.find(':');
            const std::size_t eq = item.find('=');
            if (std::string::npos == colon || std::string::npos == eq || eq < colon)
            {
                continue;
            }
            One &one = m_sources[static_cast<std::size_t>(std::stoul(item.substr(0, colon)))];
            const std::string key = item.substr(colon + 1, eq - colon - 1);
            const std::string value = item.substr(eq + 1);
            if ("drop" == key)
            {
                std::size_t v = 0;
                while (v < value.size())
                {
                    std::size_t comma = value.find(',', v);
                    comma = std::string::npos == comma ? value.size() : comma;
                    one.drop.push_back(std::stoll(value.substr(v, comma - v)));
                    v = comma + 1;
                }
            }
            else if ("below" == key)
            {
                one.below = std::stoll(value);
            }
            else if ("rate" == key)
            {
                one.rate = std::stoll(value);
            }
        }
    }

    Verdict apply(std::size_t source, std::int64_t seq, std::int64_t nowNs)
    {
        const auto it = m_sources.find(source);
        if (m_sources.end() == it)
        {
            return Verdict::PASS;
        }
        One &one = it->second;
        if (seq < one.below || one.drop.end() != std::find(one.drop.begin(), one.drop.end(), seq))
        {
            return Verdict::DROP;
        }
        if (one.rate > 0)
        {
            if (nowNs < one.nextNs)
            {
                return Verdict::DELAY;
            }
            one.nextNs = nowNs + 1'000'000'000LL / one.rate;
        }
        return Verdict::PASS;
    }

private:
    struct One
    {
        std::vector<std::int64_t> drop;
        std::int64_t below = 0;
        std::int64_t rate = 0;
        std::int64_t nextNs = 0;
    };
    std::map<std::size_t, One> m_sources;
};
#endif

class FollowDuty final : public DutyWork, public ar::FrameSink, public ar::FollowObserver
{
public:
    FollowDuty(const DutyContext &context, const ProjectorSettings &s, ProjectorShared &shared, const FollowParts &parts) :
        m_context(context), m_settings(s), m_shared(shared), m_exchange(*parts.exchange), m_nodes(*parts.nodes),
        m_viewBox(*parts.view), m_memo(s.archive.archives.size(), &m_metrics), m_behind(s.sourceBehindMs)
    {
        ar::FollowSettings follow;
#ifndef PROJ_NC_NO_MTU_CHECK
        follow.journalMtu = s.archive.journalMtu;
#endif
        follow.journalStreamId = s.archive.journalStreamId;
        follow.profile = ar::CORE_ORDER_JOURNAL;
        follow.neverConnectedTimeout = std::chrono::milliseconds(s.archiveTimeoutMs);
        follow.endpointResolveTimeout = std::chrono::milliseconds(s.archiveTimeoutMs);
        follow.probeInterval = std::chrono::milliseconds(s.archiveProbeMs);
        follow.backoff = std::chrono::milliseconds(s.sourceBackoffMs);
        follow.relistInterval = std::chrono::milliseconds(s.relistMs);
        follow.keyByArchiveId = false; // the core's archives take a new id at every start (the memo would never survive)
        for (std::size_t i = 0; i < s.archive.archives.size(); ++i)
        {
            aa::ArchiveSourceConfig config;
            config.identity.endpoint = s.archive.archives[i].text();
            config.identity.controlStreamId = s.archive.controlStreamId;
            config.identity.journalStreamId = s.archive.journalStreamId;
            config.identity.channel = s.archive.journalChannel;
            config.localHost = s.archive.localHost;
            config.replayStreamId = s.archive.replayStreamId;
            config.messageTimeoutNs = static_cast<std::uint64_t>(s.archiveTimeoutMs) * 1'000'000ULL;
            config.keyByArchiveId = false;
            m_followers.push_back(std::make_unique<aa::ChainFollower>(i, config, follow, m_memo, context.aeron,
                monitor(), [this] { invoker(); }, &m_metrics, this, aa::ArchiveSourceSeams{}, exitAction()));
            SourceState st;
            st.backoff = std::make_unique<Backoff>(s.sourceBackoffMs, s.sourceBackoffMaxMs);
            st.view.role = s.nodeEndpoints.size() > i && !s.nodeEndpoints[i] ? NodeRole::NOT_A_MEMBER : NodeRole::UNKNOWN;
            m_sources.push_back(std::move(st));
            m_names.push_back(config.identity.endpoint);
        }
    }

    ~FollowDuty() override = default;

    int doWork() override
    {
        const std::int64_t now = monoNs();
        const TimePoint tp = Clock::now();
        int work = 0;
        const bool writer = writerNow(m_shared, now);
        if (writer != m_writer)
        {
            m_writer = writer;
            work++;
            if (writer)
            {
                becomeWriter();
            }
            else
            {
                becomeStandby(tp);
            }
        }
        if (!writer)
        {
            work += driveCurrent(tp, 0);
            trackReplay(tp);
            publishView(now);
            return work;
        }
        if (m_session)
        {
            work += m_session->session.doWork();
            probeHead(now);
        }
        work += collect(now, tp);
        if (0 == m_generation)
        {
            publishView(now);
            return work; // pj-pg has not handed over a generation yet
        }
        evaluateEpoch(now);
        work += offerBeat();
        work += stepOrphans(now);
        work += follow(now, tp);
        publishView(now);
        return work;
    }

    void fillStatus(Status &status) const override
    {
        if (m_session)
        {
            m_session->session.fillStatus(status);
        }
        status.lastHeadProbeNs = m_lastProbeAnsweredNs;
    }

    int exitCode() const override
    {
        if (0 != m_exit)
        {
            return m_exit;
        }
        const int pg = m_shared.fatalExit.load(std::memory_order_acquire);
        if (0 != pg)
        {
            return pg;
        }
        return m_session ? m_session->session.exitCode() : 0;
    }

    void close() override
    {
        // A clean stop: the replay is stopped while the generation is still this writer's, so its row is forgotten
        // (REPLAY_ENDED queued; pj-pg writes it before it releases the lease, it stops after pj-duty). The detach is
        // driven to its end, bounded: half the stop timeout or of the duty watchdog, the smaller (this runs on pj-duty,
        // whose heartbeat does not advance meanwhile). A replay still open at the bound keeps its row: the next writer
        // stops it.
        const TimePoint tp = Clock::now();
        if (m_current)
        {
            m_followers[*m_current]->policy().detach(tp);
        }
        m_pending.reset();
        m_choosePending = true;
        const std::int64_t deadline = monoNs() +
            std::min(m_settings.common.stopTimeoutMs, m_settings.common.dutyWatchdogMs) / 2 * MS;
        while ((m_current || m_open) && monoNs() < deadline)
        {
            driveCurrent(Clock::now(), 0);
            trackReplay(Clock::now());
            std::this_thread::yield();
        }
        m_shared.writer.store(false, std::memory_order_release);
        m_writer = false;
        becomeStandby(Clock::now());
    }

private:
    // ---- plumbing

    std::shared_ptr<at::AeronFatalMonitor> monitor()
    {
        if (m_context.monitor)
        {
            return m_context.monitor;
        }
        if (!m_ownMonitor)
        {
            m_ownMonitor = std::make_shared<at::AeronFatalMonitor>([](const std::string &) {});
        }
        return m_ownMonitor;
    }

    aa::ExitAction exitAction()
    {
        return [](const std::string &message) {
            std::fprintf(stderr, "oe-projector: FATAL archive client: %s (exit 70)\n", message.c_str());
            std::fflush(stderr);
            std::_Exit(exit_code::SOFTWARE);
        };
    }

    // the archive wait loop's park: keeps the session's keepalives going while a control call waits
    void invoker()
    {
        if (m_session && !m_inInvoker)
        {
            m_inInvoker = true;
            m_session->client.doWork();
            m_inInvoker = false;
        }
        std::this_thread::yield();
    }

    void log(rt::LogLevel level, const std::string &text)
    {
        if (nullptr != m_context.log)
        {
            m_context.log->push(level, monoNs(), "pj-duty: %s", text.c_str());
        }
        else
        {
            std::fprintf(stderr, "oe-projector: pj-duty: %s\n", text.c_str());
        }
    }

    // ---- writer and standby

    void becomeWriter()
    {
        m_nodes.setActive(true);
        m_nodeRoundsAtWriter = m_nodes.rounds.load(std::memory_order_relaxed);
        m_writerSinceNs = monoNs();
        m_session = std::make_unique<SessionBundle>(m_context, m_settings,
            [this](const session::Outcome &o) { onOutcome(o); });
        if (!m_session->session.start())
        {
            log(rt::LogLevel::CRITICAL, "the cluster client refused to start: " + m_session->client.lastDetail() +
                " (exit 78)");
            m_exit = exit_code::CONFIG;
        }
        m_shared.sessionOpen.store(true, std::memory_order_release);
        log(rt::LogLevel::INFO, "writer: the cluster session is opened; the journal sources follow once the writer "
            "hands over the cursor");
    }

    void becomeStandby(TimePoint tp)
    {
        m_nodes.setActive(false);
        if (m_current)
        {
            m_followers[*m_current]->policy().detach(tp);
        }
        m_pending.reset();
        if (m_session)
        {
            m_session->session.close();
            m_session.reset();
        }
        m_shared.sessionOpen.store(false, std::memory_order_release);
        m_generation = 0;
        m_probeInFlight = false;
        m_genesis = 0;
        m_head = -1;
        m_lastProbeAnsweredNs = -1;
        m_beats.clear();
        log(rt::LogLevel::INFO, "standby: the replay is stopped and the cluster session closed");
    }

    // ---- the cluster session: the genesis and the head

    void probeHead(std::int64_t now)
    {
        if (m_probeInFlight || (m_lastProbeSentNs >= 0 && now - m_lastProbeSentNs < m_settings.headProbeMs * MS))
        {
            return;
        }
        Status st;
        m_session->session.fillStatus(st);
        if (!st.registered)
        {
            return;
        }
        wire::RequestOrderEvents q;
        q.fromEventSeq = std::max<std::int64_t>(m_applied, 0);
        q.maxEvents = 0;
        const session::SubmitResult r = m_session->session.submit(session::Command{ q });
        if (r.accepted)
        {
            m_probeInFlight = true;
            m_probeTicket = r.ticket;
            m_lastProbeSentNs = now;
        }
    }

    void onOutcome(const session::Outcome &o)
    {
        if (o.ticket != m_probeTicket)
        {
            return;
        }
        m_probeInFlight = false;
        if (session::OutcomeKind::REPLY != o.kind)
        {
            return;
        }
        if (const auto *events = std::get_if<session::EventsReply>(&o.reply))
        {
            m_head = events->end.lastSeq;
            m_genesis = events->end.genesisId;
            m_lastProbeAnsweredNs = monoNs();
            m_behind.onProbe(m_head, m_applied, readPosition());
            if (m_genesis >= 1)
            {
                // a candidate of the beat rule: complete as of the send time once the cursor reaches this head
                // (FollowRules.h: one per answered probe, the oldest dropped at HISTORY_PROJECTOR_BEAT_CANDIDATES)
                m_beatsDropped += addBeatCandidate(m_beats, BeatCandidate{ m_head, m_genesis, m_lastProbeSentNs },
                    static_cast<std::size_t>(m_settings.beatCandidates));
            }
        }
    }

    // ---- the beat rule (ProjectionStore.h): an answered probe whose head the committed cursor has reached, of the
    // projection's own genesis, with no halt, goes to pj-pg as a BEAT; the newest such probe wins. No answered probe
    // (unanswered, no leader, the session down), a probe of another genesis, a cursor below its head, or a halt: none.
    // pj-pg's statement checks the same against what its transaction committed.
    int offerBeat()
    {
        std::optional<BeatCandidate> best;
        while (!m_beats.empty() && m_beats.front().head <= m_applied)
        {
            best = m_beats.front();
            m_beats.pop_front();
        }
        if (!best)
        {
            return 0;
        }
#ifdef PROJ_NC_BEAT_NO_EPOCH
        // test build only: no genesis term
        const bool sameGenesis = true;
        const bool halted = Halt::NONE != m_halt.halt && Halt::EPOCH_MISMATCH != m_halt.halt;
#else
        const bool sameGenesis = m_dbEpoch && *m_dbEpoch == best->genesis;
        const bool halted = Halt::NONE != m_halt.halt;
#endif
        if (!sameGenesis || halted)
        {
            m_beatsRefused++;
            return 0;
        }
        Item i;
        i.kind = Item::Kind::BEAT;
        i.generation = m_generation;
        i.head = best->head;
        i.epoch = best->genesis;
        i.sentAtNs = best->sentAtNs;
        m_exchange.push(std::move(i));
        m_beatsOffered++;
        return 1;
    }

    std::int64_t clusterGenesis() const
    {
        if (0 != m_genesis)
        {
            return m_genesis;
        }
        if (m_session && m_session->session.config().reported)
        {
            return m_session->session.config().head.genesisId;
        }
        return 0;
    }

    // ---- what pj-pg committed

    int collect(std::int64_t now, TimePoint tp)
    {
        Results r = m_exchange.collect();
        int work = 0;
        if (r.reset)
        {
            onReset(*r.reset, now, tp);
            work++;
        }
        if (r.appliedSeq >= 0)
        {
            const bool advanced = r.appliedSeq > m_applied;
            m_applied = r.appliedSeq;
            if (r.epoch && (!m_dbEpoch || *m_dbEpoch != *r.epoch))
            {
                // the epoch an empty projection just committed: the followers filter by it before anything is read
#ifdef PROJ_NC_EPOCH_FROM_FIRST_RECORD
                m_dbEpoch = r.epoch; // test build only: the followers keep no epoch filter
#else
                adoptEpoch(*r.epoch, tp);
#endif
            }
            if (advanced)
            {
                clearPassedMarks();
            }
            work++;
        }
        if (m_dbEpoch && m_applied > 0)
        {
            m_memo.onReport(*m_dbEpoch, m_applied);
        }
        for (const CommittedProof &p : r.proofs)
        {
            if (p.source < m_followers.size() && p.seq <= m_applied)
            {
                m_memo.prove(p.source, p.key, p.seq, p.epoch, p.endPosition);
            }
        }
        m_verified += r.comparesVerified;
        for (std::size_t s : r.appliedFrom)
        {
            if (s < m_sources.size())
            {
                m_sources[s].backoff->progress();
                m_sources[s].view.unreachable = false;
                if (m_current && *m_current == s)
                {
                    m_behind.onDelivered();
                    m_readSeq = std::max(m_readSeq, m_applied);
                }
            }
        }
        for (const auto &[s, upTo] : r.comparedUpTo)
        {
            if (m_current && *m_current == s)
            {
                m_readSeq = std::max(m_readSeq, upTo);
            }
        }
        for (std::size_t s : r.comparedFrom)
        {
            // a source re-reading history the projection holds, its bytes verified: it delivers
            if (s < m_sources.size())
            {
                m_sources[s].view.unreachable = false;
#ifndef PROJ_NC_COMPARES_NOT_DELIVERY
                if (m_current && *m_current == s)
                {
                    m_behind.onDelivered();
                }
#endif
            }
        }
        std::vector<std::size_t> conflicted; // one leave per source, however many of its records conflicted
        for (const Conflict &c : r.conflicts)
        {
            m_view.conflicts++;
            if (conflicted.end() != std::find(conflicted.begin(), conflicted.end(), c.source))
            {
                log(rt::LogLevel::ERROR, "source " + (c.source < m_names.size() ? m_names[c.source] : c.sourceName) +
                    ": payload conflict at journalSeq " + std::to_string(c.seq) + ": stored sha256 " + c.storedSha256 +
                    ", read sha256 " + c.readSha256);
                continue;
            }
            conflicted.push_back(c.source);
            leave(c.source, Mark::PAYLOAD_CONFLICT, "payload conflict at journalSeq " + std::to_string(c.seq) +
                ": stored sha256 " + c.storedSha256 + ", read sha256 " + c.readSha256, 0, 0, now, tp);
            work++;
        }
        for (std::int64_t id : r.replaysRecorded)
        {
            if (m_open && m_open->row.replaySessionId == id)
            {
                m_open->recorded = true;
            }
        }
        return work;
    }

    void onReset(const Reset &reset, std::int64_t now, TimePoint tp)
    {
        m_generation = reset.generation;
        m_applied = reset.state.appliedSeq;
        m_dbEpoch = reset.state.epoch;
        m_next = m_applied + 1;
        m_enqueuedCompares = 0;
        m_verified = 0;
        m_epochPosted = false;
        m_halt = HaltState{};
        m_haltPosted = HaltState{};
        m_readSeq = 0;
        m_haltSince = -1;
        if (reset.state.haltReason)
        {
            m_haltPosted.halt = Halt::GAP; // anything set: the first evaluation writes the halt as it stands now
            m_haltPosted.detail = *reset.state.haltReason;
        }
        for (SourceState &s : m_sources)
        {
            s.view.mark = Mark::NONE;
        }
        if (m_dbEpoch)
        {
            adoptEpoch(*m_dbEpoch, tp);
            for (const MemoRow &row : reset.memo)
            {
                for (std::size_t i = 0; i < m_names.size(); ++i)
                {
                    if (m_names[i] == row.source && row.epoch == *m_dbEpoch && row.seq <= m_applied)
                    {
                        if (m_memo.seed(i, row.key, row.seq, row.epoch, row.endPosition))
                        {
                            log(rt::LogLevel::INFO, "memo seeded for " + row.source + ": journalSeq " +
                                std::to_string(row.seq) + " at " + std::to_string(row.endPosition) + " on recording " +
                                std::to_string(row.key.recordingId));
                        }
                    }
                }
            }
        }
        m_orphans.clear();
        for (const ReplayRow &row : reset.replays)
        {
            Orphan o;
            o.row = row;
            m_orphans.push_back(std::move(o));
        }
        // every replay this copy holds is stopped and opened again: a replay row is recorded per generation (the source
        // is chosen again once the detach ends, so reading resumes at once, an idle cluster included)
        if (m_current)
        {
            m_followers[*m_current]->policy().detach(tp);
            m_choosePending = true;
        }
        m_pending.reset();
        if (m_open)
        {
            m_open->recorded = false;
        }
        log(rt::LogLevel::INFO, "generation " + std::to_string(m_generation) + ": cursor " + std::to_string(m_applied) +
            ", epoch " + (m_dbEpoch ? std::to_string(*m_dbEpoch) : std::string("none")) + ", " +
            std::to_string(reset.memo.size()) + " memo rows, " + std::to_string(reset.replays.size()) +
            " recorded replays to stop");
        (void)now;
    }

    // The read position BEHIND measures the lag from: the highest sequence of this read the writer verified or applied
    // (what was only queued does not count: the queue runs ahead of a slow writer)
    std::int64_t readPosition() const
    {
#ifdef PROJ_NC_COMPARES_NOT_DELIVERY
        return m_applied; // test build only: the lag is measured from the cursor
#else
        return m_readSeq;
#endif
    }

    // ---- the epoch: from the cluster, never from data

    // The projection's epoch is known (the state row holds it): the memo reports it and every follower walks only
    // recordings of it from now on.
    void adoptEpoch(std::int64_t epoch, TimePoint tp)
    {
        m_dbEpoch = epoch;
        m_memo.onReport(epoch, m_applied);
#ifndef PROJ_NC_NO_EPOCH_CHECK
        for (auto &f : m_followers)
        {
            f->policy().expectEpoch(epoch, tp);
        }
#else
        (void)tp; // test build only: no epoch filter
#endif
    }

    void evaluateEpoch(std::int64_t now)
    {
        const std::int64_t g = clusterGenesis();
        if (!m_dbEpoch)
        {
            m_followAllowed = false;
            if (g >= 1 && !m_epochPosted)
            {
                Item i;
                i.kind = Item::Kind::EPOCH;
                i.generation = m_generation;
                i.epoch = g;
                m_exchange.push(i);
                m_epochPosted = true;
                log(rt::LogLevel::INFO, "empty projection: the cluster's genesisId " + std::to_string(g) +
                    " becomes its epoch");
            }
            return;
        }
#ifdef PROJ_NC_NO_EPOCH_CHECK
        if (false) // test build only: the cluster's genesis is not compared with the projection's epoch
#else
        if (g >= 1 && g != *m_dbEpoch)
#endif
        {
            if (m_followAllowed || Halt::EPOCH_MISMATCH != m_halt.halt)
            {
                log(rt::LogLevel::CRITICAL, "EPOCH_MISMATCH: the cluster's genesisId is " + std::to_string(g) +
                    ", the projection's epoch " + std::to_string(*m_dbEpoch) + "; nothing is applied (an empty database "
                    "starts a projection of the new genesis)");
            }
            m_followAllowed = false;
            setHalt(HaltState{ Halt::EPOCH_MISMATCH, "cluster genesisId " + std::to_string(g) + ", projection epoch " +
                std::to_string(*m_dbEpoch) }, now);
            return;
        }
        if (Halt::EPOCH_MISMATCH == m_halt.halt)
        {
            setHalt(HaltState{}, now);
        }
        m_followAllowed = true;
    }

    // ---- recorded replays of an earlier writer: stopped by session id on their archives

    // One step of stopping one recorded replay: the archive connect is polled once per duty cycle (pj-duty never waits
    // for an archive that does not answer), then one stopReplay. Returns true when the row is done with.
    bool stepOrphan(Orphan &o, std::int64_t now)
    {
        if (!o.port)
        {
            aa::ArchiveSourceConfig config;
            config.identity.endpoint = o.row.source;
            config.identity.controlStreamId = m_settings.archive.controlStreamId;
            config.identity.journalStreamId = m_settings.archive.journalStreamId;
            config.identity.channel = m_settings.archive.journalChannel;
            config.localHost = m_settings.archive.localHost;
            config.replayStreamId = m_settings.archive.replayStreamId;
            config.messageTimeoutNs = static_cast<std::uint64_t>(m_settings.archiveTimeoutMs) * 1'000'000ULL;
            o.port = std::make_unique<aa::ArchiveSource>(config, m_context.aeron, monitor(), [this] { invoker(); },
                &m_metrics, aa::ArchiveSourceSeams{}, exitAction());
            o.deadlineNs = now + m_settings.archiveTimeoutMs * MS;
        }
        const ar::ConnectPoll c = o.port->connectStep();
        if (ar::ConnectPoll::State::PENDING == c.state && now < o.deadlineNs)
        {
            return false;
        }
        if (ar::ConnectPoll::State::CONNECTED != c.state)
        {
            o.port.reset();
            o.retryAtNs = now + m_settings.sourceBackoffMs * MS;
            log(rt::LogLevel::WARN, "recorded replay " + std::to_string(o.row.replaySessionId) + " on " + o.row.source +
                ": the archive did not answer" + (c.error.message.empty() ? std::string() : " (" + c.error.message + ")") +
                "; tried again later");
            return false;
        }
        const std::int64_t archiveId = o.port->archiveId();
        if (archiveId != o.row.archiveId)
        {
            m_view.orphanUnknown++;
            log(rt::LogLevel::INFO, "recorded replay " + std::to_string(o.row.replaySessionId) + " on " + o.row.source +
                ": the archive restarted since (id " + std::to_string(archiveId) + ", was " +
                std::to_string(o.row.archiveId) + "); nothing to stop");
        }
        else
        {
#ifdef PROJ_NC_NO_STOP_REPLAY
            const bool stopped = false; // test build only: the recorded replay is left as it is (and not counted)
#else
            // the archive's reply: OK (the request answered; an archive also answers OK for a session it no longer
            // knows, so this is not proof of a stop), UNKNOWN_REPLAY (it had ended), or anything else (not known: kept,
            // tried again after the back-off)
            const std::string before = o.port->lastError();
            o.port->stopReplay(o.row.replaySessionId);
            const std::string after = o.port->lastError();
            const bool stopped = after == before;
            const bool unknown = !stopped && (std::string::npos != after.find("not known") ||
                std::string::npos != after.find("UNKNOWN_REPLAY") || std::string::npos != after.find("unknown replay"));
            if (!stopped && !unknown)
            {
                o.port->disconnect();
                o.port.reset();
                o.retryAtNs = now + m_settings.sourceBackoffMs * MS;
                log(rt::LogLevel::WARN, "recorded replay " + std::to_string(o.row.replaySessionId) + " on " +
                    o.row.source + ": stopReplay not answered (" + after + "); tried again later");
                return false;
            }
            (stopped ? m_view.stopRequestsAnswered : m_view.orphanUnknown)++;
#endif
            log(rt::LogLevel::INFO, "recorded replay " + std::to_string(o.row.replaySessionId) + " on " + o.row.source +
                (stopped ? ": stopReplay answered OK" : ": the archive does not know it (already ended)"));
        }
        o.port->disconnect();
        o.port.reset();
        Item i;
        i.kind = Item::Kind::REPLAY_ENDED;
        i.generation = m_generation;
        i.replay = o.row;
        m_exchange.push(i);
        return true;
    }

    int stepOrphans(std::int64_t now)
    {
        for (auto it = m_orphans.begin(); it != m_orphans.end(); ++it)
        {
            if (now < it->retryAtNs)
            {
                continue;
            }
            if (m_open && m_open->row.source == it->row.source && m_open->row.replaySessionId == it->row.replaySessionId)
            {
                m_orphans.erase(it); // this copy's own live replay: it is recorded again for this generation
                return 1;
            }
            if (stepOrphan(*it, now))
            {
                m_orphans.erase(it);
                return 1;
            }
            return 0; // one archive step per cycle
        }
        return 0;
    }

    // ---- the sources

    std::vector<SourceView> views(std::int64_t now)
    {
        const std::vector<NodeInfo> nodes = m_nodes.snapshot();
        std::vector<SourceView> out;
        for (std::size_t i = 0; i < m_sources.size(); ++i)
        {
            SourceView &v = m_sources[i].view;
            if (i < nodes.size() && !(i < m_settings.nodeEndpoints.size() && !m_settings.nodeEndpoints[i]))
            {
                const bool fresh = nodes[i].answeredAtNs >= 0 &&
                    now - nodes[i].answeredAtNs < m_settings.nodeFreshMs * MS;
                v.role = fresh ? nodes[i].role : NodeRole::UNKNOWN;
                v.replaysActive = fresh ? nodes[i].replaysActive : std::nullopt;
                v.readerBudget = fresh ? nodes[i].readerBudget : std::nullopt;
            }
            out.push_back(v);
        }
        return out;
    }

    // True until the first node round since this copy became the writer answered, bounded by _NODE_FIRST_ROUND_MS
    bool awaitingFirstNodeRound(std::int64_t now) const
    {
        bool anyNode = false;
        for (const auto &e : m_settings.nodeEndpoints)
        {
            anyNode = anyNode || e.has_value();
        }
        return anyNode && m_nodes.rounds.load(std::memory_order_relaxed) == m_nodeRoundsAtWriter &&
            now - m_writerSinceNs < m_settings.nodeFirstRoundMs * MS;
    }

    void leave(std::size_t source, Mark mark, const std::string &why, std::int64_t from, std::int64_t to,
        std::int64_t now, TimePoint tp, bool unreachable = false)
    {
        if (source >= m_sources.size())
        {
            return;
        }
        SourceState &s = m_sources[source];
        s.view.unreachable = unreachable;
        if (Mark::NONE != mark)
        {
            s.view.mark = mark;
            s.view.missingFrom = from;
            s.view.missingTo = to;
        }
        s.view.blockedUntilNs = s.backoff->leave(now);
        m_view.switches++;
        log(Mark::NONE == mark ? rt::LogLevel::WARN : rt::LogLevel::ERROR, "source " + m_names[source] + " left: " +
            why + " (next request to it in " + std::to_string((s.view.blockedUntilNs - now) / MS) + " ms)");
        if (m_current && *m_current == source)
        {
            m_followers[source]->policy().detach(tp);
            m_pending.reset();
            m_choosePending = true;
        }
    }

    void clearPassedMarks()
    {
        for (SourceState &s : m_sources)
        {
            if ((Mark::BELOW_RANGE == s.view.mark || Mark::HOLE == s.view.mark) && s.view.missingTo <= m_applied)
            {
                s.view.mark = Mark::NONE; // the range it lacked is applied: it may serve what follows
            }
        }
    }

    int driveCurrent(TimePoint tp, int limit)
    {
        if (!m_current)
        {
            return 0;
        }
        aa::ChainFollower &f = *m_followers[*m_current];
        const int work = f.doWork(*this, limit, tp);
        if (f.policy().idle() && !f.policy().wantsDisconnect())
        {
            if (m_pending)
            {
                m_current = m_pending;
                m_pending.reset();
                startRead(*m_current, tp);
            }
            else if (!m_writer || m_choosePending || !m_followAllowed)
            {
                m_current.reset();
            }
        }
        return work;
    }

    void startRead(std::size_t source, TimePoint tp)
    {
        m_freshRead = true;
        m_readSeq = 0;
        m_behind.restart(monoNs());
        m_followers[source]->policy().attach(tp);
        log(rt::LogLevel::INFO, "attaching source " + m_names[source] + " (" +
            nameOf(m_sources[source].view.role) + ") at cursor " + std::to_string(m_applied));
    }

    // the replay this copy holds: recorded before it is read, forgotten when it ends
    void trackReplay(TimePoint)
    {
        const ar::Phase phase = m_current ? m_followers[*m_current]->policy().phase() : ar::Phase::IDLE;
        const bool open = m_current && (ar::Phase::FOLLOWING == phase || ar::Phase::DETACH_DUE == phase);
        const std::int64_t id = m_current ? m_followers[*m_current]->policy().replaySessionId() : 0;
        if (m_open && (!open || m_open->source != *m_current || m_open->row.replaySessionId != id))
        {
            Item i;
            i.kind = Item::Kind::REPLAY_ENDED;
            i.generation = m_generation;
            i.replay = m_open->row;
            if (0 != m_generation)
            {
                m_exchange.push(i);
            }
            m_view.replayStops++;
            m_open.reset();
        }
        if (open && !m_open && ar::Phase::FOLLOWING == phase && 0 != m_generation)
        {
            OpenReplay o;
            o.source = *m_current;
            o.row.source = m_names[o.source];
            o.row.archiveId = m_followers[o.source]->source().archiveId();
            o.row.replaySessionId = id;
            m_open = o;
            Item i;
            i.kind = Item::Kind::REPLAY_STARTED;
            i.generation = m_generation;
            i.replay = o.row;
            m_exchange.push(i);
            m_view.replaysStarted++;
        }
        m_shared.replaysOpen.store(m_open ? 1 : 0, std::memory_order_release);
    }

    int follow(std::int64_t now, TimePoint tp)
    {
        int work = 0;
        if (!m_followAllowed)
        {
            if (m_current)
            {
                m_followers[*m_current]->policy().detach(tp);
                m_pending.reset();
            }
            work += driveCurrent(tp, 0);
            trackReplay(tp);
            return work;
        }
        std::vector<SourceView> vs = views(now);
        if (!m_current && awaitingFirstNodeRound(now))
        {
            return work; // the members' roles are not known yet: no source is chosen blind (it may be the leader)
        }
        if (!m_current)
        {
            const std::optional<std::size_t> pick = chooseSource(vs, now, std::nullopt);
            if (pick)
            {
                for (std::size_t i = 0; i < vs.size(); ++i)
                {
                    if (i != *pick && Mark::NONE == vs[i].mark && budgetFull(vs[i]) && now >= vs[i].blockedUntilNs)
                    {
                        m_view.budgetSkips++;
                    }
                }
                if (NodeRole::LEADER == vs[*pick].role)
                {
                    log(rt::LogLevel::WARN, "no follower source can be read: reading the leader's archive (" +
                        m_names[*pick] + ")");
                }
                m_current = pick;
                m_choosePending = false;
                startRead(*pick, tp);
                if (Halt::GAP == m_halt.halt || Halt::PAYLOAD_CONFLICT == m_halt.halt || Halt::SCHEMA == m_halt.halt ||
                    Halt::MTU == m_halt.halt)
                {
                    // a source can be read again: the halt is evaluated anew by what it delivers
                }
            }
            else
            {
                const HaltState h = haltOf(vs, m_applied);
                if (Halt::NONE != h.halt)
                {
                    if (m_haltSince < 0)
                    {
                        m_haltSince = now;
                    }
                    setHalt(h, now);
                    if (now - m_haltSince >= m_settings.gapRetryMs * MS)
                    {
                        // the halt is re-evaluated: every source is read again from its chain start
                        for (std::size_t i = 0; i < m_sources.size(); ++i)
                        {
                            m_sources[i].view.mark = Mark::NONE;
                            m_memo.invalidate(i, ar::MemoReason::HOLE_VERIFY);
                        }
                        m_haltSince = -1;
                        log(rt::LogLevel::INFO, "halt " + std::string(nameOf(h.halt)) + " re-evaluated: every source "
                            "is read again from its chain start");
                    }
                }
                trackReplay(tp);
                return work;
            }
        }
        // the attached source
        const std::size_t cur = *m_current;
        aa::ChainFollower &f = *m_followers[cur];
        const bool recorded = m_open && m_open->recorded;
        const bool following = ar::Phase::FOLLOWING == f.policy().phase();
        int limit = 0;
        if (!m_pending && (!following || recorded))
        {
            limit = static_cast<int>(std::min<std::size_t>(m_exchange.free(),
                static_cast<std::size_t>(m_settings.batchEvents)));
        }
        m_sinkSource = cur;
        work += driveCurrent(tp, limit);
        trackReplay(tp);
        afterPoll(cur, now, tp);
        if (m_current && *m_current == cur && !m_pending && !m_choosePending)
        {
            // (a source already being left, by afterPoll or earlier, is not left or switched from a second time)
            if (m_head >= 0 && m_behind.behind(now, m_head, readPosition()))
            {
                m_view.behind++;
                leave(cur, Mark::NONE, "BEHIND: the core's head stayed above the cursor for " +
                    std::to_string(m_settings.sourceBehindMs) + " ms and this source did not close the lag", 0, 0,
                    now, tp);
            }
            else if (betterSourceAvailable(vs, now, cur))
            {
                m_view.leaderAvoided += NodeRole::LEADER == vs[cur].role ? 1 : 0;
                m_view.switches++;
                log(rt::LogLevel::INFO, "source " + m_names[cur] + " is " + nameOf(vs[cur].role) +
                    ": a follower source can be read, switching (readers avoid the leader)");
                f.policy().detach(tp);
                m_choosePending = true;
            }
        }
        return work;
    }

    void afterPoll(std::size_t cur, std::int64_t now, TimePoint tp)
    {
        if (m_reRead)
        {
            m_reRead = false;
            m_view.reReads++;
            m_memo.invalidate(cur, ar::MemoReason::HOLE_VERIFY);
            m_freshRead = true;
            m_followers[cur]->policy().reattach(tp);
            log(rt::LogLevel::INFO, "source " + m_names[cur] + ": a jump on a read from its memo is not believed; "
                "read again from its chain start");
        }
        if (m_leaveRequest)
        {
            const LeaveRequest l = *m_leaveRequest;
            m_leaveRequest.reset();
            switch (l.mark)
            {
                case Mark::BELOW_RANGE: m_view.belowRange++; break;
                case Mark::HOLE: m_view.holes++; break;
                case Mark::SCHEMA: m_view.schemaRefusals++; break;
                case Mark::MTU: m_view.mtuRefusals++; break;
                default: break;
            }
            leave(l.source, l.mark, l.why, l.from, l.to, now, tp);
#ifdef PROJ_NC_HOLE_MARKS_ALL
            // test build only: a hole one source shows is taken for a hole of the journal, every source marked
            for (SourceState &s : m_sources)
            {
                if (Mark::HOLE == l.mark && Mark::NONE == s.view.mark)
                {
                    s.view.mark = Mark::HOLE;
                    s.view.missingFrom = l.from;
                    s.view.missingTo = l.to;
                }
            }
#endif
        }
        for (const ar::SourceFault &fault : m_faults)
        {
            onLibraryFault(fault, now, tp);
        }
        m_faults.clear();
    }

    void onLibraryFault(const ar::SourceFault &f, std::int64_t now, TimePoint tp)
    {
        switch (f.kind)
        {
            case ar::FaultKind::NONE:
            case ar::FaultKind::FATAL:
                return;
            case ar::FaultKind::POSITION_REJECTED:
            case ar::FaultKind::RESUME_BELT:
            case ar::FaultKind::SEQ_REGRESSION_IN_RECORDING:
                m_freshRead = true; // the library reads this source again without the proof
                log(rt::LogLevel::WARN, "source " + m_names[f.source] + ": " + ar::faultName(f.kind) + ": " + f.detail);
                return;
            case ar::FaultKind::SCHEMA_REFUSED:
                return; // marked by the sink already
            case ar::FaultKind::REPLAY_OPEN:
                if (static_cast<std::uint8_t>(ar::OpenFailure::MAX_REPLAYS) == f.openFailure)
                {
                    m_view.refusedReplays++;
                }
                break;
            default:
                break;
        }
        if (m_current && *m_current == f.source)
        {
            // the archive could not be reached or read: until it delivers again it does not hold the leader back
            leave(f.source, Mark::NONE, std::string(ar::faultName(f.kind)) + ": " + f.detail, 0, 0, now, tp, true);
        }
    }

    void setHalt(const HaltState &h, std::int64_t now)
    {
        m_halt = h;
        if (h == m_haltPosted)
        {
            return;
        }
        m_haltPosted = h;
        Item i;
        i.kind = Item::Kind::HALT;
        i.generation = m_generation;
        i.halt = h;
        m_exchange.push(i);
        log(Halt::NONE == h.halt ? rt::LogLevel::INFO : rt::LogLevel::CRITICAL,
            Halt::NONE == h.halt ? std::string("halt cleared") : std::string("halt ") + nameOf(h.halt) + ": " + h.detail);
        (void)now;
    }

    // ---- the frame sink (pj-duty, inside the follower's poll)

    ar::SinkAction onFrame(const ar::JournalFrame &frame) noexcept override
    {
#ifdef PROJ_NC_PIN_RECORDING
        // test build only: the first recording a record is read from is the only one ever read (no successor on its
        // source, no recording of another source); any other is left at once, before a record of it is handed on
        if (!m_pin)
        {
            m_pin = Pin{ frame.sourceIndex, frame.key.recordingId, frame.key.startTimestamp };
            log(rt::LogLevel::WARN, "test build: pinned to recording " + std::to_string(frame.key.recordingId) +
                " of source " + m_names[frame.sourceIndex]);
        }
        else if (m_pin->source != frame.sourceIndex || m_pin->recordingId != frame.key.recordingId ||
            m_pin->startTimestamp != frame.key.startTimestamp)
        {
            if (!m_leaveRequest)
            {
                m_leaveRequest = LeaveRequest{ frame.sourceIndex, Mark::NONE, "test build: pinned to recording " +
                    std::to_string(m_pin->recordingId) + " of source " + m_names[m_pin->source] + ", recording " +
                    std::to_string(frame.key.recordingId) + " of source " + m_names[frame.sourceIndex] +
                    " not followed", 0, 0 };
            }
            return ar::SinkAction::ABORT;
        }
#endif
        JournalRecord record;
        std::string why;
        if (!decodeRecord(frame.bytes, frame.length, record, why))
        {
            m_leaveRequest = LeaveRequest{ frame.sourceIndex, Mark::SCHEMA, "record refused: " + why, 0, 0 };
            return ar::SinkAction::ABORT;
        }
#if !defined(PROJ_NC_NO_EPOCH_CHECK) && !defined(PROJ_NC_EPOCH_FROM_FIRST_RECORD)
        if (!m_dbEpoch || record.epoch != *m_dbEpoch)
        {
            // the followers walk only recordings of the projection's epoch; a record of another one is never queued
            m_leaveRequest = LeaveRequest{ frame.sourceIndex, Mark::SCHEMA, "record of epoch " +
                std::to_string(record.epoch) + " at journalSeq " + std::to_string(record.seq) + ", the projection's "
                "epoch is " + (m_dbEpoch ? std::to_string(*m_dbEpoch) : std::string("not set")), 0, 0 };
            return ar::SinkAction::ABORT;
        }
#endif
#ifdef PROJ_TC_SOURCE_FAULTS
        switch (m_sourceFaults.apply(frame.sourceIndex, record.seq, monoNs()))
        {
            case SourceFaults::Verdict::DROP: return ar::SinkAction::CONSUME;
            case SourceFaults::Verdict::DELAY: return ar::SinkAction::ABORT;
            case SourceFaults::Verdict::PASS: break;
        }
#endif
        const bool first = m_freshRead;
#ifdef PROJ_NC_START_AT_FIRST
        if (first && 0 == m_applied && 1 == m_next && frame.readWasMemoFree)
        {
            m_next = record.seq; // test build only: an empty projection starts at the first record its source holds
        }
#endif
        const ForwardVerdict v = forwardRule(record.seq, m_next, frame.readWasMemoFree, first);
        Item item;
        item.generation = m_generation;
        item.source = frame.sourceIndex;
        item.sourceName = m_names[frame.sourceIndex];
        item.key = frame.key;
        item.endPosition = frame.endPosition;
        switch (v.kind)
        {
            case Forward::COMPARE:
                item.kind = Item::Kind::COMPARE;
                item.record = record;
                if (!m_exchange.push(std::move(item)))
                {
                    return ar::SinkAction::ABORT;
                }
                m_enqueuedCompares++;
                m_freshRead = false;
                // consumed, never a memo proof: a proof is recorded only for a record whose rows committed
                return ar::SinkAction::CONSUME;
            case Forward::APPLY:
                if (m_enqueuedCompares > m_verified)
                {
                    return ar::SinkAction::ABORT; // the compares before it must be verified first
                }
                item.kind = Item::Kind::APPLY;
                item.record = record;
                if (!m_exchange.push(std::move(item)))
                {
                    return ar::SinkAction::ABORT;
                }
                m_next++;
                m_freshRead = false;
                if (Halt::NONE != m_halt.halt && Halt::EPOCH_MISMATCH != m_halt.halt)
                {
                    m_clearHalt = true;
                }
                return ar::SinkAction::CONSUME;
            case Forward::RE_READ:
                m_reRead = true;
                return ar::SinkAction::ABORT;
            case Forward::BELOW_RANGE:
            case Forward::HOLE:
                m_leaveRequest = LeaveRequest{ frame.sourceIndex, Forward::HOLE == v.kind ? Mark::HOLE : Mark::BELOW_RANGE,
                    std::string(nameOf(v.kind)) + ": this source does not hold journalSeq " + std::to_string(v.from) +
                        ".." + std::to_string(v.to) + " (read from its chain start)",
                    v.from, v.to };
                return ar::SinkAction::ABORT;
        }
        return ar::SinkAction::ABORT;
    }

    ar::RefusalAction onRefused(const ar::RefusedFrame &frame) noexcept override
    {
        if (ar::Refusal::HEAD == frame.reason && frame.hasPeek && 0 == frame.peek.epoch() && frame.peek.journalSeq >= 1)
        {
            m_view.epochZeroRecords++; // a record without a genesis: foreign, never applied
            return ar::RefusalAction::SKIP_AND_CONTINUE;
        }
        m_leaveRequest = LeaveRequest{ frame.sourceIndex, Mark::SCHEMA,
            std::string("frame refused: ") + ar::refusalLabel(frame.reason), 0, 0 };
        return ar::RefusalAction::CLOSE_REPLAY;
    }

    // ---- the follower's observer (no call back into the follower from here)

    void onFault(const ar::SourceFault &f) noexcept override
    {
        try
        {
            m_faults.push_back(f);
        }
        catch (...)
        {
        }
    }

    void onAttached(const ar::AttachInfo &a) noexcept override
    {
        m_view.lastAttachFrom = a.from;
        m_view.lastAttachFromMemo = a.fromProof || !a.memoFree;
        m_view.recordingId = a.recording.key.recordingId;
        m_view.chainIndex = a.chainIndex;
        if (nullptr != m_context.log)
        {
            m_context.log->push(rt::LogLevel::INFO, monoNs(), "pj-duty: attached source %zu recording %lld at %lld "
                "(chain index %zu, %s)", a.source, static_cast<long long>(a.recording.key.recordingId),
                static_cast<long long>(a.from), a.chainIndex, a.fromProof ? "from the memo" : a.memoFree
                    ? "memo-free" : "after the memo");
        }
    }

    void onSourceRefused(std::size_t source, const ar::Recording &r) noexcept override
    {
        try
        {
            m_leaveRequest = LeaveRequest{ source, Mark::MTU, "replay MTU differs from the recording (recording " +
                std::to_string(r.key.recordingId) + " mtu " + std::to_string(r.key.mtuLength) + ", configured " +
                std::to_string(m_settings.archive.journalMtu) + ")", 0, 0 };
        }
        catch (...)
        {
        }
    }

    void onForeignEpochRecording(std::size_t, const ar::Recording &, std::int64_t) noexcept override
    {
        m_view.foreignEpochRecordings++;
    }

    // ---- /health

    void publishView(std::int64_t now)
    {
        if (m_clearHalt)
        {
            m_clearHalt = false;
            m_haltSince = -1;
            if (Halt::NONE != m_halt.halt && Halt::EPOCH_MISMATCH != m_halt.halt)
            {
                setHalt(HaltState{}, now);
            }
        }
        if (now - m_lastViewNs < m_settings.common.statusIntervalMs * MS)
        {
            return;
        }
        m_lastViewNs = now;
        FollowView v = m_view;
        v.generation = m_generation;
        v.source = m_current;
        v.endpoint = m_current ? m_names[*m_current] : std::string();
        v.archiveId = m_open ? m_open->row.archiveId : -1;
        v.phase = m_current ? ar::phaseName(m_followers[*m_current]->policy().phase()) : "IDLE";
        v.next = m_next;
        v.appliedSeq = m_applied;
        v.dbEpoch = m_dbEpoch;
        v.genesisId = clusterGenesis();
        v.headSeq = m_head;
        v.lastHeadProbeNs = m_lastProbeAnsweredNs;
        v.beatsOffered = m_beatsOffered;
        v.beatsRefused = m_beatsRefused;
        v.beatsDropped = m_beatsDropped;
        v.beatsPending = m_beats.size();
        v.halt = m_halt;
        v.foreignEpochRecordings = ar::ArchiveMetrics::get(m_metrics.foreignEpochRecordings);
        v.sourceStates.clear();
        const std::vector<SourceView> vs = views(now);
        for (const SourceView &s : vs)
        {
            v.sourceStates.push_back(std::string(nameOf(s.mark)) + "/" + nameOf(s.role) + "/" +
                std::to_string(std::max<std::int64_t>(0, (s.blockedUntilNs - now) / MS)) + "ms" +
                (budgetFull(s) ? "/budget-full" : ""));
        }
        m_viewBox.set(std::move(v));
    }

    struct LeaveRequest
    {
        std::size_t source = 0;
        Mark mark = Mark::NONE;
        std::string why;
        std::int64_t from = 0;
        std::int64_t to = 0;
    };

    DutyContext m_context;
    const ProjectorSettings &m_settings;
    ProjectorShared &m_shared;
    FollowExchange &m_exchange;
    NodeTable &m_nodes;
    FollowViewBox &m_viewBox;
    std::shared_ptr<at::AeronFatalMonitor> m_ownMonitor;
    ar::ArchiveMetrics m_metrics;
    ar::ResumeMemo m_memo;
    std::vector<std::unique_ptr<aa::ChainFollower>> m_followers;
    std::vector<SourceState> m_sources;
    std::vector<std::string> m_names;
    BehindWindow m_behind;

    bool m_writer = false;
    std::uint64_t m_nodeRoundsAtWriter = 0;
    std::int64_t m_writerSinceNs = 0;
    int m_exit = 0;
    std::unique_ptr<SessionBundle> m_session;
    bool m_inInvoker = false;
    bool m_probeInFlight = false;
    std::uint64_t m_probeTicket = 0;
    std::int64_t m_lastProbeSentNs = -1;
    std::int64_t m_lastProbeAnsweredNs = -1;
    std::int64_t m_head = -1;
    std::int64_t m_genesis = 0;
    std::deque<BeatCandidate> m_beats; // answered probes whose head the cursor has not reached yet, oldest first
    std::uint64_t m_beatsOffered = 0;
    std::uint64_t m_beatsDropped = 0;  // candidates dropped at HISTORY_PROJECTOR_BEAT_CANDIDATES (oldest first)
    std::uint64_t m_beatsRefused = 0;  // probes reached but of another genesis, or while halted

    std::uint64_t m_generation = 0;
    std::int64_t m_applied = 0;
    std::optional<std::int64_t> m_dbEpoch;
    std::int64_t m_next = 1;
    std::int64_t m_readSeq = 0; // the highest sequence of the attached source's read committed (verified or applied)
    std::uint64_t m_enqueuedCompares = 0;
    std::uint64_t m_verified = 0;
    bool m_epochPosted = false;
    bool m_followAllowed = false;
    HaltState m_halt;
    HaltState m_haltPosted;
    std::int64_t m_haltSince = -1;
    bool m_clearHalt = false;

    std::optional<std::size_t> m_current;
    std::optional<std::size_t> m_pending;
    bool m_choosePending = false;
    std::size_t m_sinkSource = 0;
    bool m_freshRead = true;
    bool m_reRead = false;
    std::optional<LeaveRequest> m_leaveRequest;
    std::vector<ar::SourceFault> m_faults;
    std::optional<OpenReplay> m_open;
    std::vector<Orphan> m_orphans;

    FollowView m_view;
    std::int64_t m_lastViewNs = 0;
#ifdef PROJ_TC_SOURCE_FAULTS
    SourceFaults m_sourceFaults;
#endif
#ifdef PROJ_NC_PIN_RECORDING
    struct Pin
    {
        std::size_t source = 0;
        std::int64_t recordingId = -1;
        std::int64_t startTimestamp = 0;
    };
    std::optional<Pin> m_pin;
#endif
};

}

void NodePoller::start()
{
    m_thread = std::thread([this] {
        pthread_setname_np(pthread_self(), "pj-node");
        for (;;)
        {
            {
                std::unique_lock<std::mutex> lock(m_lock);
                m_cv.wait_for(lock, std::chrono::milliseconds(m_settings.nodePollMs), [this] { return m_stop; });
                if (m_stop)
                {
                    return;
                }
            }
            if (m_table.active())
            {
                roundOnce();
            }
        }
    });
}

void NodePoller::stop()
{
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_stop = true;
    }
    m_cv.notify_all();
    if (m_thread.joinable())
    {
        m_thread.join();
    }
}

void NodePoller::roundOnce()
{
    std::vector<std::pair<NodeEndpoint, std::string>> requests;
    std::vector<std::size_t> index; // request pair -> source
    for (std::size_t i = 0; i < m_settings.nodeEndpoints.size(); ++i)
    {
        if (!m_settings.nodeEndpoints[i])
        {
            continue;
        }
        NodeEndpoint e;
        e.memberId = static_cast<std::int64_t>(i);
        e.host = m_settings.nodeEndpoints[i]->host;
        e.port = m_settings.nodeEndpoints[i]->port;
        requests.emplace_back(e, "/health");
        requests.emplace_back(e, "/metrics");
        index.push_back(i);
    }
    std::vector<NodeInfo> out(m_settings.nodeEndpoints.size());
    for (std::size_t i = 0; i < out.size(); ++i)
    {
        out[i].role = m_settings.nodeEndpoints[i] ? NodeRole::UNKNOWN : NodeRole::NOT_A_MEMBER;
    }
    if (!requests.empty())
    {
        const std::vector<stream::HttpAnswer> answers =
            stream::httpGetAll(requests, m_settings.nodePollTimeoutMs, m_settings.nodeMaxBodyBytes);
        const std::int64_t now = monoNs();
        for (std::size_t k = 0; k < index.size(); ++k)
        {
            NodeInfo &n = out[index[k]];
            const stream::HttpAnswer &health = answers[2 * k];
            const stream::HttpAnswer &metrics = answers[2 * k + 1];
            if (!health.answered)
            {
                n.error = health.error;
                m_table.failures.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            n.answeredAtNs = now;
            if (const auto body = stream::engine::parseHealth(health.body); body && body->role)
            {
                n.role = "LEADER" == *body->role ? NodeRole::LEADER
                    : "FOLLOWER" == *body->role  ? NodeRole::FOLLOWER
                                                 : NodeRole::UNKNOWN;
            }
            n.readerBudget = budgetOf(health.body);
            if (metrics.answered)
            {
                const auto series = stream::engine::parseMetrics(metrics.body);
                const auto active = stream::engine::metricInt(series, "core_archive_replays_active");
                n.replaysActive = active && *active >= 0 ? active : std::nullopt;
            }
        }
    }
    m_table.store(std::move(out));
    m_table.rounds.fetch_add(1, std::memory_order_relaxed);
}

std::unique_ptr<DutyWork> makeFollowDuty(const DutyContext &context, const ProjectorSettings &settings,
    ProjectorShared &shared, const FollowParts &parts)
{
    return std::make_unique<FollowDuty>(context, settings, shared, parts);
}

}}}
