#include "JournalFollow.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "cluster_tools/adapters/sbe/JournalTradeCodec.h"
#include "cluster_tools/archive/aeron/ChainFollower.h"
#include "cluster_tools/client/aeron/FatalErrors.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/Supervisor.h"

#include "gateway/common/ExitCodes.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace {

namespace aa = cluster_tools::archive::aeron_archive;
namespace at = cluster_tools::client::aeron_transport;
namespace rt = cluster_tools::runtime;
using ar::Clock;
using ar::TimePoint;

constexpr std::int64_t MS = 1'000'000LL;
// replay fragments taken per duty cycle (a batching step, not a capacity: the pending list bounds what is held)
constexpr int FRAGMENTS_PER_CYCLE = 256;
// how often the view for /health is published (it is a copy under a lock)
constexpr std::int64_t VIEW_EVERY_NS = 20 * MS;

enum class Mark : std::uint8_t
{
    NONE,
    BELOW_RANGE,
    HOLE,
    SCHEMA
};

const char *nameOf(Mark m) noexcept
{
    switch (m)
    {
        case Mark::NONE: return "NONE";
        case Mark::BELOW_RANGE: return "BELOW_RANGE";
        case Mark::HOLE: return "HOLE";
        case Mark::SCHEMA: break;
    }
    return "SCHEMA";
}

struct SourceState
{
    Mark mark = Mark::NONE;
    std::int64_t blockedUntilNs = 0;
    std::string why;
};

struct LeaveRequest
{
    std::size_t source = 0;
    Mark mark = Mark::NONE;
    std::string why;
};

}

class JournalFollow::Impl final : public ar::FrameSink, public ar::FollowObserver
{
public:
    Impl(const DutyContext &context, const MarketDataSettings &s, TradeShared &shared, std::function<void()> invoker,
        std::function<std::uint64_t()> egressSchemaRefusals, LiveTrades *live) :
        m_context(context), m_settings(s), m_shared(shared), m_ledger(shared.ledger), m_invoker(std::move(invoker)),
        m_egressSchemaRefusals(std::move(egressSchemaRefusals)), m_live(live),
        m_memo(s.journal.archives.size(), &shared.archiveMetrics)
    {
        ar::FollowSettings follow;
        follow.journalMtu = s.journal.journalMtu;
        follow.journalStreamId = s.journal.journalStreamId;
        follow.profile = ar::ME_JOURNAL;
        follow.neverConnectedTimeout = std::chrono::milliseconds(s.archiveTimeoutMs);
        follow.endpointResolveTimeout = std::chrono::milliseconds(s.archiveTimeoutMs);
        follow.probeInterval = std::chrono::milliseconds(s.sourceProbeMs);
        follow.backoff = std::chrono::milliseconds(s.sourceBackoffMs);
        follow.keyByArchiveId = false;
        for (std::size_t i = 0; i < s.journal.archives.size(); ++i)
        {
            aa::ArchiveSourceConfig config;
            config.identity.endpoint = s.journal.archives[i].text();
            config.identity.controlStreamId = s.journal.controlStreamId;
            config.identity.journalStreamId = s.journal.journalStreamId;
            config.identity.channel = s.journal.journalChannel;
            config.localHost = s.journal.localHost;
            config.replayStreamId = s.journal.replayStreamId;
            config.messageTimeoutNs = static_cast<std::uint64_t>(s.archiveTimeoutMs) * 1'000'000ULL;
            config.keyByArchiveId = false;
            m_followers.push_back(std::make_unique<aa::ChainFollower>(i, config, follow, m_memo, context.aeron,
                monitor(), [this] { invokerStep(); }, &shared.archiveMetrics, this, aa::ArchiveSourceSeams{},
                [](const std::string &message) {
                    std::fprintf(stderr, "oe-marketdata: FATAL archive client: %s (exit 70)\n", message.c_str());
                    std::fflush(stderr);
                    std::_Exit(exit_code::SOFTWARE);
                }));
            m_sources.emplace_back();
            m_names.push_back(config.identity.endpoint);
        }
    }

    int doWork(std::int64_t now)
    {
        const TimePoint tp = Clock::now();
        int work = collect();
        if (m_egressSchemaRefusals && 0 != m_egressSchemaRefusals() && !m_halts.has(MdHalt::SOURCE_SCHEMA))
        {
            setHalt(MdHalt::SOURCE_SCHEMA, "the ME egress carries an order schema other than version 12: never decoded "
                "by guess");
        }
        if (!m_started)
        {
            publish(now, tp);
            return work;
        }
        if (latched())
        {
            // a latch: the read stops for good (the book path serves on); the replay is finished and the source left
            if (m_current)
            {
                if (!m_detaching)
                {
                    m_followers[*m_current]->policy().detach(tp);
                    m_detaching = true;
                }
                work += drive(tp, 0);
            }
            publish(now, tp);
            return work;
        }
        if (!m_current)
        {
            work += choose(now, tp);
        }
        if (m_current)
        {
            const bool full = m_ledger.pending() >= m_ledger.capacity();
            if (full)
            {
                m_view.pausedFull++;
            }
            work += drive(tp, m_detaching || full ? 0 : FRAGMENTS_PER_CYCLE);
            afterPoll(now, tp);
        }
        publish(now, tp);
        return work;
    }

    void fillStatus(Status &status, std::int64_t now) const
    {
        const TimePoint tp = Clock::now();
        status.mdJournalAttached = m_started && m_current && !m_detaching && m_followers[*m_current]->policy().attached();
        status.mdJournalFresh = status.mdJournalAttached && fresh(now, tp);
        status.mdHistoryWritable = m_ledger.writable.load(std::memory_order_acquire);
        status.mdHalted = m_halts.any();
    }

    void close()
    {
        if (m_current)
        {
            m_followers[*m_current]->policy().detach(Clock::now());
            m_detaching = true;
            const std::int64_t deadline = rt::Supervisor::monotonicNs() +
                std::min(m_settings.common.stopTimeoutMs, m_settings.common.dutyWatchdogMs) / 2 * MS;
            while (m_current && rt::Supervisor::monotonicNs() < deadline)
            {
                drive(Clock::now(), 0);
                std::this_thread::yield();
            }
        }
    }

    // ---- the frame sink (md-duty, inside the follower's poll)

    ar::SinkAction onFrame(const ar::JournalFrame &frame) noexcept override
    {
        try
        {
            return onFrameImpl(frame);
        }
        catch (...)
        {
            return ar::SinkAction::ABORT;
        }
    }

    ar::RefusalAction onRefused(const ar::RefusedFrame &frame) noexcept override
    {
        try
        {
#ifdef MD_NC_NO_SCHEMA_CHECK
            // test build only: a frame refused for its schema is decoded as a trade anyway
            if (ar::Refusal::SCHEMA == frame.reason &&
                frame.length >= cluster_tools::sbe_adapter::journal_codec::TRADE_RECORD_LENGTH)
            {
                cluster_tools::sbe_adapter::journal_codec::TradeRecord t;
                cluster_tools::sbe_adapter::journal_codec::readTrade(frame.bytes, t);
                PendingEntry e;
                e.seq = ar::readLe64(frame.bytes + ar::FRAME_OFFSET_JOURNAL_SEQ);
                e.hasTrade = true;
                e.trade = TradeRow{ t.marketId, t.tradeId, t.price, t.quantity, 1 == t.takerIsBuyRaw, t.timestamp, e.seq };
                e.source = frame.sourceIndex;
                e.readAtNs = rt::Supervisor::monotonicNs();
                if (e.seq == m_next && m_ledger.push(e))
                {
                    m_next++;
                }
                return ar::RefusalAction::SKIP_AND_CONTINUE;
            }
#endif
            m_view.refusals++;
            m_leave = LeaveRequest{ frame.sourceIndex, Mark::SCHEMA, std::string("frame refused by the journal gate: ") +
                ar::refusalLabel(frame.reason) + " (the ME journal is schema 4 version 1 only; never decoded by guess)" };
        }
        catch (...)
        {
        }
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
        m_firstOfRead = true;
        if (nullptr != m_context.log)
        {
            m_context.log->push(rt::LogLevel::INFO, rt::Supervisor::monotonicNs(), "md-duty: journal source %zu "
                "recording %lld attached at %lld (%s), next journalSeq %lld", a.source,
                static_cast<long long>(a.recording.key.recordingId), static_cast<long long>(a.from),
                a.fromProof ? "from the memo" : a.memoFree ? "from the chain start" : "after the memo",
                static_cast<long long>(m_next));
        }
    }

    void onSourceRefused(std::size_t source, const ar::Recording &r) noexcept override
    {
        try
        {
            m_faultLeave = LeaveRequest{ source, Mark::NONE, "the recording's MTU (" + std::to_string(r.key.mtuLength) +
                ") is not MD_JOURNAL_MTU (" + std::to_string(m_settings.journal.journalMtu) + ")" };
        }
        catch (...)
        {
        }
    }

private:
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

    void invokerStep()
    {
        if (m_invoker && !m_inInvoker)
        {
            m_inInvoker = true;
            m_invoker();
            m_inInvoker = false;
        }
        std::this_thread::yield();
    }

    void log(rt::LogLevel level, const std::string &text)
    {
        if (nullptr != m_context.log)
        {
            m_context.log->push(level, rt::Supervisor::monotonicNs(), "md-duty: %s", text.c_str());
        }
        else
        {
            std::fprintf(stderr, "oe-marketdata: md-duty: %s\n", text.c_str());
        }
    }

    bool latched() const noexcept
    {
        return m_halts.has(MdHalt::EPOCH_MISMATCH) || m_halts.has(MdHalt::SOURCE_SCHEMA) ||
            m_halts.has(MdHalt::UNKNOWN_MARKET) || m_halts.has(MdHalt::JOURNAL_PREFIX_PURGED);
    }

    void setHalt(MdHalt h, const std::string &detail)
    {
        if (m_halts.set(h, detail))
        {
            log(rt::LogLevel::CRITICAL, std::string("trade path halt ") + nameOf(h) + ": " + detail + (MdHalt::GAP == h
                ? std::string(" (re-scanned every ") + std::to_string(m_settings.gapRescanMs) + " ms)"
                : std::string(" (the trade path is parked; the book path serves)")));
            // the log ring drains on the main thread; the halt line also goes straight to stderr for the operator
            std::fprintf(stderr, "oe-marketdata: CRITICAL trade path halt %s: %s\n", nameOf(h), detail.c_str());
            std::fflush(stderr);
        }
    }

    // the writer's reports: md_state at start, the commits
    int collect()
    {
        WriterReport r = m_ledger.collect();
        int work = 0;
        if (r.state && !m_started)
        {
            const MdState &st = *r.state;
            m_epoch = st.epoch;
            m_next = st.epoch ? st.lastSeq + 1 : 0;
            m_started = true;
            work++;
            if (nullptr != m_live)
            {
                // the memory state at the cursor, before any trade above it
                m_live->onTradePathStart(r.seed ? std::move(*r.seed) : MarketSeed{});
            }
            if (st.epoch)
            {
                m_memo.onReport(*st.epoch, st.lastSeq);
            }
            log(rt::LogLevel::INFO, "trade path starts: md_state epoch " + (st.epoch ? std::to_string(*st.epoch)
                : std::string("none (an empty history: it starts at the first record the archives hold)")) +
                ", cursor " + std::to_string(st.lastSeq));
        }
        if (r.committedSeq >= 0 && m_epoch)
        {
            m_memo.onReport(*m_epoch, r.committedSeq);
            if (r.proof && r.proof->seq == r.committedSeq && r.proof->key.recordingId >= 0 &&
                r.proof->source < m_followers.size())
            {
                m_memo.prove(r.proof->source, r.proof->key, r.proof->seq, *m_epoch, r.proof->endPosition);
            }
            work++;
        }
        return work;
    }

    int choose(std::int64_t now, TimePoint tp)
    {
        bool allMarked = true;
        for (std::size_t i = 0; i < m_sources.size(); ++i)
        {
            const SourceState &s = m_sources[i];
            if (Mark::NONE != s.mark)
            {
                continue;
            }
            allMarked = false;
            if (now >= s.blockedUntilNs)
            {
                m_current = i;
                m_detaching = false;
                m_firstOfRead = true;
                m_followers[i]->policy().attach(tp);
                log(rt::LogLevel::INFO, "attaching journal source " + m_names[i] + " at journalSeq " +
                    std::to_string(m_next));
                return 1;
            }
        }
        if (!allMarked)
        {
            return 0; // a source backs off: wait for it
        }
        bool schema = false;
        bool allBelow = true;
        for (const SourceState &s : m_sources)
        {
            schema = schema || Mark::SCHEMA == s.mark;
            allBelow = allBelow && Mark::BELOW_RANGE == s.mark;
        }
        std::string why;
        for (std::size_t i = 0; i < m_sources.size(); ++i)
        {
            why += (i ? "; " : "") + m_names[i] + ": " + m_sources[i].why;
        }
        if (schema)
        {
            setHalt(MdHalt::SOURCE_SCHEMA, why);
        }
        else if (allBelow)
        {
            setHalt(MdHalt::JOURNAL_PREFIX_PURGED, "journalSeq " + std::to_string(m_next) + " is purged from every "
                "source (" + why + "): the records after the committed cursor " + std::to_string(m_next - 1) +
                " are gone; the console's marketdata-reset restarts the history (partial)");
        }
        else
        {
            if (m_gapSinceNs < 0)
            {
                m_gapSinceNs = now;
            }
            setHalt(MdHalt::GAP, "no source holds journalSeq " + std::to_string(m_next) + " (" + why + ")");
            if (now - m_gapSinceNs >= m_settings.gapRescanMs * MS)
            {
                m_gapSinceNs = -1;
                m_view.rescans++;
                for (std::size_t i = 0; i < m_sources.size(); ++i)
                {
                    m_sources[i].mark = Mark::NONE;
                    m_sources[i].blockedUntilNs = 0;
                    m_memo.invalidate(i, ar::MemoReason::HOLE_VERIFY);
                }
                log(rt::LogLevel::INFO, "GAP re-scan: every source is read again from its chain start");
                return 1;
            }
        }
        return 0;
    }

    int drive(TimePoint tp, int limit)
    {
        if (!m_current)
        {
            return 0;
        }
        aa::ChainFollower &f = *m_followers[*m_current];
        const int work = f.doWork(*this, limit, tp);
        if (m_detaching && f.policy().idle() && !f.policy().wantsDisconnect())
        {
            m_current.reset();
            m_detaching = false;
        }
        return work;
    }

    void leave(const LeaveRequest &l, std::int64_t now, TimePoint tp)
    {
        if (l.source >= m_sources.size())
        {
            return;
        }
        SourceState &s = m_sources[l.source];
        if (Mark::NONE != l.mark)
        {
            s.mark = l.mark;
        }
        s.why = l.why;
        s.blockedUntilNs = now + m_settings.sourceBackoffMs * MS;
        m_view.switches++;
        log(Mark::NONE == l.mark ? rt::LogLevel::WARN : rt::LogLevel::ERROR, "journal source " + m_names[l.source] +
            " left: " + l.why);
        if (m_current && *m_current == l.source && !m_detaching)
        {
            m_followers[l.source]->policy().detach(tp);
            m_detaching = true;
        }
    }

    void afterPoll(std::int64_t now, TimePoint tp)
    {
        if (!m_current)
        {
            m_faults.clear();
            return;
        }
        const std::size_t cur = *m_current;
        if (m_reRead)
        {
            m_reRead = false;
            m_view.reReads++;
            m_memo.invalidate(cur, ar::MemoReason::HOLE_VERIFY);
            m_firstOfRead = true;
            m_followers[cur]->policy().reattach(tp);
            log(rt::LogLevel::INFO, "journal source " + m_names[cur] + ": a jump on a read from its memo is not "
                "believed; read again from its chain start");
        }
        if (m_leave)
        {
            const LeaveRequest l = *m_leave;
            m_leave.reset();
            if (Mark::HOLE == l.mark)
            {
                m_view.holes++;
            }
            else if (Mark::BELOW_RANGE == l.mark)
            {
                m_view.belowRange++;
            }
            leave(l, now, tp);
        }
        if (m_faultLeave)
        {
            const LeaveRequest l = *m_faultLeave;
            m_faultLeave.reset();
            leave(l, now, tp);
        }
        for (const ar::SourceFault &f : m_faults)
        {
            switch (f.kind)
            {
                case ar::FaultKind::NONE:
                case ar::FaultKind::FATAL:
                case ar::FaultKind::SCHEMA_REFUSED: // marked by the sink already
                    break;
                case ar::FaultKind::POSITION_REJECTED:
                case ar::FaultKind::RESUME_BELT:
                case ar::FaultKind::SEQ_REGRESSION_IN_RECORDING:
                    m_firstOfRead = true; // the library reads this source again without the proof
                    log(rt::LogLevel::WARN, "journal source " + m_names[f.source] + ": " + ar::faultName(f.kind) + ": " +
                        f.detail);
                    break;
                default:
                    if (m_current && *m_current == f.source)
                    {
                        leave(LeaveRequest{ f.source, Mark::NONE, std::string(ar::faultName(f.kind)) + ": " + f.detail },
                            now, tp);
                    }
                    break;
            }
        }
        m_faults.clear();
    }

    ar::SinkAction onFrameImpl(const ar::JournalFrame &frame)
    {
        const ar::Peek &p = frame.peek;
        ForwardInput in;
        in.seq = p.journalSeq;
        in.epoch = p.meEpoch;
        in.next = m_next;
        in.boundEpoch = m_epoch;
        in.memoFreeRead = frame.readWasMemoFree;
        in.firstOfRead = m_firstOfRead;
        const Forward v = forwardRule(in);
        switch (v)
        {
            case Forward::EPOCH_MISMATCH:
                setHalt(MdHalt::EPOCH_MISMATCH, std::string(ANOTHER_GENESIS_TEXT) + ": journalSeq " +
                    std::to_string(p.journalSeq) + " carries meEpoch " + std::to_string(p.meEpoch) + ", md_state.me_epoch "
                    "is " + std::to_string(m_epoch.value_or(0)) + "; nothing is written; only the genesis reset clears it");
                return ar::SinkAction::ABORT;
            case Forward::SKIP:
                m_firstOfRead = false;
                return ar::SinkAction::CONTINUE;
            case Forward::RE_READ:
                m_reRead = true;
                return ar::SinkAction::ABORT;
            case Forward::BELOW_RANGE:
            case Forward::HOLE:
                if (!m_leave)
                {
                    m_leave = LeaveRequest{ frame.sourceIndex, Forward::HOLE == v ? Mark::HOLE : Mark::BELOW_RANGE,
                        std::string(nameOf(v)) + ": does not hold journalSeq " + std::to_string(m_next) + ".." +
                            std::to_string(p.journalSeq - 1) + " (read from its chain start)" };
                }
                return ar::SinkAction::ABORT;
            case Forward::START:
            case Forward::TAKE:
                break;
        }
        PendingEntry e;
        e.seq = p.journalSeq;
        e.source = frame.sourceIndex;
        e.key = frame.key;
        e.endPosition = frame.endPosition;
        e.readAtNs = rt::Supervisor::monotonicNs();
        switch (takeRecord(frame.bytes, frame.length, p.templateId, m_settings.expectedMarkets, e.trade))
        {
            case Taken::TRADE:
                e.hasTrade = true;
                break;
            case Taken::ADVANCE:
                break;
            case Taken::UNKNOWN_MARKET:
                setHalt(MdHalt::UNKNOWN_MARKET, "journalSeq " + std::to_string(p.journalSeq) + " is trade " +
                    std::to_string(e.trade.tradeId) + " of market " + std::to_string(e.trade.marketId) + ", outside "
                    "MD_EXPECTED_MARKETS: neither stored nor skipped; a restart with the market in the set resumes here");
                return ar::SinkAction::ABORT;
            case Taken::BAD_RECORD:
                if (!m_leave)
                {
                    m_leave = LeaveRequest{ frame.sourceIndex, Mark::SCHEMA, "journalSeq " +
                        std::to_string(p.journalSeq) + ": record 1 does not decode (its takerIsBuy byte is not 0 or 1)" };
                }
                return ar::SinkAction::ABORT;
        }
        if (!m_ledger.push(e))
        {
            return ar::SinkAction::ABORT; // the pending list is full: read again later, nothing dropped
        }
        if (Forward::START == v)
        {
            m_epoch = p.meEpoch;
#ifdef MD_NC_NO_PARTIAL_FLAG
            m_ledger.bind(p.meEpoch, 1); // test build only: a history that starts above 1 is recorded as whole
#else
            m_ledger.bind(p.meEpoch, p.journalSeq);
#endif
            log(p.journalSeq > 1 ? rt::LogLevel::WARN : rt::LogLevel::INFO, "an empty market history starts at "
                "journalSeq " + std::to_string(p.journalSeq) + ", meEpoch " + std::to_string(p.meEpoch) +
                (p.journalSeq > 1 ? ": the archives do not hold the journal's first record, the history is partial"
                                  : ""));
        }
        m_next = p.journalSeq + 1;
        m_firstOfRead = false;
        if (m_halts.has(MdHalt::GAP))
        {
            m_halts.clearGap();
            m_gapSinceNs = -1;
            log(rt::LogLevel::INFO, "GAP cleared: journalSeq " + std::to_string(p.journalSeq) + " delivered");
            for (SourceState &s : m_sources)
            {
                s.mark = Mark::NONE;
            }
        }
        if (e.hasTrade && !m_halts.any() && m_liveEmit.offer(p.journalSeq))
        {
            m_view.liveEmitted++;
            if (nullptr != m_live)
            {
                m_live->onLiveTrade(e.trade);
            }
        }
        return ar::SinkAction::CONTINUE;
    }

    bool fresh(std::int64_t now, TimePoint tp) const
    {
        if (!m_current)
        {
            return false;
        }
#ifdef MD_NC_NO_STALL_CHECK
        (void)tp; // test build only: an attached follower reads fresh whatever its stall verdicts say
#else
        if (!ar::checkedClean(m_followers[*m_current]->policy().stallVerdicts(), tp,
                std::chrono::milliseconds(2 * m_settings.sourceProbeMs)))
        {
            return false;
        }
#endif
        const std::int64_t oldest = m_ledger.oldestReadAtNs();
        return oldest < 0 || now - oldest < m_settings.historyStaleMs * MS;
    }

    void publish(std::int64_t now, TimePoint tp)
    {
        if (now - m_lastViewNs < VIEW_EVERY_NS && m_lastViewNs > 0)
        {
            return;
        }
        m_lastViewNs = now;
        TradeView &v = m_view;
        v.started = m_started;
        v.next = m_next;
        v.liveEmit = m_liveEmit.value();
        v.epoch = m_epoch;
        v.halt = m_halts.primary();
        v.haltDetail = m_halts.detail(v.halt);
        for (std::size_t i = 0; i < v.halts.size(); ++i)
        {
            v.halts[i] = m_halts.has(static_cast<MdHalt>(i));
        }
        v.source = m_current;
        v.phase = m_current ? ar::phaseName(m_followers[*m_current]->policy().phase()) : "IDLE";
        v.attached = m_current && !m_detaching && m_followers[*m_current]->policy().attached();
        v.fresh = v.attached && fresh(now, tp);
        v.marks.clear();
        for (const SourceState &s : m_sources)
        {
            v.marks.push_back(std::string(nameOf(s.mark)) + (now < s.blockedUntilNs ? "/backoff " +
                std::to_string((s.blockedUntilNs - now) / MS) + " ms" : std::string()));
        }
        m_shared.view.set(v);
    }

    const DutyContext m_context;
    const MarketDataSettings &m_settings;
    TradeShared &m_shared;
    TradeLedger &m_ledger;
    std::function<void()> m_invoker;
    std::function<std::uint64_t()> m_egressSchemaRefusals;
    LiveTrades *m_live;
    std::shared_ptr<at::AeronFatalMonitor> m_ownMonitor;
    ar::ResumeMemo m_memo;
    std::vector<std::unique_ptr<aa::ChainFollower>> m_followers;
    std::vector<SourceState> m_sources;
    std::vector<std::string> m_names;
    std::optional<std::size_t> m_current;
    bool m_detaching = false;
    bool m_inInvoker = false;
    bool m_started = false;
    std::int64_t m_next = 0;
    std::optional<std::int64_t> m_epoch;
    bool m_firstOfRead = true;
    bool m_reRead = false;
    std::optional<LeaveRequest> m_leave;
    std::optional<LeaveRequest> m_faultLeave;
    std::vector<ar::SourceFault> m_faults;
    TradeHalts m_halts;
    LiveEmitCursor m_liveEmit;
    std::int64_t m_gapSinceNs = -1;
    std::int64_t m_lastViewNs = 0;
    TradeView m_view;
};

JournalFollow::JournalFollow(const DutyContext &context, const MarketDataSettings &settings, TradeShared &shared,
    std::function<void()> invoker, std::function<std::uint64_t()> egressSchemaRefusals, LiveTrades *live) :
    m_impl(std::make_unique<Impl>(context, settings, shared, std::move(invoker), std::move(egressSchemaRefusals), live))
{
}

JournalFollow::~JournalFollow() = default;

int JournalFollow::doWork(std::int64_t nowNs)
{
    return m_impl->doWork(nowNs);
}

void JournalFollow::fillStatus(Status &status, std::int64_t nowNs) const
{
    m_impl->fillStatus(status, nowNs);
}

void JournalFollow::close()
{
    m_impl->close();
}

}}}
