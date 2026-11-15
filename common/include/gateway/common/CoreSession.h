#ifndef GATEWAY_COMMON_CORE_SESSION_H
#define GATEWAY_COMMON_CORE_SESSION_H

#include <cinttypes>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/CoreSessionRegistrar.h"
#include "cluster_tools/runtime/LogRing.h"
#include "cluster_tools/runtime/StorageStarvation.h"

#include "gateway/common/SessionParts.h"
#include "gateway/common/Status.h"
#include "gateway/common/Wire.h"

// CoreSession - one cluster session to the core and everything a request needs on it, driven by the duty thread
// (the only thread that touches it). Over any ClusterClient (Aeron in the binaries, the FakeTransport in T0).
//
// Registration. RegisterSession {class, resumable, channels, identity} through the cluster-tools registrar, after
// every connect and every new leader. Registered means the AdminAck arrived, never that the frame was offered: no
// command or query is offered while the registrar is not registered in the client's current session and term. The
// AdminAck reaches the registrar only after the money-frame decoder accepted it, so an AdminAck of any other schema
// version never registers the session (the exported registrar reads one at any version). REJECTED (by ack, or by the
// registrar's bound of unanswered sessions) ends the session for good: exitCode() 78.
//
// Requests. submit() takes a command, gives it a correlation id unique for the life of the process, encodes it once and
// holds it in the request table (bounded by maxInflight; full = OVERLOADED, nothing sent) until its first reply or its
// deadline (requestTimeoutMs). Every duty cycle offers the waiting requests in submission order, one tryOffer each:
//   OK                                     the request is in flight
//   BACK_PRESSURED, NOT_READY, NOT_CONNECTED   nothing was sent; the pass stops and the request is offered again next
//                                          cycle (NOT_CONNECTED is the normal answer during an election)
//   CLOSED, MAX_POSITION_EXCEEDED          the session is over; the pass stops, the client reconnects and registers,
//                                          then the request is offered on the new session inside its deadline
//   TRANSPORT_ERROR                        not retried: INTERNAL, counted (the frames are fixed length and short, so
//                                          this is a defect)
// After a new registration on another session or term every request still in flight is offered once more, in its
// original order, with the same correlation id and the same request id: a keyed command replays its stored decision.
// A core reject UNREGISTERED_SESSION or NOT_RESUMABLE is never an answer: the session registers again and the command
// is offered again inside its deadline. The first reply for a correlation id completes the request; a reply for a
// correlation id the table no longer holds is dropped and counted (late replies).
// At the deadline: a request offered at least once and unanswered is OUTCOME_UNKNOWN (504; it may have been applied)
// and counts toward the timeout streak; one never offered is CORE_UNAVAILABLE (nothing was sent), or OVERLOADED when a
// query waited for its budget token.
//
// Queries (53, 54, 55, 56) take a token of the session's query bucket (queryRateCount per queryRatePeriodMs, burst
// queryBurst) before their offer and wait for one in submission order; commands never wait behind a query.
// RequestOrderSnapshot replies (106) carry no correlation id, so one 53 is in flight at a time and its frames are taken
// only from the leadership term it was offered in.
//
// Config. After every registration, and after every ConfigApplied newer than the report held, the session reads
// CoreConfigReport (56, through the query bucket) into its ConfigCache; AdmissionState frames move the gate states in
// between, ordered by log position. config() is the duty thread's view, configSnapshot() the published one.
//
// Egress decode. Every egress frame goes through wire::Decoder first. A refused frame is counted (decode_refused) and
// logged rate limited (the first, then every 1000th, with the running count); an ignored template (OrderFault, which
// the core broadcasts on CONTROL) is counted by template (wire_ignored) and never logged.
//
// Readiness. fillStatus() writes what /ready reads: connected (client CONNECTED), registered (ack), config
// loaded (a recorded CoreConfig was read), global gate OPEN, the time of the last successful keepalive offer, the
// timeout streak (deadlines missed in a row; one reply clears it), and the storage-starvation bit.
//
// Storage starvation. A connect attempt the media driver refuses for storage space (its detail classified by
// cluster_tools::runtime::isStorageSpaceError) starts an episode; a successful connect or a failure for another reason
// ends it. While it lasts the session reads not ready (storage_space); once it lasted STORAGE_STARVED_EXIT_NS (the
// runtime's 30 s, a constant in code) exitCode() is 70: restartable, never 78. Without it a session on a full tmpfs
// retries forever, alive and never progressing.
//
// Test-only controls (never in a published build): GW_NC_NOT_CONNECTED_FATAL, GW_NC_REGISTERED_AT_OFFER,
// GW_NC_NO_REOFFER, GW_NC_LATE_REPLY_BY_KEY, GW_NC_FRESH_KEY, GW_NC_NO_QUERY_BUDGET, GW_NC_ADMINACK_ANY_VERSION,
// GW_NC_NO_STORAGE_STARVATION each
// switch one rule off; tests/CMakeLists.txt names the failure text each must produce.

namespace openexchange { namespace gateway { namespace session {

struct SessionCounters
{
    std::uint64_t submitted = 0;
    std::uint64_t refusedFull = 0;
    std::uint64_t offers = 0;            // successful offers
    std::uint64_t reoffers = 0;          // successful offers after the first of a request
    std::uint64_t requeuedAfterRegistration = 0;
    std::uint64_t offerBackPressured = 0;
    std::uint64_t offerNotConnected = 0;
    std::uint64_t offerNotReady = 0;
    std::uint64_t offerClosed = 0;
    std::uint64_t transportErrors = 0;
    std::uint64_t replies = 0;
    std::uint64_t lateReplies = 0;
    std::uint64_t outcomeUnknown = 0;
    std::uint64_t notSent = 0;           // CORE_UNAVAILABLE at the deadline
    std::uint64_t budgetExpired = 0;     // OVERLOADED at the deadline (query budget)
    std::uint64_t budgetWaitNs = 0;      // total time queries waited for a token
    std::uint64_t unregisteredRejects = 0;
    std::uint64_t registrations = 0;     // registration rounds that reached REGISTERED
    std::uint64_t configReads = 0;
    std::uint64_t configReports = 0;
    std::uint64_t reconnects = 0;
    std::uint64_t staleTermFrames = 0;   // snapshot frames of another leadership term, dropped
    std::uint64_t commandsOfferedUnregistered = 0; // stays 0: no command before the registration ack; counted where that rule would break
    std::uint64_t refusalWarnings = 0;   // WARN lines written for refused egress frames (rate limited)
};

template<typename Client>
class CoreSession : private wire::ReplyHandler
{
public:
    using Registrar = cl::CoreSessionRegistrar<Client>;
    using Sink = std::function<void(const Outcome &)>;
    static constexpr std::int64_t MS = 1'000'000LL;

    CoreSession(Client &client, SessionSettings settings, Sink sink, cluster_tools::runtime::LogRing *log = nullptr) :
        m_client(client),
        m_settings(settings),
        m_registrar(client, registrarSettings(settings)),
        m_sink(std::move(sink)),
        m_log(log),
        m_bucket(settings.queryRateCount, settings.queryRatePeriodMs, settings.queryBurst),
        m_nextCorrelationId(settings.correlationIdBase)
    {
    }

    static cl::RegistrarSettings registrarSettings(const SessionSettings &s)
    {
        cl::RegistrarSettings r;
        r.sessionClass = s.sessionClass;
        r.credentialClass = s.credentialClass;
        r.channels = s.channels;
        r.identity = s.identity;
        r.resumable = true;
        r.resendIntervalNs = s.registerResendMs * MS;
        return r;
    }

    // Empty = valid.
    std::string validate() const
    {
        std::string out = session::validate(m_settings);
        const std::string r = m_registrar.validateSettings();
        if (!r.empty())
        {
            out += (out.empty() ? "" : "\n") + r;
        }
        return out;
    }

    // The first connect. False with the client's detail when it refused (its settings).
    bool start()
    {
        m_started = true;
        return m_client.connect();
    }

    // Takes one request; the outcome reaches the sink later, on this thread. Never sends anything itself.
    SubmitResult submit(Command command)
    {
        return enqueue(std::move(command), false);
    }

    // One duty cycle: reconnect, the client's and the registrar's work, the egress poll (pollEgress false keeps the
    // egress unread: a test throttle), the registration round, the offers and the deadlines. Returns the work done.
    int doWork(bool pollEgress = true)
    {
        if (0 != m_exitCode)
        {
            return 0;
        }
        int work = reconnectIfDue();
        work += m_client.doWork();
        if (watchStorage())
        {
            return work + 1;
        }
        noteKeepalive();
        work += m_registrar.doWork();
        if (m_registrar.isRejected())
        {
            terminate(78, "registration REJECTED: " + m_registrar.detail());
            return work;
        }
        if (pollEgress)
        {
            work += m_client.poll([this](const cl::EgressMessage &m) { onEgress(m); }, m_settings.pollFragments);
        }
        noteKeepalive();
        work += registrationRound();
        work += offerPass();
        work += expire();
        return work;
    }

    // 0 while the session may still work; 78 for a configuration refusal the operator must fix; 70 when every connect
    // attempt was refused for storage space for the runtime's bound (restartable).
    int exitCode() const { return m_exitCode; }
    const std::string &exitDetail() const { return m_exitDetail; }

    void fillStatus(Status &s) const
    {
        s.connected = m_client.isConnected();
        s.registered = registeredNow();
        s.sessionEpoch = static_cast<std::int64_t>(m_client.sessionEpoch());
        s.leaderMemberId = s.connected ? m_client.leaderMemberId() : -1;
        s.lastKeepaliveOfferNs = m_lastKeepaliveNs;
        s.configLoaded = m_config.current().loaded();
        s.gateOpen = m_config.current().gateOpen();
        s.timeoutStreak = m_timeoutStreak;
        s.inflight = static_cast<std::uint64_t>(m_entries.size());
        s.reoffers = m_counters.reoffers;
        s.lateReplies = m_counters.lateReplies;
        s.outcomeUnknown = m_counters.outcomeUnknown;
        s.queryBudgetWaitNs = m_counters.budgetWaitNs;
        s.registrations = m_counters.registrations;
        s.decodeRefused = m_decoder.counters().refusedTotal();
        static_assert(wire::IGNORED_TEMPLATES.size() <= WIRE_IGNORED_SLOTS, "Status::wireIgnored has a slot per template");
        for (std::size_t k = 0; k < wire::IGNORED_TEMPLATES.size(); ++k)
        {
            s.wireIgnored[k] = m_decoder.counters().ignored[k];
        }
        s.storageStarved = m_storage.active();
        s.storageStarvedSeconds = m_storage.starvedSeconds(m_client.nanoTime());
    }

    const SessionCounters &counters() const { return m_counters; }
    const Registrar &registrar() const { return m_registrar; }
    const wire::Decoder &decoder() const { return m_decoder; }
    const CoreConfig &config() const { return m_config.current(); }
    std::shared_ptr<const CoreConfig> configSnapshot() const { return m_config.snapshot(); }
    std::uint32_t timeoutStreak() const { return m_timeoutStreak; }
    std::size_t held() const { return m_entries.size(); }
    std::size_t inFlight() const
    {
        std::size_t n = 0;
        for (const auto &e : m_entries)
        {
            n += State::IN_FLIGHT == e.second.state ? 1 : 0;
        }
        return n;
    }
    std::int64_t lastKeepaliveOfferNs() const { return m_lastKeepaliveNs; }

    // Polite close of the session (SessionCloseRequest). The held requests are not completed: the process is stopping.
    void close() { m_client.close(); }

private:
    enum class State : std::uint8_t
    {
        WAITING,   // not offered in the current registration round
        IN_FLIGHT, // offered in the round recorded in the entry
    };

    struct Entry
    {
        std::uint64_t ticket = 0;
        std::uint16_t templateId = 0;
        bool query = false;
        bool internal = false;            // the session's own config read: no sink
        Command command;
        std::int64_t correlationId = 0;
        std::vector<std::uint8_t> frame;
        State state = State::WAITING;
        std::uint32_t offers = 0;
        std::uint64_t offeredEpoch = 0;
        std::int64_t offeredTerm = -1;
        std::int64_t submittedNs = 0;
        std::int64_t deadlineNs = 0;
        std::int64_t budgetWaitStartNs = -1; // -1 = not waiting for a token now
        std::int64_t budgetWaitNs = 0;
        bool budgetBlocked = false;          // waited for a token at least once and holds none now
        std::int64_t requestIdHigh = 0;
        std::int64_t requestIdLow = 0;
        Reply partial;                       // a multi-frame reply being assembled
    };

    // ---- submit

    SubmitResult enqueue(Command command, bool internal)
    {
        SubmitResult r;
        if (0 != m_exitCode)
        {
            r.refusal = OutcomeKind::CORE_UNAVAILABLE;
            return r;
        }
        if (static_cast<std::int64_t>(m_entries.size()) >= m_settings.maxInflight)
        {
            m_counters.refusedFull++;
            r.refusal = OutcomeKind::OVERLOADED;
            return r;
        }
        const std::int64_t now = m_client.nanoTime();
        Entry e;
        e.ticket = ++m_nextTicket;
        e.templateId = templateOf(command);
        e.query = isQuery(e.templateId);
        e.internal = internal;
        e.correlationId = ++m_nextCorrelationId;
        e.command = std::move(command);
        requestIdOf(e.command, e.requestIdHigh, e.requestIdLow);
        e.frame.resize(wire::maxCommandLength());
        const std::size_t length = encodeCommand(e.command, e.correlationId, e.frame.data(), e.frame.size());
        e.frame.resize(length);
        e.submittedNs = now;
        e.deadlineNs = now + m_settings.requestTimeoutMs * MS;
        m_counters.submitted++;
        r.accepted = true;
        r.ticket = e.ticket;
        const std::uint64_t seq = e.ticket;
        m_byCorrelation.emplace(e.correlationId, seq);
        m_entries.emplace(seq, std::move(e));
        return r;
    }

    // ---- session lifecycle

    int reconnectIfDue()
    {
        if (!m_started)
        {
            return 0;
        }
        const cl::ClientState state = m_client.state();
        if (cl::ClientState::CLOSED != state && cl::ClientState::FAILED != state)
        {
            m_downNoted = false;
            return 0;
        }
        const std::int64_t now = m_client.nanoTime();
        if (!m_downNoted)
        {
            m_downNoted = true;
            m_reconnectAtNs = now + m_settings.reconnectDelayMs * MS;
            if (cl::ClientState::FAILED == state)
            {
                const cl::ConnectError error = m_client.connectError();
                if (cl::ConnectError::AUTHENTICATION_REJECTED == error || cl::ConnectError::CHALLENGE_UNSUPPORTED == error ||
                    cl::ConnectError::INVALID_SETTINGS == error)
                {
                    terminate(78, std::string("the core refused the session: ") + cl::nameOf(error) + " (" +
                        m_client.lastDetail() + ")");
                    return 1;
                }
                logLine(cluster_tools::runtime::LogLevel::WARN, "session: connect failed (%s): %s; retry in %" PRId64 " ms",
                    cl::nameOf(error), m_client.lastDetail().c_str(), m_settings.reconnectDelayMs);
            }
            else
            {
                logLine(cluster_tools::runtime::LogLevel::WARN, "session: closed (%s): %s; reconnect in %" PRId64 " ms",
                    cl::nameOf(m_client.closeReason()), m_client.lastDetail().c_str(), m_settings.reconnectDelayMs);
            }
        }
        if (now - m_reconnectAtNs < 0)
        {
            return 0;
        }
        m_downNoted = false;
        m_counters.reconnects++;
        m_client.connect();
        return 1;
    }

    // Feeds the storage-starvation episode from the client's failed connect attempts. True when it ended the session.
    bool watchStorage()
    {
#ifdef GW_NC_NO_STORAGE_STARVATION
        // test-only control: a connect refused for storage space is retried forever, like any other failure
        return false;
#else
        const std::int64_t now = m_client.nanoTime();
        if (m_storageFeed.update(m_client, m_client.isConnected(), now, m_storage))
        {
            logLine(cluster_tools::runtime::LogLevel::ERROR,
                "session: connect refused for storage space (not ready; exit 70 if it lasts %" PRId64 " s): %s",
                cluster_tools::runtime::STORAGE_STARVED_EXIT_NS / 1'000'000'000LL, m_storage.why());
        }
        if (m_storage.expired(now))
        {
            terminate(70, "every connect attempt refused for storage space for " +
                std::to_string(m_storage.heldNs(now) / 1'000'000'000LL) + " s (the driver dir's tmpfs is too small or "
                "full): " + m_storage.why());
            return true;
        }
        return false;
#endif
    }

    void noteKeepalive()
    {
        const std::uint64_t sent = m_client.counters().keepAlivesSent;
        if (sent != m_keepalivesSeen)
        {
            m_keepalivesSeen = sent;
            m_lastKeepaliveNs = m_client.nanoTime();
        }
    }

    bool registeredNow() const
    {
#ifdef GW_NC_REGISTERED_AT_OFFER
        // test-only control: "registered" set when the RegisterSession was offered, not when its ack arrived
        return m_client.isConnected() &&
            (m_registrar.isRegistered() || cl::RegistrationState::PENDING == m_registrar.state());
#else
        return m_registrar.isRegistered();
#endif
    }

    // A registration round reached REGISTERED: offer again what is in flight from another session or term, and read
    // the config.
    int registrationRound()
    {
        if (!registeredNow())
        {
            return 0;
        }
        const std::int64_t round = m_registrar.correlationId();
        if (round == m_registeredRound)
        {
            return 0;
        }
        m_registeredRound = round;
        m_counters.registrations++;
        const std::uint64_t epoch = m_client.sessionEpoch();
        const std::int64_t term = m_client.leadershipTermId();
        std::uint64_t requeued = 0;
        for (auto &[seq, e] : m_entries)
        {
            if (State::IN_FLIGHT == e.state && (e.offeredEpoch != epoch || e.offeredTerm != term))
            {
#ifndef GW_NC_NO_REOFFER
                requeue(e);
                requeued++;
#endif
            }
        }
        m_counters.requeuedAfterRegistration += requeued;
        logLine(cluster_tools::runtime::LogLevel::INFO,
            "session: registered (session epoch %" PRIu64 ", term %" PRId64 ", leader %d, round %" PRId64 "); %" PRIu64
            " in-flight request(s) to offer again", epoch, term, m_client.leaderMemberId(), round, requeued);
        if (m_settings.readConfig)
        {
            requestConfigRead();
        }
        return 1;
    }

    void requeue(Entry &e)
    {
        e.state = State::WAITING;
        e.partial = Reply{};
        if (m_snapshotSeq == e.ticket)
        {
            m_snapshotSeq = 0;
        }
#ifdef GW_NC_FRESH_KEY
        // test-only control: a new request id minted for every attempt (the rule "same requestId on a re-offer" off)
        e.requestIdLow += 0x1000;
        setRequestId(e.command, e.requestIdHigh, e.requestIdLow);
        e.frame.resize(wire::maxCommandLength());
        e.frame.resize(encodeCommand(e.command, e.correlationId, e.frame.data(), e.frame.size()));
#endif
    }

#ifdef GW_NC_FRESH_KEY
    static void setRequestId(Command &command, std::int64_t high, std::int64_t low)
    {
        std::visit(
            [&](auto &c) {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, wire::CreateOrder> || std::is_same_v<T, wire::CancelOrder> ||
                    std::is_same_v<T, wire::AmendOrder>)
                {
                    c.requestIdHigh = high;
                    c.requestIdLow = low;
                }
            },
            command);
    }
#endif

    void requestConfigRead()
    {
        if (m_configReadTicket != 0 && m_entries.count(m_configReadTicket) > 0)
        {
            m_configRereadWanted = true; // one is waiting or in flight; read again once it answers
            return;
        }
        const SubmitResult r = enqueue(Command{ wire::RequestCoreConfig{} }, true);
        if (r.accepted)
        {
            m_configReadTicket = r.ticket;
            m_counters.configReads++;
            m_configRereadWanted = false;
        }
        else
        {
            m_configRereadWanted = true; // the table is full: retried by the next registration round or ConfigApplied
        }
    }

    // ---- offers

    int offerPass()
    {
        if (!registeredNow())
        {
            return 0;
        }
        const std::int64_t now = m_client.nanoTime();
        int work = 0;
        bool queriesWait = false;
        for (auto it = m_entries.begin(); it != m_entries.end();)
        {
            Entry &e = it->second;
            if (State::WAITING != e.state)
            {
                ++it;
                continue;
            }
            bool tookToken = false;
            if (e.query)
            {
                if (wire::tid::REQUEST_ORDER_SNAPSHOT == e.templateId && 0 != m_snapshotSeq)
                {
                    ++it; // one 53 in flight at a time: its frames carry no correlation id
                    continue;
                }
                if (queriesWait)
                {
                    noteBudgetWait(e, now); // behind a query that waits for its token
                    ++it;
                    continue;
                }
#ifdef GW_NC_NO_QUERY_BUDGET
                tookToken = true; // test-only control: the query budget off
#else
                tookToken = m_bucket.tryTake(now);
#endif
                if (!tookToken)
                {
                    noteBudgetWait(e, now);
                    queriesWait = true; // later queries wait behind this one; commands do not
                    ++it;
                    continue;
                }
                if (e.budgetWaitStartNs >= 0)
                {
                    e.budgetWaitNs += now - e.budgetWaitStartNs;
                    m_counters.budgetWaitNs += static_cast<std::uint64_t>(now - e.budgetWaitStartNs);
                    e.budgetWaitStartNs = -1;
                }
                e.budgetBlocked = false;
            }
            if (!m_registrar.isRegistered())
            {
                m_counters.commandsOfferedUnregistered++; // reachable only with the GW_NC_REGISTERED_AT_OFFER control
            }
            const cl::OfferResult result = m_client.tryOffer(e.frame.data(), e.frame.size());
            switch (result.status)
            {
                case cl::OfferStatus::OK:
                    e.state = State::IN_FLIGHT;
                    e.offers++;
                    e.offeredEpoch = m_client.sessionEpoch();
                    e.offeredTerm = m_client.leadershipTermId();
                    m_counters.offers++;
                    if (e.offers > 1)
                    {
                        m_counters.reoffers++;
                    }
                    if (wire::tid::REQUEST_ORDER_SNAPSHOT == e.templateId)
                    {
                        m_snapshotSeq = e.ticket;
                    }
                    work++;
                    ++it;
                    continue;
                case cl::OfferStatus::TRANSPORT_ERROR:
                    m_counters.transportErrors++;
                    logLine(cluster_tools::runtime::LogLevel::ERROR,
                        "session: the transport refused a %zu-byte %s frame (TRANSPORT_ERROR, a defect): INTERNAL",
                        e.frame.size(), wire::templateName(e.templateId));
                    it = complete(it, OutcomeKind::INTERNAL, Reply{}, now);
                    work++;
                    continue;
                case cl::OfferStatus::NOT_CONNECTED:
                    m_counters.offerNotConnected++;
#ifdef GW_NC_NOT_CONNECTED_FATAL
                    // test-only control: NOT_CONNECTED treated as the end of the request
                    it = complete(it, OutcomeKind::CORE_UNAVAILABLE, Reply{}, now);
                    continue;
#endif
                    break;
                case cl::OfferStatus::BACK_PRESSURED:
                    m_counters.offerBackPressured++;
                    break;
                case cl::OfferStatus::NOT_READY:
                    m_counters.offerNotReady++;
                    break;
                case cl::OfferStatus::CLOSED:
                case cl::OfferStatus::MAX_POSITION_EXCEEDED:
                    m_counters.offerClosed++;
                    break;
            }
            // nothing was sent: the token goes back, the pass stops (the next request would meet the same
            // publication) and this request leads the next cycle
            if (tookToken)
            {
                m_bucket.refund();
            }
            return work;
        }
        return work;
    }

    static void noteBudgetWait(Entry &e, std::int64_t now)
    {
        if (e.budgetWaitStartNs < 0)
        {
            e.budgetWaitStartNs = now;
        }
        e.budgetBlocked = true;
    }

    // ---- deadlines

    int expire()
    {
        const std::int64_t now = m_client.nanoTime();
        int work = 0;
        for (auto it = m_entries.begin(); it != m_entries.end();)
        {
            Entry &e = it->second;
            if (now - e.deadlineNs < 0)
            {
                ++it;
                continue;
            }
            OutcomeKind kind = OutcomeKind::CORE_UNAVAILABLE;
            if (e.offers > 0)
            {
                kind = OutcomeKind::OUTCOME_UNKNOWN;
                m_counters.outcomeUnknown++;
                m_timeoutStreak++;
            }
            else if (e.budgetBlocked)
            {
                kind = OutcomeKind::OVERLOADED;
                m_counters.budgetExpired++;
                if (e.budgetWaitStartNs >= 0)
                {
                    e.budgetWaitNs += now - e.budgetWaitStartNs;
                    m_counters.budgetWaitNs += static_cast<std::uint64_t>(now - e.budgetWaitStartNs);
                }
            }
            else
            {
                m_counters.notSent++;
            }
            const bool configRead = it->first == m_configReadTicket;
            it = complete(it, kind, Reply{}, now);
            if (configRead && m_settings.readConfig)
            {
                requestConfigRead(); // the config read is never abandoned
            }
            work++;
        }
        return work;
    }

    using Iterator = typename std::map<std::uint64_t, Entry>::iterator;

    Iterator complete(Iterator it, OutcomeKind kind, Reply reply, std::int64_t now)
    {
        Entry &e = it->second;
        if (OutcomeKind::REPLY == kind)
        {
            m_counters.replies++;
            m_timeoutStreak = 0;
        }
        if (m_snapshotSeq == e.ticket)
        {
            m_snapshotSeq = 0;
        }
        m_byCorrelation.erase(e.correlationId);
        if (!e.internal)
        {
            Outcome o;
            o.ticket = e.ticket;
            o.kind = kind;
            o.templateId = e.templateId;
            o.correlationId = e.correlationId;
            o.offers = e.offers;
            o.submittedNs = e.submittedNs;
            o.completedNs = now;
            o.budgetWaitNs = e.budgetWaitNs;
            o.reply = std::move(reply);
            if (m_sink)
            {
                m_sink(o);
            }
        }
        return m_entries.erase(it);
    }

    // ---- egress

    void onEgress(const cl::EgressMessage &message)
    {
        m_message = &message;
        m_frameTemplate = 0;
        m_frameCorrelation = 0;
#ifdef GW_NC_ADMINACK_ANY_VERSION
        // test-only control: the registrar reads the frame before the decoder's exact-version gate
        if (m_registrar.onEgress(message))
        {
            m_message = nullptr;
            return;
        }
#endif
        const wire::Refusal refusal = m_decoder.decode(message.payload, message.length, *this);
        if (wire::Refusal::NONE != refusal)
        {
            const std::uint64_t refused = m_decoder.counters().refusedTotal();
            if (wire::refusalLogged(refused))
            {
                m_counters.refusalWarnings++;
                logLine(cluster_tools::runtime::LogLevel::WARN, "session: %s (%" PRIu64 " egress frames refused so far)",
                    wire::describe(m_decoder.lastRefusal()).c_str(), refused);
            }
        }
        else
        {
            finishFrame();
        }
        m_message = nullptr;
    }

    // a frame that carried a head and its group entries is complete once decode() returned
    void finishFrame()
    {
        if (wire::tid::USER_BALANCES == m_frameTemplate || wire::tid::CORE_CONFIG_REPORT == m_frameTemplate)
        {
            if (wire::tid::CORE_CONFIG_REPORT == m_frameTemplate)
            {
                m_config.endReport();
                m_counters.configReports++;
            }
            auto found = m_byCorrelation.find(m_frameCorrelation);
            if (found == m_byCorrelation.end())
            {
                m_counters.lateReplies++;
                return;
            }
            auto it = m_entries.find(found->second);
            Reply reply = std::move(it->second.partial);
            if (wire::tid::CORE_CONFIG_REPORT == m_frameTemplate)
            {
                reply = ConfigReply{ m_config.snapshot() };
            }
            const bool configRead = it->first == m_configReadTicket;
            complete(it, OutcomeKind::REPLY, std::move(reply), m_client.nanoTime());
            if (configRead)
            {
                m_configReadTicket = 0;
                if (m_configRereadWanted && m_settings.readConfig)
                {
                    requestConfigRead();
                }
            }
        }
    }

    // the entry a reply with this correlation id answers, or end() (a late reply, counted)
    Iterator answered(std::int64_t correlationId, std::int64_t requestIdHigh = 0, std::int64_t requestIdLow = 0)
    {
        auto found = m_byCorrelation.find(correlationId);
        if (found != m_byCorrelation.end())
        {
            return m_entries.find(found->second);
        }
#ifdef GW_NC_LATE_REPLY_BY_KEY
        // test-only control: a reply for an unknown correlation is matched to a live request by its request id
        if (0 != requestIdHigh || 0 != requestIdLow)
        {
            for (auto it = m_entries.begin(); it != m_entries.end(); ++it)
            {
                if (it->second.requestIdHigh == requestIdHigh && it->second.requestIdLow == requestIdLow)
                {
                    return it;
                }
            }
        }
#else
        (void)requestIdHigh;
        (void)requestIdLow;
#endif
        m_counters.lateReplies++;
        return m_entries.end();
    }

    void onAdminAck(const wire::AdminAck &ack) override
    {
        if (wire::tid::REGISTER_SESSION == ack.refTemplateId && nullptr != m_message)
        {
            m_registrar.onEgress(*m_message); // only after the decoder accepted the frame at the exact version
        }
    }

    void onAdmissionState(const wire::AdmissionState &admission) override
    {
        m_config.onAdmission(admission);
    }

    void onConfigApplied(const wire::ConfigApplied &applied) override
    {
        if (m_settings.readConfig && m_config.staleAfter(applied))
        {
            requestConfigRead();
        }
    }

    void onOrderAck(const wire::OrderAck &ack) override
    {
        auto it = answered(ack.correlationId, ack.requestIdHigh, ack.requestIdLow);
        if (it != m_entries.end())
        {
            complete(it, OutcomeKind::REPLY, Reply{ ack }, m_client.nanoTime());
        }
    }

    void onOrderReject(const wire::OrderReject &reject) override
    {
        auto it = answered(reject.correlationId, reject.requestIdHigh, reject.requestIdLow);
        if (it == m_entries.end())
        {
            return;
        }
        if (wire::money::CoreRejectReason::UNREGISTERED_SESSION == reject.reason ||
            wire::money::CoreRejectReason::NOT_RESUMABLE == reject.reason)
        {
            // never an answer: register again, then this command is offered again inside its deadline
            m_counters.unregisteredRejects++;
            requeue(it->second);
            m_registrar.requestReRegistration();
            logLine(cluster_tools::runtime::LogLevel::WARN,
                "session: the core answered %s to %s (correlation %" PRId64 "): registering again",
                wire::money::CoreRejectReason::c_str(reject.reason), wire::templateName(it->second.templateId),
                reject.correlationId);
            return;
        }
        complete(it, OutcomeKind::REPLY, Reply{ reject }, m_client.nanoTime());
    }

    void onOrderSnapshotBatch(std::uint16_t) override
    {
        m_snapshotFrameOk = false;
        if (0 == m_snapshotSeq || nullptr == m_message)
        {
            m_counters.lateReplies++;
            return;
        }
        auto it = m_entries.find(m_snapshotSeq);
        if (it == m_entries.end() || m_message->leadershipTermId != it->second.offeredTerm)
        {
            m_counters.staleTermFrames++; // rule S: a frame of another term is not this request's
            return;
        }
        if (!std::holds_alternative<SnapshotReply>(it->second.partial))
        {
            it->second.partial = SnapshotReply{};
        }
        m_snapshotFrameOk = true;
    }

    void onOrderSnapshotEntry(const wire::OrderSnapshotEntry &entry) override
    {
        if (!m_snapshotFrameOk)
        {
            return;
        }
        auto it = m_entries.find(m_snapshotSeq);
        std::get<SnapshotReply>(it->second.partial).entries.push_back(entry);
    }

    void onOrderSnapshotEnd(const wire::OrderSnapshotEnd &end) override
    {
        auto it = answered(end.correlationId);
        if (it == m_entries.end())
        {
            return;
        }
        if (nullptr != m_message && m_message->leadershipTermId != it->second.offeredTerm)
        {
            m_counters.staleTermFrames++;
            return;
        }
        SnapshotReply reply;
        if (std::holds_alternative<SnapshotReply>(it->second.partial))
        {
            reply = std::move(std::get<SnapshotReply>(it->second.partial));
        }
        reply.end = end;
        complete(it, OutcomeKind::REPLY, Reply{ std::move(reply) }, m_client.nanoTime());
    }

    void onOrderEventsPage(std::int64_t correlationId, std::uint16_t) override
    {
        auto found = m_byCorrelation.find(correlationId);
        if (found == m_byCorrelation.end())
        {
            m_counters.lateReplies++;
            return;
        }
        Entry &e = m_entries.find(found->second)->second;
        if (!std::holds_alternative<EventsReply>(e.partial))
        {
            e.partial = EventsReply{};
        }
    }

    void onPageEvent(std::int64_t correlationId, const wire::OrderEvent &event) override
    {
        auto found = m_byCorrelation.find(correlationId);
        if (found != m_byCorrelation.end())
        {
            Entry &e = m_entries.find(found->second)->second;
            if (std::holds_alternative<EventsReply>(e.partial))
            {
                std::get<EventsReply>(e.partial).events.push_back(event);
            }
        }
    }

    void onOrderEventsEnd(const wire::OrderEventsEnd &end) override
    {
        auto it = answered(end.correlationId);
        if (it == m_entries.end())
        {
            return;
        }
        EventsReply reply;
        if (std::holds_alternative<EventsReply>(it->second.partial))
        {
            reply = std::move(std::get<EventsReply>(it->second.partial));
        }
        reply.end = end;
        complete(it, OutcomeKind::REPLY, Reply{ std::move(reply) }, m_client.nanoTime());
    }

    void onUserBalances(const wire::UserBalancesHead &head) override
    {
        m_frameTemplate = wire::tid::USER_BALANCES;
        m_frameCorrelation = head.correlationId;
        auto found = m_byCorrelation.find(head.correlationId);
        if (found != m_byCorrelation.end())
        {
            BalancesReply reply;
            reply.head = head;
            m_entries.find(found->second)->second.partial = std::move(reply);
        }
    }

    void onUserBalanceLine(std::int64_t correlationId, const wire::BalanceLine &line) override
    {
        auto found = m_byCorrelation.find(correlationId);
        if (found != m_byCorrelation.end())
        {
            Entry &e = m_entries.find(found->second)->second;
            if (std::holds_alternative<BalancesReply>(e.partial))
            {
                std::get<BalancesReply>(e.partial).lines.push_back(line);
            }
        }
    }

    void onCoreConfigReport(const wire::CoreConfigHead &head) override
    {
        m_frameTemplate = wire::tid::CORE_CONFIG_REPORT;
        m_frameCorrelation = head.correlationId;
        m_config.beginReport(head);
    }

    void onCoreConfigMarket(const wire::MarketConfig &market) override { m_config.addMarket(market); }
    void onCoreConfigAsset(const wire::AssetConfig &asset) override { m_config.addAsset(asset); }

    void onDepositAck(const wire::MoneyAck &ack) override
    {
        auto it = answered(ack.correlationId);
        if (it != m_entries.end())
        {
            complete(it, OutcomeKind::REPLY, Reply{ DepositAckReply{ ack } }, m_client.nanoTime());
        }
    }

    void onWithdrawAck(const wire::MoneyAck &ack) override
    {
        auto it = answered(ack.correlationId);
        if (it != m_entries.end())
        {
            complete(it, OutcomeKind::REPLY, Reply{ WithdrawAckReply{ ack } }, m_client.nanoTime());
        }
    }

    void onDepositReject(const wire::DepositReject &reject) override
    {
        auto it = answered(reject.correlationId, reject.requestIdHigh, reject.requestIdLow);
        if (it != m_entries.end())
        {
            complete(it, OutcomeKind::REPLY, Reply{ reject }, m_client.nanoTime());
        }
    }

    void onWithdrawReject(const wire::WithdrawReject &reject) override
    {
        auto it = answered(reject.correlationId);
        if (it != m_entries.end())
        {
            complete(it, OutcomeKind::REPLY, Reply{ reject }, m_client.nanoTime());
        }
    }

    // ---- misc

    void terminate(int code, const std::string &why)
    {
        if (0 != m_exitCode)
        {
            return;
        }
        m_exitCode = code;
        m_exitDetail = why;
        logLine(cluster_tools::runtime::LogLevel::CRITICAL, "session: %s (exit %d)", why.c_str(), code);
    }

    template<typename... Args>
    void logLine(cluster_tools::runtime::LogLevel level, const char *format, Args... args)
    {
        if (nullptr != m_log)
        {
            m_log->push(level, m_client.nanoTime(), format, args...);
        }
    }

    Client &m_client;
    SessionSettings m_settings;
    Registrar m_registrar;
    Sink m_sink;
    cluster_tools::runtime::LogRing *m_log;
    wire::Decoder m_decoder;
    ConfigCache m_config;
    TokenBucket m_bucket;
    SessionCounters m_counters;

    std::map<std::uint64_t, Entry> m_entries; // by ticket = submission order
    std::unordered_map<std::int64_t, std::uint64_t> m_byCorrelation;
    std::uint64_t m_nextTicket = 0;
    std::int64_t m_nextCorrelationId;
    std::uint64_t m_snapshotSeq = 0;   // the RequestOrderSnapshot in flight, 0 = none
    bool m_snapshotFrameOk = false;
    std::uint64_t m_configReadTicket = 0;
    bool m_configRereadWanted = false;

    const cl::EgressMessage *m_message = nullptr;
    std::uint16_t m_frameTemplate = 0;
    std::int64_t m_frameCorrelation = 0;

    bool m_started = false;
    bool m_downNoted = false;
    std::int64_t m_reconnectAtNs = 0;
    std::int64_t m_registeredRound = 0;
    std::uint64_t m_keepalivesSeen = 0;
    std::int64_t m_lastKeepaliveNs = -1;
    std::uint32_t m_timeoutStreak = 0;
    int m_exitCode = 0;
    cluster_tools::runtime::StorageStarvation m_storage;
    cluster_tools::runtime::ClientStorageFeed<Client> m_storageFeed;
    std::string m_exitDetail;
};

}}}

#endif
