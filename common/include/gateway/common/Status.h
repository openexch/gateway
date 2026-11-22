#ifndef GATEWAY_COMMON_STATUS_H
#define GATEWAY_COMMON_STATUS_H

#include <array>
#include <cstddef>
#include <cstdint>

// Status - what the duty thread publishes (StatusBoardT<Status>, at most every 100 ms, never blocking) and the admin
// thread reads. Every field starts at the value a process with no cluster session has: nothing connected, nothing
// registered, no config, the gate not known open, no keepalive offered, no head probe answered. The session wrapper
// fills them in; until then /ready reads not ready and names every condition that fails.

namespace openexchange { namespace gateway {

inline constexpr std::size_t WIRE_IGNORED_SLOTS = 4; // at least wire::IGNORED_TEMPLATES.size() (CoreSession asserts it)
// oe-marketdata: one slot per book ladder of the cluster-tools library (markets 1..5, its TICK_MARKETS; the marketdata
// sources static_assert the two agree)
inline constexpr std::size_t MD_MARKET_SLOTS = 5;

// oe-marketdata: what readiness needs of one market's book (Readiness.h judges it against the clock and the bounds)
struct MdMarketStatus
{
    bool expected = false;              // in MD_EXPECTED_MARKETS
    bool snapshot = false;              // a BookSnapshot was applied in the current ME session
    bool ladderOk = false;              // the ladder is OK (not EMPTY, not BROKEN)
    bool empty = true;                  // no level on either side
    std::int64_t lastBookFrameNs = -1;  // monotonic ns of the last book frame the ladder applied; -1 = none this session
};

struct Status
{
    std::uint64_t sequence = 0;            // StatusBoardT stamps it

    // the cluster session (both binaries)
    bool connected = false;                // a session to the current leader
    bool registered = false;               // the registration ack arrived (not "offered")
    std::int64_t sessionEpoch = 0;         // new sessions so far
    std::int32_t leaderMemberId = -1;      // -1 = no leader known
    std::int64_t lastKeepaliveOfferNs = -1; // monotonic ns of the last successful keepalive offer; -1 = never
    bool storageStarved = false;           // every connect attempt of the current episode was refused for storage space
    std::uint64_t storageStarvedSeconds = 0; // every starved second so far (closed episodes plus the current one)

    // oe-gateway
    bool configLoaded = false;             // a config report is loaded
    bool gateOpen = false;                 // the global admission gate is OPEN
    std::uint32_t timeoutStreak = 0;       // request deadlines missed in a row (cleared by one reply)
    bool publicListenerAccepting = false;  // the REST listener accepts connections
    std::uint64_t inflight = 0;            // requests the session holds (offered or waiting to be offered)
    std::uint64_t reoffers = 0;            // successful offers after the first of a request
    std::uint64_t lateReplies = 0;         // replies for a correlation id no longer held, dropped
    std::uint64_t outcomeUnknown = 0;      // requests that reached their deadline offered and unanswered (504)
    std::uint64_t queryBudgetWaitNs = 0;   // total time queries waited for a budget token
    std::uint64_t registrations = 0;       // registration rounds that reached REGISTERED
    std::uint64_t decodeRefused = 0;       // egress frames the money-frame decoder refused
    std::array<std::uint64_t, WIRE_IGNORED_SLOTS> wireIgnored{}; // egress frames of an ignored template, by wire::IGNORED_TEMPLATES position

    // oe-stream
    bool gapOpen = false;                  // an unresolved event gap
    std::int64_t lastHeadProbeNs = -1;     // monotonic ns of the last answered head probe; -1 = never
    bool wsListenerAccepting = false;      // the WebSocket listener accepts connections

    // oe-projector: the writer thread's part is copied in by the admin thread at request time (FrontWork::fillStatus)
    bool leaseHeld = false;                // this copy holds the writer lease (a standby does not)
    bool halted = false;                   // a halt is set (none is a latch: re-evaluated continuously)
    std::int64_t lastBeatNs = -1;          // monotonic ns of the last beat this copy committed; -1 = never

    // oe-marketdata: the ME session and the books (the duty thread), the WebSocket side (the admin thread copies it in)
    bool mdSessionUp = false;              // an ME leader session is open
    std::int64_t mdLastHeartbeatNs = -1;   // monotonic ns of the last ClusterHeartbeat of this session; -1 = none
    std::int64_t mdMeEpoch = 0;            // the meEpoch of the last heartbeat (0 = none yet)
    std::array<MdMarketStatus, MD_MARKET_SLOTS> mdMarkets{};
    bool mdClientsServed = true;           // no frame waits for a client longer than MD_SEND_STALL_MS
    bool mdJournalAttached = false;        // the trade path: false until the journal follower exists in this build
    bool mdJournalFresh = false;
    bool mdHistoryWritable = false;
    bool mdHalted = false;

    // process
    bool driverConnected = false;          // the Aeron client is connected and the process counter registered; set by
                                           // the process shell when the admin thread reads, not by the duty thread
    std::uint64_t aeronErrors = 0;         // errors the Aeron client conductor reported
    std::int64_t dutyMaxCycleUs = 0;       // the longest duty iteration so far (idle excluded)
    std::uint64_t dutyIterations = 0;
};

}}

#endif
