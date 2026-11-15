#ifndef GATEWAY_COMMON_STATUS_H
#define GATEWAY_COMMON_STATUS_H

#include <cstdint>

// Status - what the duty thread publishes (StatusBoardT<Status>, at most every 100 ms, never blocking) and the admin
// thread reads. Every field starts at the value a process with no cluster session has: nothing connected, nothing
// registered, no config, the gate not known open, no keepalive offered, no head probe answered. The session wrapper
// fills them in; until then /ready reads not ready and names every condition that fails.

namespace openexchange { namespace gateway {

struct Status
{
    std::uint64_t sequence = 0;            // StatusBoardT stamps it

    // the cluster session (both binaries)
    bool connected = false;                // a session to the current leader
    bool registered = false;               // the registration ack arrived (not "offered")
    std::int64_t sessionEpoch = 0;         // new sessions so far
    std::int32_t leaderMemberId = -1;      // -1 = no leader known
    std::int64_t lastKeepaliveOfferNs = -1; // monotonic ns of the last successful keepalive offer; -1 = never

    // oe-gateway
    bool configLoaded = false;             // a config report is loaded
    bool gateOpen = false;                 // the global admission gate is OPEN
    std::uint32_t timeoutStreak = 0;       // request deadlines missed in a row (cleared by one reply)
    bool publicListenerAccepting = false;  // the REST listener accepts connections

    // oe-stream
    bool gapOpen = false;                  // an unresolved event gap
    std::int64_t lastHeadProbeNs = -1;     // monotonic ns of the last answered head probe; -1 = never
    bool wsListenerAccepting = false;      // the WebSocket listener accepts connections

    // process
    bool driverConnected = false;          // the Aeron client is connected and the process counter registered; set by
                                           // the process shell when the admin thread reads, not by the duty thread
    std::uint64_t aeronErrors = 0;         // errors the Aeron client conductor reported
    std::int64_t dutyMaxCycleUs = 0;       // the longest duty iteration so far (idle excluded)
    std::uint64_t dutyIterations = 0;
};

}}

#endif
