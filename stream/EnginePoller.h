#ifndef GATEWAY_STREAM_ENGINE_POLLER_H
#define GATEWAY_STREAM_ENGINE_POLLER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "gateway/common/DutyBridge.h"

#include "EngineStatus.h"

// EnginePoller - the engine status feed's own thread (st-engine): every STREAM_ENGINE_POLL_MS it asks every configured
// node of the core and of the matching engine for GET /health and GET /metrics on that node's own endpoint (all at
// once; the whole round, name lookups included, ends by STREAM_ENGINE_POLL_TIMEOUT_MS, each body bounded by
// STREAM_ENGINE_MAX_BODY_BYTES), feeds the round to the two ClusterModels and hands the duty thread, through a bounded
// lock-free ring, the election frames of the round and the `engine` frame when the FrameGate lets it out (changed, and
// at most one per STREAM_ENGINE_FRAME_MIN_MS; a held frame goes out when the cap allows, between rounds). The duty
// thread sends them to every connection; it never waits for this thread and this thread never touches a connection.
//
// Addresses. An IP literal is used as is. A host name is looked up on a lookup thread of its own (getaddrinfo cannot be
// cancelled once it runs); the round waits for it only until its deadline, and the node is unreachable for that round
// if the lookup has not finished ("name lookup ... still running"). A lookup thread owns only its own result, never
// the poller, so an abandoned lookup outlives nothing it uses; at most one lookup per node runs at a time. The address
// is kept and looked up again after a request to that node gets no answer (the node may have moved).
//
// Failure reasons. A frame carries a node's failure only as a code (engine::node_error); the text behind it names hosts
// and addresses and goes to the process log, one line when a node's reason changes (and one when it recovers).
//
// Cost per stream instance: 2 x (core nodes + matching-engine nodes) HTTP requests per STREAM_ENGINE_POLL_MS (12 per
// second for 3 + 3 nodes at the default), a new TCP connection each; no cluster session, no log entry. What it read and
// sent is counted in EngineStats (stream_engine_* on /metrics).

namespace openexchange { namespace gateway { namespace stream {

struct NodeAddress; // one node's address (EnginePoller.cpp)

struct EngineOut
{
    enum class Kind : std::uint8_t
    {
        NONE,
        STATUS, // the engine frame (the newest replaces an older one in every connection's queue)
        EVENT,  // an election frame (never coalesced)
    };
    Kind kind = Kind::NONE;
    std::shared_ptr<const std::string> text;
};

using EngineRing = MpmcRing<EngineOut>;

// Frames handed over per round at most before the rest wait for the next round: a batching step (the ring's size).
inline constexpr std::size_t ENGINE_RING_CELLS = 64;

struct EnginePollerSettings
{
    std::vector<engine::NodeEndpointSpec> coreNodes;
    std::vector<engine::NodeEndpointSpec> meNodes;
    std::int64_t pollMs = 1000;
    std::int64_t timeoutMs = 500;
    std::int64_t frameMinMs = 1000;
    std::int64_t maxBodyBytes = 1'048'576;
    std::string threadPrefix = "st";
};

// What the admin thread reads (metrics). Relaxed atomics, written by st-engine only.
struct EngineStats
{
    std::atomic<std::uint64_t> rounds{ 0 };
    std::atomic<std::uint64_t> requests{ 0 };
    std::atomic<std::uint64_t> requestFailures{ 0 };  // no HTTP answer (refused, timeout, too large, unreadable)
    std::atomic<std::uint64_t> bytesRead{ 0 };        // response bytes read (status line, headers and body)
    std::atomic<std::uint64_t> frames{ 0 };           // engine frames handed to the duty thread
    std::atomic<std::uint64_t> electionEvents{ 0 };   // election frames handed to the duty thread
    std::atomic<std::uint64_t> unchanged{ 0 };        // rounds whose frame equalled the last one sent
    std::atomic<std::int64_t> lastFrameBytes{ 0 };
    std::atomic<std::int64_t> lastRoundMs{ 0 };       // how long the last round took
};

// One HTTP GET with a deadline, on the caller's thread (used by the poller for every request, and by the tests).
struct HttpAnswer
{
    bool answered = false;
    int status = 0;
    std::string body;
    std::size_t bytes = 0;
    std::string error; // the full reason, for the process log only (it can name the host)
    std::string code;  // its engine::node_error code (empty when answered)
};

class EnginePoller
{
public:
    EnginePoller(EnginePollerSettings settings, EngineRing &out, EngineStats &stats);
    ~EnginePoller();
    EnginePoller(const EnginePoller &) = delete;
    EnginePoller &operator=(const EnginePoller &) = delete;

    // Hands the duty thread the first frame (every node null: nothing polled yet), then starts st-engine.
    void start();
    // Stops st-engine and joins it (never detaches: the thread uses this object). A round ends by its deadline, so the
    // join takes at most the request timeout; false when it took longer than boundMs + the request timeout.
    bool stop(std::int64_t boundMs);

    // One round on the caller's thread (the thread body; the tests drive it directly). nowWallMs: the time written into
    // the frames; nowSteadyMs: the rate cap's clock.
    void roundOnce(std::int64_t nowWallMs, std::int64_t nowSteadyMs);

private:
    void run();
    void hand(EngineOut out);
    void flush();
    void releaseHeld(std::int64_t nowSteadyMs);
    void logReasons(); // a node's failure text, written to the process log when it changes

    EnginePollerSettings m_settings;
    EngineRing &m_out;
    EngineStats &m_stats;
    engine::ClusterModel m_core;
    engine::ClusterModel m_me;
    engine::FrameGate m_gate;
    std::deque<EngineOut> m_pending; // frames the ring could not take yet (an engine frame is replaced by a newer one)
    std::vector<std::unique_ptr<NodeAddress>> m_addresses; // core nodes, then matching-engine nodes
    std::vector<std::string> m_loggedDetail; // per node (same order): the reason last written to the process log
    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_stop = false;
    std::thread m_thread;
};

// GETs `target` from every endpoint at once (all of it, name lookups included, bounded by timeoutMs; each body by
// maxBodyBytes); answers in input order.
std::vector<HttpAnswer> httpGetAll(const std::vector<std::pair<engine::NodeEndpointSpec, std::string>> &requests,
    std::int64_t timeoutMs, std::int64_t maxBodyBytes);

}}}

#endif
