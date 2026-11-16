#ifndef GATEWAY_STREAM_STREAM_CORE_H
#define GATEWAY_STREAM_STREAM_CORE_H

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "cluster_tools/runtime/LogRing.h"

#include "gateway/common/SessionParts.h"
#include "gateway/common/Wire.h"

// StreamCore - everything oe-stream knows, owned by the duty thread (st-duty) and touched by no other thread: the event
// window and its watermark, the gap and replay machine, the per-user caches, the stitch of a snapshot with the stream,
// the resume rules, the per-connection outbound queues and the router. Pure: no Aeron, no socket, no clock of its own.
// The core session feeds it (onRegistered, onLiveOrderEvent, onLiveBalance, onAdmission, onOutcome), it asks the core
// through a QueryPort, and it hands frames to each connection's Outlet (the I/O side). Nothing here blocks.
//
// The event window. The last `eventWindow` order events in eventSeq order and the watermark W: every event up to W was
// received (live or replayed) and routed. Derived state, empty at start: the first head probe (RequestOrderEvents with maxEvents 0)
// sets W to the core's last eventSeq.
//
// Gaps. eventSeq is dense. A live event above W + 1, a new registration (session lost or leader changed: broadcast frames
// may be gone) or a head probe that reports lastSeq above W opens a gap: the stream asks RequestOrderEvents from W in
// pages of `replayPage` through the query budget and buffers live events meanwhile (at most `gapBufferEvents`). A gap
// that fills is only delay for the clients. An overrun (the core's ring starts above W + 1, the core is behind the
// cursor, or the buffer bound is reached) clears the window, sends every connection `resync` and re-reads each connected
// user's open orders through the budget, oldest connection first.
//
// Per-user cache. While a user has a connection, and for `userLingerMs` after the last one closed, the stream keeps the
// user's open orders (built from one snapshot and the events after it) and balances (one UserBalances reply and the
// lines after it). A second connection or a quick reconnect is answered from it with no core query. Every cache is
// dropped at a new registration: derived state never outlives the session it came from.
//
// The stitch (a cache miss). RequestOrderSnapshot page by page until nextOrderId is 0, then every window event of the
// user with logPosition above the FIRST page's asOfLogPosition and at or below the LAST page's is folded into the merged
// pages in eventSeq order (events are full state). The snapshot frame carries the result and asOf.eventSeq S, the highest
// eventSeq at or below the last page's position; the FILL events of the folded span follow as `fill` frames, and the
// user's events above S follow as live frames. The stitch waits while a gap is open (the window must hold every event up
// to the last page's position) and starts again when the window lost an event of the span. No page is read before
// the first head probe has set the watermark (the span could not be checked against the window).
//
// Balances have no sequence: a line that arrives while the UserBalances read is in flight is older than its reply and is
// discarded; a line after the reply is newer and applies. This relies on the core's per-session egress keeping ORIGIN and
// BROADCAST frames in apply order (one queue per session; the balance race of the T1 suite tests it).
//
// Resume. subscribe with resume.eventSeq N: the window holds every event above N -> the user's events above N are
// replayed from it, no core query; the window is younger but the core's ring covers N -> one backfill into the window
// (one at a time, shared by every waiting client, budgeted); otherwise `resync` RESUME_TOO_OLD and a snapshot.
//
// Slow clients. Each connection has an outbound queue bounded in messages and bytes (`wsQueueMessages`, `wsQueueBytes`,
// counting what is queued here and what the I/O side holds unwritten, whichever path a frame took; the byte test
// leaves out the largest frame since the connection was last empty, so one large snapshot still goes out). An `order` frame replaces a queued `order` frame
// of the same orderId (the latest state wins, it moves to the tail); `fill` frames never coalesce. At the bound the
// connection is closed 4001 RESYNC: nothing is dropped silently, and one client never delays another or this thread.
//
// Engine status. The `engine` frame (EngineStatus.h) is not user data: every connection gets the newest one when it opens
// and every new one after, before and after it subscribes, like `admission`; a queued older one is replaced (key
// ENGINE_FRAME_KEY, never an orderId). `election` frames go to every connection, never coalesced.
//
// Test-only controls (never in a published build; tests/CMakeLists.txt names the failure text each must produce):
// GW_NC_STITCH_INCLUSIVE, GW_NC_SINGLE_PAGE_ASOF, GW_NC_NO_REPLAY, GW_NC_NO_HEARTBEAT, GW_NC_BALANCE_REPLY_LAST,
// GW_NC_NO_RESNAPSHOT_PACING, GW_NC_UNBOUNDED_QUEUE, GW_NC_ROUTE_BY_FRAME_USER, GW_NC_COALESCE_FILLS.

namespace openexchange { namespace gateway { namespace stream {

namespace channel {
inline constexpr unsigned ORDERS = 1u;
inline constexpr unsigned FILLS = 2u;
inline constexpr unsigned BALANCES = 4u;
}

// The coalescing key of the engine frame in a connection's queue (orderIds are positive).
inline constexpr std::int64_t ENGINE_FRAME_KEY = -1;

namespace close_code {
inline constexpr std::uint16_t RESYNC = 4001;        // the outbound queue reached its bound
inline constexpr std::uint16_t PROTOCOL = 4003;      // a client frame named a user (or another protocol error of binding)
}

enum class ResyncReason : std::uint8_t
{
    EVENT_GAP,
    LEADER_CHANGE,
    SESSION_LOST,
    RESUME_TOO_OLD,
};
inline constexpr std::size_t RESYNC_REASONS = 4;
const char *nameOf(ResyncReason reason) noexcept;

using Text = std::shared_ptr<const std::string>;

// The I/O side of one connection, as the duty thread sees it. Every call is non-blocking.
class Outlet
{
public:
    virtual ~Outlet() = default;
    // Hands one frame to the writer; false when its handover is full now (the frame stays queued here).
    virtual bool offer(const Text &frame) = 0;
    virtual std::int64_t unwrittenMessages() const = 0;
    virtual std::int64_t unwrittenBytes() const = 0;
    virtual void close(std::uint16_t code, const char *reason) = 0; // asynchronous
    virtual bool closed() const = 0;                                // the socket is gone
};

// How the stream asks the core: the session's submit (the query budget and the deadline are the session's).
class QueryPort
{
public:
    virtual ~QueryPort() = default;
    virtual session::SubmitResult submit(session::Command command) = 0;
};

struct StreamCoreSettings
{
    std::int64_t eventWindow = 65'536;
    std::int64_t replayPage = 1024;
    std::int64_t gapBufferEvents = 65'536;
    std::int64_t queryBurst = 50;       // queries this core keeps outstanding at most (the session's burst)
    std::int64_t userLingerMs = 30'000;
    std::int64_t headProbeMs = 5000;
    std::int64_t heartbeatMs = 5000;
    std::int64_t wsQueueMessages = 256;
    std::int64_t wsQueueBytes = 1'048'576;
    bool balanceStream = true;
};

// What the admin thread reads (metrics). Written by the duty thread with relaxed stores, never a lock.
struct StreamStats
{
    std::atomic<std::uint64_t> gaps{ 0 };              // gaps opened
    std::atomic<std::uint64_t> replayedEvents{ 0 };    // events taken from a replay or backfill page
    std::atomic<std::uint64_t> replayPages{ 0 };
    std::array<std::atomic<std::uint64_t>, RESYNC_REASONS> resyncs{}; // resync frames sent, by reason
    std::atomic<std::int64_t> windowFirstSeq{ 0 };
    std::atomic<std::int64_t> watermark{ -1 };
    std::atomic<std::int64_t> usersCached{ 0 };        // users with an order or balance cache
    std::atomic<std::int64_t> users{ 0 };              // users known (connected or lingering)
    std::atomic<std::int64_t> connections{ 0 };        // connections open on the duty side
    std::atomic<std::int64_t> queueHighWater{ 0 };     // the most messages one connection held queued (here + unwritten)
    std::atomic<std::uint64_t> snapshotQueries{ 0 };   // RequestOrderSnapshot pages submitted
    std::atomic<std::uint64_t> balanceQueries{ 0 };    // RequestUserBalances submitted
    std::atomic<std::uint64_t> eventQueries{ 0 };      // RequestOrderEvents submitted (replay, backfill, head probe)
    std::atomic<std::uint64_t> cacheServes{ 0 };       // snapshots answered from a cache (no core query)
    std::atomic<std::uint64_t> windowResumes{ 0 };     // resumes served from the window
    std::atomic<std::uint64_t> backfills{ 0 };         // backfill rounds completed
    std::atomic<std::uint64_t> closedAtBound{ 0 };     // connections closed 4001
    std::atomic<std::uint64_t> staleBalanceLines{ 0 }; // lines discarded while a balance read was in flight
    std::atomic<std::uint64_t> queryFailures{ 0 };     // queries that ended without a reply (retried)
};

class StreamCore
{
public:
    using ConfigSource = std::function<std::shared_ptr<const session::CoreConfig>()>;

    // `stats`: where the counters go (the process keeps them for the admin thread); null = this core's own.
    StreamCore(StreamCoreSettings settings, QueryPort &queries, ConfigSource config,
        cluster_tools::runtime::LogRing *log = nullptr, StreamStats *stats = nullptr);
    ~StreamCore();
    StreamCore(const StreamCore &) = delete;
    StreamCore &operator=(const StreamCore &) = delete;

    // ---- the core session
    void onRegistered(std::uint64_t sessionEpoch, std::int64_t leadershipTermId, std::int64_t nowNs);
    void onLiveOrderEvent(const wire::OrderEvent &event, std::int64_t nowNs);
    void onLiveBalance(const wire::BalanceUpdate &line, std::int64_t nowNs);
    void onAdmission(const wire::AdmissionState &admission);
    // ---- the engine status feed (EnginePoller, through the duty thread): an `engine` frame goes to every connection
    // and is kept for the connections that open later (a queued older one is replaced); an `election` frame goes to
    // every connection and is not kept
    void onEngineStatus(Text frame);
    void onEngineEvent(const Text &frame);
    // The outcome of a query this core submitted (others are ignored: false).
    bool onOutcome(const session::Outcome &outcome, std::int64_t nowNs);

    // ---- connections (the I/O side through the command ring)
    void open(std::uint64_t connId, std::int64_t userId, std::shared_ptr<Outlet> outlet, std::int64_t nowNs);
    // `frameUserId`: the userId a client frame named (0 = none); only the test-only control GW_NC_ROUTE_BY_FRAME_USER
    // reads it (the I/O side closes such a connection 4003 before it gets here).
    void subscribe(std::uint64_t connId, unsigned channels, std::optional<std::int64_t> resumeFrom, std::int64_t nowNs,
        std::int64_t frameUserId = 0);
    void unsubscribe(std::uint64_t connId, unsigned channels);
    void ping(std::uint64_t connId);
    void gone(std::uint64_t connId, std::int64_t nowNs); // the I/O side says the socket is gone

    // ---- one duty cycle: timers (head probe, heartbeat), the query schedule, closed sockets, linger, the outbound pump
    int doWork(std::int64_t nowNs);

    // ---- views (duty thread)
    bool gapOpen() const noexcept { return m_watermark < 0 || m_gap.open; }
    std::int64_t lastHeadProbeNs() const noexcept { return m_lastProbeAnswerNs; }
    std::int64_t watermark() const noexcept { return m_watermark; }
    std::int64_t windowFirstSeq() const noexcept;
    std::size_t windowSize() const noexcept { return m_window.size(); }
    std::size_t connectionCount() const noexcept { return m_conns.size(); }
    std::size_t queuedWork() const noexcept { return m_work.size(); }
    std::size_t outstandingQueries() const noexcept { return m_outstanding.size(); }
    const StreamStats &stats() const noexcept { return m_stats; }
    bool userCached(std::int64_t userId) const;

private:
    struct OrderRow
    {
        std::variant<wire::OrderSnapshotEntry, wire::OrderEvent> v;
    };

    struct Pending
    {
        Text text;
        std::int64_t key = 0; // != 0: coalesces with a queued frame of the same key (an order frame: its orderId)
        bool dead = false;
    };

    struct Conn
    {
        std::uint64_t id = 0;
        std::int64_t userId = 0;
        std::uint64_t openOrder = 0; // arrival order (oldest first for re-snapshots)
        std::shared_ptr<Outlet> out;
        unsigned subscribed = 0;
        bool ordersActive = false;
        bool fillsActive = false;
        bool balancesActive = false;
        bool ordersPending = false;   // waits for an order snapshot
        bool fillsWithOrders = false; // fills start with the order snapshot (the folded fills first)
        bool balancesPending = false;
        bool resumePending = false;
        std::int64_t resumeFrom = -1;
        unsigned resumeChannels = 0;  // ORDERS and FILLS asked with the resume
        bool closing = false;
        bool dirty = false;           // in m_dirty
        std::deque<Pending> queue;
        std::uint64_t queueBase = 0;                          // absolute index of queue.front()
        std::unordered_map<std::int64_t, std::uint64_t> orderFrameAt; // orderId -> absolute index of its queued frame
        std::int64_t queuedMessages = 0;
        std::int64_t queuedBytes = 0;
        std::int64_t largestFrameBytes = 0; // the largest frame since the connection was last empty (the byte test)
    };

    enum class Cache : std::uint8_t
    {
        NONE,
        STITCHING, // order pages in flight or waiting for the gap to close
        READY,
    };
    enum class Bal : std::uint8_t
    {
        NONE,
        LOADING,
        READY,
    };

    struct User
    {
        std::int64_t id = 0;
        std::vector<std::uint64_t> conns;
        std::int64_t lingerUntilNs = -1; // no connection: dropped after this
        Cache orders = Cache::NONE;
        std::map<std::int64_t, OrderRow> open; // READY: the open orders as of `asOfSeq`
        std::uint64_t stitchGen = 0;
        std::map<std::int64_t, OrderRow> pages; // STITCHING: the merged pages
        std::int64_t firstAsOf = -1;
        std::int64_t lastAsOf = -1;
        std::int64_t nextOrderId = 0;
        bool pagesDone = false;
        Bal bal = Bal::NONE;
        std::uint64_t balGen = 0;
        std::map<std::int32_t, std::pair<std::int64_t, std::int64_t>> balances; // assetId -> available, locked
        std::optional<session::BalancesReply> heldReply; // GW_NC_BALANCE_REPLY_LAST only
    };

    enum class WorkKind : std::uint8_t
    {
        SNAPSHOT_PAGE,
        BALANCES,
        REPLAY,
        BACKFILL,
        PROBE,
    };
    struct Work
    {
        WorkKind kind = WorkKind::SNAPSHOT_PAGE;
        std::int64_t userId = 0;
        std::uint64_t gen = 0;
    };

    // ---- events
    void acceptInOrder(const wire::OrderEvent &event);
    void append(const wire::OrderEvent &event);
    void route(const wire::OrderEvent &event);
    void openGap(ResyncReason reason);
    void drainBuffer();
    void closeGapIfDone();
    void overrun(std::int64_t newWatermark, ResyncReason reason);
    void onEventsReply(const Work &work, const session::EventsReply &reply, std::int64_t nowNs);
    void onProbeReply(const session::EventsReply &reply, std::int64_t nowNs);
    void onBackfillReply(const session::EventsReply &reply);
    void afterGapClosed();

    // ---- users and caches
    User &userOf(std::int64_t userId);
    void dropUser(std::int64_t userId);
    void startStitch(User &u);
    void onSnapshotPage(User &u, const session::SnapshotReply &reply);
    void finishStitch(User &u);
    void serveOrdersFromCache(User &u, Conn &c);
    void startBalances(User &u);
    void onBalancesReply(User &u, const session::BalancesReply &reply);
    void sendBalanceSnapshot(User &u, Conn &c);
    static void applyEvent(std::map<std::int64_t, OrderRow> &rows, const wire::OrderEvent &e);

    // ---- connections
    Conn *conn(std::uint64_t connId);
    void ensureSnapshots(Conn &c, User &u);
    void processResume(Conn &c, std::int64_t nowNs);
    void sendResync(Conn &c, ResyncReason reason);
    void replayFromWindow(Conn &c, std::int64_t fromSeq, bool orders, bool fills);
    void enqueue(Conn &c, Text text, std::int64_t key = 0);
    void closeConn(Conn &c, std::uint16_t code, const char *reason);
    void removeConn(std::uint64_t connId, std::int64_t nowNs);
    int pump();

    // ---- queries
    void queueWork(Work w, bool front = false);
    void submitDirect(Work w, session::Command command);
    int schedule(std::int64_t nowNs);
    void retryLater(const Work &w);

    // ---- frames
    Text orderFrame(const wire::OrderEvent &e) const;
    Text fillFrame(const wire::OrderEvent &e) const;
    std::string assetCode(std::int32_t assetId) const;

    template<typename... Args>
    void logLine(cluster_tools::runtime::LogLevel level, std::int64_t nowNs, const char *format, Args... args)
    {
        if (nullptr != m_log)
        {
            m_log->push(level, nowNs, format, args...);
        }
    }

    StreamCoreSettings m_settings;
    QueryPort &m_queries;
    ConfigSource m_config;
    cluster_tools::runtime::LogRing *m_log;
    StreamStats m_ownStats;
    StreamStats &m_stats;

    Text m_engineStatus; // the newest engine frame (sent on open)

    // the window
    std::deque<wire::OrderEvent> m_window;
    std::int64_t m_watermark = -1;        // W; -1 = not known yet (no head probe answered)
    std::int64_t m_evictedMaxPosition = -1; // the highest logPosition of an event the window no longer holds
    std::int64_t m_coreLastSeq = -1;      // the core's lastSeq in its latest OrderEventsEnd
    std::int64_t m_coreFirstAvailable = -1;

    struct Gap
    {
        bool open = false;
        ResyncReason reason = ResyncReason::EVENT_GAP;
        bool inFlight = false;
        std::map<std::int64_t, wire::OrderEvent> buffer; // live events above W while the gap is open
        std::int64_t targetSeq = -1;                     // the core's lastSeq at the latest reply
    } m_gap;
    std::vector<wire::OrderEvent> m_initBuffer; // live events before the first head probe answered

    struct Backfill
    {
        bool running = false;
        std::int64_t from = 0;      // exclusive: the oldest resume being served
        std::int64_t to = 0;        // inclusive: the window's first seq - 1 at the start
        std::int64_t frontSeq = 0;  // the window's first seq at the start (must not move)
        std::int64_t cursor = 0;
        bool inFlight = false;
        std::vector<wire::OrderEvent> events;
    } m_backfill;

    // sessions
    bool m_registeredOnce = false;
    std::uint64_t m_epoch = 0;
    std::int64_t m_term = -1;

    // timers
    std::int64_t m_lastProbeSentNs = -1;
    bool m_probeInFlight = false;
    std::int64_t m_lastProbeAnswerNs = -1;
    std::int64_t m_lastHeartbeatNs = -1;
    std::int64_t m_lastSweepNs = -1;

    // users and connections
    std::unordered_map<std::int64_t, User> m_users;
    std::unordered_map<std::uint64_t, Conn> m_conns;
    std::uint64_t m_nextOpenOrder = 0;
    std::vector<std::uint64_t> m_dirty; // connections with queued frames to hand over
    std::vector<std::uint64_t> m_toRemove;

    // queries
    std::deque<Work> m_work;
    std::unordered_map<std::uint64_t, Work> m_outstanding; // ticket -> what it asked
    std::uint64_t m_generation = 0; // stitch and balance generations: one counter, so a re-created user never reuses one
    std::int64_t m_outstandingUserWork = 0;                 // snapshot pages and balance reads outstanding
};

}}}

#endif
