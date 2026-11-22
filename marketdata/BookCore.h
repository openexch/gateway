#ifndef GATEWAY_MARKETDATA_BOOK_CORE_H
#define GATEWAY_MARKETDATA_BOOK_CORE_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cluster_tools/domain/forwarder/BookLadder.h"
#include "cluster_tools/domain/forwarder/MarketData.h"

#include "gateway/common/Readiness.h"
#include "gateway/common/Status.h"

#include "BookFrames.h"

// BookCore - the book side of oe-marketdata, owned by the duty thread: the ME egress of one GATEWAY-class session goes
// in, the public book frames and the subscriptions come out. Pure: no Aeron, no socket, no wall clock of its own (the
// duty cycle hands in the monotonic time, the PONG time comes from an injected clock), so the tests drive it with the
// engine's golden bytes.
//
// Egress. Only 21 ClusterHeartbeat, 22 BookSnapshot and 25 BookDelta of the order schema (id 1, version exactly 12) are
// decoded. Every other frame (24 OrderStatusBatch and 26 TradeExecutionBatch carry user ids) is counted by its
// template and dropped after the 8-byte message header, before any field is read. A book frame of an ended ME term is
// dropped (a stopped leader's late egress). A frame of another order-schema version is refused, never decoded by
// guess. The book ladder is the cluster-tools BookStitcher (markets 1..5, 20 levels a side; a market outside 1..5 is
// counted foreign by the library). A book frame of a market inside 1..5 and outside MD_EXPECTED_MARKETS is dropped
// here, before the ladder, and counted foreign the same way: no frame of it reaches a client.
//
// Book frames. A snapshot the ladder applied goes to the market's subscribers as BOOK_SNAPSHOT; a delta it applied as
// BOOK_DELTA (the ME's own changes and versions). A delta the ladder ignores (no chain) is never sent. When the book
// stops being fresh (a chain break, an unversioned snapshot, the ME session's end, no book frame within
// MD_BOOK_STALE_MS while it holds levels) the subscribers get one BOOK_SNAPSHOT with "stale":true: with the ladder's
// levels while it is OK, with none once the ladder cannot prove its state. The next snapshot clears it. Freshness is
// the one rule readiness uses (Readiness.h mdBookState).
//
// Subscriptions (the rule match-gateway had): one market per connection; `subscribe` replaces it; there is no
// unsubscribe. A new connection gets the all-market TICKERS at once, before any subscribe. On subscribe:
// SUBSCRIPTION_CONFIRMED, then the initial state: the market's TICKER_STATS, the all-market TICKERS, the book
// snapshot, the recent tape (TRADES_BATCH) and the 1m CANDLE_HISTORY. `refresh` (and a resync after a queue bound)
// sends the initial state again. Every frame of a connection leaves in the order this thread queued it, so live
// frames wait behind the initial state.
// Before the trade path started (TradeCore: the market history was down at the start) the tape, the candles and the
// ticker do not exist: the initial state is the book snapshot and one ERROR HISTORY_UNAVAILABLE naming the parts it
// stands for (tape, candles, ticker), and a new connection gets no TICKERS. An empty series is never sent as if it were
// whole. When the trade path starts, every connection gets a fresh initial state (TICKERS alone while it has no
// market). Live trade frames (TradeCore's flush) go to the market's subscribers, TICKERS to every connection.

namespace openexchange { namespace gateway { namespace marketdata {

class TradeCore;

static_assert(MD_MARKET_SLOTS == static_cast<std::size_t>(cluster_tools::domain::forwarder::TICK_MARKETS),
    "the readiness slots are the library's ladders");

enum class FrameType : std::uint8_t
{
    BOOK_SNAPSHOT,
    BOOK_DELTA,
    SUBSCRIPTION_CONFIRMED,
    PONG,
    ERROR,
    TRADES_BATCH,
    TICKER_STATS,
    TICKERS,
    CANDLE_HISTORY,
    CANDLE_UPDATE,
    COUNT
};
const char *nameOf(FrameType type) noexcept;

struct Frame
{
    std::shared_ptr<const std::string> text;
    FrameType type = FrameType::ERROR;
};

// One client connection as the duty thread sees it. offer() never blocks.
class Outlet
{
public:
    virtual ~Outlet() = default;
    // Queues one frame; false = not queued (the connection hit its queue bound and waits for a resync, or it is gone).
    virtual bool offer(const Frame &frame) = 0;
    // The fresh initial state of a resync follows: queue it although a resync was pending.
    virtual void resyncBegins() = 0;
};

// The counters of the book side, written by the duty thread (relaxed) and read by the admin thread.
struct MdStats
{
    std::atomic<std::uint64_t> snapshots{ 0 };          // md_book_frames_total{template="22"}
    std::atomic<std::uint64_t> deltas{ 0 };             // md_book_frames_total{template="25"}
    std::atomic<std::uint64_t> heartbeats{ 0 };
    std::atomic<std::uint64_t> breaks{ 0 };             // md_book_breaks_total
    std::atomic<std::uint64_t> foreignMarket{ 0 };      // md_foreign_market_frames_total
    std::atomic<std::uint64_t> malformedNoMarket{ 0 };
    std::atomic<std::uint64_t> staleTerm{ 0 };          // book frames of an ended ME term
    std::atomic<std::uint64_t> schemaRefused{ 0 };      // order-schema frames of another version
    // md_egress_dropped_templates_total{template}: 23, 24, 26, 27, then any other
    static constexpr std::array<std::uint16_t, 4> DROPPED_TEMPLATES = { 23, 24, 26, 27 };
    std::array<std::atomic<std::uint64_t>, DROPPED_TEMPLATES.size() + 1> dropped{};
    std::atomic<std::int64_t> lastEgressNs{ -1 };       // md_egress_age_ms
    std::atomic<std::int64_t> booksStale{ 0 };          // md_books_stale (expected markets not fresh)
    std::atomic<std::uint64_t> sessionsEnded{ 0 };
    std::atomic<std::uint64_t> resyncs{ 0 };            // md_ws_resyncs_total
    std::array<std::atomic<std::uint64_t>, static_cast<std::size_t>(FrameType::COUNT)> queued{};
};

struct BookCoreSettings
{
    std::vector<int> expectedMarkets;
    MarketDataReadyBounds bounds;
};

class BookCore
{
public:
    BookCore(BookCoreSettings settings, MdStats &stats, std::function<std::int64_t()> wallClockMs);
    // The trade side (the duty thread's). Null: no trade path (the initial state is the book and HISTORY_UNAVAILABLE).
    void attachTrades(TradeCore *trades) noexcept { m_trades = trades; }

    // ---- the ME session (the duty thread)
    void onSessionUp(std::int64_t nowNs);
    // The session closed, or a new leader term began: every ladder empties and heals on the next snapshot.
    void onSessionEnd(std::int64_t nowNs);
    // One egress message: `frameTerm` is the term stamped on it, `sessionTerm` the client's current one.
    void onEgress(const std::uint8_t *payload, std::size_t length, std::int64_t frameTerm, std::int64_t sessionTerm,
        std::int64_t nowNs);

    // ---- the clients (commands from the I/O side, in arrival order)
    void open(std::uint64_t connId, std::shared_ptr<Outlet> outlet);
    void subscribe(std::uint64_t connId, std::int64_t marketId, std::int64_t nowNs);
    void refresh(std::uint64_t connId, std::int64_t marketId, std::int64_t nowNs);
    void ping(std::uint64_t connId);
    void badRequest(std::uint64_t connId, const std::string &message);
    void resync(std::uint64_t connId, std::int64_t nowNs); // the connection drained after its queue bound
    void gone(std::uint64_t connId);

    // Stale transitions by age, the trade path's start, its flushes and TICKERS; returns the work done.
    int doWork(std::int64_t nowNs);

    void fillStatus(Status &status) const;
    bool expected(std::int64_t marketId) const noexcept;
    MdBookState bookState(int marketId, std::int64_t nowNs) const noexcept;
    const cluster_tools::domain::forwarder::BookStitcher &stitcher() const noexcept { return m_stitcher; }
    std::size_t connections() const noexcept { return m_conns.size(); }

private:
    enum class Sent : std::uint8_t
    {
        NONE,
        FRESH,
        STALE_LEVELS,
        STALE_EMPTY
    };
    struct Market
    {
        MdMarketStatus status;
        BookStamps stamps;
        Sent sent = Sent::NONE;
        std::vector<std::uint64_t> subscribers;
    };
    struct Conn
    {
        std::shared_ptr<Outlet> outlet;
        int market = 0; // 0 = none
    };

    void onBookFrame(const cluster_tools::domain::forwarder::BookFrame &frame, const std::uint8_t *payload,
        std::size_t length, std::int64_t nowNs);
    void emit(int marketId, const Frame &frame);
    void emitStale(int marketId, std::int64_t nowNs);
    Frame snapshotFrame(int marketId, std::int64_t nowNs, Sent *kind) const;
    void initialState(std::uint64_t connId, Conn &conn, std::int64_t nowNs);
    bool tradesLive() const noexcept { return nullptr != m_trades && m_tradesLive; }
    int tradeWork(std::int64_t nowNs);
    void send(Conn &conn, const Frame &frame);
    Market &market(int marketId) { return m_markets[static_cast<std::size_t>(marketId - 1)]; }
    const Market &market(int marketId) const { return m_markets[static_cast<std::size_t>(marketId - 1)]; }
    void refreshLadderFacts(int marketId);

    BookCoreSettings m_settings;
    MdStats &m_stats;
    std::function<std::int64_t()> m_wallClockMs;
    cluster_tools::domain::forwarder::BookStitcher m_stitcher;
    cluster_tools::domain::forwarder::BookFrame m_frame; // decode scratch (large: kept out of the stack)
    std::array<Market, MD_MARKET_SLOTS> m_markets{};
    std::unordered_map<std::uint64_t, Conn> m_conns;
    TradeCore *m_trades = nullptr;
    bool m_tradesLive = false; // the trade path started and every connection had its fresh initial state
    bool m_sessionUp = false;
    std::int64_t m_lastHeartbeatNs = -1;
    std::int64_t m_meEpoch = 0;
};

// The 22 / 25 root fields the library's BookFrame does not carry, read at their schema offsets from a frame the codec
// accepted (order schema v12: marketId 0, timestamp 4, bidVersion 12, askVersion 20, bookVersion 28).
BookStamps readBookStamps(const std::uint8_t *payload, std::size_t length) noexcept;

}}}

#endif
