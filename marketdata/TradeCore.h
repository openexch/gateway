#ifndef GATEWAY_MARKETDATA_TRADE_CORE_H
#define GATEWAY_MARKETDATA_TRADE_CORE_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "BookCore.h"
#include "Candles.h"
#include "TradePath.h"

// TradeCore - the live trade side of oe-marketdata, owned by the duty thread: the recent tape, the 1m candles and the
// tickers in memory, and the frames made of them. Pure: no socket, no PostgreSQL, the clocks are injected.
//
// It starts once, from the relations (onTradePathStart: the writer read them at the commit cursor the trade path
// starts from), and then moves only by the live trades the journal follower hands on (each once, in journal order, never
// under a halt; TradePath.h). Before the start it holds nothing: the tape, the candles and the ticker do not exist, and
// BookCore sends HISTORY_UNAVAILABLE instead of an empty series.
//
// Live trades are buffered and applied at the flush, MD_TRADE_FLUSH_MS after the first buffered one. So the memory
// state is always "as of the last flush": an initial state built from it never holds a trade a later flush sends
// again, and a subscriber sees each trade once (in its initial tape or in a flush after it). Per market with trades,
// in market id order, a flush makes: TRADES_BATCH (the market's trades in journal order), its TICKER_STATS, then one
// CANDLE_UPDATE per 1m bucket the batch touched, oldest first, with the bucket's whole value.
//
// The ticker is tickerOf (Candles.h) over the memory 1m candles, as of max(the wall clock, the newest trade's journal
// timestamp): a trade stamped ahead of this host's clock still counts. TICKERS is sent every MD_TICKERS_INTERVAL_MS
// when its entries changed (the entries are compared, not the frame's timestamp). The memory keeps the newest
// MD_RECENT_TRADES trades per market, and the newest MD_CANDLE_HISTORY 1m buckets plus every bucket the 24 h window
// can still reach.

namespace openexchange { namespace gateway { namespace marketdata {

struct TradeCoreSettings
{
    std::vector<int> expectedMarkets;
    std::size_t recentTrades = 50;
    std::size_t candleHistory = 200;
    std::int64_t flushMs = 20;
    std::int64_t tickersIntervalMs = 1000;
};

class TradeCore final : public LiveTrades
{
public:
    TradeCore(TradeCoreSettings settings, std::function<std::int64_t()> wallClockMs,
        std::function<std::int64_t()> monoNs);

    // ---- the journal follower (LiveTrades)
    void onTradePathStart(MarketSeed seed) override;
    void onLiveTrade(const TradeRow &trade) override;

    // ---- BookCore (the duty thread)
    bool started() const noexcept { return m_started; }
    // True once, the first time it is asked after the start: every connection gets a fresh initial state.
    bool takeStart() noexcept;
    // The frames of a flush that is due, per market (in market id order, each market's frames in sending order).
    std::vector<std::pair<int, Frame>> flushDue(std::int64_t nowNs);
    // The TICKERS frame when the interval passed and its entries changed.
    std::optional<Frame> tickersDue(std::int64_t nowNs);

    // the initial state's parts (after the start)
    Frame tickerStats(int marketId) const;
    Frame tickers() const;
    Frame tape(int marketId) const;
    Frame candleHistory(int marketId) const;

    std::size_t buffered() const noexcept { return m_buffer.size(); }

private:
    struct Market
    {
        std::deque<TradeRow> tape;
        std::deque<CandleRow> candles; // 1m, ascending by bucket
        std::int64_t lastTradeMs = 0;
    };

    std::int64_t asOf() const;
    std::optional<Ticker> tickerOf(int marketId, std::int64_t asOfMs) const;
    std::string tickerEntries(std::int64_t asOfMs) const;
    void prune(Market &m, std::int64_t asOfMs);
    Market *market(int marketId);
    const Market *market(int marketId) const;

    TradeCoreSettings m_settings;
    std::function<std::int64_t()> m_wallClockMs;
    std::function<std::int64_t()> m_monoNs;
    std::map<int, Market> m_markets;
    bool m_started = false;
    bool m_startTaken = false;
    std::vector<TradeRow> m_buffer;
    std::int64_t m_firstBufferedNs = -1;
    std::int64_t m_maxTradeMs = 0;
    std::int64_t m_nextTickersNs = -1;
    std::string m_lastTickers;
};

}}}

#endif
