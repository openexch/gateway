#include "TradeCore.h"

#include <algorithm>
#include <memory>

namespace openexchange { namespace gateway { namespace marketdata {

namespace {

constexpr std::int64_t MINUTE_MS = 60'000;

Frame make(std::string text, FrameType type)
{
    return Frame{ std::make_shared<const std::string>(std::move(text)), type };
}

}

TradeCore::TradeCore(TradeCoreSettings settings, std::function<std::int64_t()> wallClockMs,
    std::function<std::int64_t()> monoNs) :
    m_settings(std::move(settings)), m_wallClockMs(std::move(wallClockMs)), m_monoNs(std::move(monoNs))
{
}

TradeCore::Market *TradeCore::market(int marketId)
{
    const auto it = m_markets.find(marketId);
    return m_markets.end() == it ? nullptr : &it->second;
}

const TradeCore::Market *TradeCore::market(int marketId) const
{
    const auto it = m_markets.find(marketId);
    return m_markets.end() == it ? nullptr : &it->second;
}

void TradeCore::onTradePathStart(MarketSeed seed)
{
    m_markets.clear();
    m_buffer.clear();
    m_firstBufferedNs = -1;
    m_maxTradeMs = 0;
    for (const int id : m_settings.expectedMarkets)
    {
        m_markets[id] = Market{};
    }
    for (MarketSeed::Market &s : seed.markets)
    {
        Market *m = market(s.marketId);
        if (nullptr == m)
        {
            continue;
        }
        for (const TradeRow &t : s.tape)
        {
            m->tape.push_back(t);
            m->lastTradeMs = std::max(m->lastTradeMs, t.executedAtMs);
        }
        while (m->tape.size() > m_settings.recentTrades)
        {
            m->tape.pop_front();
        }
        std::sort(s.candles1m.begin(), s.candles1m.end(),
            [](const CandleRow &a, const CandleRow &b) { return a.bucketStartMs < b.bucketStartMs; });
        for (const CandleRow &c : s.candles1m)
        {
            m->candles.push_back(c);
            // a bucket starts at or before its trades: the newest bucket bounds the newest trade from below
            m->lastTradeMs = std::max(m->lastTradeMs, c.bucketStartMs);
        }
        m_maxTradeMs = std::max(m_maxTradeMs, m->lastTradeMs);
    }
    const std::int64_t at = asOf();
    for (auto &kv : m_markets)
    {
        prune(kv.second, at);
    }
    m_lastTickers = tickerEntries(at);
    m_nextTickersNs = -1;
    m_started = true;
    m_startTaken = false;
}

void TradeCore::onLiveTrade(const TradeRow &trade)
{
    if (!m_started || nullptr == market(trade.marketId))
    {
        return; // never before the start; a market outside the set never reaches here (UNKNOWN_MARKET halts first)
    }
    if (m_buffer.empty())
    {
        m_firstBufferedNs = m_monoNs();
    }
    m_buffer.push_back(trade);
}

bool TradeCore::takeStart() noexcept
{
    if (!m_started || m_startTaken)
    {
        return false;
    }
    m_startTaken = true;
    return true;
}

std::int64_t TradeCore::asOf() const
{
    return std::max(m_wallClockMs(), m_maxTradeMs);
}

std::optional<Ticker> TradeCore::tickerOf(int marketId, std::int64_t asOfMs) const
{
    const Market *m = market(marketId);
    if (nullptr == m)
    {
        return std::nullopt;
    }
    if (!m->candles.empty())
    {
        const std::vector<CandleRow> candles(m->candles.begin(), m->candles.end());
        if (std::optional<Ticker> t = marketdata::tickerOf(marketId, candles, asOfMs))
        {
            return t;
        }
    }
    // a quiet market (trades, none in the 24 h window): the newest trade's price, every 24 h value 0, and it stays in
    // TICKERS (the old service's rule); a market that never traded has no price and no entry
    Ticker quiet;
    quiet.marketId = marketId;
    if (!m->tape.empty())
    {
        quiet.lastPrice = m->tape.back().price; // the tape is in journal order: the newest trade last
    }
    else if (!m->candles.empty())
    {
        quiet.lastPrice = m->candles.back().close;
    }
    else
    {
        return std::nullopt;
    }
    return quiet;
}

std::string TradeCore::tickerEntries(std::int64_t asOfMs) const
{
    std::vector<std::pair<Ticker, std::int64_t>> list;
    for (const auto &kv : m_markets)
    {
        if (const std::optional<Ticker> t = tickerOf(kv.first, asOfMs))
        {
            list.emplace_back(*t, kv.second.lastTradeMs);
        }
    }
    return renderTickerEntries(list);
}

void TradeCore::prune(Market &m, std::int64_t asOfMs)
{
    const std::int64_t reach = asOfMs - TICKER_WINDOW_MS - MINUTE_MS;
    while (m.candles.size() > m_settings.candleHistory && m.candles.front().bucketStartMs < reach)
    {
        m.candles.pop_front();
    }
}

std::vector<std::pair<int, Frame>> TradeCore::flushDue(std::int64_t nowNs)
{
    std::vector<std::pair<int, Frame>> out;
    if (m_buffer.empty() || nowNs - m_firstBufferedNs < m_settings.flushMs * 1'000'000LL)
    {
        return out;
    }
    const std::int64_t wall = m_wallClockMs();
    std::map<int, std::vector<TradeRow>> byMarket;
    for (const TradeRow &t : m_buffer)
    {
        byMarket[t.marketId].push_back(t);
        m_maxTradeMs = std::max(m_maxTradeMs, t.executedAtMs);
    }
    m_buffer.clear();
    m_firstBufferedNs = -1;
    const std::int64_t at = std::max(wall, m_maxTradeMs);
    for (auto &kv : byMarket)
    {
        const int id = kv.first;
        Market &m = *market(id);
        const std::vector<TradeRow> &trades = kv.second;
        for (const TradeRow &t : trades)
        {
            m.tape.push_back(t);
            m.lastTradeMs = std::max(m.lastTradeMs, t.executedAtMs);
        }
        while (m.tape.size() > m_settings.recentTrades)
        {
            m.tape.pop_front();
        }
        // the batch's 1m aggregates merged into the memory buckets by the rule the store uses
        std::vector<std::int64_t> touched;
        for (const auto &ckv : aggregate(trades, wall))
        {
            const CandleRow &c = ckv.second;
            if (60 != c.intervalS)
            {
                continue;
            }
            const auto pos = std::lower_bound(m.candles.begin(), m.candles.end(), c.bucketStartMs,
                [](const CandleRow &a, std::int64_t b) { return a.bucketStartMs < b; });
            if (m.candles.end() != pos && pos->bucketStartMs == c.bucketStartMs)
            {
                mergeCandle(*pos, c);
            }
            else
            {
                m.candles.insert(pos, c);
            }
            touched.push_back(c.bucketStartMs);
        }
        out.emplace_back(id, make(renderTradesBatch(id, trades, wall), FrameType::TRADES_BATCH));
        out.emplace_back(id, make(renderTickerStats(id, tickerOf(id, at), m.lastTradeMs), FrameType::TICKER_STATS));
        std::sort(touched.begin(), touched.end());
        for (const std::int64_t bucket : touched)
        {
            const auto pos = std::lower_bound(m.candles.begin(), m.candles.end(), bucket,
                [](const CandleRow &a, std::int64_t b) { return a.bucketStartMs < b; });
            out.emplace_back(id, make(renderCandleUpdate(id, *pos), FrameType::CANDLE_UPDATE));
        }
        prune(m, at);
    }
    return out;
}

std::optional<Frame> TradeCore::tickersDue(std::int64_t nowNs)
{
    if (!m_started)
    {
        return std::nullopt;
    }
    if (m_nextTickersNs >= 0 && nowNs - m_nextTickersNs < 0)
    {
        return std::nullopt;
    }
    m_nextTickersNs = nowNs + m_settings.tickersIntervalMs * 1'000'000LL;
    const std::int64_t wall = m_wallClockMs();
    const std::int64_t at = std::max(wall, m_maxTradeMs);
    for (auto &kv : m_markets)
    {
        prune(kv.second, at);
    }
    std::string entries = tickerEntries(at);
    if (entries == m_lastTickers)
    {
        return std::nullopt;
    }
    m_lastTickers = std::move(entries);
    return make(renderTickers(m_lastTickers, wall), FrameType::TICKERS);
}

Frame TradeCore::tickerStats(int marketId) const
{
    const Market *m = market(marketId);
    return make(renderTickerStats(marketId, tickerOf(marketId, asOf()), nullptr == m ? 0 : m->lastTradeMs),
        FrameType::TICKER_STATS);
}

Frame TradeCore::tickers() const
{
    return make(renderTickers(tickerEntries(asOf()), m_wallClockMs()), FrameType::TICKERS);
}

Frame TradeCore::tape(int marketId) const
{
    const Market *m = market(marketId);
    std::vector<TradeRow> trades;
    if (nullptr != m)
    {
        trades.assign(m->tape.begin(), m->tape.end());
    }
    return make(renderTradesBatch(marketId, trades, m_wallClockMs()), FrameType::TRADES_BATCH);
}

Frame TradeCore::candleHistory(int marketId) const
{
    const Market *m = market(marketId);
    std::vector<CandleRow> candles;
    if (nullptr != m)
    {
        const std::size_t n = std::min(m->candles.size(), m_settings.candleHistory);
        candles.assign(m->candles.end() - static_cast<std::ptrdiff_t>(n), m->candles.end());
    }
    return make(renderCandleHistory(marketId, "1m", candles), FrameType::CANDLE_HISTORY);
}

}}}
