#include "Candles.h"

#include <algorithm>

namespace openexchange { namespace gateway { namespace marketdata {

std::int64_t bucketStart(std::int64_t ms, std::int32_t intervalS) noexcept
{
    const std::int64_t width = static_cast<std::int64_t>(intervalS) * 1000;
    std::int64_t q = ms / width;
    if (ms % width != 0 && ms < 0)
    {
        --q; // floor, not toward zero
    }
    return q * width;
}

Int128 quoteOf(std::int64_t price, std::int64_t quantity) noexcept
{
#ifdef MD_NC_QUOTE_64
    // test build only: the 64-bit product (wraps past 2^63)
    return static_cast<Int128>(static_cast<std::int64_t>(static_cast<std::uint64_t>(price) *
        static_cast<std::uint64_t>(quantity)));
#else
    return static_cast<Int128>(price) * static_cast<Int128>(quantity);
#endif
}

std::string decimal(Int128 v)
{
    if (0 == v)
    {
        return "0";
    }
    const bool negative = v < 0;
    UInt128 u = negative ? static_cast<UInt128>(0) - static_cast<UInt128>(v)
                                   : static_cast<UInt128>(v);
    std::string out;
    while (u > 0)
    {
        out.push_back(static_cast<char>('0' + static_cast<int>(u % 10)));
        u /= 10;
    }
    if (negative)
    {
        out.push_back('-');
    }
    std::reverse(out.begin(), out.end());
    return out;
}

bool parseDecimal(const std::string &text, Int128 &out) noexcept
{
    std::size_t i = 0;
    bool negative = false;
    if (i < text.size() && ('-' == text[i] || '+' == text[i]))
    {
        negative = '-' == text[i];
        ++i;
    }
    if (i == text.size())
    {
        return false;
    }
    UInt128 u = 0;
    const UInt128 limit = (static_cast<UInt128>(1) << 127) - 1;
    for (; i < text.size(); ++i)
    {
        const char c = text[i];
        if (c < '0' || c > '9')
        {
            return false;
        }
        u = u * 10 + static_cast<unsigned>(c - '0');
        if (u > limit)
        {
            return false;
        }
    }
    out = negative ? -static_cast<Int128>(u) : static_cast<Int128>(u);
    return true;
}

void mergeCandle(CandleRow &into, const CandleRow &later) noexcept
{
    into.high = std::max(into.high, later.high);
    into.low = std::min(into.low, later.low);
    into.close = later.close;
    into.volumeBase += later.volumeBase;
    into.volumeQuote += later.volumeQuote;
    into.tradeCount += later.tradeCount;
    into.lastTradeId = later.lastTradeId;
}

std::map<CandleKey, CandleRow> aggregate(const std::vector<TradeRow> &trades, std::int64_t arrivalMs)
{
    (void)arrivalMs;
    std::map<CandleKey, CandleRow> out;
    for (const TradeRow &t : trades)
    {
#ifdef MD_NC_WALLCLOCK_BUCKET
        const std::int64_t at = arrivalMs; // test build only: the bucket of the time the trade arrived
#else
        const std::int64_t at = t.executedAtMs;
#endif
        for (const std::int32_t interval : CANDLE_INTERVALS_S)
        {
            CandleRow one;
            one.marketId = t.marketId;
            one.intervalS = interval;
            one.bucketStartMs = bucketStart(at, interval);
            one.open = one.high = one.low = one.close = t.price;
            one.volumeBase = t.quantity;
            one.volumeQuote = quoteOf(t.price, t.quantity);
            one.tradeCount = 1;
            one.lastTradeId = t.tradeId;
            const CandleKey key{ one.marketId, one.intervalS, one.bucketStartMs };
            const auto it = out.find(key);
            if (out.end() == it)
            {
                out.emplace(key, one);
            }
            else
            {
                mergeCandle(it->second, one);
            }
        }
    }
    return out;
}

std::optional<Ticker> tickerOf(std::int32_t marketId, const std::vector<CandleRow> &candles1m, std::int64_t asOfMs)
{
    const CandleRow *oldest = nullptr;
    const CandleRow *newest = nullptr;
    Ticker t;
    t.marketId = marketId;
    for (const CandleRow &c : candles1m)
    {
        if (c.marketId != marketId || 60 != c.intervalS || c.bucketStartMs > asOfMs ||
            c.bucketStartMs < asOfMs - TICKER_WINDOW_MS)
        {
            continue;
        }
        if (nullptr == oldest)
        {
            t.high24h = c.high;
            t.low24h = c.low;
        }
        else
        {
            t.high24h = std::max(t.high24h, c.high);
            t.low24h = std::min(t.low24h, c.low);
        }
        if (nullptr == oldest || c.bucketStartMs < oldest->bucketStartMs)
        {
            oldest = &c;
        }
        if (nullptr == newest || c.bucketStartMs > newest->bucketStartMs)
        {
            newest = &c;
        }
        t.volume24hQuote += c.volumeQuote;
        t.volume24hBase += c.volumeBase;
    }
    if (nullptr == oldest)
    {
        return std::nullopt;
    }
    t.lastPrice = newest->close;
    t.openPrice = oldest->open;
    t.priceChange = t.lastPrice - t.openPrice;
    t.priceChangePctE8 = 0 == t.openPrice ? 0
        : static_cast<std::int64_t>(static_cast<Int128>(t.priceChange) * 100 * 100'000'000 / t.openPrice);
    return t;
}

}}}
