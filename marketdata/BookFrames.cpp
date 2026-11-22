#include "BookFrames.h"

#include "gateway/common/Admin.h"
#include "gateway/common/Rest.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace fwd = cluster_tools::domain::forwarder;

namespace {

std::string dec(std::int64_t units)
{
#ifdef MD_NC_NUMBER_PRICE
    // control build only (never published): a price or quantity written as a JSON number
    return rest::formatMoney(units);
#else
    return "\"" + rest::formatMoney(units) + "\"";
#endif
}

std::string marketField(int marketId)
{
#ifdef MD_NC_MARKETID_STRING
    // control build only (never published): marketId written as a JSON string
    return "\"marketId\":\"" + std::to_string(marketId) + "\"";
#else
    return "\"marketId\":" + std::to_string(marketId);
#endif
}

std::string bookVersionField(std::int64_t version)
{
#ifdef MD_NC_NO_BOOK_VERSION
    // control build only (never published): bookVersion left out
    (void)version;
    return "";
#else
    return ",\"bookVersion\":" + std::to_string(version);
#endif
}

void level(std::string &out, std::int64_t price, std::int64_t quantity, std::int32_t orderCount)
{
    out += "{\"price\":" + dec(price) + ",\"quantity\":" + dec(quantity) + ",\"orderCount\":" +
        std::to_string(orderCount) + "}";
}

const char *sideName(std::uint8_t raw)
{
    return fwd::BOOK_SIDE_BID == raw ? "BID" : "ASK";
}

const char *updateName(std::uint8_t raw)
{
    switch (raw)
    {
        case fwd::BOOK_UPDATE_NEW: return "NEW_LEVEL";
        case fwd::BOOK_UPDATE_UPDATE: return "UPDATE_LEVEL";
        default: break;
    }
    return "DELETE_LEVEL";
}

}

std::string renderBookSnapshot(int marketId, const fwd::BookLadder *ladder, std::int64_t bookVersion,
    std::int64_t timestampMs, bool stale)
{
    const bool levels = nullptr != ladder && fwd::LadderState::OK == ladder->state();
    std::string out = "{\"type\":\"BOOK_SNAPSHOT\"," + marketField(marketId) + ",\"bids\":[";
    for (int i = 0; levels && i < ladder->bidCount(); ++i)
    {
        out += 0 == i ? "" : ",";
        level(out, ladder->bid(i).price, ladder->bid(i).quantity, ladder->bid(i).orderCount);
    }
    out += "],\"asks\":[";
    for (int i = 0; levels && i < ladder->askCount(); ++i)
    {
        out += 0 == i ? "" : ",";
        level(out, ladder->ask(i).price, ladder->ask(i).quantity, ladder->ask(i).orderCount);
    }
    out += "]" + bookVersionField(bookVersion) + ",\"timestamp\":" + std::to_string(timestampMs);
    if (stale)
    {
        out += ",\"stale\":true";
    }
    return out + "}";
}

std::string renderBookDelta(int marketId, const fwd::BookFrame &f, const BookStamps &stamps)
{
    std::string out = "{\"type\":\"BOOK_DELTA\"," + marketField(marketId) + ",\"changes\":[";
    for (int i = 0; i < f.nChanges; ++i)
    {
        const fwd::BookChange &c = f.changes[i];
        out += (0 == i ? "" : ",") + std::string("{\"price\":") + dec(c.price) + ",\"quantity\":" + dec(c.quantity) +
            ",\"orderCount\":" + std::to_string(c.orderCount) + ",\"side\":\"" + sideName(c.side) +
#ifdef MD_NC_RENAMED_UPDATE_TYPE
            // control build only (never published): the change's updateType under another name
            "\",\"type\":\"" +
#else
            "\",\"updateType\":\"" +
#endif
            updateName(c.updateType) + "\"}";
    }
    out += "],\"bidVersion\":" + std::to_string(stamps.bidVersion) + ",\"askVersion\":" +
        std::to_string(stamps.askVersion) + bookVersionField(f.bookVersion) + ",\"fromVersion\":" +
        std::to_string(f.fromVersion) + ",\"timestamp\":" + std::to_string(stamps.timestampMs) + "}";
    return out;
}

std::string renderSubscribed(int marketId)
{
    return "{\"type\":\"SUBSCRIPTION_CONFIRMED\",\"status\":\"subscribed\"," + marketField(marketId) + "}";
}

std::string renderPong(std::int64_t timestampMs)
{
    return "{\"type\":\"PONG\",\"timestamp\":" + std::to_string(timestampMs) + "}";
}

std::string renderError(const std::string &code, const std::string &message, const std::vector<std::string> &parts)
{
    std::string out = "{\"type\":\"ERROR\",\"code\":\"" + jsonEscape(code) + "\",\"message\":\"" + jsonEscape(message) +
        "\"";
    if (!parts.empty())
    {
        out += ",\"parts\":[";
        for (std::size_t i = 0; i < parts.size(); ++i)
        {
            out += (0 == i ? "\"" : ",\"") + jsonEscape(parts[i]) + "\"";
        }
        out += "]";
    }
    return out + "}";
}

std::string formatScaled(Int128 units, int scaleDigits)
{
    // cut to 8 fraction digits toward zero, then the money format of 10^-8 units
    Int128 v = units;
    for (int i = 8; i < scaleDigits; ++i)
    {
        v /= 10;
    }
    for (int i = scaleDigits; i < 8; ++i)
    {
        v *= 10;
    }
    const bool negative = v < 0;
    const UInt128 magnitude = negative ? static_cast<UInt128>(0) - static_cast<UInt128>(v) : static_cast<UInt128>(v);
    const UInt128 whole = magnitude / static_cast<UInt128>(rest::SCALE);
    const std::uint64_t frac = static_cast<std::uint64_t>(magnitude % static_cast<UInt128>(rest::SCALE));
    std::string out = (negative && 0 != magnitude ? "-" : "") + decimal(static_cast<Int128>(whole));
    if (0 != frac)
    {
        std::string digits = std::to_string(frac);
        digits.insert(0, 8 - digits.size(), '0');
        while (!digits.empty() && '0' == digits.back())
        {
            digits.pop_back();
        }
        out += "." + digits;
    }
    return out;
}

namespace {

std::string dec128(Int128 units, int scaleDigits)
{
#ifdef MD_NC_NUMBER_PRICE
    return formatScaled(units, scaleDigits);
#else
    return "\"" + formatScaled(units, scaleDigits) + "\"";
#endif
}

std::string tickerFields(int marketId, const std::optional<Ticker> &t, std::int64_t lastTradeMs)
{
    const Ticker z{};
    const Ticker &v = t ? *t : z;
    return marketField(marketId) + ",\"lastPrice\":" + dec(v.lastPrice) + ",\"priceChange\":" + dec(v.priceChange) +
        ",\"priceChangePercent\":" + dec(v.priceChangePctE8) + ",\"high24h\":" + dec(v.high24h) + ",\"low24h\":" +
        dec(v.low24h) + ",\"volume24h\":" + dec128(v.volume24hQuote, 16) + ",\"timestamp\":" +
        std::to_string(t ? lastTradeMs : 0);
}

}

std::string renderTradesBatch(int marketId, const std::vector<TradeRow> &trades, std::int64_t timestampMs)
{
    std::string out = "{\"type\":\"TRADES_BATCH\"," + marketField(marketId) + ",\"trades\":[";
    for (std::size_t i = 0; i < trades.size(); ++i)
    {
        const TradeRow &t = trades[i];
        out += (0 == i ? "" : ",") + std::string("{\"tradeId\":\"") + std::to_string(t.tradeId) + "\",\"journalSeq\":" +
            std::to_string(t.journalSeq) + ",\"price\":" +
            dec(t.price) + ",\"quantity\":" + dec(t.quantity) + ",\"tradeCount\":1,\"side\":\"" +
            (t.takerIsBuy ? "BUY" : "SELL") + "\",\"timestamp\":" + std::to_string(t.executedAtMs) +
#ifdef MD_NC_TAKER_IN_TRADE
            ",\"takerUserId\":\"" + std::to_string(t.takerUserId) + "\"" + // control build only (never published)
#endif
            "}";
    }
    return out + "],\"timestamp\":" + std::to_string(timestampMs) + "}";
}

std::string renderTickerStats(int marketId, const std::optional<Ticker> &ticker, std::int64_t lastTradeMs)
{
    return "{\"type\":\"TICKER_STATS\"," + tickerFields(marketId, ticker, lastTradeMs) + "}";
}

std::string renderTickerEntries(const std::vector<std::pair<Ticker, std::int64_t>> &tickers)
{
    std::string out;
    for (std::size_t i = 0; i < tickers.size(); ++i)
    {
        out += (0 == i ? "{" : ",{") + tickerFields(tickers[i].first.marketId, tickers[i].first, tickers[i].second) + "}";
    }
    return out;
}

std::string renderTickers(const std::string &entries, std::int64_t timestampMs)
{
    return "{\"type\":\"TICKERS\",\"tickers\":[" + entries + "],\"timestamp\":" + std::to_string(timestampMs) + "}";
}

std::string renderCandle(const CandleRow &c)
{
    return "{\"time\":" + std::to_string(c.bucketStartMs / 1000) + ",\"open\":" + dec(c.open) + ",\"high\":" +
        dec(c.high) + ",\"low\":" + dec(c.low) + ",\"close\":" + dec(c.close) + ",\"volume\":" +
        dec128(c.volumeBase, 8) + ",\"tradeCount\":" + std::to_string(c.tradeCount) + "}";
}

namespace {

std::string candleArray(const std::vector<CandleRow> &candles)
{
    std::string out = "[";
    for (std::size_t i = 0; i < candles.size(); ++i)
    {
        out += (0 == i ? "" : ",") + renderCandle(candles[i]);
    }
    return out + "]";
}

}

std::string renderCandleHistory(int marketId, const std::string &interval, const std::vector<CandleRow> &candles)
{
    return "{\"type\":\"CANDLE_HISTORY\"," + marketField(marketId) + ",\"interval\":\"" + jsonEscape(interval) +
        "\",\"candles\":" + candleArray(candles) + "}";
}

std::string renderCandleUpdate(int marketId, const CandleRow &c)
{
    return "{\"type\":\"CANDLE_UPDATE\"," + marketField(marketId) + ",\"interval\":\"1m\",\"candle\":" +
        renderCandle(c) + "}";
}

std::string renderCandlesBody(int marketId, const std::string &interval, const std::vector<CandleRow> &candles)
{
    return "{" + marketField(marketId) + ",\"interval\":\"" + jsonEscape(interval) + "\",\"candles\":" +
        candleArray(candles) + "}";
}

}}}
