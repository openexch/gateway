#ifndef GATEWAY_MARKETDATA_BOOK_FRAMES_H
#define GATEWAY_MARKETDATA_BOOK_FRAMES_H

#include <cstdint>
#include <optional>
#include <utility>
#include <string>
#include <vector>

#include "cluster_tools/domain/forwarder/BookLadder.h"
#include "cluster_tools/domain/forwarder/MarketData.h"

#include "Candles.h"

// BookFrames - the JSON text of the public market frames this build sends, one function per frame. Pure, no state.
//
// Prices and quantities are decimal strings with at most 8 fraction digits, formatted from the int64 wire values with
// integer arithmetic (rest::formatMoney, the v2 API's convention): no double exists anywhere here. The envelope ids the
// edge relay inspects stay JSON numbers: marketId, bookVersion, fromVersion, timestamp. Neither `market` (a name; the
// names come from the gateway's market list) nor the old snapshot `version` is sent. Field order is fixed, so a frame
// is a byte-for-byte golden value.
//
//   BOOK_SNAPSHOT  {"type","marketId","bids":[level],"asks":[level],"bookVersion","timestamp"[,"stale":true]}
//   level          {"price","quantity","orderCount"}
//   BOOK_DELTA     {"type","marketId","changes":[change],"bidVersion","askVersion","bookVersion","fromVersion","timestamp"}
//   change         {"price","quantity","orderCount","side":"BID"|"ASK","updateType":"NEW_LEVEL"|"UPDATE_LEVEL"|"DELETE_LEVEL"}
//   SUBSCRIPTION_CONFIRMED {"type","status":"subscribed","marketId"}
//   PONG           {"type","timestamp"}
//   ERROR          {"type","code","message"[,"parts":[...]]}
//   TRADES_BATCH   {"type","marketId","trades":[trade],"timestamp"}
//   trade          {"tradeId"(dec),"price","quantity","tradeCount":1,"side":"BUY"|"SELL","timestamp"}
//   TICKER_STATS   {"type","marketId","lastPrice","priceChange","priceChangePercent","high24h","low24h","volume24h",
//                  "timestamp"}
//   TICKERS        {"type","tickers":[the TICKER_STATS fields without "type"],"timestamp"}
//   CANDLE_HISTORY {"type","marketId","interval","candles":[candle]}
//   CANDLE_UPDATE  {"type","marketId","interval":"1m","candle":candle}
//   candle         {"time"(seconds),"open","high","low","close","volume"(base),"tradeCount"}
//   GET /api/candles body {"marketId","interval","candles":[candle]}
// Scales: a price, a quantity and a base volume are 10^-8 units (formatMoney); the quote volume (price x quantity) is
// 10^-16 units and priceChangePercent 10^-8 percent; every one is cut to at most 8 fraction digits toward zero.
// tradeId is a decimal string (the v2 API's rule for ids); the trade's and the ticker's timestamp are the journal's
// (ms), the envelope timestamp is the time of sending (ms).

namespace openexchange { namespace gateway { namespace marketdata {

// The ME frame fields the ladder does not keep (the 22 / 25 root block): the ME's timestamp (ms) and the two side
// versions.
struct BookStamps
{
    std::int64_t timestampMs = 0;
    std::int64_t bidVersion = 0;
    std::int64_t askVersion = 0;
};

// `ladder` null or not OK: no level is listed (the levels of a ladder that cannot prove its state are never sent).
std::string renderBookSnapshot(int marketId, const cluster_tools::domain::forwarder::BookLadder *ladder,
    std::int64_t bookVersion, std::int64_t timestampMs, bool stale);
std::string renderBookDelta(int marketId, const cluster_tools::domain::forwarder::BookFrame &frame,
    const BookStamps &stamps);
std::string renderSubscribed(int marketId);
std::string renderPong(std::int64_t timestampMs);
std::string renderError(const std::string &code, const std::string &message,
    const std::vector<std::string> &parts = {});

// An exact integer in units of 10^-`scaleDigits` as a decimal text with at most 8 fraction digits (cut toward zero,
// trailing zeros dropped).
std::string formatScaled(Int128 units, int scaleDigits);

std::string renderTradesBatch(int marketId, const std::vector<TradeRow> &trades, std::int64_t timestampMs);
// `ticker` empty: a market with no price in the window (every value 0, timestamp 0). `lastTradeMs`: the newest
// trade's journal timestamp.
std::string renderTickerStats(int marketId, const std::optional<Ticker> &ticker, std::int64_t lastTradeMs);
// The entries of TICKERS (the TICKER_STATS fields without "type"), one per market with a price, by market id.
std::string renderTickerEntries(const std::vector<std::pair<Ticker, std::int64_t>> &tickers);
std::string renderTickers(const std::string &entries, std::int64_t timestampMs);
std::string renderCandle(const CandleRow &c);
std::string renderCandleHistory(int marketId, const std::string &interval, const std::vector<CandleRow> &candles);
std::string renderCandleUpdate(int marketId, const CandleRow &c);
std::string renderCandlesBody(int marketId, const std::string &interval, const std::vector<CandleRow> &candles);

}}}

#endif
