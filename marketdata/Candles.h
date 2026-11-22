#ifndef GATEWAY_MARKETDATA_CANDLES_H
#define GATEWAY_MARKETDATA_CANDLES_H

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

// Candles and the ticker of the market history, as pure functions of the trade set (no clock, no I/O):
//   - a trade's bucket is floor(executed_at_ms / interval) * interval, from the journal's own timestamp, never from the
//     time the trade arrived here;
//   - the quote volume is price x quantity in 128 bits (the 64-bit product overflows at prices and sizes the exchange
//     takes; the FixedPoint.multiply class of defect); volumes are exact integers, stored as NUMERIC;
//   - a batch's candles are aggregated in journal order and merged into the stored row by the same rule (open kept,
//     high and low widened, close and last_trade_id replaced, volumes and counts added), so any split of one trade set
//     into batches committed in journal order gives the same rows.
// The ticker is computed from the 1m candles of the last 24 h and an "as of" time (live: the wall clock at each
// MD_TICKERS_INTERVAL_MS; the tests pass a fixed one). It is not stored.
// Test-only controls (never in a published build): MD_NC_WALLCLOCK_BUCKET buckets by the arrival time the caller passes
// instead of the trade's timestamp; MD_NC_QUOTE_64 computes the quote volume in 64 bits.

namespace openexchange { namespace gateway { namespace marketdata {

__extension__ typedef __int128 Int128;          // -Wpedantic: a GCC/Clang extension, used on purpose
__extension__ typedef unsigned __int128 UInt128;

inline constexpr std::array<std::int32_t, 6> CANDLE_INTERVALS_S = { 60, 300, 900, 3600, 14400, 86400 };
inline constexpr std::int64_t TICKER_WINDOW_MS = 86'400'000;

// One trade as the market history stores it (journal record 1 without its parties).
struct TradeRow
{
    std::int32_t marketId = 0;
    std::int64_t tradeId = 0;
    std::int64_t price = 0;
    std::int64_t quantity = 0;
    bool takerIsBuy = false;
    std::int64_t executedAtMs = 0;
    std::int64_t journalSeq = 0;
    bool operator==(const TradeRow &) const = default;
};

struct CandleRow
{
    std::int32_t marketId = 0;
    std::int32_t intervalS = 0;
    std::int64_t bucketStartMs = 0;
    std::int64_t open = 0;
    std::int64_t high = 0;
    std::int64_t low = 0;
    std::int64_t close = 0;
    Int128 volumeBase = 0;
    Int128 volumeQuote = 0;
    std::int64_t tradeCount = 0;
    std::int64_t lastTradeId = 0;
    bool operator==(const CandleRow &) const = default;
};

// floor(ms / (intervalS * 1000)) * intervalS * 1000
std::int64_t bucketStart(std::int64_t ms, std::int32_t intervalS) noexcept;
// price x quantity, exact
Int128 quoteOf(std::int64_t price, std::int64_t quantity) noexcept;
// The decimal text of an exact integer (NUMERIC parameters and test output).
std::string decimal(Int128 v);
// The value of a decimal integer text; false when it is not one or does not fit.
bool parseDecimal(const std::string &text, Int128 &out) noexcept;

// The merge rule: `later` (a bucket's trades that follow `into`'s in journal order) folded into `into`.
void mergeCandle(CandleRow &into, const CandleRow &later) noexcept;

// The candles one batch of trades (in journal order) touches, every interval, keyed by (market, interval, bucket),
// each the batch's own aggregate (merged into the stored row by mergeCandle / the upsert). `arrivalMs` is ignored
// (only the MD_NC_WALLCLOCK_BUCKET test build buckets by it).
using CandleKey = std::tuple<std::int32_t, std::int32_t, std::int64_t>;
std::map<CandleKey, CandleRow> aggregate(const std::vector<TradeRow> &trades, std::int64_t arrivalMs = 0);

// The ticker of one market at `asOfMs` from its 1m candles whose bucket starts in [asOfMs - 24 h, asOfMs] (any order;
// others are ignored). Empty when no such candle exists (a market with no price is absent from TICKERS).
struct Ticker
{
    std::int32_t marketId = 0;
    std::int64_t lastPrice = 0;        // close of the newest bucket
    std::int64_t openPrice = 0;        // open of the oldest bucket of the window
    std::int64_t priceChange = 0;      // lastPrice - openPrice
    std::int64_t priceChangePctE8 = 0; // priceChange * 100 / openPrice, in units of 1e-8 percent, toward zero
    std::int64_t high24h = 0;
    std::int64_t low24h = 0;
    Int128 volume24hQuote = 0;         // sum of volume_quote (the quote volume, as today)
    Int128 volume24hBase = 0;
    bool operator==(const Ticker &) const = default;
};
std::optional<Ticker> tickerOf(std::int32_t marketId, const std::vector<CandleRow> &candles1m, std::int64_t asOfMs);

}}}

#endif
