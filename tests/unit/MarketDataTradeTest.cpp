// oe-marketdata trade path, T0: the forward rule, the record take (journal record 1 without its parties), the halts,
// the pending list, the live-emit cursor, and the candles and the ticker as functions of the trade set (the pure
// part; the PostgreSQL part is tests/pg/MarketHistoryPgTest.cpp).
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "Check.h"
#include "Candles.h"
#include "TradePath.h"

using namespace openexchange::gateway::marketdata;

namespace {

void put64(std::vector<std::uint8_t> &b, std::size_t at, std::int64_t v)
{
    std::memcpy(b.data() + at, &v, 8);
}

// One journal record 1 (schema 4, version 1, block 109): the parties carry marker values.
std::vector<std::uint8_t> tradeBytes(std::int64_t seq, std::int64_t epoch, std::int64_t tradeId, std::int32_t market,
    std::int64_t price, std::int64_t qty, std::uint8_t takerIsBuy, std::int64_t ts)
{
    std::vector<std::uint8_t> b(117, 0);
    const std::uint16_t header[4] = { 109, 1, 4, 1 };
    std::memcpy(b.data(), header, 8);
    const std::size_t o = 8;
    put64(b, o + 0, seq);
    put64(b, o + 8, epoch);
    put64(b, o + 16, 64 * seq);
    put64(b, o + 24, tradeId);
    std::memcpy(b.data() + o + 32, &market, 4);
    put64(b, o + 36, 900001); // takerOrderId
    put64(b, o + 44, 900002); // takerUserId
    put64(b, o + 52, 900003); // makerOrderId
    put64(b, o + 60, 900004); // makerUserId
    put64(b, o + 68, price);
    put64(b, o + 76, qty);
    b[o + 84] = takerIsBuy;
    put64(b, o + 85, ts);
    put64(b, o + 93, 900005);
    put64(b, o + 101, 900006);
    return b;
}

ForwardInput in(std::int64_t seq, std::int64_t next, bool memoFree = true, bool first = false,
    std::optional<std::int64_t> epoch = 7, std::int64_t recordEpoch = 7)
{
    ForwardInput i;
    i.seq = seq;
    i.next = next;
    i.memoFreeRead = memoFree;
    i.firstOfRead = first;
    i.boundEpoch = epoch;
    i.epoch = recordEpoch;
    return i;
}

std::vector<TradeRow> tradeSet(std::uint32_t seed, std::size_t n)
{
    std::mt19937 rng(seed);
    std::vector<TradeRow> out;
    std::int64_t ts = 1'790'000'000'000;
    for (std::size_t i = 0; i < n; ++i)
    {
        TradeRow t;
        t.marketId = 1 + static_cast<std::int32_t>(rng() % 3);
        t.tradeId = 1000 + static_cast<std::int64_t>(i);
        t.price = 100'000'000LL * (40'000 + static_cast<std::int64_t>(rng() % 2000));
        t.quantity = 1'000'000LL * (1 + static_cast<std::int64_t>(rng() % 500));
        t.takerIsBuy = 0 == rng() % 2;
        ts += static_cast<std::int64_t>(rng() % 40'000); // up to 40 s apart: buckets of every interval are shared
        if (0 == i % 17)
        {
            ts -= 5000; // a timestamp that steps back: the bucket still comes from it
        }
        t.executedAtMs = ts;
        t.journalSeq = static_cast<std::int64_t>(i + 1);
        out.push_back(t);
    }
    // the 128-bit case: 50 000.00000000 x 100 000.00000000 overflows a 64-bit product
    TradeRow big = out.back();
    big.tradeId += 1;
    big.journalSeq += 1;
    big.price = 5'000'000'000'000LL;
    big.quantity = 10'000'000'000'000LL;
    out.push_back(big);
    return out;
}

// The stored rows after the set is committed in batches of `split`, each batch's aggregate merged into the stored row.
std::map<CandleKey, CandleRow> apply(const std::vector<TradeRow> &set, std::size_t split, std::int64_t arrivalBase)
{
    std::map<CandleKey, CandleRow> stored;
    std::int64_t arrival = arrivalBase;
    for (std::size_t at = 0; at < set.size(); at += split)
    {
        const std::vector<TradeRow> batch(set.begin() + static_cast<std::ptrdiff_t>(at),
            set.begin() + static_cast<std::ptrdiff_t>(std::min(set.size(), at + split)));
        for (const auto &[key, row] : aggregate(batch, arrival))
        {
            const auto it = stored.find(key);
            if (stored.end() == it)
            {
                stored.emplace(key, row);
            }
            else
            {
                mergeCandle(it->second, row);
            }
        }
        arrival += 61'000; // the batches arrive a minute apart
    }
    return stored;
}

}

TEST(md_trade_forward_rule)
{
    CHECK(Forward::SKIP == forwardRule(in(5, 9)));
    CHECK(Forward::TAKE == forwardRule(in(9, 9)));
    const bool jumpBelieved = Forward::HOLE == forwardRule(in(11, 9, true, false)) &&
        Forward::BELOW_RANGE == forwardRule(in(11, 9, true, true));
    CHECK_MSG(jumpBelieved, "trades skipped over a purged prefix");
    CHECK(Forward::RE_READ == forwardRule(in(11, 9, false, true)));   // a jump on a memo read is not believed
    CHECK(Forward::SKIP == forwardRule(in(3, 9, false, true)));
    // the epoch first, whatever the sequence
    const bool epochChecked = Forward::EPOCH_MISMATCH == forwardRule(in(1, 9, true, true, 7, 8)) &&
        Forward::EPOCH_MISMATCH == forwardRule(in(9, 9, true, false, 7, 8));
    CHECK_MSG(epochChecked, "trades of two genesis epochs in one history");
    // an empty history: the first record of a read from the chain start is where it starts
    CHECK(Forward::START == forwardRule(in(1, 0, true, true, std::nullopt, 8)));
    CHECK(Forward::START == forwardRule(in(4000, 0, true, true, std::nullopt, 8)));
    CHECK(Forward::RE_READ == forwardRule(in(4000, 0, false, true, std::nullopt, 8)));
}

TEST(md_trade_take_record)
{
    const std::vector<int> expected = { 1, 2, 3 };
    TradeRow t;
    auto b = tradeBytes(12, 7, 555, 2, 4'000'000'000'000LL, 25'000'000, 1, 1'790'000'000'123LL);
    CHECK(Taken::TRADE == takeRecord(b.data(), b.size(), 1, expected, t));
    CHECK(2 == t.marketId && 555 == t.tradeId && 4'000'000'000'000LL == t.price && 25'000'000 == t.quantity);
    CHECK(t.takerIsBuy && 1'790'000'000'123LL == t.executedAtMs && 12 == t.journalSeq);
    // the parties are never read: no value of them is anywhere in the row
    for (const std::int64_t marker : { 900001, 900002, 900003, 900004, 900005, 900006 })
    {
        CHECK(marker != t.tradeId && marker != t.price && marker != t.quantity && marker != t.executedAtMs);
    }
    auto seller = tradeBytes(13, 7, 556, 3, 1, 1, 0, 1);
    CHECK(Taken::TRADE == takeRecord(seller.data(), seller.size(), 1, expected, t) && !t.takerIsBuy);
    // records 2..5: only the sequence advances (the bytes are not even read)
    CHECK(Taken::ADVANCE == takeRecord(nullptr, 0, 2, expected, t));
    CHECK(Taken::ADVANCE == takeRecord(nullptr, 0, 5, expected, t));
    // a market outside MD_EXPECTED_MARKETS
    auto four = tradeBytes(14, 7, 557, 4, 1, 1, 1, 1);
    const Taken unknown = takeRecord(four.data(), four.size(), 1, expected, t);
    CHECK_MSG(Taken::UNKNOWN_MARKET == unknown, "trade stored for a market with no book");
    CHECK(4 == t.marketId && 557 == t.tradeId);
    // a takerIsBuy byte that is neither 0 nor 1, a record of the wrong length
    auto bad = tradeBytes(15, 7, 558, 1, 1, 1, 2, 1);
    CHECK(Taken::BAD_RECORD == takeRecord(bad.data(), bad.size(), 1, expected, t));
    CHECK(Taken::BAD_RECORD == takeRecord(b.data(), b.size() - 1, 1, expected, t));
}

TEST(md_trade_halts)
{
    TradeHalts h;
    CHECK(!h.any() && MdHalt::NONE == h.primary());
    CHECK(h.set(MdHalt::GAP, "g"));
    CHECK(!h.set(MdHalt::GAP, "again")); // the first detail is kept
    CHECK("g" == h.detail(MdHalt::GAP));
    CHECK(MdHalt::GAP == h.primary());
    CHECK(h.set(MdHalt::UNKNOWN_MARKET, "u"));
    CHECK(MdHalt::UNKNOWN_MARKET == h.primary());
    CHECK(h.set(MdHalt::EPOCH_MISMATCH, "e"));
    CHECK(MdHalt::EPOCH_MISMATCH == h.primary());
    h.clearGap();
    CHECK(!h.has(MdHalt::GAP) && h.has(MdHalt::EPOCH_MISMATCH) && h.any());
    // each kind keeps its own name
    CHECK(std::string("GAP") == nameOf(MdHalt::GAP));
    CHECK(std::string("EPOCH_MISMATCH") == nameOf(MdHalt::EPOCH_MISMATCH));
    CHECK(std::string("JOURNAL_PREFIX_PURGED") == nameOf(MdHalt::JOURNAL_PREFIX_PURGED));
    CHECK(std::string("SOURCE_SCHEMA") == nameOf(MdHalt::SOURCE_SCHEMA));
    CHECK(std::string("UNKNOWN_MARKET") == nameOf(MdHalt::UNKNOWN_MARKET));
}

TEST(md_trade_pending_list)
{
    TradeLedger l(3);
    CommitBatch b;
    CHECK(!l.nextBatch(10, b)); // no md_state yet
    MdState st;
    st.epoch = 7;
    st.lastSeq = 10;
    st.firstSeq = 1;
    l.publishState(st);
    CHECK(l.stateKnown() && 10 == l.committedSeq());
    auto entry = [](std::int64_t seq, bool trade) {
        PendingEntry e;
        e.seq = seq;
        e.hasTrade = trade;
        e.trade.tradeId = seq * 10;
        e.trade.journalSeq = seq;
        return e;
    };
    CHECK(l.push(entry(11, true)));
    CHECK(l.push(entry(12, false)));
    CHECK(l.push(entry(13, false))); // folded into 12
    CHECK(l.push(entry(14, true)));
    CHECK(3 == l.pending());
    CHECK(!l.push(entry(15, true))); // full: refused, nothing dropped
    CHECK(!l.push(entry(15, false))); // behind a trade a record 2..5 is a new entry: refused too
    CHECK(3 == l.pending());
    TradeLedger m(3);
    m.publishState(st);
    m.push(entry(11, true));
    m.push(entry(12, false));
    CHECK(m.nextBatch(2, b));
    CHECK(10 == b.fromSeq && 12 == b.toSeq && 1 == b.trades.size() && 7 == b.epoch && 1 == b.firstSeq);
    m.push(entry(13, false)); // the batch is in flight: not folded into its entry 12
    m.committed(b);
    CHECK(12 == m.committedSeq() && 1 == m.pending());
    CHECK(m.nextBatch(5, b) && 12 == b.fromSeq && 13 == b.toSeq && b.trades.empty());
    // an empty history binds its epoch and first record with its first commit
    TradeLedger e(4);
    e.publishState(MdState{});
    e.push(entry(4000, true));
    CHECK(!e.nextBatch(4, b)); // not bound yet
    e.bind(9, 4000);
    CHECK(e.nextBatch(4, b) && 0 == b.fromSeq && 4000 == b.toSeq && 9 == b.epoch && 4000 == b.firstSeq);
    e.committed(b);
    CHECK(e.partial.load() && 9 == e.epoch.load() && 4000 == e.firstSeq.load());
}

TEST(md_trade_live_emit_cursor)
{
    LiveEmitCursor c;
    CHECK(c.offer(5));
    CHECK(c.offer(6));
    // a failover or a resume reads 5 and 6 again: never handed on twice
    const bool again = c.offer(6) || c.offer(5);
    CHECK_MSG(!again, "trade 6 sent twice to one connection");
    CHECK(c.offer(7) && 7 == c.value());
}

TEST(md_candles_function_of_trade_set)
{
    const std::vector<TradeRow> set = tradeSet(11, 400);
    const auto one = apply(set, 1, 1'800'000'000'000);
    const auto seven = apply(set, 7, 1'800'100'000'000);
    const auto all = apply(set, set.size(), 1'800'200'000'000);
    const bool equal = one == seven && seven == all;
    CHECK_MSG(equal, "candle bucket depends on arrival time");
    // the 128-bit case
    const TradeRow &big = set.back();
    const auto c = all.find(CandleKey{ big.marketId, 60, bucketStart(big.executedAtMs, 60) });
    Int128 want = 0;
    parseDecimal("50000000000000000000000000", want);
    const bool exact = quoteOf(big.price, big.quantity) == want && (all.end() == c || c->second.volumeQuote >= want);
    CHECK_MSG(exact, "quote volume overflow: " + decimal(quoteOf(big.price, big.quantity)));
    // every bucket's counts and volumes equal a recomputation from the set
    for (const auto &[key, row] : all)
    {
        std::int64_t count = 0;
        Int128 base = 0;
        for (const TradeRow &t : set)
        {
            if (t.marketId == std::get<0>(key) && bucketStart(t.executedAtMs, std::get<1>(key)) == std::get<2>(key))
            {
                count++;
                base += t.quantity;
            }
        }
        CHECK(count == row.tradeCount && base == row.volumeBase);
    }
    // the ticker at one fixed "as of" time from each
    const std::int64_t asOf = set.back().executedAtMs + 30'000;
    for (int market = 1; market <= 3; ++market)
    {
        auto minutes = [&](const std::map<CandleKey, CandleRow> &rows) {
            std::vector<CandleRow> out;
            for (const auto &[key, row] : rows)
            {
                if (60 == row.intervalS)
                {
                    out.push_back(row);
                }
            }
            return out;
        };
        const auto a = tickerOf(market, minutes(one), asOf);
        const auto b = tickerOf(market, minutes(all), asOf);
        CHECK(a.has_value() && a == b);
    }
}

TEST(md_candles_bucket_and_merge)
{
    CHECK(1'790'000'040'000 == bucketStart(1'790'000'059'999, 60));
    CHECK(1'789'948'800'000 == bucketStart(1'790'000'059'999, 86400));
    CHECK(-60'000 == bucketStart(-1, 60));
    TradeRow a{ 1, 10, 500, 2, true, 1'790'000'040'001, 1 };
    TradeRow b{ 1, 11, 700, 3, false, 1'790'000'040'002, 2 };
    TradeRow c{ 1, 12, 400, 5, true, 1'790'000'040'003, 3 };
    const auto rows = aggregate({ a, b, c });
    const CandleRow &m = rows.at(CandleKey{ 1, 60, 1'790'000'040'000 });
    CHECK(500 == m.open && 700 == m.high && 400 == m.low && 400 == m.close && 10 == m.volumeBase);
    CHECK(500 * 2 + 700 * 3 + 400 * 5 == m.volumeQuote && 3 == m.tradeCount && 12 == m.lastTradeId);
    CHECK(6 * 1 == static_cast<int>(rows.size()));
    // the ticker: newest close, oldest open, 24 h window only
    std::vector<CandleRow> minutes;
    for (const auto &[key, row] : rows)
    {
        if (60 == row.intervalS)
        {
            minutes.push_back(row);
        }
    }
    CandleRow old = minutes[0];
    old.bucketStartMs -= TICKER_WINDOW_MS + 60'000; // outside the window
    old.open = 1;
    minutes.push_back(old);
    const auto t = tickerOf(1, minutes, 1'790'000'100'000);
    CHECK(t && 400 == t->lastPrice && 500 == t->openPrice && -100 == t->priceChange);
    CHECK(t && -2'000'000'000 == t->priceChangePctE8); // -20 %
    CHECK(!tickerOf(2, minutes, 1'790'000'100'000));
    Int128 v = 0;
    CHECK(parseDecimal("-170141183460469231731687303715884105727", v) && "-170141183460469231731687303715884105727" == decimal(v));
    CHECK(!parseDecimal("12x", v) && !parseDecimal("", v));
}
