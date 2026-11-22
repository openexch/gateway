#ifndef GATEWAY_TESTS_MD_LIVE_RIG_H
#define GATEWAY_TESTS_MD_LIVE_RIG_H

// The live trade side of oe-marketdata as the duty thread drives it (BookCore with a TradeCore attached), with fixed
// clocks and a fixed market history, for the T0 cases (MarketDataLiveTest.cpp) and the frame samples the relay and
// shape rows read (MarketDataTest.cpp).

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gateway/common/Readiness.h"

#include "BookCore.h"
#include "TradeCore.h"

namespace mdlive {

using namespace openexchange::gateway;
using namespace openexchange::gateway::marketdata;

constexpr std::int64_t MS = 1'000'000LL;
constexpr std::int64_t E8 = 100'000'000LL;
constexpr std::int64_t T0 = 1'700'000'040'000LL; // a minute boundary (ms)

struct Seen
{
    std::string text;
    FrameType type;
};

class Client final : public Outlet
{
public:
    bool offer(const Frame &f) override
    {
        frames.push_back({ *f.text, f.type });
        return true;
    }
    void resyncBegins() override {}
    std::vector<Seen> frames;
};

inline TradeRow trade(std::int64_t id, std::int32_t market, std::int64_t price, std::int64_t qty, bool buy,
    std::int64_t ts)
{
    TradeRow t;
    t.marketId = market;
    t.tradeId = id;
    t.price = price;
    t.quantity = qty;
    t.takerIsBuy = buy;
    t.executedAtMs = ts;
    t.journalSeq = id * 2;
    return t;
}

// The market history the rig starts from: market 3 with trades 101..104 over two minutes, market 1 with one trade a
// day and a half old (no price in the 24 h window), market 2 empty.
inline std::vector<TradeRow> seedTrades()
{
    return { trade(101, 3, 1'500'000'000'000, 50'000'000, true, T0 + 1'000),
        trade(102, 3, 1'500'100'000'000, 25'000'000, false, T0 + 30'000),
        trade(103, 3, 1'499'900'000'000, 10'000'000, true, T0 + 61'000),
        trade(104, 3, 1'500'050'000'000, 40'000'000, false, T0 + 90'000),
        trade(51, 1, 20 * E8, 3 * E8, true, T0 - 36 * 3'600'000LL) };
}

inline MarketSeed seedOf(const std::vector<TradeRow> &trades, std::size_t recent)
{
    MarketSeed seed;
    for (const int id : { 1, 2, 3 })
    {
        MarketSeed::Market m;
        m.marketId = id;
        std::vector<TradeRow> mine;
        for (const TradeRow &t : trades)
        {
            if (t.marketId == id)
            {
                mine.push_back(t);
            }
        }
        for (const auto &kv : aggregate(mine))
        {
            if (60 == kv.second.intervalS)
            {
                m.candles1m.push_back(kv.second);
            }
        }
        const std::size_t from = mine.size() > recent ? mine.size() - recent : 0;
        m.tape.assign(mine.begin() + static_cast<std::ptrdiff_t>(from), mine.end());
        seed.markets.push_back(std::move(m));
    }
    return seed;
}

struct LiveRig
{
    explicit LiveRig(std::vector<int> expected = { 1, 2, 3 }, std::size_t recent = 3, std::size_t history = 4) :
        core(BookCoreSettings{ expected, MarketDataReadyBounds{ 10'000, 15'000 } }, stats, [this] { return wallMs; }),
        trades(TradeCoreSettings{ expected, recent, history, 20, 1000 }, [this] { return wallMs; }, [this] { return now; }),
        recentTrades(recent)
    {
        core.attachTrades(&trades);
    }
    std::shared_ptr<Client> open(std::uint64_t id)
    {
        auto c = std::make_shared<Client>();
        core.open(id, c);
        return c;
    }
    void egress(const std::vector<std::uint8_t> &frame)
    {
        core.onEgress(frame.data(), frame.size(), 1, 1, now);
    }
    // the trade path starts from the market history (the writer read it) and the duty cycle runs
    void start(const std::vector<TradeRow> &history)
    {
        trades.onTradePathStart(seedOf(history, recentTrades));
        core.doWork(now);
    }
    void live(const TradeRow &t) { trades.onLiveTrade(t); }
    // one duty cycle `ms` later
    void cycle(std::int64_t ms)
    {
        now += ms * MS;
        wallMs += ms;
        core.doWork(now);
    }

    std::int64_t wallMs = T0 + 150'000;
    std::int64_t now = 5'000 * MS;
    MdStats stats;
    BookCore core;
    TradeCore trades;
    std::size_t recentTrades;
};

}

#endif
