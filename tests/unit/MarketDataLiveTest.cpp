// oe-marketdata live trade side, T0: the tape, the 1m candles and the tickers in memory (TradeCore) behind the
// subscription rule of BookCore: the golden trade frames (the TICKER_STATS that follows a TRADES_BATCH flush, the
// TICKERS of the handshake), the subscribe order, the initial state while the market history was down at the start,
// each trade once per connection, the ticker window and TICKERS on change only, no party field on the public
// surface, and GET /api/candles (parameters, the body, the history down, CORS).
#include <chrono>
#include <cstring>
#include <fstream>
#include <future>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <boost/json.hpp>

#include "Check.h"
#include "MdLiveRig.h"

#include "BookFrames.h"
#include "CandleRoute.h"

#include "golden_frames.inc"

using namespace mdlive;
namespace json = boost::json;

namespace {

void goldenLive(const std::string &name, const std::string &produced)
{
    const std::string path = std::string(GW_MD_GOLDEN_DIR) + "/" + name + ".json";
    const char *write = std::getenv("GW_MD_GOLDEN_WRITE");
    if (nullptr != write && 0 == std::strcmp(write, "1"))
    {
        std::ofstream(path, std::ios::binary) << produced << "\n";
        return;
    }
    std::ifstream in(path, std::ios::binary);
    std::stringstream buffer;
    buffer << in.rdbuf();
    std::string want = buffer.str();
    if (!want.empty() && '\n' == want.back())
    {
        want.pop_back();
    }
    CHECK_MSG(in.is_open(), "golden file missing: " + path);
    CHECK_MSG(want == produced, "golden mismatch " + name + ":\n  want " + want + "\n  got  " + produced);
}

std::vector<FrameType> typesOf(const std::vector<Seen> &frames, std::size_t from = 0)
{
    std::vector<FrameType> out;
    for (std::size_t i = from; i < frames.size(); ++i)
    {
        out.push_back(frames[i].type);
    }
    return out;
}

std::string names(const std::vector<FrameType> &types)
{
    std::string out;
    for (const FrameType t : types)
    {
        out += (out.empty() ? "" : ",") + std::string(nameOf(t));
    }
    return out;
}

// The tradeIds a connection was sent, each frame's trades in order; a second sight of an id is reported.
std::string firstDuplicateTrade(const std::vector<Seen> &frames)
{
    std::set<std::string> seen;
    for (const Seen &f : frames)
    {
        if (FrameType::TRADES_BATCH != f.type)
        {
            continue;
        }
        const json::value doc = json::parse(f.text);
        for (const json::value &t : doc.as_object().at("trades").as_array())
        {
            const std::string id(t.as_object().at("tradeId").as_string().c_str());
            if (!seen.insert(id).second)
            {
                return id;
            }
        }
    }
    return "";
}

const std::vector<FrameType> INITIAL = { FrameType::TICKER_STATS, FrameType::TICKERS, FrameType::BOOK_SNAPSHOT,
    FrameType::TRADES_BATCH, FrameType::CANDLE_HISTORY };

}

// ---------------------------------------------------------------- golden trade frames

TEST(md_live_golden_frames)
{
    for (int round = 0; round < 2; ++round)
    {
        LiveRig rig;
        rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
        rig.egress(JAVA_BOOK_SNAPSHOT_20x20); // market 3
        rig.start(seedTrades());
        auto c = rig.open(1);
        CHECK(1 == c->frames.size() && FrameType::TICKERS == c->frames[0].type);
        if (!c->frames.empty())
        {
            goldenLive("tickers_handshake", c->frames[0].text);
        }
        rig.core.subscribe(1, 3, rig.now);
        const std::vector<FrameType> want = { FrameType::TICKERS, FrameType::SUBSCRIPTION_CONFIRMED,
            FrameType::TICKER_STATS, FrameType::TICKERS, FrameType::BOOK_SNAPSHOT, FrameType::TRADES_BATCH,
            FrameType::CANDLE_HISTORY };
        CHECK_MSG(typesOf(c->frames) == want, "subscribe order: " + names(typesOf(c->frames)));
        if (typesOf(c->frames) == want)
        {
            goldenLive("ticker_stats_initial", c->frames[2].text);
            goldenLive("trades_batch_initial", c->frames[5].text);
            goldenLive("candle_history_1m", c->frames[6].text);
        }
        // two live trades, one of them in a new minute: one flush after MD_TRADE_FLUSH_MS
        rig.live(trade(105, 3, 1'500'200'000'000, 5'000'000, true, T0 + 100'000));
        rig.live(trade(106, 3, 1'500'300'000'000, 7'500'000, false, T0 + 125'000));
        const std::size_t before = c->frames.size();
        rig.cycle(10);
        CHECK(c->frames.size() == before); // the flush window has not passed
        rig.cycle(10);
        const std::vector<FrameType> flush = { FrameType::TRADES_BATCH, FrameType::TICKER_STATS,
            FrameType::CANDLE_UPDATE, FrameType::CANDLE_UPDATE };
        CHECK_MSG(typesOf(c->frames, before) == flush, "flush order: " + names(typesOf(c->frames, before)));
        if (typesOf(c->frames, before) == flush)
        {
            goldenLive("trades_batch_flush", c->frames[before].text);
            goldenLive("ticker_stats_flush", c->frames[before + 1].text);
            goldenLive("candle_update_open", c->frames[before + 2].text);
            goldenLive("candle_update_new", c->frames[before + 3].text);
        }
        // TICKERS on change only, every MD_TICKERS_INTERVAL_MS
        rig.cycle(1000);
        CHECK(FrameType::TICKERS == c->frames.back().type);
        goldenLive("tickers_changed", c->frames.back().text);
        const std::size_t quiet = c->frames.size();
        rig.cycle(1000);
        rig.cycle(1000);
        CHECK_MSG(c->frames.size() == quiet, "TICKERS sent without a change");
    }
}

TEST(md_live_subscribe_order_and_refresh)
{
    LiveRig rig;
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    rig.start(seedTrades());
    auto c = rig.open(1);
    rig.core.subscribe(1, 3, rig.now);
    rig.core.refresh(1, 3, rig.now);
    // TICKERS (handshake), CONFIRMED + initial state, then the initial state again
    std::vector<FrameType> want = { FrameType::TICKERS, FrameType::SUBSCRIPTION_CONFIRMED };
    want.insert(want.end(), INITIAL.begin(), INITIAL.end());
    want.insert(want.end(), INITIAL.begin(), INITIAL.end());
    CHECK_MSG(typesOf(c->frames) == want, "order: " + names(typesOf(c->frames)));
    // a subscribe replaces the market: market 2 (no trade ever) has an empty tape and history, both whole
    const std::size_t before = c->frames.size();
    rig.core.subscribe(1, 2, rig.now);
    CHECK(c->frames.size() == before + 1 + INITIAL.size());
    if (c->frames.size() == before + 1 + INITIAL.size())
    {
        CHECK(std::string::npos != c->frames[before + 1].text.find("\"marketId\":2,\"lastPrice\":\"0\""));
        CHECK(std::string::npos != c->frames[before + 4].text.find("\"trades\":[]"));
        CHECK(std::string::npos != c->frames[before + 5].text.find("\"candles\":[]"));
    }
    // trades of market 3 no longer reach it
    const std::size_t after = c->frames.size();
    rig.live(trade(105, 3, 1'500'200'000'000, 5'000'000, true, T0 + 100'000));
    rig.cycle(25);
    CHECK(c->frames.size() == after);
    // a resync of a connection with no market: TICKERS again
    auto bare = rig.open(2);
    rig.core.resync(2, rig.now);
    CHECK((typesOf(bare->frames) == std::vector<FrameType>{ FrameType::TICKERS, FrameType::TICKERS }));
}

// ---------------------------------------------------------------- the market history down at the start

TEST(md_live_history_down_at_start)
{
    for (int round = 0; round < 2; ++round)
    {
        LiveRig rig;
        rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
        rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
        auto bare = rig.open(1);
        auto sub = rig.open(2);
        rig.core.subscribe(2, 3, rig.now);
        // a trade the follower could not have handed on before the start is ignored, not shown
        rig.live(trade(105, 3, 1'500'200'000'000, 5'000'000, true, T0 + 100'000));
        rig.cycle(25);
        CHECK_MSG(bare->frames.empty(), "handshake TICKERS sent without the history");
        bool emptySeries = false;
        for (const Seen &f : sub->frames)
        {
            emptySeries = emptySeries || FrameType::TRADES_BATCH == f.type || FrameType::CANDLE_HISTORY == f.type ||
                FrameType::TICKER_STATS == f.type || FrameType::TICKERS == f.type;
        }
        CHECK_MSG(!emptySeries, "empty series sent without the history");
        const std::vector<FrameType> down = { FrameType::SUBSCRIPTION_CONFIRMED, FrameType::BOOK_SNAPSHOT,
            FrameType::ERROR };
        CHECK_MSG(typesOf(sub->frames) == down, "down at start: " + names(typesOf(sub->frames)));
        if (3 == sub->frames.size())
        {
            const json::object e = json::parse(sub->frames[2].text).as_object();
            CHECK("HISTORY_UNAVAILABLE" == e.at("code").as_string());
            CHECK(std::string::npos != sub->frames[2].text.find("\"parts\":[\"tape\",\"candles\",\"ticker\"]"));
        }
        // the trade path starts: a fresh initial state for every connection, TICKERS included
        rig.start(seedTrades());
        CHECK((typesOf(bare->frames) == std::vector<FrameType>{ FrameType::TICKERS }));
        CHECK_MSG(typesOf(sub->frames, 3) == INITIAL, "after the start: " + names(typesOf(sub->frames, 3)));
        CHECK(firstDuplicateTrade(sub->frames).empty());
        // and only once
        rig.cycle(1);
        CHECK(sub->frames.size() == 3 + INITIAL.size());
    }
}

// ---------------------------------------------------------------- each trade once per connection

TEST(md_live_each_trade_once)
{
    LiveRig rig({ 1, 2, 3 }, 50, 200);
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    rig.start(seedTrades());
    auto early = rig.open(1);
    rig.core.subscribe(1, 3, rig.now);
    std::int64_t id = 200;
    std::vector<std::shared_ptr<Client>> late;
    for (int step = 0; step < 40; ++step)
    {
        rig.live(trade(id, 3, 1'500'000'000'000 + id, 1'000'000, 0 == id % 2, T0 + 100'000 + id));
        id++;
        if (0 == step % 3)
        {
            // a subscriber that arrives while trades wait in the flush window
            late.push_back(rig.open(static_cast<std::uint64_t>(100 + step)));
            rig.core.subscribe(static_cast<std::uint64_t>(100 + step), 3, rig.now);
        }
        rig.cycle(0 == step % 4 ? 25 : 5);
    }
    rig.cycle(25);
    late.push_back(early);
    for (const auto &c : late)
    {
        const std::string dup = firstDuplicateTrade(c->frames);
        CHECK_MSG(dup.empty(), "trade " + dup + " sent twice to one connection");
        // and none missing: the initial tape plus the flushes end at the last trade
        std::set<std::string> ids;
        for (const Seen &f : c->frames)
        {
            if (FrameType::TRADES_BATCH == f.type)
            {
                const json::value doc = json::parse(f.text);
                for (const json::value &t : doc.as_object().at("trades").as_array())
                {
                    ids.insert(std::string(t.as_object().at("tradeId").as_string().c_str()));
                }
            }
        }
        CHECK(ids.count(std::to_string(id - 1)) == 1);
    }
}

// ---------------------------------------------------------------- the ticker

TEST(md_live_ticker_window_and_scales)
{
    LiveRig rig;
    rig.start(seedTrades());
    // market 1's only trade is 36 h old: a quiet market keeps its newest trade's price, every 24 h value is 0, and it
    // stays in TICKERS; market 2 never traded: all zero and absent from TICKERS
    const std::string all = rig.trades.tickers().text->c_str();
    const std::string quiet1 = "\"marketId\":1,\"lastPrice\":\"20\",\"priceChange\":\"0\",\"priceChangePercent\":\"0\","
        "\"high24h\":\"0\",\"low24h\":\"0\",\"volume24h\":\"0\",\"timestamp\":1699870440000";
    CHECK_MSG(std::string::npos != all.find("{" + quiet1 + "}"), "quiet market missing from TICKERS: " + all);
    CHECK(std::string::npos == all.find("\"marketId\":2,"));
    CHECK(std::string::npos != all.find("\"marketId\":3,"));
    CHECK(*rig.trades.tickerStats(1).text == "{\"type\":\"TICKER_STATS\"," + quiet1 + "}");
    CHECK(*rig.trades.tickerStats(2).text ==
        "{\"type\":\"TICKER_STATS\",\"marketId\":2,\"lastPrice\":\"0\",\"priceChange\":\"0\",\"priceChangePercent\":\"0\","
        "\"high24h\":\"0\",\"low24h\":\"0\",\"volume24h\":\"0\",\"timestamp\":0}");
    // the exact scales: a quote volume of 10^-16 units and a percent of 10^-8 percent, cut to 8 digits toward zero
    CHECK("15000.00000001" == formatScaled(static_cast<Int128>(1'500'000'000'001LL) * 100'000'000LL, 16));
    CHECK("0.00000001" == formatScaled(199'999'999LL, 16));
    CHECK("0" == formatScaled(99'999'999LL, 16));
    CHECK("-1.5" == formatScaled(-150'000'000LL, 8));
    CHECK("12345678901234567891.23456789" ==
        formatScaled(static_cast<Int128>(1234567890123456789LL) * 10'000'000'000LL + 1234567899LL, 9));
    Int128 big = 0;
    parseDecimal("500000000000000000000000000000000", big); // 5e32 units of 10^-16
    CHECK("50000000000000000" == formatScaled(big, 16));
    // a day later market 3's window is empty: it turns quiet (its newest price, the 24 h values 0) and stays in TICKERS;
    // the change is sent once
    auto c = rig.open(1);
    rig.cycle(1);
    const std::size_t before = c->frames.size();
    rig.wallMs += 25 * 3'600'000LL;
    rig.cycle(1000);
    const std::string quiet3 = "{\"marketId\":3,\"lastPrice\":\"15000.5\",\"priceChange\":\"0\",\"priceChangePercent\":"
        "\"0\",\"high24h\":\"0\",\"low24h\":\"0\",\"volume24h\":\"0\",\"timestamp\":1700000130000}";
    CHECK_MSG(c->frames.size() == before + 1 &&
            std::string::npos != c->frames.back().text.find("\"tickers\":[{" + quiet1 + "}," + quiet3 + "]"),
        "quiet markets in TICKERS: " + (c->frames.empty() ? std::string() : c->frames.back().text));
    rig.cycle(1000);
    CHECK(c->frames.size() == before + 1);
    // a live trade makes the market active again
    rig.live(trade(105, 3, 1'501'000'000'000, 10'000'000, true, rig.wallMs));
    rig.cycle(25);
    CHECK(std::string::npos != rig.trades.tickerStats(3).text->find("\"volume24h\":\"1501\""));
}

// ---------------------------------------------------------------- no party field on the public surface

TEST(md_live_no_party_fields)
{
    // a journal record 1 whose four order ids and two user ids carry marker values, taken as the follower takes it
    std::vector<std::uint8_t> b(117, 0);
    const std::uint16_t header[4] = { 109, 1, 4, 1 };
    std::memcpy(b.data(), header, 8);
    auto put = [&](std::size_t at, std::int64_t v) { std::memcpy(b.data() + 8 + at, &v, 8); };
    const std::int32_t market = 3;
    put(0, 77);
    put(8, 7);
    put(16, 64 * 77);
    put(24, 4242);
    std::memcpy(b.data() + 8 + 32, &market, 4);
    put(36, 913'371'001);
    put(44, 913'371'002);
    put(52, 913'371'003);
    put(60, 913'371'004);
    put(68, 1'500'000'000'000);
    put(76, 20'000'000);
    b[8 + 84] = 1;
    put(85, T0 + 100'000);
    put(93, 913'371'005);
    put(101, 913'371'006);
    TradeRow row;
    CHECK(Taken::TRADE == takeRecord(b.data(), b.size(), 1, { 1, 2, 3 }, row));
    LiveRig rig;
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    std::vector<TradeRow> history = seedTrades();
    history.push_back(row);
    rig.start(history);
    auto c = rig.open(1);
    rig.core.subscribe(1, 3, rig.now);
    TradeRow live = row;
    live.tradeId = 4243;
    live.journalSeq = 79;
    rig.live(live);
    rig.cycle(25);
    rig.cycle(1000);
    std::string all;
    for (const Seen &f : c->frames)
    {
        all += f.text;
    }
    CHECK(!c->frames.empty() && std::string::npos != all.find("\"tradeId\":\"4242\""));
    CHECK_MSG(std::string::npos == all.find("913371"), "user field on the public surface: takerUserId");
}

// ---------------------------------------------------------------- GET /api/candles

namespace {

RestAnswer call(CandleRoute &route, const std::string &method, const std::string &target,
    std::optional<std::string> origin = std::nullopt)
{
    std::promise<RestAnswer> p;
    auto f = p.get_future();
    route.handle(method, target, origin, [&p](RestAnswer a) { p.set_value(std::move(a)); });
    if (std::future_status::ready != f.wait_for(std::chrono::seconds(5)))
    {
        return RestAnswer{ 0, "no answer", {} };
    }
    return f.get();
}

std::string header(const RestAnswer &a, const std::string &name)
{
    for (const auto &h : a.headers)
    {
        if (h.first == name)
        {
            return h.second;
        }
    }
    return "";
}

std::string detailOf(const RestAnswer &a)
{
    const json::object o = json::parse(a.body).as_object();
    return o.at("detail").is_string() ? std::string(o.at("detail").as_string().c_str()) : "";
}

}

TEST(md_candles_route_parameters)
{
    CandleRouteSettings s;
    s.expectedMarkets = { 1, 2, 3 };
    s.limitMax = 500;
    CandleQuery q;
    RestAnswer bad;
    CHECK(parseCandleQuery("marketId=3&interval=5m&limit=200", s.expectedMarkets, 500, q, bad));
    CHECK(3 == q.marketId && 300 == q.intervalS && 200 == q.limit && "5m" == q.interval);
    CHECK(parseCandleQuery("limit=9000&interval=1d&marketId=1", s.expectedMarkets, 500, q, bad) && 500 == q.limit &&
        86'400 == q.intervalS);
    // every missing or unknown value is 400 naming the field: no silent default
    const std::vector<std::pair<std::string, std::string>> cases = { { "", "marketId" },
        { "interval=1m&limit=5", "marketId" }, { "marketId=&interval=1m&limit=5", "marketId" },
        { "marketId=4&interval=1m&limit=5", "marketId" }, { "marketId=x&interval=1m&limit=5", "marketId" },
        { "marketId=1&limit=5", "interval" }, { "marketId=1&interval=2m&limit=5", "interval" },
        { "marketId=1&interval=1m", "limit" }, { "marketId=1&interval=1m&limit=0", "limit" },
        { "marketId=1&interval=1m&limit=-3", "limit" }, { "marketId=1&interval=1m&limit=5&symbol=BTC", "symbol" },
        { "marketId=1&marketId=2&interval=1m&limit=5", "marketId" } };
    for (const auto &c : cases)
    {
        bad = RestAnswer{};
        const bool ok = parseCandleQuery(c.first, s.expectedMarkets, 500, q, bad);
        CHECK_MSG(!ok && 400 == bad.status && c.second == detailOf(bad), "'" + c.first + "' -> " + bad.body);
    }
}

TEST(md_candles_route_body)
{
    CandleRouteSettings s;
    s.expectedMarkets = { 1, 2, 3 };
    s.limitMax = 2;
    CandleQuery asked;
    CandleRoute route(s, [&asked](const CandleQuery &q, std::vector<CandleRow> &out, std::string &) {
        asked = q;
        for (const auto &kv : aggregate(seedTrades()))
        {
            if (3 == kv.second.marketId && 300 == kv.second.intervalS)
            {
                out.push_back(kv.second);
            }
        }
        return true;
    });
    route.start();
    const RestAnswer a = call(route, "GET", "/api/candles?marketId=3&interval=5m&limit=200");
    CHECK(200 == a.status && 2 == asked.limit && 3 == asked.marketId);
    goldenLive("candles_body_5m", a.body);
    const json::object o = json::parse(a.body).as_object();
    CHECK(o.at("marketId").is_int64() && 3 == o.at("marketId").as_int64() && "5m" == o.at("interval").as_string());
    CHECK(!CandleRoute::owns("/api/candlesx") && CandleRoute::owns("/api/candles?x=1") && CandleRoute::owns("/api/candles"));
    const RestAnswer post = call(route, "POST", "/api/candles?marketId=3&interval=5m&limit=200");
    CHECK(405 == post.status && "GET, OPTIONS" == header(post, "Allow"));
    route.stop(2000);
}

TEST(md_candles_route_history_down)
{
    CandleRouteSettings s;
    s.expectedMarkets = { 1, 2, 3 };
    CandleRoute route(s, [](const CandleQuery &, std::vector<CandleRow> &, std::string &error) {
        error = "connection refused";
        return false;
    });
    route.start();
    const RestAnswer a = call(route, "GET", "/api/candles?marketId=3&interval=1h&limit=10");
    const bool refused = 503 == a.status && std::string::npos != a.body.find("\"code\":\"HISTORY_UNAVAILABLE\"");
    CHECK_MSG(refused, "REST candles answered without the history: " + std::to_string(a.status) + " " + a.body);
    CHECK(!refused || !header(a, "Retry-After").empty());
    const auto counts = route.requests();
    const auto it = counts.find({ "candles", a.status });
    CHECK(counts.end() != it && 1 == it->second);
    route.stop(2000);
}

TEST(md_candles_route_cors)
{
    CandleRouteSettings s;
    s.expectedMarkets = { 1, 2, 3 };
    s.corsOrigins = { "https://app.example.test" };
    CandleRoute route(s, [](const CandleQuery &, std::vector<CandleRow> &, std::string &) { return true; });
    route.start();
    RestAnswer a = call(route, "OPTIONS", "/api/candles", std::string("https://app.example.test"));
    CHECK_MSG(204 == a.status && "https://app.example.test" == header(a, "Access-Control-Allow-Origin") &&
            "GET" == header(a, "Access-Control-Allow-Methods"),
        "preflight refused for the UI origin");
    a = call(route, "OPTIONS", "/api/candles", std::string("https://other.example.test"));
    CHECK(403 == a.status && header(a, "Access-Control-Allow-Origin").empty());
    a = call(route, "GET", "/api/candles?marketId=1&interval=1m&limit=1", std::string("https://app.example.test"));
    CHECK(200 == a.status && "https://app.example.test" == header(a, "Access-Control-Allow-Origin") &&
        "Origin" == header(a, "Vary"));
    a = call(route, "GET", "/api/candles?marketId=1&interval=1m&limit=1", std::string("https://other.example.test"));
    CHECK(200 == a.status && header(a, "Access-Control-Allow-Origin").empty());
    a = call(route, "GET", "/api/candles?marketId=1&limit=1", std::string("https://app.example.test"));
    CHECK(400 == a.status && "https://app.example.test" == header(a, "Access-Control-Allow-Origin"));
    route.stop(2000);
    // MD_CORS_ORIGINS=none: every preflight is refused
    CandleRouteSettings none = s;
    none.corsOrigins.clear();
    CandleRoute closed(none, [](const CandleQuery &, std::vector<CandleRow> &, std::string &) { return true; });
    closed.start();
    a = call(closed, "OPTIONS", "/api/candles", std::string("https://app.example.test"));
    CHECK(403 == a.status);
    closed.stop(2000);
}
