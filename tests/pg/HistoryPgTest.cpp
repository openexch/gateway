// The history read layer against a real PostgreSQL (tests/pg/pg_it.py starts a throwaway container and removes it),
// seeded from the read contract's reference DDL (sql/history_read_contract_v1.sql):
//   - a keyset walk returns every row exactly once while rows are inserted ahead of and behind the cursor (also at the
//     cursor's own time, one id below and one above it) and at random (control: GW_NC_OFFSET_PAGING pages by OFFSET
//     and must fail with "row skipped or repeated");
//   - over HTTP (the real GatewayApi, demo login on the users store, a model core): another user's rows never appear,
//     the closed-order lookup, pool exhaustion answers 503 HISTORY_BUSY while login and order entry still answer;
//   - an absent relation, a projection that never reported, an unreachable server and a statement timeout answer 503
//     HISTORY_UNAVAILABLE, each cause in the metrics, one public body; the next read reconnects;
//   - a closed-order lookup above the projection's applied_order_id is 503 (not projected yet), never 404;
//   - one deadline per read, queue wait included; a connection the server dropped is retried once.
#include <atomic>
#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <thread>

#include <boost/json.hpp>

#include "Check.h"
#include "HttpClient.h"
#include "PgTestSupport.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/HttpServer.h"
#include "gateway/common/Password.h"
#include "gateway/common/PgHistoryStore.h"
#include "gateway/common/PgUserStore.h"
#include "GatewayApi.h"

using namespace openexchange::gateway;
using gwtest::freshDatabase;
using gwtest::httpCall;
using gwtest::HttpResult;
using gwtest::sql;
namespace json = boost::json;

namespace {

constexpr std::int64_t T0 = 1'800'000'000'000; // the seeded rows' times start here
constexpr std::int64_t SEEDED = 400;            // orders (and fills) of the walking user

std::string contractDdl()
{
    std::ifstream in(GW_HISTORY_CONTRACT_SQL);
    std::stringstream s;
    s << in.rdbuf();
    if (s.str().empty())
    {
        gwtest::fail(__FILE__, __LINE__, std::string("cannot read ") + GW_HISTORY_CONTRACT_SQL);
    }
    return s.str();
}

// A fresh database with the contract's relations and a heartbeat.
std::string historyDatabase(const std::string &name, bool heartbeat = true)
{
    const std::string url = freshDatabase(name);
    sql(url, contractDdl());
    if (heartbeat)
    {
        sql(url, "INSERT INTO gateway_history_heartbeat_v1 (beat_at, applied_order_id) VALUES (now() - interval "
            "'1500 milliseconds', 1000000000000)");
    }
    return url;
}

// Orders from, from + step, ... (count of them) of `user`; updated_at_ms = `time` (an SQL expression of g, the order id).
std::string insertOrders(std::int64_t user, std::int64_t from, std::int64_t count, const std::string &time,
    std::int64_t step = 1)
{
    return "INSERT INTO gateway_order_history_v1 (order_id, user_id, market_id, side, type, time_in_force, price, "
        "quantity, filled_quantity, remaining_quantity, status, hold_asset_id, hold_amount, created_at_ms, updated_at_ms) "
        "SELECT g, " + std::to_string(user) + ", 1 + g % 2, 'BUY', 'LIMIT', 'GTC', 10000000000, 100000000, 100000000, "
        "0, 'FILLED', 2, 10000000000, " + time + ", " + time + " FROM generate_series(" + std::to_string(from) + ", " +
        std::to_string(from + (count - 1) * step) + ", " + std::to_string(step) + ") g";
}

// Fills: row g is trade g / 2 (two rows of one trade: a self-trade), order g; executed_at_ms = `time`.
std::string insertFills(std::int64_t user, std::int64_t from, std::int64_t count, const std::string &time,
    std::int64_t step = 1)
{
    return "INSERT INTO gateway_fills_v1 (trade_id, order_id, user_id, market_id, side, price, quantity, maker, "
        "executed_at_ms) SELECT g / 2 + 1, g, " + std::to_string(user) + ", 1, CASE WHEN g % 2 = 0 THEN 'BUY' ELSE "
        "'SELL' END, 10000000000, 50000000, g % 2 = 0, " + time + " FROM generate_series(" + std::to_string(from) + ", " +
        std::to_string(from + (count - 1) * step) + ", " + std::to_string(step) + ") g";
}

// One row of `user` at an exact key, unless that key is taken; true when it was inserted. Orders: (a = updated_at_ms,
// b = order_id); fills: (a = executed_at_ms, b = trade_id, c = order_id).
bool insertAt(const std::string &url, history::Kind kind, std::int64_t user, const history::Key &k)
{
    const std::string inserted = history::Kind::ORDERS == kind
        ? sql(url, insertOrders(user, k.b, 1, std::to_string(k.a)) + " ON CONFLICT DO NOTHING RETURNING order_id")
        : sql(url, "INSERT INTO gateway_fills_v1 (trade_id, order_id, user_id, market_id, side, price, quantity, maker, "
              "executed_at_ms) VALUES (" + std::to_string(k.b) + ", " + std::to_string(k.c) + ", " + std::to_string(user) +
              ", 1, 'BUY', 10000000000, 50000000, false, " + std::to_string(k.a) + ") ON CONFLICT DO NOTHING RETURNING "
              "trade_id");
    return !inserted.empty();
}

// The key of one page item (what the cursor after it carries).
history::Key keyOf(const std::string &item, history::Kind kind)
{
    const json::object o = json::parse(item).as_object();
    if (history::Kind::ORDERS == kind)
    {
        return history::Key{ o.at("updatedAt").as_int64(), std::stoll(std::string(o.at("orderId").as_string())), 0 };
    }
    return history::Key{ o.at("executedAt").as_int64(), std::stoll(std::string(o.at("tradeId").as_string())),
        std::stoll(std::string(o.at("orderId").as_string())) };
}

// A deadline far enough for any one read of these tests.
std::int64_t soon()
{
    return pg::nowNs() + 10'000'000'000LL;
}

PgHistorySettings historySettings(const std::string &url, int workers = 1, std::int64_t timeoutMs = 2000)
{
    PgHistorySettings s;
    s.url = url;
    s.workers = workers;
    s.statementTimeoutMs = timeoutMs;
    s.retryMs = 300;
    return s;
}

// The id a page item names: orderId (orders) or tradeId:orderId (fills).
std::string idOf(const std::string &item, history::Kind kind)
{
    const json::object o = json::parse(item).as_object();
    return history::Kind::ORDERS == kind ? std::string(o.at("orderId").as_string())
        : std::string(o.at("tradeId").as_string()) + ":" + std::string(o.at("orderId").as_string());
}

// Walks every page of `user` while a writer inserts rows: two ahead of the cursor and one behind it before each next
// page, one at the cursor's own time just behind it (b - 1) and one just ahead (b + 1), plus rows at random positions
// from another thread the whole time. Every seeded row and every row inserted behind must come exactly once, no row
// ahead and no row of user 8 at all, a random row at most once.
void walk(const std::string &url, history::Kind kind)
{
    const bool orders = history::Kind::ORDERS == kind;
    auto insert = orders ? insertOrders : insertFills;
    // seeded order ids are 1, 5, 9, ...: the ids next to a cursor's are free for the rows at its own time
    const std::int64_t step = orders ? 4 : 1;
    std::set<std::string> seeded;
    std::set<std::string> behind;
    std::set<std::string> ahead;
    std::set<std::string> edgeBehind;
    std::set<std::string> edgeAhead;
    auto idOfKey = [&](const history::Key &k) {
        return orders ? std::to_string(k.b) : std::to_string(k.b) + ":" + std::to_string(k.c);
    };
    auto idsOf = [&](std::int64_t from, std::int64_t count, std::set<std::string> &into, std::int64_t by = 1) {
        for (std::int64_t i = 0, g = from; i < count; ++i, g += by)
        {
            into.insert(orders ? std::to_string(g) : std::to_string(g / 2 + 1) + ":" + std::to_string(g));
        }
    };
    // ties on purpose: three orders share each updated_at_ms, four fills each executed_at_ms (the rest of the key
    // breaks them)
    sql(url, insert(7, 1, SEEDED, orders ? std::to_string(T0) + " + g / 12" : std::to_string(T0) + " + g / 4", step));
    idsOf(1, SEEDED, seeded, step);
    sql(url, insert(8, 100'001, 100, std::to_string(T0) + " + g % 50", 1)); // another user, interleaved in time

    PgHistoryStore store(historySettings(url));
    std::atomic<bool> walking{ true };
    std::atomic<std::int64_t> randoms{ 0 };
    std::thread writer([&] {
        std::mt19937_64 rng(42);
        std::int64_t next = 500'000;
        while (walking.load())
        {
            const std::int64_t at = T0 + static_cast<std::int64_t>(rng() % (SEEDED / 3));
            // a row at the cursor's own time may already hold this key: the random row is then not written
            sql(url, insert(7, next, 2, std::to_string(at), 1) + " ON CONFLICT DO NOTHING");
            next += 2;
            randoms += 2;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    });
    std::map<std::string, int> seen;
    history::PageRequest request;
    request.kind = kind;
    request.userId = 7;
    request.limit = 37;
    std::int64_t aheadId = 1'000'000;
    std::int64_t behindId = 2'000'000;
    int pages = 0;
    for (; pages < 1000; ++pages)
    {
        HistoryPage page;
        const HistoryAnswer a = store.page(0, request, page, soon());
        if (HistoryStatus::OK != a.status)
        {
            gwtest::fail(__FILE__, __LINE__, std::string("page failed: ") + nameOf(a.cause) + " " + store.state());
            break;
        }
        for (const std::string &item : page.items)
        {
            ++seen[idOf(item, kind)];
        }
        if (!page.nextCursor)
        {
            break;
        }
        // newer than every row (ahead of the cursor) and older than every row (behind it)
        sql(url, insert(7, aheadId, 2, std::to_string(T0 + 10'000'000) + " + g", 1));
        idsOf(aheadId, 2, ahead);
        aheadId += 2;
        sql(url, insert(7, behindId, 1, std::to_string(T0 - 10'000'000) + " - g", 1));
        idsOf(behindId, 1, behind);
        behindId += 2; // fills: one row per id pair, so ids never share a trade
        // the equal-time edge of the row comparison: the cursor's own time, one id below (behind) and one above (ahead)
        const history::Key last = keyOf(page.items.back(), kind);
        history::Key below = last;
        history::Key above = last;
        below.b -= 1;
        above.b += 1;
        if (below.b > 0 && insertAt(url, kind, 7, below))
        {
            edgeBehind.insert(idOfKey(below));
        }
        if (insertAt(url, kind, 7, above))
        {
            edgeAhead.insert(idOfKey(above));
        }
        history::Key key;
        if (history::CursorError::NONE != history::decodeCursor(*page.nextCursor, kind, request.filter, key))
        {
            gwtest::fail(__FILE__, __LINE__, "nextCursor does not decode");
            break;
        }
        request.after = key;
    }
    walking = false;
    writer.join();
    CHECK_MSG(pages > 10, "the walk took " + std::to_string(pages) + " pages");
    int problems = 0;
    auto expect = [&](const std::set<std::string> &ids, int times, const char *what) {
        for (const std::string &id : ids)
        {
            const int n = seen.count(id) ? seen[id] : 0;
            if (n != times && problems++ < 5)
            {
                gwtest::fail(__FILE__, __LINE__, std::string("row skipped or repeated: ") + what + " row " + id +
                    " seen " + std::to_string(n) + " times, expected " + std::to_string(times));
            }
        }
    };
    expect(seeded, 1, orders ? "seeded order" : "seeded fill");
    expect(behind, 1, "behind-the-cursor");
    expect(ahead, 0, "ahead-of-the-cursor");
    expect(edgeBehind, 1, "same-time-behind-the-cursor");
    expect(edgeAhead, 0, "same-time-ahead-of-the-cursor");
    CHECK_MSG(edgeBehind.size() >= 5 && edgeAhead.size() >= 5, "same-time rows written: " +
        std::to_string(edgeBehind.size()) + " behind, " + std::to_string(edgeAhead.size()) + " ahead");
    for (const auto &[id, n] : seen)
    {
        if (n > 1 && problems++ < 5)
        {
            gwtest::fail(__FILE__, __LINE__, "row skipped or repeated: row " + id + " seen " + std::to_string(n) + " times");
        }
        const std::int64_t orderId = std::stoll(orders ? id : id.substr(id.find(':') + 1));
        if (orderId >= 100'001 && orderId < 100'101)
        {
            gwtest::fail(__FILE__, __LINE__, "another user's row " + id + " in the walk");
        }
    }
    CHECK_MSG(randoms.load() > 0, "the concurrent writer inserted nothing");
}

// ---- the HTTP rig: the real GatewayApi over the real users store and history store

struct HttpRig
{
    GatewaySettings g;
    std::unique_ptr<PgUserStore> users;
    std::unique_ptr<PgHistoryStore> history;
    std::unique_ptr<WorkPool> storePool;
    std::unique_ptr<WorkPool> hashPool;
    std::unique_ptr<WorkPool> historyPool;
    std::unique_ptr<WorkPool> corePool;
    std::unique_ptr<Authenticator> auth;
    std::unique_ptr<GatewayApi> api;
    std::unique_ptr<HttpServer> server;
    std::uint16_t port = 0;
    std::atomic<std::int64_t> nextOrderId{ 5'000'000 };

    HttpRig(const std::string &url, int historyWorkers, std::size_t historyQueue, std::int64_t timeoutMs)
    {
        g.common.authModes = auth_mode::DEMO;
        g.common.ioThreads = 4;
        g.pbkdf2Iterations = 1000;
        for (Rate *r : { &g.registerRate, &g.loginRate, &g.authSourceRate, &g.tokenLookupRate, &g.principalRate })
        {
            *r = Rate{ 1000, 1000, "1000/s" };
        }
        g.registerBurst = g.loginBurst = g.authSourceBurst = g.tokenLookupBurst = g.principalBurst = 1000;
        g.tokenLookupInflight = 32;
        g.pgStatementTimeoutMs = timeoutMs; // one history read's deadline, queue wait included
        PgSettings ps;
        ps.url = url;
        ps.workers = 2;
        ps.statementTimeoutMs = timeoutMs;
        users = std::make_unique<PgUserStore>(ps);
        history = std::make_unique<PgHistoryStore>(historySettings(url, historyWorkers, timeoutMs));
        storePool = std::make_unique<WorkPool>("t-pg-auth", 2, 64);
        hashPool = std::make_unique<WorkPool>("t-hash", 1, 32);
        historyPool = std::make_unique<WorkPool>("t-pg-hist", historyWorkers, historyQueue);
        corePool = std::make_unique<WorkPool>("t-core", 1, 1024);
        AuthenticatorSettings as;
        as.modes = g.common.authModes;
        as.unknownLookupInflight = g.tokenLookupInflight;
        auth = std::make_unique<Authenticator>(as, AuthMaterial{}, users.get(), storePool.get());
        ApiDeps d;
        d.settings = &g;
        d.auth = auth.get();
        d.store = users.get();
        d.storePool = storePool.get();
        d.hashPool = hashPool.get();
        d.history = history.get();
        d.historyPool = historyPool.get();
        // a model core that holds no open order: every GET /orders/{id} goes to history
        d.askCore = [this](session::Command c, std::function<void(const session::Outcome &)> done) {
            return corePool->trySubmit([this, c, done](int) {
                session::Outcome o;
                o.templateId = session::templateOf(c);
                o.offers = 1;
                if (const auto *x = std::get_if<wire::CreateOrder>(&c))
                {
                    wire::OrderAck ack;
                    ack.orderId = ++nextOrderId;
                    ack.userId = x->userId;
                    ack.marketId = x->marketId;
                    ack.status = wire::money::OrderStatus::PENDING_NEW;
                    o.reply = ack;
                }
                else if (std::holds_alternative<wire::RequestOrderSnapshot>(c))
                {
                    o.reply = session::SnapshotReply{};
                }
                else
                {
                    o.kind = session::OutcomeKind::INTERNAL;
                }
                done(o);
            });
        };
        d.config = [] { return std::make_shared<const session::CoreConfig>(); };
        d.registered = [] { return true; };
        api = std::make_unique<GatewayApi>(std::move(d));
        HttpServerSettings hs;
        hs.host = "127.0.0.1";
        hs.ioThreads = 4;
        server = std::make_unique<HttpServer>(hs, [this](std::shared_ptr<HttpExchange> ex) { api->handle(std::move(ex)); });
        std::string error;
        CHECK_MSG(server->bind(error), error);
        storePool->start();
        hashPool->start();
        historyPool->start();
        corePool->start();
        server->start();
        port = server->boundPort();
    }

    ~HttpRig()
    {
        server->stop(5000);
        historyPool->stop(5000);
        hashPool->stop(5000);
        storePool->stop(5000);
        corePool->stop(5000);
    }

    // a demo user written straight to the users store; its userId
    std::int64_t user(const std::string &name, const std::string &password)
    {
        std::int64_t id = 0;
        CHECK(StoreStatus::OK == users->reserveUserId(0, name, id));
        CHECK(StoreStatus::OK == users->insertUser(0, id, name, password::hash(password, 1000),
            std::string(crypto::view(crypto::sha256("seed-token-" + name)))));
        return id;
    }

    HttpResult login(const std::string &name, const std::string &password)
    {
        return httpCall(port, "POST", "/api/v2/auth/login", R"({"username":")" + name + R"(","password":")" + password +
            R"("})");
    }

    std::vector<std::pair<std::string, std::string>> bearer(const std::string &name, const std::string &password)
    {
        const HttpResult r = login(name, password);
        std::string token;
        if (200 == r.status)
        {
            token = std::string(json::parse(r.body).as_object().at("token").as_string());
        }
        CHECK_MSG(!token.empty(), "login: " + std::to_string(r.status) + " " + r.body);
        return { { "Authorization", "Bearer " + token } };
    }

    HttpResult get(const std::string &target, const std::vector<std::pair<std::string, std::string>> &h)
    {
        return httpCall(port, "GET", target, "", h);
    }
};

std::string what(const HttpResult &r)
{
    return std::to_string(r.status) + " " + r.body + r.error;
}

std::string codeOf(const HttpResult &r)
{
    boost::system::error_code ec;
    const json::value v = json::parse(r.body, ec);
    return !ec && v.is_object() && v.as_object().contains("code") ? std::string(v.as_object().at("code").as_string()) : "";
}

}

TEST(history_keyset_walk_under_concurrent_inserts)
{
    walk(historyDatabase("gw_hist_walk_orders"), history::Kind::ORDERS);
    walk(historyDatabase("gw_hist_walk_fills"), history::Kind::FILLS);
}

TEST(history_routes_over_postgres)
{
    const std::string url = historyDatabase("gw_hist_http");
    HttpRig rig(url, 2, 64, 2000);
    const std::int64_t alice = rig.user("alice", "secret-alice");
    const std::int64_t bob = rig.user("bob", "secret-bob");
    sql(url, insertOrders(alice, 1, 120, std::to_string(T0) + " + g"));
    sql(url, insertOrders(bob, 1001, 50, std::to_string(T0) + " + g"));
    sql(url, insertFills(alice, 1, 30, std::to_string(T0) + " + g"));
    sql(url, insertFills(bob, 1001, 30, std::to_string(T0) + " + g"));
    sql(url, "INSERT INTO gateway_order_history_v1 VALUES (9001, " + std::to_string(alice) + ", "
        "'11111111-2222-4333-8444-555555555555', 3, 'SELL', 'STOP_LIMIT', 'GTD', 2500000000, 2600000000, 200000000, "
        "50000000, 150000000, 'EXPIRED', 'INSUFFICIENT_FUNDS', 12, 777, 1, 200000000, 0, 1800000000001, 1800000000002)");
    const auto a = rig.bearer("alice", "secret-alice");

    // every page through HTTP: alice's rows once each, never bob's
    std::map<std::string, int> seen;
    std::string target = "/api/v2/orders/history?limit=25";
    int pages = 0;
    for (; pages < 20; ++pages)
    {
        const HttpResult r = rig.get(target, a);
        if (200 != r.status)
        {
            CHECK_MSG(false, what(r));
            break;
        }
        const json::object b = json::parse(r.body).as_object();
        CHECK(b.at("asOf").as_object().at("ageMs").as_int64() >= 1500);
        for (const json::value &item : b.at("items").as_array())
        {
            ++seen[std::string(item.as_object().at("orderId").as_string())];
        }
        if (b.at("nextCursor").is_null())
        {
            break;
        }
        target = "/api/v2/orders/history?limit=25&cursor=" + std::string(b.at("nextCursor").as_string());
    }
    CHECK_MSG(121 == seen.size() && 4 == pages, std::to_string(seen.size()) + " rows in " + std::to_string(pages + 1) +
        " pages"); // 120 seeded and 9001: four full pages of 25 and one of 21
    for (const auto &[id, n] : seen)
    {
        CHECK_MSG(1 == n, "order " + id + " seen " + std::to_string(n) + " times");
        CHECK_MSG(std::stoll(id) < 1001 || std::stoll(id) > 1050, "bob's order " + id + " shown to alice");
    }
    // filters and the fills route; a cursor reused with another filter is refused
    HttpResult r = rig.get("/api/v2/orders/history?status=EXPIRED&marketId=3", a);
    CHECK_MSG(200 == r.status && 1 == json::parse(r.body).as_object().at("items").as_array().size(), what(r));
    r = rig.get("/api/v2/fills?limit=1000", a);
    {
        const json::array items = json::parse(r.body).as_object().at("items").as_array();
        CHECK_MSG(200 == r.status && 30 == items.size(), what(r));
        for (const json::value &item : items)
        {
            CHECK(std::stoll(std::string(item.as_object().at("orderId").as_string())) <= 30);
        }
    }
    r = rig.get("/api/v2/fills?limit=2&orderId=5", a);
    CHECK_MSG(200 == r.status && r.body.find(R"("orderId":"5")") != std::string::npos &&
            json::parse(r.body).as_object().at("nextCursor").is_null(), what(r));
    r = rig.get("/api/v2/orders/history?limit=2", a);
    const std::string cursor = std::string(json::parse(r.body).as_object().at("nextCursor").as_string());
    r = rig.get("/api/v2/orders/history?limit=2&marketId=1&cursor=" + cursor, a);
    CHECK_MSG(400 == r.status && "VALIDATION" == codeOf(r), what(r));

    // the closed-order lookup: the caller's row, every field; another user's order and an unknown id are one 404
    r = rig.get("/api/v2/orders/9001", a);
    CHECK_MSG(200 == r.status && r.body == R"({"orderId":"9001","clientOrderId":"11111111-2222-4333-8444-555555555555",)"
        R"("marketId":"3","side":"SELL","type":"STOP_LIMIT","timeInForce":"GTD","price":"25","stopPrice":"26",)"
        R"("quantity":"2","filledQuantity":"0.5","remainingQuantity":"1.5","status":"EXPIRED",)"
        R"("reason":"INSUFFICIENT_FUNDS","meReason":12,"meOrderId":"777","hold":{"assetId":"1","amount":"2",)"
        R"("remaining":"0"},"createdAt":1800000000001,"updatedAt":1800000000002})", what(r));
    const HttpResult foreign = rig.get("/api/v2/orders/1001", a);
    const HttpResult unknown = rig.get("/api/v2/orders/424242", a);
    CHECK_MSG(404 == unknown.status && "NOT_FOUND" == codeOf(unknown), what(unknown));
    CHECK_MSG(foreign.status == unknown.status && foreign.body == unknown.body, what(foreign) + " vs " + what(unknown));
    const auto b = rig.bearer("bob", "secret-bob");
    r = rig.get("/api/v2/orders/1001", b);
    CHECK_MSG(200 == r.status, what(r));
    (void)bob;
}

TEST(history_pool_exhaustion_leaves_login_and_orders)
{
    const std::string url = historyDatabase("gw_hist_busy");
    // one history connection, a queue of one, a statement timeout well above the time the test holds the lock
    HttpRig rig(url, 1, 1, 6000);
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 1, 10, std::to_string(T0) + " + g"));
    const auto a = rig.bearer("alice", "secret-alice");
    CHECK(200 == rig.get("/api/v2/orders/history", a).status); // the history connection is open

    // a writer holds the relation: every history read waits on it
    PGconn *locker = PQconnectdb(url.c_str());
    PGresult *lr = PQexec(locker, "BEGIN; LOCK TABLE gateway_order_history_v1 IN ACCESS EXCLUSIVE MODE");
    CHECK_MSG(PGRES_COMMAND_OK == PQresultStatus(lr), PQresultErrorMessage(lr));
    PQclear(lr);
    std::vector<HttpResult> held(2);
    std::vector<std::thread> readers;
    readers.emplace_back([&] { held[0] = rig.get("/api/v2/orders/history", a); });
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (sql(url, "SELECT count(*) FROM pg_locks WHERE NOT granted") != "1" && std::chrono::steady_clock::now() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    readers.emplace_back([&] { held[1] = rig.get("/api/v2/orders/history", a); });
    while (rig.historyPool->queued() != 1 && std::chrono::steady_clock::now() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK_MSG(1 == rig.historyPool->queued(), "the second read is not queued");

    const HttpResult busy = rig.get("/api/v2/fills", a);
    CHECK_MSG(503 == busy.status && "HISTORY_BUSY" == codeOf(busy) && busy.headers.count("retry-after"), what(busy));
    const HttpResult busyOrder = rig.get("/api/v2/orders/77", a);
    CHECK_MSG(503 == busyOrder.status && "HISTORY_BUSY" == codeOf(busyOrder), what(busyOrder));
    // login and token lookups have connections of their own; order entry never touches PostgreSQL
    const HttpResult login = rig.login("alice", "secret-alice");
    CHECK_MSG(200 == login.status && login.ms < 2000, "login during a history storm: " + what(login) + " in " +
        std::to_string(login.ms) + " ms");
    const std::string token = std::string(json::parse(login.body).as_object().at("token").as_string());
    const HttpResult order = httpCall(rig.port, "POST", "/api/v2/orders",
        R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"100","quantity":"0.5"})",
        { { "Authorization", "Bearer " + token }, { "Idempotency-Key", "11111111-2222-4333-8444-555555555555" } });
    CHECK_MSG(201 == order.status && order.ms < 2000, "order during a history storm: " + what(order));
    const std::string m = rig.api->metrics("gw_");
    CHECK_MSG(m.find("gw_pg_history_refused_total 2") != std::string::npos, m);

    lr = PQexec(locker, "COMMIT");
    PQclear(lr);
    PQfinish(locker);
    for (std::thread &t : readers)
    {
        t.join();
    }
    CHECK_MSG(200 == held[0].status && 200 == held[1].status, what(held[0]) + " / " + what(held[1]));
}

TEST(history_unavailable_causes)
{
    // the relations absent (a database the projection never touched): 503, cause absent
    {
        const std::string url = freshDatabase("gw_hist_absent");
        HttpRig rig(url, 1, 8, 2000);
        rig.user("alice", "secret-alice");
        const auto a = rig.bearer("alice", "secret-alice");
        HttpResult r = rig.get("/api/v2/orders/history", a);
        CHECK_MSG(503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r) && r.headers["retry-after"] == "60", what(r));
        const HttpResult o = rig.get("/api/v2/orders/5", a);
        CHECK_MSG(503 == o.status && "HISTORY_UNAVAILABLE" == codeOf(o) && o.body == r.body, what(o));
        const std::string m = rig.api->metrics("gw_");
        CHECK_MSG(m.find("history_reads_total{query=\"orders\",result=\"unavailable\",cause=\"absent\"} 1") !=
            std::string::npos, m);
        // the projection creates its relations later: the next read answers, no restart
        sql(url, contractDdl());
        sql(url, "INSERT INTO gateway_history_heartbeat_v1 (beat_at, applied_order_id) VALUES (now(), 0)");
        CHECK(200 == rig.get("/api/v2/orders/history", a).status);
    }
    // relations present, the heartbeat empty: the projection never reported
    {
        PgHistoryStore store(historySettings(historyDatabase("gw_hist_nobeat", false)));
        HistoryPage page;
        history::PageRequest req;
        req.userId = 7;
        req.limit = 10;
        HistoryAnswer x = store.page(0, req, page, soon());
        CHECK_MSG(HistoryStatus::UNAVAILABLE == x.status && HistoryCause::NO_HEARTBEAT == x.cause, nameOf(x.cause));
        std::string json;
        x = store.order(0, 7, 5, json, soon()); // never 404 from a projection that never reported
        CHECK_MSG(HistoryStatus::UNAVAILABLE == x.status && HistoryCause::NO_HEARTBEAT == x.cause, nameOf(x.cause));
    }
    // nothing listens: unreachable, bounded, then fail-fast inside the retry window
    {
        PgHistoryStore store(historySettings("postgresql://gw@127.0.0.1:1/gw"));
        HistoryPage page;
        history::PageRequest req;
        req.userId = 7;
        req.limit = 10;
        HistoryAnswer x = store.page(0, req, page, soon());
        CHECK_MSG(HistoryStatus::UNAVAILABLE == x.status && HistoryCause::UNREACHABLE == x.cause, nameOf(x.cause));
        x = store.page(0, req, page, soon());
        CHECK(HistoryCause::UNREACHABLE == x.cause);
    }
    // the connection is read-only: the gateway cannot write the projection even by mistake
    {
        const std::string url = historyDatabase("gw_hist_readonly");
        PgConn c;
        std::string error;
        CHECK_MSG(c.connect(url, 2000, error, { "SET default_transaction_read_only = on" }), error);
        pg::Result r;
        CHECK(pg::Exec::FAILED == c.exec("DELETE FROM gateway_order_history_v1", {}, 2000, r, error));
        CHECK_MSG("25006" == c.lastSqlState(), c.lastSqlState() + " " + error); // read_only_sql_transaction
    }
}

TEST(history_order_lookup_waits_for_the_projection)
{
    const std::string url = historyDatabase("gw_hist_lag");
    HttpRig rig(url, 1, 8, 2000);
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 9001, 1, std::to_string(T0)));
    sql(url, "UPDATE gateway_history_heartbeat_v1 SET applied_order_id = 9001");
    const auto a = rig.bearer("alice", "secret-alice");
    HttpResult r = rig.get("/api/v2/orders/9001", a);
    CHECK_MSG(200 == r.status, what(r));
    r = rig.get("/api/v2/orders/9000", a); // at or below applied_order_id and no row of the caller: no such order
    CHECK_MSG(404 == r.status && "NOT_FOUND" == codeOf(r), what(r));
    // above applied_order_id and no row: the order may exist and not be projected yet (an IOC that filled and left the
    // core before the projection reached it): 503 with a short Retry-After, never 404
    r = rig.get("/api/v2/orders/9002", a);
    CHECK_MSG(503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r) && r.headers["retry-after"] == "1", what(r));
    const std::string m = rig.api->metrics("gw_");
    CHECK_MSG(m.find("history_reads_total{query=\"order\",result=\"unavailable\",cause=\"lagging\"} 1") !=
        std::string::npos, m);
    // the projection reaches it: the same read answers
    sql(url, insertOrders(alice, 9002, 1, std::to_string(T0 + 1)));
    r = rig.get("/api/v2/orders/9002", a);
    CHECK_MSG(200 == r.status, what(r));
    sql(url, "UPDATE gateway_history_heartbeat_v1 SET applied_order_id = 9010");
    r = rig.get("/api/v2/orders/9005", a);
    CHECK_MSG(404 == r.status && "NOT_FOUND" == codeOf(r), what(r));
}

TEST(history_statement_timeout_then_reconnect)
{
    const std::string url = historyDatabase("gw_hist_timeout");
    HttpRig rig(url, 1, 8, 300); // one history connection, a 300 ms statement timeout
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 1, 10, std::to_string(T0) + " + g"));
    const auto a = rig.bearer("alice", "secret-alice");
    CHECK(200 == rig.get("/api/v2/orders/history", a).status); // the history connection is open

    PGconn *locker = PQconnectdb(url.c_str());
    PGresult *lr = PQexec(locker, "BEGIN; LOCK TABLE gateway_order_history_v1 IN ACCESS EXCLUSIVE MODE");
    CHECK_MSG(PGRES_COMMAND_OK == PQresultStatus(lr), PQresultErrorMessage(lr));
    PQclear(lr);
    HttpResult r = rig.get("/api/v2/orders/history", a);
    CHECK_MSG(503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r) && r.headers["retry-after"] == "60", what(r));
    CHECK_MSG(r.ms < 1500, "a 300 ms statement timeout answered in " + std::to_string(r.ms) + " ms");
    const std::string m = rig.api->metrics("gw_");
    CHECK_MSG(m.find("history_reads_total{query=\"orders\",result=\"unavailable\",cause=\"timeout\"} 1") !=
        std::string::npos, m);
    std::this_thread::sleep_for(std::chrono::milliseconds(400)); // the lock outlives the timeout
    lr = PQexec(locker, "COMMIT");
    PQclear(lr);
    PQfinish(locker);
    // the same (only) worker answers the next read: its connection is reopened when the deadline closed it
    const HttpResult after = rig.get("/api/v2/orders/history", a);
    CHECK_MSG(200 == after.status, what(after));
}

TEST(history_one_deadline_per_read_queue_wait_included)
{
    const std::string url = historyDatabase("gw_hist_deadline");
    HttpRig rig(url, 1, 8, 1000); // one history connection, one read's deadline 1000 ms
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 1, 10, std::to_string(T0) + " + g"));
    const auto a = rig.bearer("alice", "secret-alice");
    CHECK(200 == rig.get("/api/v2/orders/history", a).status);

    PGconn *locker = PQconnectdb(url.c_str());
    PGresult *lr = PQexec(locker, "BEGIN; LOCK TABLE gateway_order_history_v1 IN ACCESS EXCLUSIVE MODE");
    CHECK_MSG(PGRES_COMMAND_OK == PQresultStatus(lr), PQresultErrorMessage(lr));
    PQclear(lr);
    HttpResult first;
    HttpResult second;
    std::thread one([&] { first = rig.get("/api/v2/orders/history", a); });
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (sql(url, "SELECT count(*) FROM pg_locks WHERE NOT granted") != "1" && std::chrono::steady_clock::now() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::thread two([&] { second = rig.get("/api/v2/orders/history", a); });
    while (rig.historyPool->queued() != 1 && std::chrono::steady_clock::now() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK_MSG(1 == rig.historyPool->queued(), "the second read is not queued");
    one.join();
    two.join();
    lr = PQexec(locker, "COMMIT");
    PQclear(lr);
    PQfinish(locker);
    CHECK_MSG(503 == first.status && "HISTORY_UNAVAILABLE" == codeOf(first), what(first));
    // the second read waited in the queue for most of its deadline: it gets only what is left (or HISTORY_BUSY when
    // nothing is), never a fresh statement timeout per step
    CHECK_MSG(503 == second.status && ("HISTORY_UNAVAILABLE" == codeOf(second) || "HISTORY_BUSY" == codeOf(second)),
        what(second));
    CHECK_MSG(second.ms < 1500, "a queued read with a 1000 ms deadline answered in " + std::to_string(second.ms) + " ms");
    const HttpResult after = rig.get("/api/v2/orders/history", a);
    CHECK_MSG(200 == after.status, what(after));
}

TEST(history_dropped_connection_is_retried_once)
{
    const std::string url = historyDatabase("gw_hist_dropped");
    sql(url, insertOrders(7, 1, 5, std::to_string(T0) + " + g"));
    PgHistoryStore store(historySettings(url));
    history::PageRequest req;
    req.userId = 7;
    req.limit = 10;
    HistoryPage page;
    HistoryAnswer x = store.page(0, req, page, soon());
    CHECK_MSG(HistoryStatus::OK == x.status && 5 == page.items.size(), nameOf(x.cause));
    // the server drops the connection (a restart, an idle kill): only the next statement sees it
    sql(url, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = current_database() AND "
        "pid <> pg_backend_pid()");
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (sql(url, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> "
               "pg_backend_pid()") != "0" && std::chrono::steady_clock::now() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    // a read is idempotent: one retry on a fresh connection answers, no 503 for a server that is up
    x = store.page(0, req, page, soon());
    CHECK_MSG(HistoryStatus::OK == x.status && 5 == page.items.size(), nameOf(x.cause));
    CHECK(1 == store.connected());
}
