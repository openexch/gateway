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
//   - one deadline per read, queue wait included; a connection the server dropped is retried once;
//   - over the projection's own schema (sql/history_projection_v1.sql) read as a member of its reader role: an order's
//     trail (own order, another user's, unknown, lagging, five fills, an order filled inside a 64-trade batch; control:
//     GW_NC_TRAIL_NO_OWNER must fail with "trail of another user's order"), and the reader role can SELECT exactly the
//     four read relations and nothing else.
#include <atomic>
#include <functional>
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
constexpr std::int64_t GENESIS = 7;             // the core genesis of the seeded heartbeat and of the rig's model core
const HistoryExpect EXPECT{ GENESIS, 0 };       // what a read of the store is checked against

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
        sql(url, "INSERT INTO gateway_history_heartbeat_v1 (beat_at, applied_order_id, applied_event_seq, epoch) VALUES "
            "(now() - interval '1500 milliseconds', 1000000000000, 1000000000000, " + std::to_string(GENESIS) + ")");
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
        const HistoryAnswer a = store.page(0, request, page, soon(), EXPECT);
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
    std::shared_ptr<const session::CoreConfig> config;
    std::atomic<bool> coreUp{ true };

    // the model core's last CoreConfigReport names `genesis`; nullopt: no report read yet
    void setGenesis(std::optional<std::int64_t> genesis)
    {
        auto c = std::make_shared<session::CoreConfig>();
        c->reported = genesis.has_value();
        c->head.genesisId = genesis.value_or(0);
        std::atomic_store(&config, std::shared_ptr<const session::CoreConfig>(std::move(c)));
    }

    // `historyUrl`: the history store's own URL (another role), "" for `url`
    HttpRig(const std::string &url, int historyWorkers, std::size_t historyQueue, std::int64_t timeoutMs,
        const std::string &historyUrl = "")
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
        history = std::make_unique<PgHistoryStore>(historySettings(historyUrl.empty() ? url : historyUrl, historyWorkers,
            timeoutMs));
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
        // the model core's config: a CoreConfigReport of genesis GENESIS was read (a test swaps it with setGenesis)
        setGenesis(GENESIS);
        d.config = [this] { return std::atomic_load(&config); };
        d.registered = [this] { return coreUp.load(); };
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

    // the closed-order lookup: the caller's row, every field, then the read's asOf; another user's order and an unknown
    // id are one 404
    r = rig.get("/api/v2/orders/9001", a);
    const std::string row = R"({"orderId":"9001","clientOrderId":"11111111-2222-4333-8444-555555555555",)"
        R"("marketId":"3","side":"SELL","type":"STOP_LIMIT","timeInForce":"GTD","price":"25","stopPrice":"26",)"
        R"("quantity":"2","filledQuantity":"0.5","remainingQuantity":"1.5","status":"EXPIRED",)"
        R"("reason":"INSUFFICIENT_FUNDS","meReason":12,"meOrderId":"777","hold":{"assetId":"1","amount":"2",)"
        R"("remaining":"0"},"createdAt":1800000000001,"updatedAt":1800000000002,"asOf":{"ageMs":)";
    CHECK_MSG(200 == r.status && 0 == r.body.rfind(row, 0) &&
        r.body.find(R"(,"stale":false,"eventSeq":)", row.size()) != std::string::npos, what(r));
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
        sql(url, "INSERT INTO gateway_history_heartbeat_v1 (beat_at, applied_order_id, applied_event_seq, epoch) VALUES "
            "(now(), 0, 0, " + std::to_string(GENESIS) + ")");
        CHECK(200 == rig.get("/api/v2/orders/history", a).status);
    }
    // relations present, the heartbeat empty: the projection never reported
    {
        PgHistoryStore store(historySettings(historyDatabase("gw_hist_nobeat", false)));
        HistoryPage page;
        history::PageRequest req;
        req.userId = 7;
        req.limit = 10;
        HistoryAnswer x = store.page(0, req, page, soon(), EXPECT);
        CHECK_MSG(HistoryStatus::UNAVAILABLE == x.status && HistoryCause::NO_HEARTBEAT == x.cause, nameOf(x.cause));
        std::string json;
        HistoryAsOf asOf;
        x = store.order(0, 7, 5, json, asOf, soon(), EXPECT); // never 404 from a projection that never reported
        CHECK_MSG(HistoryStatus::UNAVAILABLE == x.status && HistoryCause::NO_HEARTBEAT == x.cause, nameOf(x.cause));
    }
    // nothing listens: unreachable, bounded, then fail-fast inside the retry window
    {
        PgHistoryStore store(historySettings("postgresql://gw@127.0.0.1:1/gw"));
        HistoryPage page;
        history::PageRequest req;
        req.userId = 7;
        req.limit = 10;
        HistoryAnswer x = store.page(0, req, page, soon(), EXPECT);
        CHECK_MSG(HistoryStatus::UNAVAILABLE == x.status && HistoryCause::UNREACHABLE == x.cause, nameOf(x.cause));
        x = store.page(0, req, page, soon(), EXPECT);
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
    HistoryAnswer x = store.page(0, req, page, soon(), EXPECT);
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
    x = store.page(0, req, page, soon(), EXPECT);
    CHECK_MSG(HistoryStatus::OK == x.status && 5 == page.items.size(), nameOf(x.cause));
    CHECK(1 == store.connected());
}

// A heartbeat ten minutes old: the page answers with asOf.stale true (asOf.ageMs alone does not say it to a client
// that does not know the gateway's bound). The single order read from history carries the same asOf.
TEST(history_stale_label)
{
    const std::string url = historyDatabase("gw_hist_stale");
    sql(url, "UPDATE gateway_history_heartbeat_v1 SET beat_at = clock_timestamp() - interval '10 minutes'");
    HttpRig rig(url, 1, 8, 2000);
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 1, 3, std::to_string(T0) + " + g"));
    const auto a = rig.bearer("alice", "secret-alice");
    for (const char *target : { "/api/v2/orders/history", "/api/v2/fills", "/api/v2/orders/2" })
    {
        const HttpResult r = rig.get(target, a);
        CHECK_MSG(200 == r.status, what(r));
        if (200 != r.status)
        {
            continue;
        }
        const json::object body = json::parse(r.body).as_object();
        if (!body.contains("asOf") || !body.at("asOf").is_object())
        {
            CHECK_MSG(false, std::string("stale label missing: ") + target + " " + r.body);
            continue;
        }
        const json::object asOf = body.at("asOf").as_object();
        CHECK(asOf.at("ageMs").as_int64() >= 600'000);
        CHECK_MSG(asOf.contains("eventSeq"), std::string(target) + " " + r.body);
        if (!asOf.contains("stale") || !asOf.at("stale").is_bool() || !asOf.at("stale").as_bool())
        {
            CHECK_MSG(false, std::string("stale label missing: ") + target + " " + r.body);
        }
    }
    // refusal is a setting: with GATEWAY_HISTORY_MAX_AGE_MS set, the same reads are 503 with the usual Retry-After
    rig.g.historyMaxAgeMs = 60'000;
    for (const char *target : { "/api/v2/orders/history", "/api/v2/fills", "/api/v2/orders/2" })
    {
        HttpResult r = rig.get(target, a);
        CHECK_MSG(503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r) && r.headers["retry-after"] == "60" &&
            std::string::npos == r.body.find("\"items\""), std::string(target) + ": " + what(r));
    }
    const std::string m = rig.api->metrics("gw_");
    CHECK_MSG(m.find("history_reads_total{query=\"orders\",result=\"unavailable\",cause=\"stale\"} 1") !=
        std::string::npos, m);
    // a fresh heartbeat: served again, stale false
    sql(url, "UPDATE gateway_history_heartbeat_v1 SET beat_at = clock_timestamp()");
    rig.g.historyMaxAgeMs = 0;
    for (const char *target : { "/api/v2/orders/history", "/api/v2/orders/2" })
    {
        const HttpResult r = rig.get(target, a);
        CHECK_MSG(200 == r.status && json::parse(r.body).as_object().at("asOf").as_object().at("stale") == false,
            std::string(target) + ": " + what(r));
    }
}

// The epoch against the cluster: a heartbeat of another genesis than the gateway's last CoreConfigReport, and a gateway
// that has read no report yet, answer 503 (cause epoch), never rows; a gateway that had a report and lost the core
// keeps serving history.
TEST(history_epoch_check)
{
    const std::string url = historyDatabase("gw_hist_epoch");
    HttpRig rig(url, 1, 8, 2000);
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 1, 3, std::to_string(T0) + " + g"));
    sql(url, insertFills(alice, 1, 2, std::to_string(T0) + " + g"));
    const auto a = rig.bearer("alice", "secret-alice");
    bool served = false;
    auto refused = [&](const char *when) {
        for (const char *target : { "/api/v2/orders/history", "/api/v2/fills", "/api/v2/orders/2" })
        {
            const HttpResult r = rig.get(target, a);
            const bool ok = 503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r);
            if (!ok && 200 == r.status)
            {
                served = true;
            }
            CHECK_MSG(ok, std::string(when) + " " + target + ": " + what(r));
        }
    };
    CHECK(200 == rig.get("/api/v2/orders/history", a).status);
    rig.setGenesis(GENESIS + 1); // the core was re-created: its genesis is not the projection's
    refused("another genesis");
    rig.setGenesis(std::nullopt); // no CoreConfigReport read yet
    refused("no report");
    if (served)
    {
        CHECK_MSG(false, "history of another genesis served");
    }
    const std::string m = rig.api->metrics("gw_");
    CHECK_MSG(m.find("history_reads_total{query=\"orders\",result=\"unavailable\",cause=\"epoch\"} 2") !=
        std::string::npos && m.find("history_reads_total{query=\"order\",result=\"unavailable\",cause=\"epoch\"} 2") !=
        std::string::npos, m);
    // the report read, then the core lost (the session down, its last report kept): history is still served
    rig.setGenesis(GENESIS);
    rig.coreUp = false;
    for (const char *target : { "/api/v2/orders/history", "/api/v2/fills" })
    {
        const HttpResult r = rig.get(target, a);
        CHECK_MSG(200 == r.status && !json::parse(r.body).as_object().at("items").as_array().empty(),
            std::string("core lost: ") + target + ": " + what(r));
    }
}

namespace {

json::object healthOf(HttpRig &rig)
{
    const std::string h = rig.api->healthJson();
    return json::parse("{" + h.substr(1) + "}").as_object().at("history").as_object();
}

std::int64_t ageMetric(HttpRig &rig)
{
    const std::string m = rig.api->metrics("gw_");
    const std::size_t at = m.find("\ngw_history_age_ms ");
    return std::string::npos == at ? -2 : std::stoll(m.substr(at + 19));
}

// waits up to `ms` for /health's history block to satisfy `ok`; the block it last saw
bool healthWithin(HttpRig &rig, std::int64_t ms, const std::function<bool(const json::object &)> &ok, json::object &last)
{
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    do
    {
        last = healthOf(rig);
        if (ok(last))
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < until);
    return false;
}

std::string causeOf(const json::object &h)
{
    return h.at("cause").is_null() ? std::string("null") : std::string(h.at("cause").as_string());
}

}

// /health's history block and the age metric follow the probe: fresh, stale, no heartbeat, PostgreSQL down, each
// entered twice, each shown inside one probe interval (plus the read deadline); PostgreSQL down is 503 on history and
// never reaches the order path. Catch order: a heartbeat of another epoch and older than MAX_AGE is cause epoch; PG down
// while the cached probe says stale is cause pg.
TEST(history_health_probe)
{
    const std::string url = historyDatabase("gw_hist_health");
    HttpRig rig(url, 2, 8, 800);
    rig.g.historyProbeMs = 300;
    rig.g.historyStaleMs = 5000;
    const std::int64_t alice = rig.user("alice", "secret-alice");
    sql(url, insertOrders(alice, 1, 3, std::to_string(T0) + " + g"));
    const auto a = rig.bearer("alice", "secret-alice");
    const std::int64_t bound = rig.g.historyProbeMs + rig.g.pgStatementTimeoutMs + 300;
    {
        // before the first probe: never fresh
        const json::object h = healthOf(rig);
        CHECK_MSG(h.at("stale") == true && "unprobed" == causeOf(h), json::serialize(h));
    }
    rig.api->startHistoryProbe();
    bool shownWithoutProbe = false;
    int entered[4] = { 0, 0, 0, 0 };
    for (int round = 0; round < 2; ++round)
    {
        json::object h;
        // fresh: a beat now (a read finds it too, so a read-fed /health would look the same here)
        sql(url, "UPDATE gateway_history_heartbeat_v1 SET beat_at = clock_timestamp()");
        CHECK(200 == rig.get("/api/v2/orders/history", a).status);
        const bool fresh = healthWithin(rig, bound, [](const json::object &x) {
            return x.at("stale") == false && x.at("cause").is_null() && x.at("ageMs").is_int64() &&
                x.at("eventSeq").is_int64();
        }, h);
        CHECK_MSG(fresh, "fresh not shown: " + json::serialize(h));
        CHECK_MSG(ageMetric(rig) >= 0, "the age metric while fresh: " + std::to_string(ageMetric(rig)));
        entered[0] += fresh ? 1 : 0;
        // stale: the beat 10 s old, nobody reads
        sql(url, "UPDATE gateway_history_heartbeat_v1 SET beat_at = clock_timestamp() - interval '10 seconds'");
        const bool stale = healthWithin(rig, bound, [](const json::object &x) {
            return x.at("stale") == true && x.at("cause").is_null() && x.at("ageMs").is_int64() &&
                x.at("ageMs").as_int64() >= 10'000;
        }, h);
        if (!stale && h.at("stale") == false)
        {
            shownWithoutProbe = true;
        }
        CHECK_MSG(stale, "stale not shown: " + json::serialize(h));
        CHECK_MSG(ageMetric(rig) >= 10'000, "the age metric while stale: " + std::to_string(ageMetric(rig)));
        entered[1] += stale ? 1 : 0;
        // no heartbeat
        sql(url, "DELETE FROM gateway_history_heartbeat_v1");
        const bool none = healthWithin(rig, bound, [](const json::object &x) {
            return x.at("stale") == true && "heartbeat" == causeOf(x) && x.at("ageMs").is_null();
        }, h);
        if (!none && h.at("stale") == false)
        {
            shownWithoutProbe = true;
        }
        CHECK_MSG(none, "no heartbeat not shown: " + json::serialize(h));
        CHECK_MSG(-1 == ageMetric(rig), "the age metric without a heartbeat: " + std::to_string(ageMetric(rig)));
        entered[2] += none ? 1 : 0;
        // PostgreSQL down (the server frozen) while the cached probe says no heartbeat: a read is 503 cause pg, the
        // order path answers, /health shows pg
        sql(url, "INSERT INTO gateway_history_heartbeat_v1 (beat_at, applied_order_id, applied_event_seq, epoch) VALUES "
            "(clock_timestamp() - interval '10 seconds', 100, 100, " + std::to_string(GENESIS) + ")");
        healthWithin(rig, bound, [](const json::object &x) { return x.at("ageMs").is_int64(); }, h); // stale cached
        gwtest::run("GW_TEST_PG_PAUSE");
        const HttpResult down = rig.get("/api/v2/fills", a);
        const HttpResult order = httpCall(rig.port, "POST", "/api/v2/orders",
            R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"100","quantity":"0.5"})",
            { a[0], { "Idempotency-Key", "11111111-2222-4333-8444-55555555555" + std::to_string(round) } });
        const bool pgShown = healthWithin(rig, bound + rig.g.pgStatementTimeoutMs, [](const json::object &x) {
            return x.at("stale") == true && "pg" == causeOf(x);
        }, h);
        const std::int64_t metricDown = ageMetric(rig);
        gwtest::run("GW_TEST_PG_UNPAUSE");
        CHECK_MSG(503 == down.status && "HISTORY_UNAVAILABLE" == codeOf(down), "a read while PostgreSQL is down: " +
            what(down));
        CHECK_MSG(201 == order.status && order.ms < 2000, "the order path while PostgreSQL is down: " + what(order));
        if (!pgShown && h.at("stale") == false)
        {
            shownWithoutProbe = true;
        }
        CHECK_MSG(pgShown, "PostgreSQL down not shown: " + json::serialize(h));
        CHECK_MSG(-1 == metricDown, "the age metric while PostgreSQL is down: " + std::to_string(metricDown));
        entered[3] += pgShown ? 1 : 0;
    }
    if (shownWithoutProbe)
    {
        CHECK_MSG(false, "health shows fresh history without a probe");
    }
    CHECK_MSG(2 == entered[0] && 2 == entered[1] && 2 == entered[2] && 2 == entered[3], "states entered: fresh " +
        std::to_string(entered[0]) + ", stale " + std::to_string(entered[1]) + ", no heartbeat " +
        std::to_string(entered[2]) + ", pg down " + std::to_string(entered[3]));
    const std::string m = rig.api->metrics("gw_");
    CHECK_MSG(m.find("history_reads_total{query=\"fills\",result=\"unavailable\",cause=\"timeout\"} 2") !=
        std::string::npos || m.find("cause=\"unreachable\"") != std::string::npos, m);
    // catch order: another epoch and older than MAX_AGE at once answers epoch (the epoch is checked first)
    rig.g.historyMaxAgeMs = 6000;
    sql(url, "UPDATE gateway_history_heartbeat_v1 SET epoch = " + std::to_string(GENESIS + 1) + ", beat_at = "
        "clock_timestamp() - interval '1 minute'");
    const HttpResult both = rig.get("/api/v2/orders/history", a);
    CHECK_MSG(503 == both.status, what(both));
    const std::string m2 = rig.api->metrics("gw_");
    CHECK_MSG(m2.find("history_reads_total{query=\"orders\",result=\"unavailable\",cause=\"epoch\"} 1") !=
        std::string::npos && m2.find("cause=\"stale\"") == std::string::npos, m2);
    json::object h;
    CHECK_MSG(healthWithin(rig, bound, [](const json::object &x) { return "epoch" == causeOf(x); }, h),
        "epoch before stale on /health: " + json::serialize(h));
    rig.api->stopHistoryProbe();
}

// Read order under a concurrent apply: a page's asOf.eventSeq is read before its rows, and the writer commits that
// cursor with the rows it covers, so every row of an event at or below asOf.eventSeq is on the page (it may hold newer
// rows, never fewer).
TEST(history_read_order_under_concurrent_apply)
{
    const std::string url = historyDatabase("gw_hist_order");
    const std::int64_t user = 41;
    // the writer: one transaction per batch, the rows of events (n + 1 .. n + 5) and the heartbeat's cursor moved with
    // them; fill f of event e is trade e
    sql(url, "UPDATE gateway_history_heartbeat_v1 SET applied_event_seq = 0");
    std::atomic<bool> writing{ true };
    std::atomic<std::int64_t> committed{ 0 };
    std::thread writer([&] {
        PGconn *c = PQconnectdb(url.c_str());
        std::int64_t n = 0;
        while (writing.load() && n < 20'000)
        {
            const std::string from = std::to_string(n + 1);
            const std::string to = std::to_string(n + 5);
            PGresult *r = PQexec(c, ("BEGIN; INSERT INTO gateway_fills_v1 (trade_id, order_id, user_id, market_id, side, "
                "price, quantity, maker, executed_at_ms) SELECT g, g, " + std::to_string(user) + ", 1, 'BUY', 1, 1, false, "
                "g FROM generate_series(" + from + ", " + to + ") g; SELECT pg_sleep(0.002); UPDATE "
                "gateway_history_heartbeat_v1 SET applied_event_seq = " + to + ", beat_at = clock_timestamp(); COMMIT")
                .c_str());
            PQclear(r);
            n += 5;
            committed = n;
        }
        PQfinish(c);
    });
    PgHistoryStore store(historySettings(url));
    int pages = 0;
    int lacking = 0;
    std::string example;
    while (pages < 300)
    {
        history::PageRequest req;
        req.kind = history::Kind::FILLS;
        req.userId = user;
        req.limit = 1000;
        HistoryPage page;
        const HistoryAnswer x = store.page(0, req, page, soon(), EXPECT);
        CHECK_MSG(HistoryStatus::OK == x.status, nameOf(x.cause));
        if (HistoryStatus::OK != x.status)
        {
            break;
        }
        pages++;
        // the newest 1000 rows: every trade in (eventSeq - 999 .. eventSeq] must be there
        std::set<std::int64_t> trades;
        for (const std::string &item : page.items)
        {
            trades.insert(std::stoll(std::string(json::parse(item).as_object().at("tradeId").as_string())));
        }
        for (std::int64_t t = std::max<std::int64_t>(1, page.eventSeq - 900); t <= page.eventSeq; ++t)
        {
            if (0 == trades.count(t))
            {
                if (lacking++ == 0)
                {
                    example = "eventSeq " + std::to_string(page.eventSeq) + " without trade " + std::to_string(t);
                }
                break;
            }
        }
    }
    writing = false;
    writer.join();
    std::printf("  read order: %d pages over %lld committed events\n", pages, static_cast<long long>(committed.load()));
    CHECK_MSG(committed.load() > 100, "the writer committed " + std::to_string(committed.load()) + " events");
    if (lacking > 0)
    {
        CHECK_MSG(false, "asOf.eventSeq claims rows the page lacks: " + std::to_string(lacking) + " pages, first " +
            example);
    }
}

// ---- the order trail and the reader role, over the projection's own schema

namespace {

constexpr const char *READER = "gw_history_login";      // a login role made a member of oe_history_reader
constexpr const char *READER_PASSWORD = "reader-secret";

// A fresh database with the projection's schema applied as the server's owner role (it creates oe_history_reader and
// grants it the read relations), and READER a member of that role; the database URL and the reader's URL.
std::pair<std::string, std::string> projectionDatabase(const std::string &name)
{
    const std::string url = freshDatabase(name);
    std::ifstream in(GW_HISTORY_PROJECTION_SQL);
    std::stringstream ddl;
    ddl << in.rdbuf();
    CHECK_MSG(!ddl.str().empty(), std::string("cannot read ") + GW_HISTORY_PROJECTION_SQL);
    sql(url, ddl.str());
    sql(url, std::string("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '") + READER + "') THEN "
        "CREATE ROLE " + READER + " LOGIN PASSWORD '" + READER_PASSWORD + "'; END IF; END $$; GRANT oe_history_reader TO " +
        READER);
    return { url, "postgresql://" + std::string(READER) + ":" + READER_PASSWORD + url.substr(url.find('@')) };
}

// One journal record of an order in oe_order_events (the payload is not this test's concern: zero bytes).
std::string event(std::int64_t seq, std::int64_t order, std::int64_t user, const char *kind, const char *status,
    std::int64_t logPosition, const char *liquidity = "NONE", std::int64_t meEpoch = 0, std::int64_t meSeq = 0,
    std::int64_t meLog = 0, std::int64_t trade = 0, std::int64_t qty = 0)
{
    return "(" + std::to_string(seq) + ", " + std::to_string(order) + ", " + std::to_string(user) + ", '" + kind + "', '" +
        status + "', " + std::to_string(logPosition) + ", '" + liquidity + "', " + std::to_string(meEpoch) + ", " +
        std::to_string(meSeq) + ", " + std::to_string(meLog) + ", " + std::to_string(0 == meEpoch ? 0 : T0 + seq) + ", " +
        std::to_string(trade) + ", " + std::to_string(0 == trade ? 0 : 10000000000) + ", " + std::to_string(qty) +
        ", 500000000, " + std::to_string(T0 + seq) + ", decode(repeat('00', 216), 'hex'))";
}

std::string insertEvents(const std::vector<std::string> &rows)
{
    std::string values;
    for (const std::string &r : rows)
    {
        values += (values.empty() ? "" : ", ") + r;
    }
    // stored in random order: the trail's order must come from event_seq, not from the heap
    return "INSERT INTO oe_order_events (journal_seq, order_id, user_id, event, status, core_log_position, liquidity, "
        "me_epoch, me_journal_seq, me_log_position, me_timestamp_ms, trade_id, last_price, last_qty, hold_remaining, "
        "at_ms, payload) SELECT * FROM (VALUES " + values + ") v ORDER BY random()";
}

// Every page of a trail through HTTP (limit per page); the items in order, false when a page is not 200.
bool walkTrail(HttpRig &rig, const std::vector<std::pair<std::string, std::string>> &h, std::int64_t order, int limit,
    std::vector<json::object> &items, json::object &lastPage)
{
    std::string target = "/api/v2/orders/" + std::to_string(order) + "/trail?limit=" + std::to_string(limit);
    for (int pages = 0; pages < 100; ++pages)
    {
        const HttpResult r = rig.get(target, h);
        if (200 != r.status)
        {
            CHECK_MSG(false, "trail page " + target + ": " + what(r));
            return false;
        }
        lastPage = json::parse(r.body).as_object();
        CHECK_MSG(lastPage.at("orderId").as_string() == std::to_string(order), r.body);
        for (const json::value &v : lastPage.at("items").as_array())
        {
            items.push_back(v.as_object());
        }
        if (lastPage.at("nextCursor").is_null())
        {
            return true;
        }
        target = "/api/v2/orders/" + std::to_string(order) + "/trail?limit=" + std::to_string(limit) + "&cursor=" +
            std::string(lastPage.at("nextCursor").as_string());
    }
    CHECK_MSG(false, "trail walk did not end");
    return false;
}

std::int64_t seqOf(const json::object &item)
{
    return item.at("eventSeq").as_int64();
}

}

TEST(history_order_trail)
{
    const auto [url, readerUrl] = projectionDatabase("gw_hist_trail");
    HttpRig rig(url, 1, 16, 2000, readerUrl); // history read as a member of the reader role only
    const std::int64_t alice = rig.user("alice", "secret-alice");
    const std::int64_t bob = rig.user("bob", "secret-bob");
    std::vector<std::string> rows;
    std::int64_t q = 0;
    // order 1 (alice): accepted with its hold, forwarded, booked, five fills
    rows.push_back(event(++q, 1, alice, "ACCEPTED", "NEW", 1000));
    rows.push_back(event(++q, 1, alice, "DISPATCHED", "NEW", 1100));
    rows.push_back(event(++q, 1, alice, "ACK_NEW", "NEW", 1200, "NONE", 5, 7, 70000));
    rows.push_back(event(++q, 2, bob, "ACCEPTED", "NEW", 1250)); // order 2: bob's
    for (int k = 0; k < 5; ++k)
    {
        rows.push_back(event(++q, 1, alice, "FILL", 4 == k ? "FILLED" : "PARTIALLY_FILLED", 1300 + k,
            0 == k % 2 ? "TAKER" : "MAKER", 5, 8 + k, 70100 + 10 * k, 11 + k, 20000000));
    }
    // order 3 (alice) filled inside one 64-trade batch against bob's makers 100..163: one core log position for all
    // 128 FILL records
    rows.push_back(event(++q, 3, alice, "ACCEPTED", "NEW", 2000));
    rows.push_back(event(++q, 3, alice, "DISPATCHED", "NEW", 2100));
    for (int k = 0; k < 64; ++k)
    {
        rows.push_back(event(++q, 100 + k, bob, "ACCEPTED", "NEW", 2200 + k));
    }
    for (int k = 0; k < 64; ++k)
    {
        rows.push_back(event(++q, 3, alice, "FILL", 63 == k ? "FILLED" : "PARTIALLY_FILLED", 900000, "TAKER", 5, 100 + k,
            80000, 1000 + k, 1000000));
        rows.push_back(event(++q, 100 + k, bob, "FILL", "FILLED", 900000, "MAKER", 5, 100 + k, 80000, 1000 + k, 1000000));
    }
    sql(url, insertEvents(rows));
    sql(url, "UPDATE oe_projection_state SET epoch = " + std::to_string(GENESIS) + ", applied_seq = " + std::to_string(q) +
        ", applied_order_id = 163, beat_at = clock_timestamp()");
    const auto a = rig.bearer("alice", "secret-alice");
    const auto b = rig.bearer("bob", "secret-bob");

    // own order with five fills: every record in event_seq order, with its positions and me_epoch
    std::vector<json::object> items;
    json::object page;
    CHECK(walkTrail(rig, a, 1, 100, items, page));
    const std::vector<std::string> kinds = { "ACCEPTED", "DISPATCHED", "ACK_NEW", "FILL", "FILL", "FILL", "FILL", "FILL" };
    CHECK_MSG(items.size() == kinds.size(), std::to_string(items.size()) + " items");
    for (std::size_t i = 0; i < std::min(items.size(), kinds.size()); ++i)
    {
        CHECK_MSG(items[i].at("event").as_string() == kinds[i], json::serialize(items[i]));
        CHECK(i == 0 || seqOf(items[i]) > seqOf(items[i - 1]));
        CHECK(items[i].contains("coreLogPosition") && items[i].contains("holdRemaining") && items[i].contains("at"));
    }
    if (items.size() == kinds.size())
    {
        CHECK_MSG(items[0].at("me").is_null() && items[0].at("tradeId").is_null() && items[0].at("liquidity").is_null(),
            json::serialize(items[0]));
        CHECK_MSG(items[2].at("me").as_object().at("epoch").as_string() == "5" &&
            items[2].at("me").as_object().at("logPosition").as_int64() == 70000 &&
            items[2].at("me").as_object().at("journalSeq").is_int64(), json::serialize(items[2]));
        for (int k = 0; k < 5; ++k)
        {
            const json::object &f = items[3 + static_cast<std::size_t>(k)];
            CHECK_MSG(f.at("tradeId").as_string() == std::to_string(11 + k) && f.at("lastQty").as_string() == "0.2" &&
                f.at("lastPrice").as_string() == "100" && f.at("coreLogPosition").as_int64() == 1300 + k &&
                f.at("me").as_object().at("epoch").as_string() == "5" &&
                f.at("me").as_object().at("logPosition").as_int64() == 70100 + 10 * k &&
                f.at("liquidity").as_string() == (0 == k % 2 ? "TAKER" : "MAKER"), json::serialize(f));
        }
        CHECK(items[7].at("status").as_string() == "FILLED");
    }
    const json::object asOf = page.at("asOf").as_object();
    CHECK_MSG(asOf.at("eventSeq").as_int64() == q && !asOf.at("stale").as_bool() && asOf.at("ageMs").as_int64() >= 0,
        json::serialize(asOf));

    // another user's order and an unknown one (at or below applied_order_id): one 404, the same body
    const HttpResult other = rig.get("/api/v2/orders/2/trail", a);
    const HttpResult unknown = rig.get("/api/v2/orders/50/trail", a);
    if (200 == other.status)
    {
        CHECK_MSG(false, "trail of another user's order: " + other.body);
    }
    CHECK_MSG(404 == other.status && "NOT_FOUND" == codeOf(other), what(other));
    CHECK_MSG(404 == unknown.status && unknown.body == other.body, what(unknown) + " | " + what(other));
    // the owner reads it
    {
        std::vector<json::object> own;
        CHECK(walkTrail(rig, b, 2, 10, own, page) && 1 == own.size());
    }
    // lagging: above applied_order_id with no row is 503 with the short Retry-After, never 404
    HttpResult r = rig.get("/api/v2/orders/500/trail", a);
    CHECK_MSG(503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r) && r.headers["retry-after"] == "1", what(r));

    // an order filled inside a 64-trade batch, walked in pages of 10: 66 records, event_seq order, one core log
    // position for its 64 fills, trades in order
    items.clear();
    CHECK(walkTrail(rig, a, 3, 10, items, page));
    CHECK_MSG(66 == items.size(), std::to_string(items.size()) + " items");
    int fills = 0;
    for (std::size_t i = 0; i < items.size(); ++i)
    {
        CHECK(i == 0 || seqOf(items[i]) > seqOf(items[i - 1]));
        if (items[i].at("event").as_string() == "FILL")
        {
            CHECK_MSG(items[i].at("coreLogPosition").as_int64() == 900000 &&
                items[i].at("tradeId").as_string() == std::to_string(1000 + fills) &&
                items[i].at("liquidity").as_string() == "TAKER", json::serialize(items[i]));
            fills++;
        }
    }
    CHECK_MSG(64 == fills, std::to_string(fills) + " fills");
    // a maker of that batch: its owner sees ACCEPTED and its one fill; alice gets the 404
    {
        std::vector<json::object> maker;
        CHECK(walkTrail(rig, b, 140, 10, maker, page) && 2 == maker.size() &&
            maker.back().at("liquidity").as_string() == "MAKER");
        const HttpResult m = rig.get("/api/v2/orders/140/trail", a);
        if (200 == m.status)
        {
            CHECK_MSG(false, "trail of another user's order: " + m.body);
        }
        CHECK_MSG(404 == m.status && m.body == other.body, what(m));
    }

    // refusals of the request itself
    r = rig.get("/api/v2/orders/3/trail?limit=100000", a);
    CHECK_MSG(400 == r.status, what(r));
    {
        const HttpResult first = rig.get("/api/v2/orders/3/trail?limit=10", a);
        const std::string cursor(json::parse(first.body).as_object().at("nextCursor").as_string());
        r = rig.get("/api/v2/orders/1/trail?cursor=" + cursor, a); // a cursor of another order
        CHECK_MSG(400 == r.status && "VALIDATION" == codeOf(r), what(r));
        r = rig.get("/api/v2/orders/history?cursor=" + cursor, a); // of another route
        CHECK_MSG(400 == r.status, what(r));
    }
    r = rig.get("/api/v2/orders/1/trail?marketId=1", a);
    CHECK_MSG(400 == r.status, what(r));
    r = httpCall(rig.port, "DELETE", "/api/v2/orders/1/trail", "", a);
    CHECK_MSG(405 == r.status && r.headers["allow"] == "GET", what(r));
    r = rig.get("/api/v2/orders/0/trail", a);
    CHECK_MSG(400 == r.status, what(r));

    // the freshness rules of the other history routes: stale label, another genesis
    sql(url, "UPDATE oe_projection_state SET beat_at = clock_timestamp() - interval '10 minutes'");
    r = rig.get("/api/v2/orders/1/trail", a);
    CHECK_MSG(200 == r.status && json::parse(r.body).as_object().at("asOf").as_object().at("stale").as_bool(), what(r));
    rig.setGenesis(GENESIS + 1);
    r = rig.get("/api/v2/orders/1/trail", a);
    CHECK_MSG(503 == r.status && "HISTORY_UNAVAILABLE" == codeOf(r), what(r));
    rig.setGenesis(GENESIS);

    const std::string m = rig.api->metrics("gw_");
    for (const char *line : { "history_reads_total{query=\"trail\",result=\"not_found\",cause=\"none\"} 3",
             "history_reads_total{query=\"trail\",result=\"unavailable\",cause=\"lagging\"} 1",
             "history_reads_total{query=\"trail\",result=\"unavailable\",cause=\"epoch\"} 1" })
    {
        CHECK_MSG(m.find(line) != std::string::npos, std::string(line) + " not in\n" + m);
    }
}

TEST(history_reader_grant)
{
    const auto [url, readerUrl] = projectionDatabase("gw_hist_grant");
    // every relation of the database the reader can do anything with, by privilege
    auto granted = [&](const char *privilege) {
        return sql(url, std::string("SELECT coalesce(string_agg(c.relname, ',' ORDER BY c.relname), '') FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f') AND "
            "has_table_privilege('") + READER + "', c.oid, '" + privilege + "')");
    };
    const std::string select = granted("SELECT");
    CHECK_MSG(select == "gateway_fills_v1,gateway_history_heartbeat_v1,gateway_order_history_v1,gateway_order_trail_v1",
        "the reader can SELECT: " + select);
    for (const char *privilege : { "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" })
    {
        const std::string any = granted(privilege);
        CHECK_MSG(any.empty(), std::string("the reader has ") + privilege + " on: " + any);
    }
    CHECK_MSG(sql(url, std::string("SELECT has_schema_privilege('") + READER + "', 'public', 'CREATE')") == "f",
        "the reader can create in the public schema");
    // and through a connection of its own: the four answer, a base table is refused
    PGconn *c = PQconnectdb(readerUrl.c_str());
    CHECK_MSG(CONNECTION_OK == PQstatus(c), PQerrorMessage(c));
    auto run = [&](const std::string &statement) {
        PGresult *r = PQexec(c, statement.c_str());
        const std::string state = PQresultErrorField(r, PG_DIAG_SQLSTATE) ? PQresultErrorField(r, PG_DIAG_SQLSTATE) : "";
        const bool ok = PGRES_TUPLES_OK == PQresultStatus(r) || PGRES_COMMAND_OK == PQresultStatus(r);
        PQclear(r);
        return ok ? std::string("ok") : state;
    };
    for (const char *relation : { "gateway_order_history_v1", "gateway_fills_v1", "gateway_history_heartbeat_v1",
             "gateway_order_trail_v1" })
    {
        CHECK_MSG(run(std::string("SELECT count(*) FROM ") + relation) == "ok", relation);
    }
    for (const char *statement : { "SELECT count(*) FROM oe_order_events", "SELECT count(*) FROM oe_projection_state",
             "SELECT count(*) FROM oe_orders", "DELETE FROM gateway_fills_v1",
             "UPDATE oe_projection_state SET beat_at = now()" })
    {
        const std::string got = run(statement);
        CHECK_MSG("42501" == got, std::string(statement) + ": " + got);
    }
    PQfinish(c);
}
