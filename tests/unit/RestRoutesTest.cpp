// The order, balance, market, status and treasury routes in process (T0): the real HttpServer and GatewayApi on
// loopback, the core replaced by a small in-memory core (FakeCore) answering from a pool thread as the duty bridge's
// completion thread would, with hooks that answer one command with any outcome (504, 503, a reject, a re-offered
// reply). Every row of the status table through a route, the cancel re-offer rule, bulk cancel, the read cache, the
// query budget and gate answers, and the treasury lane on and off with its caps and the register funding id.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <tuple>

#include <boost/json.hpp>

#include "Check.h"
#include "HttpClient.h"
#include "MemoryStore.h"
#include "StubHistory.h"
#include "gateway/common/HttpServer.h"
#include "gateway/common/Rest.h"
#include "GatewayApi.h"

using namespace openexchange::gateway;
using gwtest::httpCall;
using gwtest::HttpResult;
using gwtest::MemoryStore;
using gwtest::StubHistory;
namespace json = boost::json;
namespace money = wire::money;

namespace {

using Headers = std::vector<std::pair<std::string, std::string>>;
constexpr std::int64_t FP = 100'000'000;
const char *KEY_A = "11111111-2222-4333-8444-555555555555";

// A small core: orders by id with their owner and open flag, stored create and money decisions by (user, key),
// balances. `hook` answers a command before the model does (nullopt: the model answers).
struct FakeCore
{
    std::mutex mutex;
    std::int64_t nextOrderId = 1000;
    struct Order
    {
        std::int64_t userId = 0;
        bool open = true;
        wire::CreateOrder create;
    };
    std::map<std::int64_t, Order> orders;
    std::map<std::tuple<std::int64_t, std::int64_t, std::int64_t>, std::int64_t> createKeys; // -> orderId
    std::map<std::tuple<std::int64_t, std::int64_t, std::int64_t>, std::int64_t> moneyKeys;  // -> amount
    std::map<std::pair<std::int64_t, std::int32_t>, std::int64_t> available;
    std::vector<session::Command> seen;
    int pageSize = 2;
    std::atomic<int> applied{ 0 };
    std::function<std::optional<session::Outcome>(const session::Command &)> hook;

    session::Outcome answer(const session::Command &c)
    {
        std::lock_guard<std::mutex> lock(mutex);
        seen.push_back(c);
        if (hook)
        {
            if (auto o = hook(c))
            {
                return *o;
            }
        }
        return model(c);
    }

    session::Outcome model(const session::Command &c)
    {
        session::Outcome o;
        o.templateId = session::templateOf(c);
        o.offers = 1;
        if (const auto *x = std::get_if<wire::CreateOrder>(&c))
        {
            const auto key = std::make_tuple(x->userId, x->requestIdHigh, x->requestIdLow);
            wire::OrderAck ack;
            ack.marketId = x->marketId;
            ack.userId = x->userId;
            ack.holdAssetId = 2;
            ack.holdAmount = x->price / FP * x->quantity;
            ack.acceptedAtMs = 1'800'000'000'000;
            const auto found = createKeys.find(key);
            if (found != createKeys.end())
            {
                ack.orderId = found->second;
                ack.duplicate = true;
            }
            else
            {
                ack.orderId = ++nextOrderId;
                orders[ack.orderId] = Order{ x->userId, true, *x };
                createKeys[key] = ack.orderId;
                applied++;
            }
            ack.status = money::OrderStatus::PENDING_NEW;
            ack.eventSeq = ack.orderId * 10;
            o.reply = ack;
        }
        else if (const auto *x = std::get_if<wire::CancelOrder>(&c))
        {
            const auto it = orders.find(x->orderId);
            if (it == orders.end() || it->second.userId != x->userId || !it->second.open)
            {
                wire::OrderReject r;
                r.refTemplateId = wire::tid::CANCEL_ORDER;
                r.reason = it == orders.end() ? money::CoreRejectReason::UNKNOWN_ORDER
                    : it->second.userId != x->userId ? money::CoreRejectReason::NOT_OWNER : money::CoreRejectReason::NOT_OPEN;
                o.reply = r;
            }
            else
            {
                it->second.open = false;
                wire::OrderAck ack;
                ack.orderId = x->orderId;
                ack.status = money::OrderStatus::PENDING_CANCEL;
                o.reply = ack;
            }
        }
        else if (const auto *x = std::get_if<wire::AmendOrder>(&c))
        {
            wire::OrderAck ack;
            ack.orderId = x->orderId;
            ack.status = money::OrderStatus::PENDING_REPLACE;
            o.reply = ack;
        }
        else if (const auto *x = std::get_if<wire::RequestOrderSnapshot>(&c))
        {
            session::SnapshotReply page;
            std::int64_t last = 0;
            bool more = false;
            for (const auto &[id, order] : orders)
            {
                if (id <= x->fromOrderId || order.userId != x->userId || !order.open)
                {
                    continue;
                }
                if (static_cast<int>(page.entries.size()) == pageSize)
                {
                    more = true;
                    break;
                }
                wire::OrderSnapshotEntry e;
                e.orderId = id;
                e.userId = order.userId;
                e.marketId = order.create.marketId;
                e.status = money::OrderStatus::NEW;
                e.side = order.create.side;
                e.orderType = order.create.orderType;
                e.timeInForce = order.create.timeInForce;
                e.price = order.create.price;
                e.quantity = order.create.quantity;
                e.remainingQty = order.create.quantity;
                e.holdAssetId = 2;
                e.holdAmount = 5;
                page.entries.push_back(e);
                last = id;
            }
            page.end.entryCount = static_cast<std::int32_t>(page.entries.size());
            page.end.nextOrderId = more ? last : 0;
            page.end.asOfLogPosition = 4096 + static_cast<std::int64_t>(seen.size());
            o.reply = page;
        }
        else if (const auto *x = std::get_if<wire::RequestUserBalances>(&c))
        {
            session::BalancesReply b;
            b.head.userId = x->userId;
            b.head.asOfLogPosition = 777;
            for (const auto &[k, v] : available)
            {
                if (k.first == x->userId)
                {
                    b.lines.push_back(wire::BalanceLine{ k.second, v, 3 * FP });
                    b.head.known = true;
                }
            }
            o.reply = b;
        }
        else if (const auto *x = std::get_if<session::Deposit>(&c))
        {
            const wire::MoneyCommand &m = x->command;
            const auto key = std::make_tuple(m.userId, m.requestIdHigh, m.requestIdLow);
            wire::MoneyAck ack{ 0, m.userId, m.assetId, m.amount, 0, false };
            if (moneyKeys.count(key))
            {
                ack.duplicate = true;
            }
            else
            {
                moneyKeys[key] = m.amount;
                available[{ m.userId, m.assetId }] += m.amount;
                applied++;
            }
            ack.newAvailable = available[{ m.userId, m.assetId }];
            o.reply = session::DepositAckReply{ ack };
        }
        else if (const auto *x = std::get_if<session::Withdraw>(&c))
        {
            const wire::MoneyCommand &m = x->command;
            if (available[{ m.userId, m.assetId }] < m.amount)
            {
                wire::WithdrawReject r;
                r.reason = money::RejectReason::INSUFFICIENT_FUNDS;
                o.reply = r;
            }
            else
            {
                available[{ m.userId, m.assetId }] -= m.amount;
                o.reply = session::WithdrawAckReply{ wire::MoneyAck{ 0, m.userId, m.assetId, m.amount,
                    available[{ m.userId, m.assetId }], false } };
            }
        }
        return o;
    }

    std::size_t count(std::uint16_t templateId)
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::size_t n = 0;
        for (const auto &c : seen)
        {
            n += session::templateOf(c) == templateId ? 1 : 0;
        }
        return n;
    }
};

std::shared_ptr<const session::CoreConfig> makeConfig(money::GateState::Value global = money::GateState::OPEN)
{
    auto c = std::make_shared<session::CoreConfig>();
    c->reported = true;
    c->head.configVersion = 3;
    c->head.behaviorVersion = 4;
    c->head.requestTtlMs = 900'000;
    wire::MarketConfig m;
    m.marketId = 1;
    std::memcpy(m.symbol.data(), "BTC-USD", 7);
    m.baseAssetId = 1;
    m.quoteAssetId = 2;
    m.tickSize = FP / 100;
    m.lotSize = FP / 1000;
    m.minPrice = FP;
    m.maxPrice = 1'000'000 * FP;
    m.minQuantity = FP / 1000;
    m.maxQuantity = 1000 * FP;
    m.minNotional = FP;
    m.maxNotional = 10'000'000 * FP;
    c->markets.push_back(m);
    wire::AssetConfig btc;
    btc.assetId = 1;
    std::memcpy(btc.code.data(), "BTC", 3);
    btc.scale = 8;
    wire::AssetConfig usd;
    usd.assetId = 2;
    std::memcpy(usd.code.data(), "USD", 3);
    usd.scale = 2;
    c->assets = { btc, usd };
    c->global.state = global;
    c->global.logPosition = 10;
    if (money::GateState::SUSPENDED == global)
    {
        c->global.reasons = 1u; // ADMIN
    }
    session::GateView g;
    g.state = money::GateState::OPEN;
    g.logPosition = 10;
    g.circuitBreaker = true;
    c->marketGates.push_back(g);
    return c;
}

struct Rig
{
    GatewaySettings g;
    MemoryStore store;
    FakeCore core;
    std::unique_ptr<WorkPool> storePool;
    std::unique_ptr<WorkPool> hashPool;
    std::unique_ptr<WorkPool> corePool;
    StubHistory *history = nullptr; // set before start(): history reads through this store
    int historyThreads = 2;
    std::size_t historyQueue = 64;
    std::unique_ptr<WorkPool> historyPool;
    StubHistory *money = nullptr; // set before start(): balance movements through this store (the money database)
    std::unique_ptr<WorkPool> moneyPool;
    std::unique_ptr<Authenticator> auth;
    std::unique_ptr<GatewayApi> api;
    std::unique_ptr<HttpServer> server;
    std::atomic<bool> ringFull{ false };
    bool treasury = false;
    std::mutex configMutex;
    std::shared_ptr<const session::CoreConfig> config = makeConfig();
    std::uint16_t port = 0;

    Rig()
    {
        g.common.authModes = auth_mode::DEV | auth_mode::DEMO;
        g.common.allowDevAuth = true;
        g.common.ioThreads = 2;
        g.pbkdf2Iterations = 1000;
        for (Rate *r : { &g.registerRate, &g.loginRate, &g.authSourceRate, &g.tokenLookupRate })
        {
            *r = Rate{ 1000, 1000, "1000/s" };
        }
        g.registerBurst = g.loginBurst = g.authSourceBurst = g.tokenLookupBurst = 1000;
        g.tokenLookupInflight = 32;
        g.principalRate = Rate{ 1000, 1000, "1000/s" };
        g.principalBurst = 1000;
        g.queryMinIntervalMs = 250;
        g.faucetRate = Rate{ 1000, 1000, "1000/s" };
        g.faucetMaxAmount = { FaucetCap{ 1, "2" }, FaucetCap{ 2, "10000" } };
    }

    void setConfig(std::shared_ptr<const session::CoreConfig> c)
    {
        std::lock_guard<std::mutex> l(configMutex);
        config = std::move(c);
    }

    void start()
    {
        storePool = std::make_unique<WorkPool>("gw-pg-auth", 2, 64);
        hashPool = std::make_unique<WorkPool>("gw-hash", 1, 32);
        corePool = std::make_unique<WorkPool>("t-core", 1, 4096);
        AuthenticatorSettings as;
        as.modes = g.common.authModes;
        as.unknownLookupInflight = g.tokenLookupInflight;
        auth = std::make_unique<Authenticator>(as, AuthMaterial{}, &store, storePool.get());
        ApiDeps d;
        d.settings = &g;
        d.auth = auth.get();
        d.store = &store;
        d.storePool = storePool.get();
        d.hashPool = hashPool.get();
        if (nullptr != history)
        {
            historyPool = std::make_unique<WorkPool>("gw-pg-hist", historyThreads, historyQueue);
            d.history = history;
            d.historyPool = historyPool.get();
        }
        if (nullptr != money)
        {
            moneyPool = std::make_unique<WorkPool>("gw-pg-money", 1, 64);
            d.money = money;
            d.moneyPool = moneyPool.get();
        }
        auto asker = [this](session::Command c, std::function<void(const session::Outcome &)> done) {
            if (ringFull.load())
            {
                return false;
            }
            return corePool->trySubmit([this, c, done](int) { done(core.answer(c)); });
        };
        d.askCore = asker;
        if (treasury)
        {
            d.askTreasury = asker;
        }
        d.config = [this] {
            std::lock_guard<std::mutex> l(configMutex);
            return config;
        };
        d.registered = [] { return true; };
        api = std::make_unique<GatewayApi>(std::move(d));
        HttpServerSettings hs;
        hs.host = "127.0.0.1";
        hs.ioThreads = 2;
        server = std::make_unique<HttpServer>(hs, [this](std::shared_ptr<HttpExchange> ex) { api->handle(std::move(ex)); });
        std::string error;
        CHECK_MSG(server->bind(error), error);
        storePool->start();
        hashPool->start();
        corePool->start();
        if (historyPool)
        {
            historyPool->start();
        }
        if (moneyPool)
        {
            moneyPool->start();
        }
        server->start();
        port = server->boundPort();
    }

    ~Rig()
    {
        if (server)
        {
            if (nullptr != history)
            {
                history->hold(false);
            }
            server->stop(5000);
            if (nullptr != money)
            {
                money->hold(false);
            }
            if (historyPool)
            {
                historyPool->stop(5000);
            }
            if (moneyPool)
            {
                moneyPool->stop(5000);
            }
            corePool->stop(5000);
            hashPool->stop(5000);
            storePool->stop(5000);
        }
    }

    HttpResult call(const std::string &method, const std::string &target, const std::string &body = "",
        const Headers &headers = {})
    {
        return httpCall(port, method, target, body, headers);
    }
};

Headers as(std::int64_t user, const char *key = nullptr)
{
    Headers h = { { "Authorization", "Bearer dev:" + std::to_string(user) } };
    if (nullptr != key)
    {
        h.emplace_back("Idempotency-Key", key);
    }
    return h;
}

json::object body(const HttpResult &r)
{
    boost::system::error_code ec;
    json::value v = json::parse(r.body, ec);
    return !ec && v.is_object() ? v.as_object() : json::object{};
}

std::string str(const json::object &o, const char *key)
{
    const json::value *v = o.if_contains(key);
    return nullptr == v ? "<missing>" : v->is_string() ? std::string(v->as_string()) : json::serialize(*v);
}

std::string what(const HttpResult &r)
{
    return std::to_string(r.status) + " " + r.body + r.error;
}

const std::string LIMIT_BUY = R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"100","quantity":"0.5"})";

session::Outcome rejectOf(money::CoreRejectReason::Value reason, std::uint16_t templateId, std::uint32_t offers = 1)
{
    session::Outcome o;
    o.templateId = templateId;
    o.offers = offers;
    wire::OrderReject r;
    r.reason = reason;
    r.refTemplateId = templateId;
    o.reply = r;
    return o;
}

session::Outcome kindOf(session::OutcomeKind kind, std::uint32_t offers = 1)
{
    session::Outcome o;
    o.kind = kind;
    o.offers = offers;
    return o;
}

}

TEST(routes_markets_and_status)
{
    Rig rig;
    rig.setConfig(std::make_shared<const session::CoreConfig>());
    rig.start();
    HttpResult r = rig.call("GET", "/api/v2/markets");
    CHECK_MSG(503 == r.status && str(body(r), "code") == "CORE_UNAVAILABLE" && r.headers.count("retry-after"), what(r));
    r = rig.call("GET", "/api/v2/status");
    CHECK_MSG(200 == r.status && str(body(r), "ready") == "false" && str(body(r), "admission") == "[]", what(r));
    rig.setConfig(makeConfig());
    r = rig.call("GET", "/api/v2/markets");
    json::object b = body(r);
    CHECK_MSG(200 == r.status && str(b, "configVersion") == "3", what(r));
    const json::object m = b.at("markets").as_array().at(0).as_object();
    CHECK(str(m, "marketId") == "1" && str(m, "symbol") == "BTC-USD" && str(m, "baseAsset") == "BTC" &&
        str(m, "quoteAsset") == "USD" && str(m, "tickSize") == "0.01" && str(m, "lotSize") == "0.001" &&
        str(m, "minNotional") == "1" && str(m, "maxQuantity") == "1000");
    CHECK(b.at("assets").as_array().size() == 2 && str(b.at("assets").as_array().at(1).as_object(), "scale") == "2");
    r = rig.call("GET", "/api/v2/status");
    b = body(r);
    CHECK_MSG(200 == r.status && str(b, "ready") == "true" && str(b, "behaviorVersion") == "4" &&
            str(b, "idempotencyWindowMs") == "900000", what(r));
    const json::array adm = b.at("admission").as_array();
    CHECK(adm.size() == 2 && str(adm.at(0).as_object(), "marketId") == "0" && str(adm.at(0).as_object(), "gate") == "OPEN");
    CHECK(str(adm.at(1).as_object(), "circuitBreaker") == "true");
    // gate SUSPENDED: status says so, with its reasons, and ready is false
    rig.setConfig(makeConfig(money::GateState::SUSPENDED));
    r = rig.call("GET", "/api/v2/status");
    b = body(r);
    CHECK_MSG(str(b, "ready") == "false" && str(b.at("admission").as_array().at(0).as_object(), "gate") == "SUSPENDED" &&
            str(b.at("admission").as_array().at(0).as_object(), "reasons") == R"(["ADMIN"])", what(r));
    r = rig.call("GET", "/api/v2/markets?symbol=BTC");
    CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", what(r));
}

TEST(routes_create_and_idempotent_retry)
{
    Rig rig;
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    json::object b = body(r);
    CHECK_MSG(201 == r.status && str(b, "duplicate") == "false" && str(b, "status") == "PENDING_NEW" &&
            str(b, "marketId") == "1" && str(b, "orderId") == "1001" && str(b, "eventSeq") == "10010" &&
            str(b.at("hold").as_object(), "assetId") == "2" && str(b.at("hold").as_object(), "amount") == "50", what(r));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const auto &c = std::get<wire::CreateOrder>(rig.core.seen.back());
        CHECK(7 == c.userId && 0x1111111122224333LL == c.requestIdHigh && 1 == c.marketId && 100 * FP == c.price &&
            FP / 2 == c.quantity && money::TimeInForce::GTC == c.timeInForce);
    }
    // the same key again: 200 duplicate with the same orderId, one order in the core
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    b = body(r);
    CHECK_MSG(200 == r.status && str(b, "duplicate") == "true" && str(b, "orderId") == "1001", what(r));
    CHECK(1 == rig.core.applied.load());
    // a text key is the first 16 bytes of its SHA-256; another user with the same key is another request
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(8, KEY_A));
    CHECK_MSG(201 == r.status && str(body(r), "orderId") == "1002", what(r));
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(8, "client-retry-1"));
    CHECK_MSG(201 == r.status, what(r));
}

TEST(routes_create_shape_refusals)
{
    Rig rig;
    rig.start();
    struct Case
    {
        std::string body;
        const char *key;
        const char *code;
        const char *needle;
    };
    const std::vector<Case> cases = {
        { LIMIT_BUY, nullptr, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key" },
        { LIMIT_BUY, "00000000-0000-0000-0000-000000000000", "VALIDATION", "nil UUID" },
        { LIMIT_BUY, "has space", "VALIDATION", "visible ASCII" },
        { R"({"marketId":"1","side":"BUY","orderType":"LIMIT","timeInForce":"GTC","price":"1","quantity":"1"})", KEY_A,
            "VALIDATION", "orderType" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"1","quantity":"1","userId":"9"})",
            KEY_A, "VALIDATION", "userId" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","price":"1","quantity":"1"})", KEY_A, "VALIDATION", "timeInForce" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":100,"quantity":"1"})", KEY_A,
            "VALIDATION", "price" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"1.000000001","quantity":"1"})",
            KEY_A, "VALIDATION", "price" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"1e2","quantity":"1"})", KEY_A,
            "VALIDATION", "price" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"99999999999","quantity":"1"})",
            KEY_A, "VALIDATION", "price" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTD","price":"1","quantity":"1"})", KEY_A,
            "VALIDATION", "expiresAt" },
        { R"({"marketId":"1","side":"HOLD","type":"LIMIT","timeInForce":"GTC","price":"1","quantity":"1"})", KEY_A,
            "VALIDATION", "side" },
        { R"({"marketId":1,"side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"1","quantity":"1"})", KEY_A,
            "VALIDATION", "marketId" },
        { R"({"marketId":"1","side":"BUY","type":"MARKET","timeInForce":"IOC","price":"1","quantity":"1"})", KEY_A,
            "VALIDATION", "price" },
        { R"({"marketId":"1","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"1","quantity":"1","clientOrderId":"abc"})",
            KEY_A, "VALIDATION", "clientOrderId" },
    };
    for (const Case &c : cases)
    {
        const HttpResult r = rig.call("POST", "/api/v2/orders", c.body, as(7, c.key));
        CHECK_MSG(400 == r.status && str(body(r), "code") == c.code && r.body.find(c.needle) != std::string::npos &&
                str(body(r), "keyFinal") == "false", c.body + " -> " + what(r));
    }
    CHECK(rig.core.seen.empty()); // nothing reached the core
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, { { "Idempotency-Key", KEY_A } });
    CHECK_MSG(401 == r.status, what(r));
    // a GTD order with its expiry and a clientOrderId travel as given
    r = rig.call("POST", "/api/v2/orders", R"({"marketId":"1","side":"SELL","type":"LIMIT","timeInForce":"GTD","price":"1",)"
        R"("quantity":"1","expiresAt":1900000000000,"clientOrderId":"0fa1b2c3-d4e5-4f60-8172-93a4b5c6d7e8"})", as(7, KEY_A));
    CHECK_MSG(201 == r.status, what(r));
    std::lock_guard<std::mutex> l(rig.core.mutex);
    const auto &c = std::get<wire::CreateOrder>(rig.core.seen.back());
    CHECK(1'900'000'000'000 == c.expiresAtMs && 0x0fa1b2c3d4e54f60LL == c.clientOrderIdHigh && money::Side::SELL == c.side);
}

// Every reject reason through POST /orders: the status, code, reason and keyFinal of the table, Retry-After where set.
TEST(routes_reject_table_through_create)
{
    Rig rig;
    std::atomic<int> reason{ 0 };
    rig.core.hook = [&reason](const session::Command &c) -> std::optional<session::Outcome> {
        return rejectOf(static_cast<money::CoreRejectReason::Value>(reason.load()), session::templateOf(c));
    };
    rig.start();
    using R = money::CoreRejectReason;
    for (const R::Value v : { R::INSUFFICIENT_FUNDS, R::GATE_CLOSED, R::CIRCUIT_BREAKER, R::RATE_LIMIT,
             R::TOO_MANY_PENDING_REQUESTS, R::PRICE_OFF_TICK, R::UNKNOWN_MARKET, R::REQUEST_ID_PAYLOAD_MISMATCH,
             R::UNSUPPORTED_TIF, R::INVALID, R::UNAUTHORIZED_CLASS, R::HOLD_FAULT, R::ME_REJECTED })
    {
        reason = v;
        const rest::Mapping m = rest::mapOrderReject(v, wire::tid::CREATE_ORDER);
        const HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
        const json::object b = body(r);
        CHECK_MSG(static_cast<int>(m.status) == r.status && str(b, "code") == m.code && str(b, "reason") == R::c_str(v) &&
                str(b, "keyFinal") == (m.keyFinal ? "true" : "false") && (m.retryAfterMs >= 0) == (r.headers.count("retry-after") > 0),
            std::string(R::c_str(v)) + " -> " + what(r));
    }
    // gate SUSPENDED: a create is 503 ADMISSION_CLOSED, keyFinal false, with Retry-After
    reason = R::GATE_CLOSED;
    const HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "ADMISSION_CLOSED" && str(body(r), "keyFinal") == "false" &&
            r.headers.count("retry-after"), what(r));
}

TEST(routes_outcomes_504_503_overloaded)
{
    Rig rig;
    std::atomic<int> mode{ 0 };
    rig.core.hook = [&mode](const session::Command &) -> std::optional<session::Outcome> {
        switch (mode.load())
        {
            case 1: return kindOf(session::OutcomeKind::OUTCOME_UNKNOWN);
            case 2: return kindOf(session::OutcomeKind::CORE_UNAVAILABLE, 0);
            case 3: return kindOf(session::OutcomeKind::OVERLOADED, 0); // the query budget at the deadline
            case 4: return kindOf(session::OutcomeKind::INTERNAL);
            default: return std::nullopt;
        }
    };
    rig.start();
    mode = 1;
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(504 == r.status && str(body(r), "code") == "OUTCOME_UNKNOWN" &&
            r.body.find("same Idempotency-Key") != std::string::npos && r.body.find("900000") != std::string::npos, what(r));
    // the late reply: the core applied it after all, the retry with the same key gets the stored order
    mode = 0;
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(201 == r.status, what(r)); // (the fake applied nothing under the 504; the real core's row is in T1)
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(200 == r.status && str(body(r), "duplicate") == "true", what(r));
    mode = 1;
    r = rig.call("GET", "/api/v2/balances", "", as(7));
    CHECK_MSG(504 == r.status && r.body.find("nothing was changed") != std::string::npos, what(r));
    mode = 2;
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "CORE_UNAVAILABLE" && r.headers.count("retry-after"), what(r));
    mode = 3;
    r = rig.call("GET", "/api/v2/orders", "", as(9));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "OVERLOADED" && r.headers.count("retry-after"), what(r));
    mode = 4;
    r = rig.call("DELETE", "/api/v2/orders/5", "", as(7));
    CHECK_MSG(500 == r.status && str(body(r), "code") == "INTERNAL", what(r));
    mode = 0;
    rig.ringFull = true; // the command ring is full: nothing queued
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, "another-key"));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "OVERLOADED" && r.headers.count("retry-after"), what(r));
}

TEST(routes_cancel_reoffer_rule)
{
    Rig rig;
    std::atomic<int> mode{ 0 };
    rig.core.hook = [&mode](const session::Command &c) -> std::optional<session::Outcome> {
        using R = money::CoreRejectReason;
        switch (mode.load())
        {
            case 1: return rejectOf(R::UNKNOWN_ORDER, session::templateOf(c), 2); // offered twice, the order is gone
            case 2: return rejectOf(R::NOT_OPEN, session::templateOf(c), 3);
            case 3: return rejectOf(R::NOT_OWNER, session::templateOf(c), 2);
            case 4: return rejectOf(R::UNKNOWN_ORDER, session::templateOf(c), 1);
            case 5: return rejectOf(R::NOT_OPEN, session::templateOf(c), 1);
            default: return std::nullopt;
        }
    };
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    const std::string id = str(body(r), "orderId");
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7));
    CHECK_MSG(200 == r.status && str(body(r), "status") == "PENDING_CANCEL" && str(body(r), "duplicate") == "false" &&
            !body(r).contains("reoffered"), what(r));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const auto &c = std::get<wire::CancelOrder>(rig.core.seen.back());
        CHECK(0 == c.requestIdHigh && 0 == c.requestIdLow && 7 == c.userId); // the gateway mints no cancel key
    }
    mode = 1;
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7));
    CHECK_MSG(200 == r.status && r.body == R"({"orderId":")" + id + R"(","status":"NOT_OPEN","duplicate":true,"reoffered":true})",
        "successful cancel answered NOT_FOUND: " + what(r));
    mode = 2;
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7));
    CHECK_MSG(200 == r.status && str(body(r), "reoffered") == "true", "successful cancel answered NOT_FOUND: " + what(r));
    mode = 3; // NOT_OWNER stays 404 even after a re-offer
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7));
    CHECK_MSG(404 == r.status && str(body(r), "code") == "NOT_FOUND", what(r));
    mode = 4; // a single-offer cancel of an unknown id is 404
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7));
    CHECK_MSG(404 == r.status, what(r));
    mode = 5;
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7));
    CHECK_MSG(409 == r.status && str(body(r), "code") == "CONFLICT", what(r));
    mode = 1; // a keyed cancel is not covered by the rule: its key replays the exact answer
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(7, "cancel-key-1"));
    CHECK_MSG(404 == r.status, what(r));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const auto &c = std::get<wire::CancelOrder>(rig.core.seen.back());
        CHECK(0 != c.requestIdHigh || 0 != c.requestIdLow);
    }
    mode = 0;
    r = rig.call("DELETE", "/api/v2/orders/abc", "", as(7));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("DELETE", "/api/v2/orders/99999999999999999999", "", as(7));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("PATCH", "/api/v2/orders/5", "", as(7));
    CHECK_MSG(405 == r.status && r.headers["allow"] == "GET, PUT, DELETE", what(r));
}

TEST(routes_amend_and_get)
{
    Rig rig;
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    const std::string id = str(body(r), "orderId");
    r = rig.call("PUT", "/api/v2/orders/" + id, R"({"price":"101"})", as(7));
    CHECK_MSG(400 == r.status && str(body(r), "code") == "MISSING_IDEMPOTENCY_KEY", what(r));
    r = rig.call("PUT", "/api/v2/orders/" + id, R"({})", as(7, "amend-1"));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("PUT", "/api/v2/orders/" + id, R"({"price":"0"})", as(7, "amend-1"));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("PUT", "/api/v2/orders/" + id, R"({"price":"101","quantity":"2"})", as(7, "amend-1"));
    CHECK_MSG(200 == r.status && r.body == R"({"orderId":")" + id + R"(","status":"PENDING_REPLACE","duplicate":false})", what(r));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const auto &c = std::get<wire::AmendOrder>(rig.core.seen.back());
        CHECK(101 * FP == c.newPrice && 2 * FP == c.newQuantity && 7 == c.userId && std::stoll(id) == c.orderId);
    }
    r = rig.call("GET", "/api/v2/orders/" + id, "", as(7));
    json::object b = body(r);
    CHECK_MSG(200 == r.status && str(b, "orderId") == id && str(b, "price") == "100" && str(b, "quantity") == "0.5" &&
            str(b, "type") == "LIMIT" && str(b, "timeInForce") == "GTC" && str(b, "clientOrderId") == "null" &&
            b.contains("displayQuantity") && b.contains("budget") && !b.at("hold").as_object().contains("remaining"), what(r));
    r = rig.call("GET", "/api/v2/orders/" + id, "", as(8)); // another user's order: never shown
    CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE", what(r));
    r = rig.call("GET", "/api/v2/orders/424242", "", as(7));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE", what(r));
    // no retry loop: an order that is not open is not served until history exists, so it waits as long as history does
    CHECK_MSG(r.headers["retry-after"] == "60", "order read not open: Retry-After " + r.headers["retry-after"]);
}

TEST(routes_open_orders_pages_and_read_cache)
{
    Rig rig;
    rig.g.queryMinIntervalMs = 60'000;
    rig.start();
    for (int i = 0; i < 5; ++i)
    {
        const std::string key = "k-" + std::to_string(i);
        const std::string market = i % 2 == 0 ? "1" : "2";
        const HttpResult r = rig.call("POST", "/api/v2/orders", R"({"marketId":")" + market +
            R"(","side":"BUY","type":"LIMIT","timeInForce":"GTC","price":"100","quantity":"1"})", as(7, key.c_str()));
        CHECK(201 == r.status);
    }
    rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(8, KEY_A)); // another user's order
    const std::size_t before = rig.core.count(wire::tid::REQUEST_ORDER_SNAPSHOT);
    HttpResult r = rig.call("GET", "/api/v2/orders", "", as(7));
    json::object b = body(r);
    CHECK_MSG(200 == r.status && b.at("orders").as_array().size() == 5, what(r)); // three pages of two joined
    CHECK(rig.core.count(wire::tid::REQUEST_ORDER_SNAPSHOT) - before == 3);
    CHECK(b.at("asOf").as_object().contains("logPosition"));
    // the identical read inside the interval: from the cache, no query
    r = rig.call("GET", "/api/v2/orders", "", as(7));
    CHECK_MSG(200 == r.status && body(r).at("orders").as_array().size() == 5, what(r));
    CHECK(rig.core.count(wire::tid::REQUEST_ORDER_SNAPSHOT) - before == 3);
    r = rig.call("GET", "/api/v2/orders?marketId=2", "", as(7));
    CHECK_MSG(200 == r.status && body(r).at("orders").as_array().size() == 2, what(r));
    // a command of the user completed here drops the cache
    r = rig.call("DELETE", "/api/v2/orders/1001", "", as(7));
    CHECK(200 == r.status);
    r = rig.call("GET", "/api/v2/orders", "", as(7));
    CHECK_MSG(200 == r.status && body(r).at("orders").as_array().size() == 4, what(r));
    r = rig.call("GET", "/api/v2/orders?marketId=x", "", as(7));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("GET", "/api/v2/orders?status=NEW", "", as(7));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("GET", "/api/v2/orders?userId=8", "", as(7));
    CHECK_MSG(400 == r.status && r.body.find("userId") != std::string::npos, what(r));
}

TEST(routes_bulk_cancel)
{
    Rig rig;
    rig.g.bulkCancelMax = 3;
    rig.start();
    std::vector<std::string> mine;
    for (int i = 0; i < 6; ++i)
    {
        const std::string key = "b-" + std::to_string(i);
        mine.push_back(str(body(rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, key.c_str()))), "orderId"));
    }
    const std::string foreign = str(body(rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(8, KEY_A))), "orderId");
    CHECK(200 == rig.call("DELETE", "/api/v2/orders/" + mine[1], "", as(7)).status); // own, closed
    const std::size_t cancelsBefore = rig.core.count(wire::tid::CANCEL_ORDER);
    // mixed set: own open, own closed, foreign, unknown
    HttpResult r = rig.call("POST", "/api/v2/orders/cancel",
        R"({"orderIds":[")" + mine[0] + R"(",")" + mine[1] + R"(",")" + foreign + R"(","777777"]})", as(7));
    json::object b = body(r);
    CHECK_MSG(200 == r.status && str(b, "requested") == "4" && str(b, "accepted") == "1" && str(b, "truncated") == "true" &&
            str(b, "remaining") == "1", what(r));
    const json::array res = b.at("results").as_array();
    CHECK_MSG(res.size() == 3 && str(res.at(0).as_object(), "state") == "CANCEL_PENDING" &&
            str(res.at(1).as_object(), "state") == "NOT_OPEN" && str(res.at(2).as_object(), "state") == "NOT_FOUND", what(r));
    CHECK(rig.core.count(wire::tid::CANCEL_ORDER) - cancelsBefore == 3);
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        for (const auto &c : rig.core.seen)
        {
            if (const auto *x = std::get_if<wire::CancelOrder>(&c))
            {
                CHECK(0 == x->requestIdHigh && 0 == x->requestIdLow); // no requestId entry consumed
            }
        }
    }
    // all:true above the bound: the first 3 open orders in orderId order, truncated, remaining
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"all":true})", as(7));
    b = body(r);
    CHECK_MSG(200 == r.status && str(b, "requested") == "4" && str(b, "accepted") == "3" && str(b, "truncated") == "true" &&
            str(b, "remaining") == "1", what(r));
    CHECK(str(b.at("results").as_array().at(0).as_object(), "orderId") == mine[2]);
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"all":true})", as(7));
    b = body(r);
    CHECK_MSG(200 == r.status && str(b, "requested") == "1" && str(b, "truncated") == "false" && str(b, "remaining") == "0", what(r));
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"all":true})", as(7));
    CHECK_MSG(200 == r.status && str(body(r), "requested") == "0", what(r));
    for (const std::string &bad : { std::string(R"({"orderIds":["1","1"]})"), std::string(R"({"orderIds":[]})"),
             std::string(R"({"orderIds":[1]})"), std::string(R"({"orderIds":["1"],"all":true})"), std::string(R"({})"),
             std::string(R"({"all":false})"), std::string(R"({"orderIds":["1"],"marketId":"1"})") })
    {
        r = rig.call("POST", "/api/v2/orders/cancel", bad, as(7));
        CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", bad + " -> " + what(r));
    }
    // a re-offered unkeyed cancel that finds the order gone: NOT_OPEN with reoffered
    rig.core.hook = [](const session::Command &c) -> std::optional<session::Outcome> {
        return std::holds_alternative<wire::CancelOrder>(c)
            ? std::optional<session::Outcome>(rejectOf(money::CoreRejectReason::UNKNOWN_ORDER, wire::tid::CANCEL_ORDER, 2))
            : std::nullopt;
    };
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"orderIds":["5"]})", as(7));
    CHECK_MSG(200 == r.status && r.body.find(R"("state":"NOT_OPEN","reoffered":true)") != std::string::npos, what(r));
}

TEST(routes_balances)
{
    Rig rig;
    rig.g.queryMinIntervalMs = 60'000;
    rig.start();
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        rig.core.available[{ 7, 2 }] = 12 * FP + 5;
        rig.core.available[{ 8, 1 }] = FP;
    }
    HttpResult r = rig.call("GET", "/api/v2/balances", "", as(7));
    json::object b = body(r);
    CHECK_MSG(200 == r.status && str(b.at("asOf").as_object(), "logPosition") == "777", what(r));
    const json::array a = b.at("assets").as_array();
    CHECK_MSG(a.size() == 2 && str(a.at(0).as_object(), "asset") == "BTC" && str(a.at(0).as_object(), "available") == "0" &&
            str(a.at(1).as_object(), "asset") == "USD" && str(a.at(1).as_object(), "available") == "12.00000005" &&
            str(a.at(1).as_object(), "locked") == "3" && str(a.at(1).as_object(), "total") == "15.00000005", what(r));
    const std::size_t asked = rig.core.count(wire::tid::REQUEST_USER_BALANCES);
    r = rig.call("GET", "/api/v2/balances", "", as(7));
    CHECK(200 == r.status && rig.core.count(wire::tid::REQUEST_USER_BALANCES) == asked); // the read cache
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const auto &q = std::get<wire::RequestUserBalances>(rig.core.seen.back());
        CHECK(7 == q.userId);
    }
    r = rig.call("GET", "/api/v2/balances", "", { { "Authorization", "Bearer dev:7" }, { "Content-Type", "x" } });
    CHECK(200 == r.status);
    r = rig.call("GET", "/api/v2/balances");
    CHECK_MSG(401 == r.status, what(r));
}

TEST(routes_principal_budget_and_history)
{
    Rig rig;
    rig.g.principalRate = Rate{ 1, 60'000, "1/min" };
    rig.g.principalBurst = 3;
    rig.start();
    for (int i = 0; i < 3; ++i)
    {
        // an order read takes a token every time (it has no read cache): 503, the order is not open
        CHECK(503 == rig.call("GET", "/api/v2/orders/5", "", as(7)).status);
    }
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(429 == r.status && str(body(r), "code") == "RATE_LIMITED" && r.headers.count("retry-after") &&
            str(body(r), "keyFinal") == "false", what(r));
    r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(8, KEY_A)); // another principal has its own budget
    CHECK_MSG(201 == r.status, what(r));
    r = rig.call("GET", "/api/v2/orders/history?limit=10", "", as(7));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE", what(r));
    r = rig.call("GET", "/api/v2/fills", "", as(7));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE", what(r));
    r = rig.call("GET", "/api/v2/fills?offset=3", "", as(7));
    CHECK_MSG(400 == r.status, what(r));
    r = rig.call("GET", "/api/v2/fills");
    CHECK_MSG(401 == r.status, what(r));
}

TEST(routes_treasury_lane_off)
{
    Rig rig;
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"5"})", as(7, KEY_A));
    CHECK_MSG(404 == r.status && str(body(r), "code") == "FEATURE_DISABLED", what(r));
    r = rig.call("POST", "/api/v2/balances/withdraw", R"({"assetId":"2","amount":"5"})", as(7, KEY_A));
    CHECK_MSG(404 == r.status && str(body(r), "code") == "FEATURE_DISABLED", what(r));
    r = rig.call("POST", "/api/v2/auth/register", R"({"username":"alice","password":"secret-1"})");
    CHECK_MSG(201 == r.status && str(body(r), "funded") == "false", what(r));
    CHECK(0 == rig.core.count(wire::tid::DEPOSIT));
}

TEST(routes_treasury_lane_on)
{
    Rig rig;
    rig.treasury = true;
    rig.g.faucetRate = Rate{ 1, 60'000, "1/min" };
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"5.5"})", as(7, KEY_A));
    CHECK_MSG(200 == r.status && r.body == R"({"assetId":"2","amount":"5.5","available":"5.5","duplicate":false})", what(r));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const auto &d = std::get<session::Deposit>(rig.core.seen.back());
        CHECK(7 == d.command.userId && 0x1111111122224333LL == d.command.requestIdHigh); // the principal, the client's key
    }
    // the faucet rate (one per minute here) refuses the next request of this principal
    r = rig.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"5.5"})", as(7, KEY_A));
    CHECK_MSG(429 == r.status && str(body(r), "code") == "RATE_LIMITED" && r.headers.count("retry-after"), what(r));

    Rig open;
    open.treasury = true;
    open.start();
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"5.5"})", as(7, KEY_A));
    CHECK(200 == r.status);
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"5.5"})", as(7, KEY_A));
    CHECK_MSG(200 == r.status && str(body(r), "duplicate") == "true" && str(body(r), "available") == "5.5", what(r));
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"10000.00000001"})", as(7, "k2"));
    CHECK_MSG(422 == r.status && str(body(r), "code") == "REJECTED" && r.body.find("10000") != std::string::npos, what(r));
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"1","amount":"2"})", as(7, "k3"));
    CHECK_MSG(200 == r.status, what(r));
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"0"})", as(7, "k4"));
    CHECK_MSG(400 == r.status, what(r));
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"1"})", as(7));
    CHECK_MSG(400 == r.status && str(body(r), "code") == "MISSING_IDEMPOTENCY_KEY", what(r));
    r = open.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"1","userId":"8"})", as(7, "k5"));
    CHECK_MSG(400 == r.status && r.body.find("userId") != std::string::npos, what(r));
    r = open.call("POST", "/api/v2/balances/withdraw", R"({"assetId":"2","amount":"6"})", as(7, "w1"));
    CHECK_MSG(422 == r.status && str(body(r), "reason") == "INSUFFICIENT_FUNDS" && str(body(r), "keyFinal") == "false", what(r));
    r = open.call("POST", "/api/v2/balances/withdraw", R"({"assetId":"2","amount":"0.5"})", as(7, "w2"));
    CHECK_MSG(200 == r.status && str(body(r), "available") == "5", what(r));
}

// No silent drop at the edge: an assetId the core's asset list does not hold never reaches the core (its decoder would drop the
// frame unanswered).
TEST(routes_treasury_unknown_asset)
{
    Rig rig;
    rig.treasury = true;
    rig.g.faucetMaxAmount.push_back(FaucetCap{ 9, "1" }); // a cap for an asset the core does not list
    rig.core.hook = [](const session::Command &c) -> std::optional<session::Outcome> {
        if (const auto *d = std::get_if<session::Deposit>(&c); d && 9 == d->command.assetId)
        {
            return kindOf(session::OutcomeKind::OUTCOME_UNKNOWN); // the decode drop: never answered
        }
        return std::nullopt;
    };
    rig.start();
    const HttpResult r = rig.call("POST", "/api/v2/balances/deposit", R"({"assetId":"9","amount":"1"})", as(7, KEY_A));
    CHECK_MSG(0 == rig.core.count(wire::tid::DEPOSIT), "request unanswered: decode drop (" + what(r) + ")");
    CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION" && r.body.find("assetId 9") != std::string::npos, what(r));
}

// Register funding: the derived requestId makes a lost ack credit once; `funded` says every deposit was acked.
TEST(reoffer_window_check_once_per_config_version)
{
    // the window must be above twice the request timeout; the check speaks once per loaded config version
    CHECK(reofferFitsWindow(10'002, 5000) && !reofferFitsWindow(10'000, 5000) && !reofferFitsWindow(0, 5000));
    ReofferWindowCheck check(5000);
    CHECK_MSG(!check.check(session::CoreConfig{}).has_value(), "a config not yet read was checked");
    const session::CoreConfig fits = *makeConfig(); // version 3, window 900000
    CHECK_MSG(!check.check(fits).has_value(), "a 900000 ms window refused against a 5000 ms timeout");
    session::CoreConfig shortWindow = fits;
    shortWindow.head.configVersion = 4;
    shortWindow.head.requestTtlMs = 10'000;
    const auto line = check.check(shortWindow);
    CHECK_MSG(line && std::string::npos != line->find("GATEWAY_REQUEST_TIMEOUT_MS") &&
            std::string::npos != line->find("version 4"),
        line ? *line : std::string("no line for a 10000 ms window against a 5000 ms timeout"));
    CHECK_MSG(!check.check(shortWindow).has_value(), "the same config version reported twice");
    shortWindow.head.configVersion = 5;
    CHECK_MSG(check.check(shortWindow).has_value(), "a new config version with the short window not reported");
}

TEST(routes_register_funding)
{
    Rig rig;
    rig.treasury = true;
    std::atomic<int> deposits{ 0 };
    rig.core.hook = [&rig, &deposits](const session::Command &c) -> std::optional<session::Outcome> {
        if (std::holds_alternative<session::Deposit>(c) && 1 == ++deposits)
        {
            rig.core.model(c); // the core applies the first deposit, and its ack is lost
            return kindOf(session::OutcomeKind::OUTCOME_UNKNOWN);
        }
        return std::nullopt;
    };
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/auth/register", R"({"username":"alice","password":"secret-1"})");
    CHECK_MSG(201 == r.status && str(body(r), "funded") == "true", what(r));
    const std::int64_t user = std::stoll(str(body(r), "userId"));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        const std::int64_t btc = rig.core.available[{ user, 1 }];
        const std::int64_t usd = rig.core.available[{ user, 2 }];
        CHECK_MSG(btc == 2 * FP && usd == 10'000 * FP, "registration funded twice (BTC " + rest::formatMoney(btc) +
            ", USD " + rest::formatMoney(usd) + ")");
        std::int64_t h = 0, l2 = 0;
        rest::fundingRequestId(user, 1, h, l2);
        CHECK(rig.core.moneyKeys.count({ user, h, l2 }) == 1);
    }
    CHECK(3 == rig.core.count(wire::tid::DEPOSIT)); // two assets, one offered again
    // a funding deposit refused: funded false, the user is still created
    rig.core.hook = [](const session::Command &c) -> std::optional<session::Outcome> {
        if (std::holds_alternative<session::Deposit>(c))
        {
            session::Outcome o;
            o.templateId = wire::tid::DEPOSIT;
            wire::DepositReject d;
            d.reason = money::RejectReason::BALANCE_OVERFLOW;
            o.reply = d;
            return o;
        }
        return std::nullopt;
    };
    r = rig.call("POST", "/api/v2/auth/register", R"({"username":"bob","password":"secret-1"})");
    CHECK_MSG(201 == r.status && str(body(r), "funded") == "false", what(r));
}

// Cancel-all at the default bucket (burst 100) with more open orders than the bucket holds: a full bucket pays for the
// whole request (the read plus at most burst - 1 cancels), so the request that matters most is never refused on a full
// bucket.
TEST(routes_bulk_cancel_all_full_bucket)
{
    Rig rig;
    rig.g.principalRate = Rate{ 1, 60'000, "1/min" }; // no refill inside the test
    rig.g.principalBurst = 100;
    rig.start();
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        rig.core.pageSize = 1000;
        wire::CreateOrder c;
        c.marketId = 1;
        c.price = 100 * FP;
        c.quantity = FP;
        for (std::int64_t id = 5001; id <= 5150; ++id)
        {
            rig.core.orders[id] = FakeCore::Order{ 7, true, c };
        }
    }
    HttpResult r = rig.call("POST", "/api/v2/orders/cancel", R"({"all":true})", as(7));
    json::object b = body(r);
    CHECK_MSG(200 == r.status && str(b, "requested") == "150" && str(b, "accepted") == "150" &&
            str(b, "truncated") == "false" && str(b, "remaining") == "0",
        "cancel-all refused on a full bucket: " + what(r));
    CHECK_MSG(150 == rig.core.count(wire::tid::CANCEL_ORDER), "cancels sent " + std::to_string(rig.core.count(wire::tid::CANCEL_ORDER)));
    // the bucket is spent now: the next request waits for the refill (429 with Retry-After), never a partial answer
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"all":true})", as(7));
    CHECK_MSG(429 == r.status && r.headers.count("retry-after"), what(r));
}

// Register funding sends its second attempt only while the idempotence window is long enough that the derived
// requestId is still remembered: window above twice the request timeout. Otherwise funded is false and nothing is
// re-sent (a re-send after the window would credit twice).
TEST(routes_register_funding_window)
{
    Rig rig;
    rig.treasury = true;
    auto c = std::make_shared<session::CoreConfig>(*makeConfig());
    c->head.requestTtlMs = 2 * rig.g.requestTimeoutMs; // not above twice the timeout
    rig.setConfig(c);
    std::atomic<int> deposits{ 0 };
    rig.core.hook = [&rig, &deposits](const session::Command &cmd) -> std::optional<session::Outcome> {
        if (std::holds_alternative<session::Deposit>(cmd) && 1 == ++deposits)
        {
            rig.core.model(cmd); // applied, the ack lost
            return kindOf(session::OutcomeKind::OUTCOME_UNKNOWN);
        }
        return std::nullopt;
    };
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/auth/register", R"({"username":"carol","password":"secret-1"})");
    CHECK_MSG(201 == r.status && str(body(r), "funded") == "false", what(r));
    CHECK_MSG(2 == rig.core.count(wire::tid::DEPOSIT),
        "funding retried outside the idempotence window (" + std::to_string(rig.core.count(wire::tid::DEPOSIT)) + " deposits)");
}

// Principal binding: a userId in a body or a query is refused, and a cancel always carries the principal's userId, so
// another user's order answers 404 and stays open.
TEST(routes_cancel_owner_binding)
{
    Rig rig;
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    const std::string id = str(body(r), "orderId");
    r = rig.call("DELETE", "/api/v2/orders/" + id, "", as(8));
    CHECK_MSG(404 == r.status && str(body(r), "code") == "NOT_FOUND", what(r));
    r = rig.call("DELETE", "/api/v2/orders/" + id + "?userId=7", "", as(8));
    CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", what(r));
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"orderIds":[")" + id + R"("],"userId":"7"})", as(8));
    CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", what(r));
    r = rig.call("POST", "/api/v2/orders/cancel", R"({"all":true,"user_id":"7"})", as(8));
    CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", what(r));
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        CHECK_MSG(rig.core.orders.at(std::stoll(id)).open, "cancel of another user's order accepted");
        for (const auto &c : rig.core.seen)
        {
            if (const auto *x = std::get_if<wire::CancelOrder>(&c))
            {
                CHECK_MSG(8 == x->userId, "cancel of another user's order accepted (cancel sent as user " +
                    std::to_string(x->userId) + ")");
            }
        }
    }
}

// keyFinal is about a key: an unkeyed cancel's reject never says keyFinal true (there is no key to replay); a keyed one
// keeps the table's value.
TEST(routes_cancel_keyfinal_unkeyed)
{
    Rig rig;
    rig.core.hook = [](const session::Command &c) -> std::optional<session::Outcome> {
        return std::holds_alternative<wire::CancelOrder>(c)
            ? std::optional<session::Outcome>(rejectOf(money::CoreRejectReason::RATE_LIMIT, wire::tid::CANCEL_ORDER))
            : std::nullopt;
    };
    rig.start();
    HttpResult r = rig.call("DELETE", "/api/v2/orders/5", "", as(7));
    CHECK_MSG(429 == r.status && str(body(r), "keyFinal") == "false", "unkeyed cancel answered keyFinal true: " + what(r));
    r = rig.call("DELETE", "/api/v2/orders/5", "", as(7, "cancel-key-2"));
    CHECK_MSG(429 == r.status && str(body(r), "keyFinal") == "true", what(r));
}

// A deposit or withdraw whose outcome is unknown: the 504 names the window and the faucet rate the retry also takes.
TEST(routes_treasury_504_names_faucet_rate)
{
    Rig rig;
    rig.treasury = true;
    rig.core.hook = [](const session::Command &c) -> std::optional<session::Outcome> {
        return std::holds_alternative<session::Deposit>(c) ? std::optional<session::Outcome>(kindOf(session::OutcomeKind::OUTCOME_UNKNOWN))
                                                             : std::nullopt;
    };
    rig.start();
    const HttpResult r = rig.call("POST", "/api/v2/balances/deposit", R"({"assetId":"2","amount":"1"})", as(7, KEY_A));
    CHECK_MSG(504 == r.status && str(body(r), "code") == "OUTCOME_UNKNOWN" &&
            str(body(r), "detail").find("GATEWAY_FAUCET_RATE") != std::string::npos, what(r));
}

// ---------------------------------------------------------------- history (the read layer over a stub store)

namespace {

const std::string HISTORY_ORDER = R"({"orderId":"900","clientOrderId":null,"marketId":"1","side":"BUY","type":"LIMIT",)"
    R"("timeInForce":"GTC","price":"100","stopPrice":null,"quantity":"1","filledQuantity":"1","remainingQuantity":"0",)"
    R"("status":"FILLED","reason":null,"meReason":null,"meOrderId":"55","hold":{"assetId":"2","amount":"100",)"
    R"("remaining":"0"},"createdAt":1800000000000,"updatedAt":1800000000500})";

std::string cursorFor(history::Kind kind, const history::Filter &f = {})
{
    return history::encodeCursor(kind, f, history::Key{ 1'800'000'000'000, 900, 901 });
}

bool waitFor(const std::function<bool()> &done, int ms = 5000)
{
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (!done() && std::chrono::steady_clock::now() < until)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return done();
}

}

TEST(routes_history_validation)
{
    StubHistory stub;
    Rig rig;
    rig.g.historyLimitMax = 50;
    rig.history = &stub;
    rig.start();
    history::Filter market1;
    market1.marketId = 1;
    const std::vector<std::pair<std::string, std::string>> bad = {
        { "/api/v2/orders/history?limit=0", "limit" },
        { "/api/v2/orders/history?limit=51", "limit" },          // above GATEWAY_HISTORY_LIMIT_MAX: 400, never clamped
        { "/api/v2/orders/history?limit=abc", "limit" },
        { "/api/v2/orders/history?limit=-1", "limit" },
        { "/api/v2/orders/history?limit=", "limit" },
        { "/api/v2/orders/history?marketId=0", "marketId" },
        { "/api/v2/orders/history?status=OPEN", "status" },
        { "/api/v2/orders/history?orderId=5", "orderId" },        // fills only
        { "/api/v2/fills?status=FILLED", "status" },              // orders only
        { "/api/v2/fills?orderId=x", "orderId" },
        { "/api/v2/orders/history?offset=10", "offset" },         // the old paging is refused, never ignored
        { "/api/v2/orders/history?userId=8", "userId" },
        { "/api/v2/orders/history?cursor=", "cursor" },
        { "/api/v2/orders/history?cursor=%%%", "escape" },
        { "/api/v2/orders/history?cursor=bm90LWEtY3Vyc29y", "cursor" },                      // "not-a-cursor"
        { "/api/v2/orders/history?cursor=" + cursorFor(history::Kind::FILLS), "another route" },
        { "/api/v2/fills?cursor=" + cursorFor(history::Kind::ORDERS), "another route" },
        { "/api/v2/orders/history?cursor=" + cursorFor(history::Kind::ORDERS, market1), "another filter" },
        { "/api/v2/orders/history?marketId=2&cursor=" + cursorFor(history::Kind::ORDERS, market1), "another filter" },
    };
    for (const auto &[target, why] : bad)
    {
        const HttpResult r = rig.call("GET", target, "", as(7));
        CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", target + " (" + why + "): " + what(r));
    }
    // a trail cursor of another order: the hint names the order, not query fields the trail does not take
    {
        history::Filter order3;
        order3.orderId = 3;
        const HttpResult r = rig.call("GET", "/api/v2/orders/1/trail?cursor=" + cursorFor(history::Kind::TRAIL, order3),
            "", as(7));
        CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION" &&
            str(body(r), "detail") == "the cursor was issued for another order", what(r));
    }
    CHECK_MSG(0 == stub.calls.load(), "a refused request reached the store");
    // the same cursor with its own filter is accepted
    const HttpResult ok = rig.call("GET", "/api/v2/orders/history?marketId=1&cursor=" +
        cursorFor(history::Kind::ORDERS, market1), "", as(7));
    CHECK_MSG(200 == ok.status, what(ok));
    CHECK(401 == rig.call("GET", "/api/v2/orders/history").status);
}

TEST(routes_history_pages)
{
    StubHistory stub;
    stub.canned.items = { HISTORY_ORDER };
    stub.canned.nextCursor = "next-page";
    stub.canned.ageMs = 1234;
    Rig rig;
    rig.history = &stub;
    rig.start();
    HttpResult r = rig.call("GET", "/api/v2/orders/history", "", as(7));
    json::object b = body(r);
    CHECK_MSG(200 == r.status && str(b, "nextCursor") == "next-page" && b.at("items").as_array().size() == 1 &&
            str(b.at("asOf").as_object(), "ageMs") == "1234" &&
            str(b.at("items").as_array().at(0).as_object(), "status") == "FILLED", what(r));
    r = rig.call("GET", "/api/v2/orders/history?limit=7&marketId=1&status=CANCELLED", "", as(8));
    CHECK_MSG(200 == r.status, what(r));
    stub.canned.nextCursor.reset();
    stub.canned.items.clear();
    r = rig.call("GET", "/api/v2/fills?limit=1000&orderId=900", "", as(9));
    CHECK_MSG(200 == r.status && r.body == R"({"items":[],"nextCursor":null,"asOf":{"ageMs":1234,"stale":false,"eventSeq":0}})",
        what(r));
    std::lock_guard<std::mutex> l(stub.mutex);
    CHECK(stub.requests.size() == 3);
    if (stub.requests.size() == 3)
    {
        const auto &a = stub.requests[0];
        CHECK(history::Kind::ORDERS == a.kind && 7 == a.userId && 100 == a.limit && !a.after && 0 == a.filter.marketId);
        const auto &c = stub.requests[1];
        CHECK(8 == c.userId && 7 == c.limit && 1 == c.filter.marketId && "CANCELLED" == c.filter.status);
        const auto &f = stub.requests[2];
        CHECK(history::Kind::FILLS == f.kind && 9 == f.userId && 1000 == f.limit && 900 == f.filter.orderId);
    }
}

TEST(routes_history_unavailable_and_busy)
{
    {
        Rig rig; // no GATEWAY_PG_URL: no store
        rig.start();
        for (const char *target : { "/api/v2/orders/history", "/api/v2/fills" })
        {
            HttpResult r = rig.call("GET", target, "", as(7));
            CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE" && r.headers["retry-after"] == "60",
                what(r));
        }
        CHECK(rig.api->metrics("gw_").find("history_reads_total{query=\"fills\",result=\"unavailable\",cause=\"unconfigured\"} 1") != std::string::npos);
    }
    StubHistory stub;
    Rig rig;
    rig.g.historyRetryAfterMs = 30'000;
    rig.historyThreads = 1;
    rig.historyQueue = 1;
    rig.history = &stub;
    rig.start();
    // every cause is one public body; the cause is in the metrics
    std::string first;
    for (HistoryCause cause : { HistoryCause::ABSENT, HistoryCause::UNREACHABLE, HistoryCause::TIMEOUT,
             HistoryCause::NO_HEARTBEAT })
    {
        stub.answer = HistoryAnswer{ HistoryStatus::UNAVAILABLE, cause };
        HttpResult r = rig.call("GET", "/api/v2/fills", "", as(7));
        CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE" && r.headers["retry-after"] == "30",
            what(r));
        first = first.empty() ? r.body : first;
        CHECK_MSG(r.body == first, std::string("the body names the cause ") + nameOf(cause) + ": " + r.body);
    }
    const std::string m = rig.api->metrics("gw_");
    for (const char *cause : { "absent", "unreachable", "timeout", "no_heartbeat" })
    {
        CHECK_MSG(m.find("history_reads_total{query=\"fills\",result=\"unavailable\",cause=\"" + std::string(cause) +
            "\"} 1") != std::string::npos, std::string("no metric for ") + cause);
    }
    // one worker, a queue of one: the third concurrent read is 503 HISTORY_BUSY at once
    stub.answer = HistoryAnswer{ HistoryStatus::OK, HistoryCause::NONE };
    stub.hold(true);
    std::vector<std::thread> readers;
    std::vector<HttpResult> results(2);
    for (int i = 0; i < 2; ++i)
    {
        readers.emplace_back([&rig, &results, i] { results[i] = rig.call("GET", "/api/v2/orders/history", "", as(7)); });
        if (0 == i)
        {
            CHECK(waitFor([&stub] { return 1 == stub.waiting.load(); }));
        }
    }
    CHECK(waitFor([&rig] { return 1 == rig.historyPool->queued(); }));
    const HttpResult busy = rig.call("GET", "/api/v2/orders/history", "", as(7));
    CHECK_MSG(503 == busy.status && str(body(busy), "code") == "HISTORY_BUSY" && busy.headers.count("retry-after") &&
            busy.ms < 1000, what(busy));
    // the order path never waits on history
    const HttpResult order = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(201 == order.status, what(order));
    stub.hold(false);
    for (std::thread &t : readers)
    {
        t.join();
    }
    CHECK_MSG(200 == results[0].status && 200 == results[1].status, what(results[0]) + " / " + what(results[1]));
    const std::string m2 = rig.api->metrics("gw_");
    CHECK(m2.find("gw_pg_history_refused_total 1") != std::string::npos);
    CHECK(m2.find("history_reads_total{query=\"orders\",result=\"busy\",cause=\"none\"} 1") != std::string::npos);
}

TEST(routes_history_queued_past_its_deadline_is_busy)
{
    StubHistory stub;
    Rig rig;
    rig.g.pgStatementTimeoutMs = 200; // one history read's deadline, queue wait included
    rig.historyThreads = 1;
    rig.historyQueue = 4;
    rig.history = &stub;
    rig.start();
    stub.hold(true);
    HttpResult first;
    HttpResult second;
    std::thread one([&] { first = rig.call("GET", "/api/v2/orders/history", "", as(7)); });
    CHECK(waitFor([&stub] { return 1 == stub.waiting.load(); }));
    std::thread two([&] { second = rig.call("GET", "/api/v2/fills", "", as(7)); });
    CHECK(waitFor([&rig] { return 1 == rig.historyPool->queued(); }));
    std::this_thread::sleep_for(std::chrono::milliseconds(400)); // past the queued read's deadline
    stub.hold(false);
    one.join();
    two.join();
    CHECK_MSG(200 == first.status, what(first)); // it started inside its deadline
    // the client of the queued read has waited out its deadline: 503 HISTORY_BUSY, the store is never asked
    CHECK_MSG(503 == second.status && str(body(second), "code") == "HISTORY_BUSY" && second.headers.count("retry-after"),
        what(second));
    CHECK_MSG(1 == stub.calls.load(), "store calls: " + std::to_string(stub.calls.load()));
    CHECK(rig.api->metrics("gw_").find("history_reads_total{query=\"fills\",result=\"busy\",cause=\"expired\"} 1") !=
        std::string::npos);
}

TEST(routes_order_from_history)
{
    StubHistory stub;
    Rig rig;
    rig.history = &stub;
    rig.start();
    HttpResult r = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    const std::string open = str(body(r), "orderId");
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        FakeCore::Order closed;
        closed.userId = 7;
        closed.open = false;
        rig.core.orders[900] = closed;
        FakeCore::Order foreign;
        foreign.userId = 8;
        foreign.open = false;
        rig.core.orders[901] = foreign;
    }
    stub.orders[{ 7, 900 }] = HISTORY_ORDER;
    stub.orders[{ 8, 901 }] = HISTORY_ORDER;
    // open: the core answers, history is not asked
    r = rig.call("GET", "/api/v2/orders/" + open, "", as(7));
    CHECK_MSG(200 == r.status && str(body(r), "orderId") == open && 0 == stub.calls.load(), what(r));
    // not open: the caller's row in history, with the read's asOf (stale above GATEWAY_HISTORY_STALE_MS, 5000)
    stub.canned.ageMs = 7000;
    stub.canned.eventSeq = 41;
    r = rig.call("GET", "/api/v2/orders/900", "", as(7));
    CHECK_MSG(200 == r.status && r.body == HISTORY_ORDER.substr(0, HISTORY_ORDER.size() - 1) +
        R"(,"asOf":{"ageMs":7000,"stale":true,"eventSeq":41}})", what(r));
    // another user's closed order, another user's open order, an unknown id: one 404 body for all three
    const HttpResult foreign = rig.call("GET", "/api/v2/orders/901", "", as(7));
    const HttpResult foreignOpen = rig.call("GET", "/api/v2/orders/" + open, "", as(8));
    const HttpResult unknown = rig.call("GET", "/api/v2/orders/424242", "", as(7));
    CHECK_MSG(404 == unknown.status && str(body(unknown), "code") == "NOT_FOUND", what(unknown));
    CHECK_MSG(foreign.status == unknown.status && foreign.body == unknown.body, what(foreign) + " vs " + what(unknown));
    CHECK_MSG(foreignOpen.status == unknown.status && foreignOpen.body == unknown.body, what(foreignOpen));
    // history down: today's 503 HISTORY_UNAVAILABLE with the history Retry-After
    stub.answer = HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::UNREACHABLE };
    r = rig.call("GET", "/api/v2/orders/900", "", as(7));
    CHECK_MSG(503 == r.status && str(body(r), "code") == "HISTORY_UNAVAILABLE" && r.headers["retry-after"] == "60",
        what(r));
    const std::string downBody = r.body;
    // not projected yet (above applied_order_id): the same body, the short Retry-After, never 404
    stub.answer = HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::LAGGING };
    r = rig.call("GET", "/api/v2/orders/902", "", as(7));
    CHECK_MSG(503 == r.status && r.body == downBody && r.headers["retry-after"] == "1", what(r));
    CHECK(rig.api->metrics("gw_").find("history_reads_total{query=\"order\",result=\"unavailable\",cause=\"lagging\"} 1") !=
        std::string::npos);
    // every store call carried one deadline, GATEWAY_PG_STATEMENT_TIMEOUT_MS (2000) from when it was queued
    {
        std::lock_guard<std::mutex> l(stub.mutex);
        const std::int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        for (const std::int64_t d : stub.deadlines)
        {
            CHECK_MSG(d > now - 2'000'000'000LL && d <= now + 2'000'000'000LL, "deadline " + std::to_string(d - now));
        }
        CHECK(!stub.deadlines.empty());
    }
}

TEST(routes_history_never_blocks_the_core_completion)
{
    StubHistory stub;
    Rig rig;
    rig.history = &stub;
    rig.start();
    {
        std::lock_guard<std::mutex> l(rig.core.mutex);
        FakeCore::Order closed;
        closed.userId = 7;
        closed.open = false;
        rig.core.orders[900] = closed;
    }
    stub.orders[{ 7, 900 }] = HISTORY_ORDER;
    stub.hold(true);
    HttpResult held;
    std::thread reader([&] { held = rig.call("GET", "/api/v2/orders/900", "", as(7)); });
    CHECK(waitFor([&stub] { return 1 == stub.waiting.load(); }));
    // the core's completion thread (one thread in this rig) handed the lookup over and is free: an order still goes
    const HttpResult order = rig.call("POST", "/api/v2/orders", LIMIT_BUY, as(7, KEY_A));
    CHECK_MSG(201 == order.status && order.ms < 1000, "core completion blocked by a history read: " + what(order));
    stub.hold(false);
    reader.join();
    CHECK_MSG(200 == held.status, what(held));
}

// GET /balances/movements: read from the money store only (a history store never stands in for it), the caller's
// own rows (the user comes from the credential, never the request), the asset filter and cursor checked before any
// read, the asset codes of the core config handed to the renderer.
TEST(routes_balance_movements)
{
    {
        // no money database: 503 MOVEMENTS_UNAVAILABLE, even with a history store at hand
        StubHistory hist;
        Rig rig;
        rig.history = &hist;
        rig.start();
        HttpResult r = rig.call("GET", "/api/v2/balances/movements", "", as(7));
        CHECK_MSG(503 == r.status && str(body(r), "code") == "MOVEMENTS_UNAVAILABLE" && !r.headers["retry-after"].empty(),
            what(r));
        CHECK_MSG(0 == hist.calls.load(), "the history store answered a movements read");
    }
    StubHistory money;
    Rig rig;
    rig.g.historyLimitMax = 50;
    rig.money = &money;
    rig.start();
    history::Filter usd;
    usd.assetId = 0;
    const std::vector<std::pair<std::string, std::string>> bad = {
        { "/api/v2/balances/movements?limit=0", "limit" },
        { "/api/v2/balances/movements?limit=51", "limit" },
        { "/api/v2/balances/movements?assetId=-1", "assetId" },
        { "/api/v2/balances/movements?assetId=01", "assetId" },
        { "/api/v2/balances/movements?assetId=x", "assetId" },
        { "/api/v2/balances/movements?userId=8", "userId" },
        { "/api/v2/balances/movements?marketId=1", "marketId" },
        { "/api/v2/balances/movements?cursor=" + cursorFor(history::Kind::FILLS), "another route" },
        { "/api/v2/balances/movements?assetId=1&cursor=" + cursorFor(history::Kind::MOVEMENTS, usd), "another filter" },
    };
    for (const auto &[target, why] : bad)
    {
        const HttpResult r = rig.call("GET", target, "", as(7));
        CHECK_MSG(400 == r.status && str(body(r), "code") == "VALIDATION", target + " (" + why + "): " + what(r));
    }
    CHECK_MSG(0 == money.calls.load(), "a refused request reached the store");
    CHECK(401 == rig.call("GET", "/api/v2/balances/movements").status);
    CHECK(405 == rig.call("POST", "/api/v2/balances/movements", "{}", as(7)).status);

    money.canned.items = { R"({"journalSeq":4,"legNo":1})", R"({"journalSeq":2,"legNo":0})" };
    money.canned.nextCursor = "NEXT";
    const HttpResult r = rig.call("GET", "/api/v2/balances/movements?assetId=0&limit=2&cursor=" +
        cursorFor(history::Kind::MOVEMENTS, usd), "", as(7));
    CHECK_MSG(200 == r.status, what(r));
    const json::object b = body(r);
    CHECK_MSG(b.at("items").as_array().size() == 2 && str(b, "nextCursor") == "NEXT" && !b.contains("asOf"), what(r));
    CHECK(1 == money.requests.size());
    if (1 == money.requests.size())
    {
        const history::PageRequest &q = money.requests[0];
        CHECK(history::Kind::MOVEMENTS == q.kind && 7 == q.userId && 0 == q.filter.assetId && 2 == q.limit && q.after);
        CHECK_MSG(!q.assetCodes.empty(), "the asset codes of the core config were not handed to the renderer");
    }
    // the database cannot answer: one 503 MOVEMENTS_UNAVAILABLE
    money.answer = HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::UNREACHABLE };
    const HttpResult down = rig.call("GET", "/api/v2/balances/movements", "", as(7));
    CHECK_MSG(503 == down.status && str(body(down), "code") == "MOVEMENTS_UNAVAILABLE", what(down));
}
