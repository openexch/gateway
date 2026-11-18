// The order, balance, market, status and treasury routes of oe-gateway (GatewayApi.h). Called on the I/O threads; a
// route reaches the core only through ApiDeps::askCore (the GATEWAY session) or askTreasury (the TREASURY session), and
// answers from the completion thread.
//
//   GET    /api/v2/markets                 public: the core config the session holds (503 CORE_UNAVAILABLE before one)
//   GET    /api/v2/status                  public: ready, behaviorVersion, idempotencyWindowMs, the gates (marketId 0 =
//                                          global)
//   POST   /api/v2/orders                  Idempotency-Key required: CreateOrder; 201 (duplicate false) or 200
//   GET    /api/v2/orders?marketId=        the caller's open orders, every page of RequestOrderSnapshot joined
//   GET    /api/v2/orders/{orderId}        the open order (one snapshot page from orderId); an order the core does not
//                                          hold open is the caller's row in history (gateway_order_history_v1), else
//                                          404 NOT_FOUND (unknown and another user's order: one answer)
//   PUT    /api/v2/orders/{orderId}        Idempotency-Key required: AmendOrder {price?, quantity?}
//   DELETE /api/v2/orders/{orderId}        Idempotency-Key optional: CancelOrder (the re-offer rule below)
//   POST   /api/v2/orders/cancel           {orderIds:[...]} or {all:true, marketId?}: unkeyed cancels, at most
//                                          GATEWAY_BULK_CANCEL_MAX per request (truncated, remaining)
//   GET    /api/v2/balances                RequestUserBalances joined with the config's asset list (zero included)
//   POST   /api/v2/balances/deposit, /withdraw   the treasury lane: 404 FEATURE_DISABLED when it is off
//   GET    /api/v2/orders/history?limit&cursor&marketId&status   the caller's orders in history, newest first
//   GET    /api/v2/fills?limit&cursor&marketId&orderId           the caller's fills in history, newest first
//          200 {items, nextCursor, asOf:{ageMs}}; keyset pages (History.h), limit GATEWAY_HISTORY_LIMIT_DEFAULT, above
//          GATEWAY_HISTORY_LIMIT_MAX 400; a cursor of another route or filter, or a malformed one, 400 VALIDATION
//
// History reads run on the history pool (gw-pg-hist, GATEWAY_PG_HISTORY_POOL connections of their own over a queue of
// GATEWAY_PG_QUEUE): a full queue is 503 HISTORY_BUSY (Retry-After GATEWAY_HISTORY_BUSY_RETRY_AFTER_MS). One read has
// one deadline, GATEWAY_PG_STATEMENT_TIMEOUT_MS from the moment it is queued, queue wait included: a read still queued
// when it passes is 503 HISTORY_BUSY, and connect, heartbeat and rows spend only what is left of it. No
// GATEWAY_PG_URL, a missing relation, a projection that never reported, an unreachable server or a statement timeout is
// 503 HISTORY_UNAVAILABLE (Retry-After GATEWAY_HISTORY_RETRY_AFTER_MS), one body for every cause (the cause is in the
// log and in gateway_history_reads_total). GET /orders/{orderId} of an order not open in the core, without a row and
// above the projection's applied_order_id, is the same 503 with Retry-After GATEWAY_HISTORY_LAG_RETRY_AFTER_MS (not
// projected yet), never 404. A history read takes one token of the principal's bucket. No order-path request touches
// PostgreSQL, and no history query runs on an I/O thread, the duty thread or the completion thread.
//
// Every core-backed route of a principal takes one token of the principal's bucket (GATEWAY_PRINCIPAL_RATE + _BURST;
// a bulk cancel one per order id plus one, never more than one full bucket); empty: 429 RATE_LIMITED with Retry-After.
// Deposit and withdraw also take a token of the principal's faucet bucket (GATEWAY_FAUCET_RATE; its burst is one
// period's count, "1/s" holds one token). A read answered from the read cache takes no token: it costs the core nothing.
//
// Answers to a core outcome: a reply maps by the status table (rest::mapOrderReject, rest::mapMoneyReject); the
// deadline passed with the command offered is 504 OUTCOME_UNKNOWN whose detail names the idempotence window; never
// offered is 503 CORE_UNAVAILABLE; a full command ring, request table or query budget is 503 OVERLOADED; every 503 and
// 429 carries Retry-After.
//
// Cancel re-offer: an unkeyed cancel the session offered more than once (the session or leader changed while it was in
// flight) that is answered UNKNOWN_ORDER or NOT_OPEN is 200 {orderId, status NOT_OPEN, duplicate true, reoffered
// true}: the order is not open, and whether this request closed it is not known. NOT_OWNER stays 404; a single-offer
// cancel keeps 404 and 409.
//
// Read cache: an identical open-orders or balances read of one user inside GATEWAY_QUERY_MIN_INTERVAL_MS is answered
// from the last answer (its asOf says how old); the user's cached answers are dropped when this gateway completes a
// command for that user.
//
// Treasury lane. Deposit and withdraw are for the principal's own userId, with the client's Idempotency-Key, an assetId
// from the config's asset list (any other would be dropped by the core's decoder, unanswered), an amount above zero and
// at most the asset's GATEWAY_FAUCET_MAX_AMOUNT (an asset without a cap is refused). Register funding deposits each
// capped asset of the config its cap, with the derived requestId (rest::fundingRequestId); a deposit whose outcome is
// unknown is offered once more inside the register request with the same id, and `funded` says whether every deposit
// was acked.
//
// Test-only controls (never in a published build): GW_NC_NO_CANCEL_REOFFER_RULE (a re-offered cancel answered by
// its reject), GW_NC_NO_ASSET_CHECK (deposit sends any assetId), GW_NC_RANDOM_FUNDING_ID (a fresh random funding id
// per attempt).

#include <pthread.h>

#include <algorithm>
#include <cinttypes>
#include <charconv>
#include <chrono>
#include <cstring>
#include <set>

#include <boost/json.hpp>

#include "gateway/common/Crypto.h"
#include "gateway/common/FrontLog.h"
#include "gateway/common/Rest.h"

#include "GatewayApi.h"

namespace openexchange { namespace gateway {

namespace json = boost::json;

namespace {

ApiResponse unavailable503(const char *code, std::string text, std::optional<std::string> detail = std::nullopt,
    std::int64_t retryAfterMs = 1000)
{
    api::ErrorSpec s;
    s.status = 503;
    s.code = code;
    s.text = std::move(text);
    s.detail = std::move(detail);
    s.retryAfterMs = retryAfterMs;
    return api::error(std::move(s));
}

ApiResponse validation(std::string text, std::optional<std::string> detail = std::nullopt)
{
    return api::error(400, api::code::VALIDATION, std::move(text), std::move(detail));
}

// Strict reads of one JSON body: the first problem wins and is the 400.
class Fields
{
public:
    explicit Fields(const json::object &o) : m_o(o) {}

    bool ok() const { return m_problem.empty(); }
    ApiResponse bad() const { return validation("invalid field", m_problem); }

    std::optional<std::string> text(const char *key, bool required)
    {
        const json::value *v = m_o.if_contains(key);
        if (nullptr == v)
        {
            missing(key, required);
            return std::nullopt;
        }
        if (!v->is_string())
        {
            fail(key, "must be a string");
            return std::nullopt;
        }
        return std::string(v->as_string());
    }

    // money: a decimal string; 0 when absent and not required
    std::int64_t money(const char *key, bool required)
    {
        const auto t = text(key, required);
        if (!t)
        {
            return 0;
        }
        const auto units = rest::parseMoney(*t);
        if (!units)
        {
            fail(key, "must be a decimal string with at most 8 fraction digits (no sign, no exponent), inside the int64 range");
            return 0;
        }
        return *units;
    }

    // an id: a decimal string of an integer within [min, max] (no sign, no leading zero)
    std::int64_t id(const char *key, bool required, std::int64_t max = INT64_MAX, std::int64_t min = 1)
    {
        const auto t = text(key, required);
        return t ? idOf(key, *t, max, min) : 0;
    }

    std::int64_t idOf(const char *key, const std::string &t, std::int64_t max = INT64_MAX, std::int64_t min = 1)
    {
        std::int64_t v = 0;
        const bool digits = !t.empty() && ('0' != t[0] || 1 == t.size()) &&
            std::all_of(t.begin(), t.end(), [](char c) { return c >= '0' && c <= '9'; });
        const auto [end, ec] = std::from_chars(t.data(), t.data() + t.size(), v);
        if (!digits || std::errc() != ec || end != t.data() + t.size() || v < min || v > max)
        {
            fail(key, "must be a decimal string of an integer from " + std::to_string(min) +
                (INT64_MAX == max ? std::string() : " to " + std::to_string(max)));
            return 0;
        }
        return v;
    }

    // epoch milliseconds: a JSON integer above 0; 0 when absent
    std::int64_t time(const char *key)
    {
        const json::value *v = m_o.if_contains(key);
        if (nullptr == v)
        {
            return 0;
        }
        if (v->is_int64() && v->as_int64() > 0)
        {
            return v->as_int64();
        }
        if (v->is_uint64() && v->as_uint64() <= static_cast<std::uint64_t>(INT64_MAX) && v->as_uint64() > 0)
        {
            return static_cast<std::int64_t>(v->as_uint64());
        }
        fail(key, "must be a positive integer (epoch milliseconds)");
        return 0;
    }

    void fail(const char *key, const std::string &rule)
    {
        if (m_problem.empty())
        {
            m_problem = std::string("field '") + key + "' " + rule;
        }
    }

private:
    void missing(const char *key, bool required)
    {
        if (required)
        {
            fail(key, "is required");
        }
    }

    const json::object &m_o;
    std::string m_problem;
};

// The Idempotency-Key of the request, or the 400 to answer.
std::optional<ApiResponse> readKey(const HttpExchange &ex, bool required, rest::RequestKey &key)
{
    key = rest::parseIdempotencyKey(ex.header("Idempotency-Key"), ex.headerCount("Idempotency-Key"));
    switch (key.status)
    {
        case rest::KeyStatus::OK:
            return std::nullopt;
        case rest::KeyStatus::MISSING:
            if (!required)
            {
                return std::nullopt;
            }
            return api::error(400, api::code::MISSING_IDEMPOTENCY_KEY, "an Idempotency-Key header is required",
                "send a UUID or 1 to 64 visible ASCII characters; retry with the same key, never a new one, after a 504");
        case rest::KeyStatus::NIL:
            return validation("invalid Idempotency-Key", "the nil UUID (and any key that encodes 0/0) is not a key");
        case rest::KeyStatus::INVALID:
            break;
    }
    return validation("invalid Idempotency-Key", "a UUID or 1 to 64 visible ASCII characters, given once");
}

const char *gateName(wire::money::GateState::Value v)
{
    return wire::money::GateState::c_str(v);
}


std::string idJson(std::int64_t v)
{
    return api::quote(std::to_string(v));
}

std::string moneyJson(std::int64_t v)
{
    return api::quote(rest::formatMoney(v));
}

}

// ---------------------------------------------------------------- shared steps

bool GatewayApi::takePrincipal(const Ex &ex, const char *route, std::int64_t userId, std::int64_t cost)
{
    const std::int64_t now = m_deps.nowMs();
    const std::string key = std::to_string(userId);
    // a request never costs more than one full bucket, so it can always pass on a full one
    const std::int64_t n = std::max<std::int64_t>(1, std::min<std::int64_t>(cost, m_s.principalBurst));
    TakeResult t;
    std::int64_t taken = 0;
    for (; taken < n; ++taken)
    {
        t = m_principalBuckets.take(key, now);
        if (!t.ok)
        {
            break;
        }
    }
    if (taken == n)
    {
        return true;
    }
    for (std::int64_t i = 0; i < taken; ++i)
    {
        m_principalBuckets.refund(key, now);
    }
    m_principalLimited.fetch_add(1, std::memory_order_relaxed);
    api::ErrorSpec s;
    s.status = 429;
    s.code = api::code::RATE_LIMITED;
    s.text = "too many requests";
    s.detail = "the principal's request budget (GATEWAY_PRINCIPAL_RATE) is spent";
    s.retryAfterMs = std::max<std::int64_t>(t.retryAfterMs, 1);
    finish(ex, route, api::error(std::move(s)));
    return false;
}

bool GatewayApi::ask(const Ex &ex, const char *route, bool treasury, session::Command command,
    std::function<void(const session::Outcome &)> done)
{
    const auto &asker = treasury ? m_deps.askTreasury : m_deps.askCore;
    if (asker && asker(std::move(command), std::move(done)))
    {
        return true;
    }
    finish(ex, route, unavailable503(api::code::OVERLOADED, "the gateway is overloaded"));
    return false;
}

std::shared_ptr<const session::CoreConfig> GatewayApi::loadedConfig(const Ex &ex, const char *route)
{
    auto config = m_deps.config();
    if (!config || !config->loaded())
    {
        finish(ex, route, unavailable503(api::code::CORE_UNAVAILABLE, "the core config is not loaded yet"));
        return nullptr;
    }
    return config;
}

ApiResponse GatewayApi::failure(const session::Outcome &o, Asked asked) const
{
    switch (o.kind)
    {
        case session::OutcomeKind::OUTCOME_UNKNOWN:
        {
            std::string detail;
            if (Asked::KEYED == asked || Asked::KEYED_FAUCET == asked)
            {
                const auto config = m_deps.config();
                const std::int64_t window = config ? config->idempotencyWindowMs() : 0;
                detail = "the command may have been applied: retry with the same Idempotency-Key inside the idempotence "
                    "window (idempotencyWindowMs " + std::to_string(window) + ", also in /api/v2/status); after the "
                    "window the same key is a new request";
                if (Asked::KEYED_FAUCET == asked)
                {
                    detail += ". The retry also takes a token of the faucet rate (GATEWAY_FAUCET_RATE): a 429 before it "
                        "carries Retry-After, wait that long and retry with the same key";
                }
            }
            else if (Asked::UNKEYED_CANCEL == asked)
            {
                detail = "the cancel may have been applied: read the order, or send the cancel again (a cancel of an "
                    "order that is no longer open answers 404 or 409; send an Idempotency-Key for an exact answer)";
            }
            else
            {
                detail = "nothing was changed: retry the read";
            }
            return api::error(504, api::code::OUTCOME_UNKNOWN, "the core did not answer in time", detail);
        }
        case session::OutcomeKind::CORE_UNAVAILABLE:
            return unavailable503(api::code::CORE_UNAVAILABLE, "the core session is not available");
        case session::OutcomeKind::OVERLOADED:
            return unavailable503(api::code::OVERLOADED, "the gateway is overloaded");
        case session::OutcomeKind::INTERNAL:
            return api::error(500, api::code::INTERNAL, "internal error");
        case session::OutcomeKind::REPLY:
            break;
    }
    rest::Mapping m;
    std::string reason;
    if (const auto *r = std::get_if<wire::OrderReject>(&o.reply))
    {
        m = rest::mapOrderReject(r->reason, 0 != r->refTemplateId ? r->refTemplateId : o.templateId);
        reason = wire::money::CoreRejectReason::c_str(r->reason);
    }
    else if (const auto *d = std::get_if<wire::DepositReject>(&o.reply))
    {
        m = rest::mapMoneyReject(d->reason);
        reason = wire::money::RejectReason::c_str(d->reason);
    }
    else if (const auto *w = std::get_if<wire::WithdrawReject>(&o.reply))
    {
        m = rest::mapMoneyReject(w->reason);
        reason = wire::money::RejectReason::c_str(w->reason);
    }
    else
    {
        frontLog(FrontLevel::ERROR, "unexpected core reply for %s (template %u)", wire::templateName(o.templateId),
            static_cast<unsigned>(o.templateId));
        return api::error(500, api::code::INTERNAL, "internal error");
    }
    if (m.critical)
    {
        frontLog(FrontLevel::CRITICAL, "the core answered %s to %s: this gateway's core credential may not send it (a "
            "deployment error)", reason.c_str(), wire::templateName(o.templateId));
    }
    if (m.defect)
    {
        frontLog(FrontLevel::ERROR, "the core answered %s to %s: a reason that cannot reach a gateway", reason.c_str(),
            wire::templateName(o.templateId));
        api::ErrorSpec s;
        s.status = 500;
        s.code = api::code::INTERNAL;
        s.text = "internal error";
        s.reason = reason;
        return api::error(std::move(s));
    }
    api::ErrorSpec s;
    s.status = m.status;
    s.code = m.code;
    s.text = 404 == m.status ? "not found" : 409 == m.status ? "conflict" : 422 == m.status ? "rejected by the core"
        : 429 == m.status ? "too many requests" : 503 == m.status ? "order entry is closed" : 403 == m.status
        ? "forbidden" : "invalid request";
    s.reason = reason;
    s.keyFinal = m.keyFinal && Asked::READ != asked && Asked::UNKEYED_CANCEL != asked; // no key, nothing to replay
    s.retryAfterMs = m.retryAfterMs;
    return api::error(std::move(s));
}

// ---------------------------------------------------------------- read cache

std::optional<std::string> GatewayApi::cached(std::int64_t userId, const std::string &key)
{
    const std::int64_t now = m_deps.nowMs();
    std::lock_guard<std::mutex> lock(m_cacheMutex);
    const auto it = m_readCache.find({ userId, key });
    if (it == m_readCache.end() || now - it->second.atMs >= m_s.queryMinIntervalMs)
    {
        return std::nullopt;
    }
    m_cacheHits.fetch_add(1, std::memory_order_relaxed);
    return it->second.body;
}

void GatewayApi::cache(std::int64_t userId, const std::string &key, const std::string &body)
{
    const std::int64_t now = m_deps.nowMs();
    std::lock_guard<std::mutex> lock(m_cacheMutex);
    m_readCache[{ userId, key }] = CacheEntry{ now, body };
    if (++m_cachePuts % 1024 == 0)
    {
        // entries past the interval are never served again: the map holds only the reads of the last interval
        for (auto it = m_readCache.begin(); it != m_readCache.end();)
        {
            it = now - it->second.atMs >= m_s.queryMinIntervalMs ? m_readCache.erase(it) : std::next(it);
        }
    }
}

void GatewayApi::dropCache(std::int64_t userId)
{
    std::lock_guard<std::mutex> lock(m_cacheMutex);
    auto it = m_readCache.lower_bound({ userId, std::string() });
    while (it != m_readCache.end() && it->first.first == userId)
    {
        it = m_readCache.erase(it);
    }
}

// ---------------------------------------------------------------- markets and status

void GatewayApi::markets(const Ex &ex)
{
    static const char *route = "markets";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto config = loadedConfig(ex, route);
    if (!config)
    {
        return;
    }
    auto code = [&config](std::int32_t assetId) -> std::string {
        for (const wire::AssetConfig &a : config->assets)
        {
            if (a.assetId == assetId)
            {
                return api::quote(rest::fixedText(a.code.data(), a.code.size()));
            }
        }
        return "null";
    };
    std::string out = "{\"configVersion\":" + std::to_string(config->head.configVersion) + ",\"markets\":[";
    bool first = true;
    for (const wire::MarketConfig &m : config->markets)
    {
        out += first ? "" : ",";
        first = false;
        out += "{\"marketId\":" + idJson(m.marketId) + ",\"symbol\":" + api::quote(rest::fixedText(m.symbol.data(),
            m.symbol.size())) + ",\"baseAsset\":" + code(m.baseAssetId) + ",\"quoteAsset\":" + code(m.quoteAssetId) +
            ",\"baseAssetId\":" + idJson(m.baseAssetId) + ",\"quoteAssetId\":" + idJson(m.quoteAssetId) +
            ",\"tickSize\":" + moneyJson(m.tickSize) + ",\"lotSize\":" + moneyJson(m.lotSize) + ",\"minPrice\":" +
            moneyJson(m.minPrice) + ",\"maxPrice\":" + moneyJson(m.maxPrice) + ",\"minQuantity\":" +
            moneyJson(m.minQuantity) + ",\"maxQuantity\":" + moneyJson(m.maxQuantity) + ",\"minNotional\":" +
            moneyJson(m.minNotional) + ",\"maxNotional\":" + moneyJson(m.maxNotional) + "}";
    }
    out += "],\"assets\":[";
    first = true;
    for (const wire::AssetConfig &a : config->assets)
    {
        out += first ? "" : ",";
        first = false;
        out += "{\"assetId\":" + idJson(a.assetId) + ",\"code\":" + api::quote(rest::fixedText(a.code.data(),
            a.code.size())) + ",\"scale\":" + std::to_string(a.scale) + "}";
    }
    out += "]}";
    finish(ex, route, api::jsonBody(200, std::move(out)));
}

void GatewayApi::status(const Ex &ex)
{
    static const char *route = "status";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto config = m_deps.config();
    const bool loaded = config && config->loaded();
    const bool ready = loaded && m_deps.registered() && config->gateOpen();
    std::string out = "{\"ready\":" + std::string(ready ? "true" : "false");
    if (!loaded)
    {
        out += ",\"configVersion\":null,\"behaviorVersion\":null,\"idempotencyWindowMs\":null,\"admission\":[]}";
        finish(ex, route, api::jsonBody(200, std::move(out)));
        return;
    }
    out += ",\"configVersion\":" + std::to_string(config->head.configVersion) + ",\"behaviorVersion\":" +
        std::to_string(config->head.behaviorVersion) + ",\"idempotencyWindowMs\":" +
        std::to_string(config->idempotencyWindowMs()) + ",\"admission\":[";
    auto gate = [](std::int32_t marketId, const session::GateView &g) {
        return "{\"marketId\":" + idJson(marketId) + ",\"gate\":" + (g.known() ? api::quote(gateName(g.state))
            : std::string("null")) + ",\"reasons\":" + rest::gateReasonsJson(g.reasons) + ",\"circuitBreaker\":" +
            (g.circuitBreaker ? "true" : "false") + "}";
    };
    out += gate(0, config->global);
    for (std::size_t i = 0; i < config->markets.size() && i < config->marketGates.size(); ++i)
    {
        out += "," + gate(config->markets[i].marketId, config->marketGates[i]);
    }
    out += "]}";
    finish(ex, route, api::jsonBody(200, std::move(out)));
}

// ---------------------------------------------------------------- orders

void GatewayApi::createOrder(const Ex &ex)
{
    static const char *route = "orders_create";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto body = api::readBody(*ex, { "marketId", "side", "type", "timeInForce", "price", "quantity", "budget",
        "stopPrice", "trailingDelta", "displayQuantity", "expiresAt", "clientOrderId" }, bad);
    if (!body)
    {
        finish(ex, route, std::move(bad));
        return;
    }
    Fields f(*body);
    wire::CreateOrder c;
    c.marketId = static_cast<std::int32_t>(f.id("marketId", true, INT32_MAX));
    if (const auto side = f.text("side", true))
    {
        const auto v = rest::parseSide(*side);
        v ? void(c.side = *v) : f.fail("side", "must be BUY or SELL");
    }
    if (const auto type = f.text("type", true))
    {
        const auto v = rest::parseOrderType(*type);
        v ? void(c.orderType = *v)
          : f.fail("type", "must be LIMIT, MARKET, LIMIT_MAKER, STOP_LOSS, STOP_LIMIT, TRAILING_STOP or ICEBERG");
    }
    if (const auto tif = f.text("timeInForce", true))
    {
        const auto v = rest::parseTimeInForce(*tif);
        v ? void(c.timeInForce = *v) : f.fail("timeInForce", "must be GTC, IOC, FOK or GTD");
    }
    c.price = f.money("price", false);
    c.quantity = f.money("quantity", true);
    c.budget = f.money("budget", false);
    c.stopPrice = f.money("stopPrice", false);
    c.trailingDelta = f.money("trailingDelta", false);
    c.displayQty = f.money("displayQuantity", false);
    c.expiresAtMs = f.time("expiresAt");
    if (const auto coid = f.text("clientOrderId", false))
    {
        const auto uuid = rest::parseUuid(*coid);
        if (!uuid || (0 == uuid->first && 0 == uuid->second))
        {
            f.fail("clientOrderId", "must be a UUID other than the nil UUID");
        }
        else
        {
            c.clientOrderIdHigh = uuid->first;
            c.clientOrderIdLow = uuid->second;
        }
    }
    if (!f.ok())
    {
        finish(ex, route, f.bad());
        return;
    }
    const std::string shape = rest::createShapeProblem(c);
    if (!shape.empty())
    {
        finish(ex, route, validation("invalid order", shape));
        return;
    }
    rest::RequestKey key;
    if (auto refused = readKey(*ex, true, key))
    {
        finish(ex, route, std::move(*refused));
        return;
    }
    c.requestIdHigh = key.high;
    c.requestIdLow = key.low;
    withPrincipal(ex, route, [this, ex, c](const Principal &p) mutable {
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        c.userId = p.userId;
        const std::int64_t userId = p.userId;
        ask(ex, route, false, session::Command{ c }, [this, ex, userId](const session::Outcome &o) {
            dropCache(userId);
            const auto *ack = std::get_if<wire::OrderAck>(&o.reply);
            if (session::OutcomeKind::REPLY != o.kind || nullptr == ack)
            {
                finish(ex, route, failure(o, Asked::KEYED));
                return;
            }
            finish(ex, route, api::jsonBody(ack->duplicate ? 200 : 201, "{\"orderId\":" + idJson(ack->orderId) +
                ",\"status\":" + api::quote(rest::nameOf(ack->status)) + ",\"duplicate\":" +
                (ack->duplicate ? "true" : "false") + ",\"marketId\":" + idJson(ack->marketId) + ",\"hold\":{\"assetId\":" +
                idJson(ack->holdAssetId) + ",\"amount\":" + moneyJson(ack->holdAmount) + "},\"acceptedAt\":" +
                std::to_string(ack->acceptedAtMs) + ",\"eventSeq\":" + std::to_string(ack->eventSeq) + "}"));
        });
    });
}

struct GatewayApi::OpenOrders
{
    std::int64_t userId = 0;
    std::int64_t from = 0;
    std::vector<wire::OrderSnapshotEntry> entries;
    std::int64_t asOf = -1;
    bool failed = false;
    session::Outcome failure;
};

// Every page of RequestOrderSnapshot for the user, in orderId order (each page a query through the session's budget).
void GatewayApi::readOpenOrders(std::int64_t userId, std::shared_ptr<OpenOrders> state,
    std::function<void(std::shared_ptr<OpenOrders>)> done)
{
    wire::RequestOrderSnapshot q;
    q.userId = userId;
    q.fromOrderId = state->from;
    const bool queued = m_deps.askCore(session::Command{ q }, [this, userId, state, done](const session::Outcome &o) {
        const auto *page = std::get_if<session::SnapshotReply>(&o.reply);
        if (session::OutcomeKind::REPLY != o.kind || nullptr == page)
        {
            state->failed = true;
            state->failure = o;
            done(state);
            return;
        }
        for (const wire::OrderSnapshotEntry &e : page->entries)
        {
            if (e.userId == userId) // the core scopes the query to the user; a foreign row would be a defect
            {
                state->entries.push_back(e);
            }
        }
        state->asOf = std::max(state->asOf, page->end.asOfLogPosition);
        if (0 != page->end.nextOrderId && page->end.nextOrderId > state->from)
        {
            state->from = page->end.nextOrderId;
            readOpenOrders(userId, state, done);
            return;
        }
        done(state);
    });
    if (!queued)
    {
        state->failed = true;
        state->failure = session::Outcome{};
        state->failure.kind = session::OutcomeKind::OVERLOADED;
        done(state);
    }
}

void GatewayApi::listOrders(const Ex &ex)
{
    static const char *route = "orders_list";
    ApiResponse bad;
    if (!api::readQuery(*ex, { "marketId" }, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    std::int32_t marketId = 0;
    const std::string_view q = ex->query();
    if (!q.empty())
    {
        const std::string value(q.substr(q.find('=') == std::string_view::npos ? q.size() : q.find('=') + 1));
        json::object none;
        Fields f(none);
        marketId = static_cast<std::int32_t>(f.idOf("marketId", value, INT32_MAX));
        if (!f.ok())
        {
            finish(ex, route, f.bad());
            return;
        }
    }
    withPrincipal(ex, route, [this, ex, marketId](const Principal &p) {
        const std::string key = "orders:" + std::to_string(marketId);
        if (const auto hit = cached(p.userId, key))
        {
            finish(ex, route, api::jsonBody(200, *hit));
            return;
        }
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        auto state = std::make_shared<OpenOrders>();
        state->userId = p.userId;
        const std::int64_t userId = p.userId;
        readOpenOrders(userId, state, [this, ex, marketId, key, userId](std::shared_ptr<OpenOrders> s) {
            if (s->failed)
            {
                finish(ex, route, failure(s->failure, Asked::READ));
                return;
            }
            std::string out = "{\"asOf\":{\"logPosition\":" + std::to_string(s->asOf) + "},\"orders\":[";
            bool first = true;
            for (const wire::OrderSnapshotEntry &e : s->entries)
            {
                if (0 != marketId && e.marketId != marketId)
                {
                    continue;
                }
                out += (first ? "" : ",") + rest::orderJson(e);
                first = false;
            }
            out += "]}";
            cache(userId, key, out);
            finish(ex, route, api::jsonBody(200, std::move(out)));
        });
    });
}

void GatewayApi::getOrder(const Ex &ex, std::int64_t orderId)
{
    static const char *route = "orders_get";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    withPrincipal(ex, route, [this, ex, orderId](const Principal &p) {
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        wire::RequestOrderSnapshot q;
        q.userId = p.userId;
        q.fromOrderId = orderId - 1; // the page starts at orderId: one query finds an open order
        const std::int64_t userId = p.userId;
        ask(ex, route, false, session::Command{ q }, [this, ex, orderId, userId](const session::Outcome &o) {
            const auto *page = std::get_if<session::SnapshotReply>(&o.reply);
            if (session::OutcomeKind::REPLY != o.kind || nullptr == page)
            {
                finish(ex, route, failure(o, Asked::READ));
                return;
            }
            if (!page->entries.empty() && page->entries.front().orderId == orderId)
            {
                finish(ex, route, api::jsonBody(200, rest::orderJson(page->entries.front())));
                return;
            }
            // not open (closed, unknown or another user's): history answers, on its own pool, never on this thread
            orderFromHistory(ex, userId, orderId);
        });
    });
}

void GatewayApi::amendOrder(const Ex &ex, std::int64_t orderId)
{
    static const char *route = "orders_amend";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto body = api::readBody(*ex, { "price", "quantity" }, bad);
    if (!body)
    {
        finish(ex, route, std::move(bad));
        return;
    }
    Fields f(*body);
    wire::AmendOrder c;
    c.orderId = orderId;
    c.newPrice = f.money("price", false);
    c.newQuantity = f.money("quantity", false);
    if (f.ok() && body->contains("price") && 0 == c.newPrice)
    {
        f.fail("price", "must be positive");
    }
    if (f.ok() && body->contains("quantity") && 0 == c.newQuantity)
    {
        f.fail("quantity", "must be positive (the new total quantity)");
    }
    if (f.ok() && 0 == c.newPrice && 0 == c.newQuantity)
    {
        f.fail("price", "or quantity is required");
    }
    if (!f.ok())
    {
        finish(ex, route, f.bad());
        return;
    }
    rest::RequestKey key;
    if (auto refused = readKey(*ex, true, key))
    {
        finish(ex, route, std::move(*refused));
        return;
    }
    c.requestIdHigh = key.high;
    c.requestIdLow = key.low;
    withPrincipal(ex, route, [this, ex, c](const Principal &p) mutable {
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        c.userId = p.userId;
        const std::int64_t userId = p.userId;
        ask(ex, route, false, session::Command{ c }, [this, ex, userId](const session::Outcome &o) {
            dropCache(userId);
            const auto *ack = std::get_if<wire::OrderAck>(&o.reply);
            if (session::OutcomeKind::REPLY != o.kind || nullptr == ack)
            {
                finish(ex, route, failure(o, Asked::KEYED));
                return;
            }
            finish(ex, route, api::jsonBody(200, "{\"orderId\":" + idJson(ack->orderId) +
                ",\"status\":\"PENDING_REPLACE\",\"duplicate\":" + (ack->duplicate ? "true" : "false") + "}"));
        });
    });
}

void GatewayApi::cancelOrder(const Ex &ex, std::int64_t orderId)
{
    static const char *route = "orders_cancel";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    if (!ex->body().empty())
    {
        finish(ex, route, validation("a cancel takes no body"));
        return;
    }
    rest::RequestKey key;
    if (auto refused = readKey(*ex, false, key))
    {
        finish(ex, route, std::move(*refused));
        return;
    }
    const bool keyed = rest::KeyStatus::OK == key.status;
    wire::CancelOrder c;
    c.orderId = orderId;
    c.requestIdHigh = key.high;
    c.requestIdLow = key.low;
    withPrincipal(ex, route, [this, ex, c, keyed](const Principal &p) mutable {
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        c.userId = p.userId;
        const std::int64_t userId = p.userId;
        const std::int64_t orderId = c.orderId;
        ask(ex, route, false, session::Command{ c }, [this, ex, userId, orderId, keyed](const session::Outcome &o) {
            dropCache(userId);
            const bool reoffered = o.offers > 1;
            if (session::OutcomeKind::REPLY == o.kind)
            {
                if (const auto *ack = std::get_if<wire::OrderAck>(&o.reply))
                {
                    finish(ex, route, api::jsonBody(200, "{\"orderId\":" + idJson(ack->orderId) + ",\"status\":" +
                        api::quote(rest::nameOf(ack->status)) + ",\"duplicate\":" + (ack->duplicate ? "true" : "false") +
                        (reoffered ? ",\"reoffered\":true" : "") + "}"));
                    return;
                }
#ifndef GW_NC_NO_CANCEL_REOFFER_RULE
                const auto *reject = std::get_if<wire::OrderReject>(&o.reply);
                if (!keyed && reoffered && nullptr != reject &&
                    (wire::money::CoreRejectReason::UNKNOWN_ORDER == reject->reason ||
                        wire::money::CoreRejectReason::NOT_OPEN == reject->reason))
                {
                    // offered more than once: the first offer may have closed the order
                    m_cancelReoffered.fetch_add(1, std::memory_order_relaxed);
                    finish(ex, route, api::jsonBody(200, "{\"orderId\":" + idJson(orderId) +
                        ",\"status\":\"NOT_OPEN\",\"duplicate\":true,\"reoffered\":true}"));
                    return;
                }
#else
                (void)keyed; // test-only control: the re-offer rule off
#endif
            }
            finish(ex, route, failure(o, keyed ? Asked::KEYED : Asked::UNKEYED_CANCEL));
        });
    });
}

void GatewayApi::bulkCancel(const Ex &ex)
{
    static const char *route = "orders_bulk_cancel";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto body = api::readBody(*ex, { "orderIds", "all", "marketId" }, bad);
    if (!body)
    {
        finish(ex, route, std::move(bad));
        return;
    }
    Fields f(*body);
    const json::value *ids = body->if_contains("orderIds");
    const json::value *all = body->if_contains("all");
    std::vector<std::int64_t> wanted;
    bool everything = false;
    std::int32_t marketId = 0;
    if ((nullptr == ids) == (nullptr == all))
    {
        f.fail("orderIds", "or all:true is required, not both");
    }
    else if (nullptr != ids)
    {
        if (!ids->is_array() || ids->as_array().empty())
        {
            f.fail("orderIds", "must be a non-empty array of order ids");
        }
        else
        {
            std::set<std::int64_t> seen;
            for (const json::value &v : ids->as_array())
            {
                const std::int64_t id = v.is_string() ? f.idOf("orderIds", std::string(v.as_string())) : 0;
                if (!v.is_string())
                {
                    f.fail("orderIds", "must hold decimal strings");
                }
                if (!f.ok())
                {
                    break;
                }
                if (!seen.insert(id).second)
                {
                    f.fail("orderIds", "lists order " + std::to_string(id) + " twice");
                    break;
                }
                wanted.push_back(id);
            }
        }
        if (f.ok() && body->contains("marketId"))
        {
            f.fail("marketId", "goes with all:true only");
        }
    }
    else
    {
        if (!all->is_bool() || !all->as_bool())
        {
            f.fail("all", "must be true");
        }
        everything = true;
        marketId = static_cast<std::int32_t>(f.id("marketId", false, INT32_MAX));
    }
    if (!f.ok())
    {
        finish(ex, route, f.bad());
        return;
    }
#ifdef GW_NC_BODY_USER
    // test-only control: the userId of the body decides whose orders are cancelled
    std::int64_t bodyUser = 0;
    for (const char *name : { "userId", "user_id" })
    {
        if (const json::value *u = body->if_contains(name); nullptr != u && u->is_string())
        {
            bodyUser = std::stoll(std::string(u->as_string()));
        }
    }
    withPrincipal(ex, route, [this, ex, wanted, everything, marketId, bodyUser](const Principal &p) {
        const std::int64_t userId = 0 != bodyUser ? bodyUser : p.userId;
#else
    withPrincipal(ex, route, [this, ex, wanted, everything, marketId](const Principal &p) {
        const std::int64_t userId = p.userId;
#endif
        const std::int64_t max = m_s.bulkCancelMax;
        // the cancels of `targets` (the first `max` of `requested`), in parallel, answered once all are back
        auto run = [this, ex, userId](std::vector<std::int64_t> requested, std::int64_t max) {
            const std::size_t n = std::min<std::size_t>(requested.size(), static_cast<std::size_t>(max));
            struct Bulk
            {
                std::mutex mutex;
                std::vector<std::string> results;
                std::size_t pending = 0;
                std::size_t accepted = 0;
                std::size_t requested = 0;
                std::size_t sent = 0;
            };
            auto bulk = std::make_shared<Bulk>();
            bulk->results.resize(n);
            bulk->pending = n;
            bulk->requested = requested.size();
            bulk->sent = n;
            auto settle = [this, ex, bulk, userId](std::size_t i, std::string item, bool accepted) {
                bool last = false;
                {
                    std::lock_guard<std::mutex> lock(bulk->mutex);
                    bulk->results[i] = std::move(item);
                    bulk->accepted += accepted ? 1 : 0;
                    last = 0 == --bulk->pending;
                }
                if (!last)
                {
                    return;
                }
                dropCache(userId);
                std::string out = "{\"results\":[";
                for (std::size_t k = 0; k < bulk->results.size(); ++k)
                {
                    out += (0 == k ? "" : ",") + bulk->results[k];
                }
                out += "],\"requested\":" + std::to_string(bulk->requested) + ",\"accepted\":" +
                    std::to_string(bulk->accepted) + ",\"truncated\":" + (bulk->requested > bulk->sent ? "true" : "false") +
                    ",\"remaining\":" + std::to_string(bulk->requested - bulk->sent) + "}";
                finish(ex, route, api::jsonBody(200, std::move(out)));
            };
            if (0 == n)
            {
                finish(ex, route, api::jsonBody(200, "{\"results\":[],\"requested\":0,\"accepted\":0,"
                    "\"truncated\":false,\"remaining\":0}"));
                return;
            }
            for (std::size_t i = 0; i < n; ++i)
            {
                const std::int64_t orderId = requested[i];
                wire::CancelOrder c; // unkeyed: a bulk cancel takes no requestId slot of the user
                c.userId = userId;
                c.orderId = orderId;
                const bool queued = m_deps.askCore(session::Command{ c }, [settle, i, orderId](const session::Outcome &o) {
                    const bool reoffered = o.offers > 1;
                    std::string head = "{\"orderId\":" + idJson(orderId) + ",\"state\":";
                    const std::string tail = reoffered ? ",\"reoffered\":true}" : "}";
                    if (session::OutcomeKind::REPLY == o.kind)
                    {
                        if (std::holds_alternative<wire::OrderAck>(o.reply))
                        {
                            settle(i, head + "\"CANCEL_PENDING\"" + tail, true);
                            return;
                        }
                        if (const auto *r = std::get_if<wire::OrderReject>(&o.reply))
                        {
                            using R = wire::money::CoreRejectReason;
                            if (R::NOT_OPEN == r->reason || (reoffered && R::UNKNOWN_ORDER == r->reason))
                            {
                                settle(i, head + "\"NOT_OPEN\"" + tail, false);
                                return;
                            }
                            if (R::UNKNOWN_ORDER == r->reason || R::NOT_OWNER == r->reason)
                            {
                                settle(i, head + "\"NOT_FOUND\"" + tail, false);
                                return;
                            }
                            settle(i, head + "\"FAILED\",\"code\":\"REJECTED\",\"reason\":" +
                                api::quote(R::c_str(r->reason)) + tail, false);
                            return;
                        }
                    }
                    if (session::OutcomeKind::OUTCOME_UNKNOWN == o.kind)
                    {
                        settle(i, head + "\"OUTCOME_UNKNOWN\",\"code\":\"OUTCOME_UNKNOWN\"" + tail, false);
                        return;
                    }
                    settle(i, head + "\"NOT_SENT\",\"code\":" + api::quote(session::OutcomeKind::INTERNAL == o.kind
                        ? api::code::INTERNAL : session::OutcomeKind::CORE_UNAVAILABLE == o.kind
                        ? api::code::CORE_UNAVAILABLE : api::code::OVERLOADED) + tail, false);
                });
                if (!queued)
                {
                    settle(i, "{\"orderId\":" + idJson(orderId) + ",\"state\":\"NOT_SENT\",\"code\":\"OVERLOADED\"}", false);
                }
            }
        };
        if (!everything)
        {
            const std::int64_t cost = std::min<std::int64_t>(static_cast<std::int64_t>(wanted.size()), max) + 1;
            if (takePrincipal(ex, route, userId, cost))
            {
                run(wanted, max);
            }
            return;
        }
        if (!takePrincipal(ex, route, userId, 1))
        {
            return;
        }
        auto state = std::make_shared<OpenOrders>();
        state->userId = userId;
        readOpenOrders(userId, state, [this, ex, run, marketId, max, userId](std::shared_ptr<OpenOrders> s) {
            if (s->failed)
            {
                finish(ex, route, failure(s->failure, Asked::READ));
                return;
            }
            std::vector<std::int64_t> open;
            for (const wire::OrderSnapshotEntry &e : s->entries)
            {
                if (0 == marketId || e.marketId == marketId)
                {
                    open.push_back(e.orderId); // ascending: the core walks its table in orderId order
                }
            }
            std::sort(open.begin(), open.end());
            const std::int64_t n = std::min<std::int64_t>(static_cast<std::int64_t>(open.size()), max);
            // the read took one token: the cancels take at most the rest of one full bucket, so a full bucket always
            // pays for the whole request (the read plus up to burst - 1 cancels)
            const std::int64_t cost = std::min<std::int64_t>(n, m_s.principalBurst - 1);
            if (cost > 0 && !takePrincipal(ex, route, userId, cost))
            {
                return;
            }
            run(std::move(open), max);
        });
    });
}

// ---------------------------------------------------------------- history (the history projection, read-only)

void GatewayApi::countHistory(const char *query, const char *result)
{
    std::lock_guard<std::mutex> lock(m_countMutex);
    ++m_historyReads[{ query, result }];
}

ApiResponse GatewayApi::historyUnavailable(HistoryCause cause, const char *query)
{
    countHistory(query, (std::string("unavailable|") + nameOf(cause)).c_str());
    // one public answer whatever the cause: the cause is in the log (the store's WARN line) and the metrics. Only the
    // Retry-After differs for an order not projected yet (LAGGING): the projection is expected to reach it soon
    return unavailable503(api::code::HISTORY_UNAVAILABLE, "history is not available",
        "order and fill history are read from the history projection, which cannot answer now",
        HistoryCause::LAGGING == cause ? m_s.historyLagRetryAfterMs : m_s.historyRetryAfterMs);
}

void GatewayApi::noteHistory(HistoryCause cause, std::int64_t ageMs, std::int64_t eventSeq)
{
    std::lock_guard<std::mutex> lock(m_historyHealthMutex);
    m_historyHealth.answered = true;
    m_historyHealth.atNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    m_historyHealth.cause = cause;
    m_historyHealth.ageMs = ageMs;
    m_historyHealth.eventSeq = eventSeq;
}

GatewayApi::HistoryHealth GatewayApi::historyHealth() const
{
    std::lock_guard<std::mutex> lock(m_historyHealthMutex);
    HistoryHealth h = m_historyHealth;
#ifndef GW_NC_NO_HISTORY_PROBE
    // a probe that stopped answering (a full pool, a hung read) never leaves an old "fresh" on /health
    const std::int64_t boundNs = (2 * m_s.historyProbeMs + m_s.pgStatementTimeoutMs) * 1'000'000;
    const std::int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    if (h.answered && now - h.atNs > boundNs)
    {
        h.answered = false;
    }
#endif
    return h;
}

void GatewayApi::probeHistoryOnce()
{
#ifdef GW_NC_NO_HISTORY_PROBE
    return; // test build only: no probe (/health shows what the last read found)
#else
    if (nullptr == m_deps.history || nullptr == m_deps.historyPool)
    {
        return;
    }
    if (m_probeInFlight.exchange(true))
    {
        return; // the previous probe has not answered yet: one at a time
    }
    const HistoryExpect expect = historyExpect();
    const std::int64_t deadlineNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count() + m_s.pgStatementTimeoutMs * 1'000'000;
    const bool queued = m_deps.historyPool->trySubmit([this, expect, deadlineNs](int worker) {
        HistoryAsOf asOf;
        const HistoryAnswer a = m_deps.history->probe(worker, asOf, deadlineNs, expect);
        noteHistory(HistoryStatus::OK == a.status ? HistoryCause::NONE : a.cause, asOf.ageMs, asOf.eventSeq);
        m_probeInFlight.store(false);
    });
    if (!queued)
    {
        m_probeInFlight.store(false); // the history queue is full: this probe is skipped (the next one tries again)
    }
#endif
}

void GatewayApi::startHistoryProbe()
{
    if (nullptr == m_deps.history || m_probeThread.joinable())
    {
        return;
    }
    m_probeThread = std::thread([this] {
        pthread_setname_np(pthread_self(), "gw-hist-probe");
        std::unique_lock<std::mutex> lock(m_probeMutex);
        while (!m_probeStop)
        {
            lock.unlock();
            probeHistoryOnce();
            lock.lock();
            m_probeWake.wait_for(lock, std::chrono::milliseconds(m_s.historyProbeMs), [this] { return m_probeStop; });
        }
    });
}

void GatewayApi::stopHistoryProbe()
{
    {
        std::lock_guard<std::mutex> lock(m_probeMutex);
        m_probeStop = true;
    }
    m_probeWake.notify_all();
    if (m_probeThread.joinable())
    {
        m_probeThread.join();
    }
}

HistoryExpect GatewayApi::historyExpect() const
{
    HistoryExpect e;
    // the genesis of the gateway's last CoreConfigReport: kept across a core outage (the session keeps its last report),
    // none before the first one (every history read is then 503, cause epoch: the gateway cannot know)
    const auto config = m_deps.config ? m_deps.config() : nullptr;
    e.genesisId = config && config->reported ? config->head.genesisId : 0;
    e.maxAgeMs = m_s.historyMaxAgeMs;
    return e;
}

// asOf of a history answer: ageMs (the database's clock minus the projection's last beat), stale (ageMs above
// GATEWAY_HISTORY_STALE_MS), eventSeq (the projection's applied_event_seq, read before the rows: every row of an event
// at or below it is in the answer)
std::string GatewayApi::asOfJson(std::int64_t ageMs, std::int64_t eventSeq) const
{
    std::string out = "{\"ageMs\":" + std::to_string(ageMs);
#ifndef GW_NC_NO_STALE_LABEL
    out += std::string(",\"stale\":") + (ageMs > m_s.historyStaleMs ? "true" : "false");
#endif
#ifndef GW_NC_NO_ASOF_EVENT_SEQ
    out += ",\"eventSeq\":" + std::to_string(eventSeq);
#else
    (void)eventSeq;
#endif
    return out + "}";
}

bool GatewayApi::runHistory(const Ex &ex, const char *route, const char *query,
    std::function<void(int, std::int64_t)> job)
{
    const auto now = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    };
    const std::int64_t deadlineNs = now() + m_s.pgStatementTimeoutMs * 1'000'000;
    auto run = [this, ex, route, query, deadlineNs, now, job = std::move(job)](int worker) {
        if (now() >= deadlineNs)
        {
            // its client has waited out the read's whole time in the queue: not run, the store is never asked
            countHistory(query, "busy|expired");
            finish(ex, route, unavailable503(api::code::HISTORY_BUSY, "too many history reads at once",
                "the read waited in the history queue past its deadline (GATEWAY_PG_STATEMENT_TIMEOUT_MS)",
                m_s.historyBusyRetryAfterMs));
            return;
        }
        job(worker, deadlineNs);
    };
    if (nullptr != m_deps.historyPool && m_deps.historyPool->trySubmit(std::move(run)))
    {
        return true;
    }
    countHistory(query, "busy");
    finish(ex, route, unavailable503(api::code::HISTORY_BUSY, "too many history reads at once",
        "the history read queue (GATEWAY_PG_QUEUE) is full", m_s.historyBusyRetryAfterMs));
    return false;
}

void GatewayApi::historyPage(const Ex &ex, history::Kind kind)
{
    const bool orders = history::Kind::ORDERS == kind;
    const char *route = orders ? "orders_history" : "fills";
    const char *query = orders ? "orders" : "fills";
    ApiResponse bad;
    const bool known = orders ? api::readQuery(*ex, { "limit", "cursor", "marketId", "status" }, bad)
                              : api::readQuery(*ex, { "limit", "cursor", "marketId", "orderId" }, bad);
    if (!known)
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto values = api::queryValues(*ex);
    if (!values)
    {
        finish(ex, route, validation("malformed query string", "a parameter value has a bad %-escape"));
        return;
    }
    auto value = [&values](const char *name) -> const std::string * {
        const auto it = values->find(name);
        return it == values->end() ? nullptr : &it->second;
    };
    history::PageRequest request;
    request.kind = kind;
    request.limit = m_s.historyLimitDefault;
    json::object none;
    Fields f(none);
    if (const std::string *v = value("limit"))
    {
        // above GATEWAY_HISTORY_LIMIT_MAX is refused, never clamped: a client never gets fewer rows than it asked for
        // without being told
        request.limit = f.idOf("limit", *v, m_s.historyLimitMax);
    }
    if (const std::string *v = value("marketId"))
    {
        request.filter.marketId = static_cast<std::int32_t>(f.idOf("marketId", *v, INT32_MAX));
    }
    if (const std::string *v = value("status"))
    {
        if (history::knownStatus(*v))
        {
            request.filter.status = *v;
        }
        else
        {
            f.fail("status", "must be one of NEW, PARTIALLY_FILLED, FILLED, CANCELLED, REJECTED, EXPIRED, PENDING_NEW, "
                "PENDING_TRIGGER, PENDING_CANCEL, PENDING_REPLACE");
        }
    }
    if (const std::string *v = value("orderId"))
    {
        request.filter.orderId = f.idOf("orderId", *v);
    }
    if (!f.ok())
    {
        finish(ex, route, f.bad());
        return;
    }
    if (const std::string *v = value("cursor"))
    {
        history::Key key;
        switch (history::decodeCursor(*v, kind, request.filter, key))
        {
            case history::CursorError::NONE:
                request.after = key;
                break;
            case history::CursorError::MALFORMED:
                finish(ex, route, validation("invalid cursor", "send the nextCursor of the previous page unchanged"));
                return;
            case history::CursorError::OTHER_ROUTE:
                finish(ex, route, validation("invalid cursor", "the cursor belongs to another route"));
                return;
            case history::CursorError::OTHER_FILTER:
                finish(ex, route, validation("invalid cursor",
                    "the cursor was issued for another filter: keep marketId, status and orderId as on the first page"));
                return;
        }
    }
    withPrincipal(ex, route, [this, ex, route, query, request](const Principal &p) mutable {
        if (nullptr == m_deps.history)
        {
            finish(ex, route, historyUnavailable(HistoryCause::UNCONFIGURED, query));
            return;
        }
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        request.userId = p.userId; // always the principal's: nothing in the request can name a user
        const HistoryExpect expect = historyExpect();
        runHistory(ex, route, query, [this, ex, route, query, request, expect](int worker, std::int64_t deadlineNs) {
            HistoryPage page;
            const HistoryAnswer a = m_deps.history->page(worker, request, page, deadlineNs, expect);
            if (HistoryStatus::OK != a.status)
            {
                finish(ex, route, historyUnavailable(a.cause, query));
                return;
            }
            countHistory(query, "ok");
#ifdef GW_NC_NO_HISTORY_PROBE
            noteHistory(HistoryCause::NONE, page.ageMs, page.eventSeq); // test build only: a read stands in for the probe
#endif
            std::string out = "{\"items\":[";
            for (std::size_t i = 0; i < page.items.size(); ++i)
            {
                out += (0 == i ? "" : ",") + page.items[i];
            }
            out += "],\"nextCursor\":" + (page.nextCursor ? api::quote(*page.nextCursor) : std::string("null")) +
                ",\"asOf\":" + asOfJson(page.ageMs, page.eventSeq) + "}";
            finish(ex, route, api::jsonBody(200, std::move(out)));
        });
    });
}

// GET /orders/{orderId} of an order the core does not hold open: the caller's row in history, or 404 (unknown and
// another user's order get the same answer). Called from the core's completion: the query runs on the history pool.
void GatewayApi::orderFromHistory(const Ex &ex, std::int64_t userId, std::int64_t orderId)
{
    static const char *route = "orders_get";
    if (nullptr == m_deps.history)
    {
        finish(ex, route, historyUnavailable(HistoryCause::UNCONFIGURED, "order"));
        return;
    }
    const HistoryExpect expect = historyExpect();
    runHistory(ex, route, "order", [this, ex, userId, orderId, expect](int worker, std::int64_t deadlineNs) {
        std::string json;
        HistoryAsOf asOf;
        const HistoryAnswer a = m_deps.history->order(worker, userId, orderId, json, asOf, deadlineNs, expect);
        switch (a.status)
        {
            case HistoryStatus::OK:
                // the history shape of Order plus the read's asOf (an order the core holds open answers without one)
                if (json.size() < 2 || '}' != json.back())
                {
                    break; // not an object: a bad row, never a body without asOf
                }
                countHistory("order", "ok");
#ifdef GW_NC_NO_HISTORY_PROBE
                noteHistory(HistoryCause::NONE, asOf.ageMs, asOf.eventSeq); // test build only: a read stands in
#endif
                json.pop_back();
                json += std::string('{' == json.back() ? "" : ",") + "\"asOf\":" +
                    asOfJson(asOf.ageMs, asOf.eventSeq) + "}";
                finish(ex, route, api::jsonBody(200, std::move(json)));
                return;
            case HistoryStatus::NOT_FOUND:
                countHistory("order", "not_found");
                finish(ex, route, api::error(404, api::code::NOT_FOUND, "not found"));
                return;
            case HistoryStatus::UNAVAILABLE:
                break;
        }
        finish(ex, route, historyUnavailable(HistoryStatus::OK == a.status ? HistoryCause::BAD_ROW : a.cause, "order"));
    });
}

void GatewayApi::ordersHistory(const Ex &ex)
{
    historyPage(ex, history::Kind::ORDERS);
}

void GatewayApi::fills(const Ex &ex)
{
    historyPage(ex, history::Kind::FILLS);
}

// ---------------------------------------------------------------- balances and the treasury lane

void GatewayApi::balances(const Ex &ex)
{
    static const char *route = "balances";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    withPrincipal(ex, route, [this, ex](const Principal &p) {
        if (const auto hit = cached(p.userId, "balances"))
        {
            finish(ex, route, api::jsonBody(200, *hit));
            return;
        }
        const auto config = loadedConfig(ex, route);
        if (!config || !takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        wire::RequestUserBalances q;
        q.userId = p.userId;
        const std::int64_t userId = p.userId;
        ask(ex, route, false, session::Command{ q }, [this, ex, userId, config](const session::Outcome &o) {
            const auto *b = std::get_if<session::BalancesReply>(&o.reply);
            if (session::OutcomeKind::REPLY != o.kind || nullptr == b)
            {
                finish(ex, route, failure(o, Asked::READ));
                return;
            }
            std::map<std::int32_t, std::pair<std::int64_t, std::int64_t>> rows; // assetId -> available, locked
            std::map<std::int32_t, std::string> codes;
            for (const wire::AssetConfig &a : config->assets)
            {
                rows[a.assetId] = { 0, 0 };
                codes[a.assetId] = rest::fixedText(a.code.data(), a.code.size());
            }
            for (const wire::BalanceLine &l : b->lines)
            {
                rows[l.assetId] = { l.available, l.locked };
            }
            std::string out = "{\"asOf\":{\"logPosition\":" + std::to_string(b->head.asOfLogPosition) + "},\"assets\":[";
            bool first = true;
            for (const auto &[assetId, v] : rows)
            {
                std::int64_t total = 0;
                if (__builtin_add_overflow(v.first, v.second, &total))
                {
                    frontLog(FrontLevel::ERROR, "balances: available + locked overflows for user %lld asset %d",
                        static_cast<long long>(userId), assetId);
                    finish(ex, route, api::error(500, api::code::INTERNAL, "internal error"));
                    return;
                }
                const auto code = codes.find(assetId);
                out += (first ? "" : ",") + std::string("{\"assetId\":") + idJson(assetId) + ",\"asset\":" +
                    (code == codes.end() ? std::string("null") : api::quote(code->second)) + ",\"available\":" +
                    moneyJson(v.first) + ",\"locked\":" + moneyJson(v.second) + ",\"total\":" + moneyJson(total) + "}";
                first = false;
            }
            out += "]}";
            cache(userId, "balances", out);
            finish(ex, route, api::jsonBody(200, std::move(out)));
        });
    });
}

void GatewayApi::deposit(const Ex &ex)
{
    moneyCommand(ex, true);
}

void GatewayApi::withdraw(const Ex &ex)
{
    moneyCommand(ex, false);
}

void GatewayApi::moneyCommand(const Ex &ex, bool isDeposit)
{
    const char *route = isDeposit ? "balances_deposit" : "balances_withdraw";
    if (!m_deps.askTreasury)
    {
        finish(ex, route, api::error(404, api::code::FEATURE_DISABLED, "the treasury lane is off",
            "deposit and withdraw are a demo feature (GATEWAY_ALLOW_TREASURY_LANE)"));
        return;
    }
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    const auto body = api::readBody(*ex, { "assetId", "amount" }, bad);
    if (!body)
    {
        finish(ex, route, std::move(bad));
        return;
    }
    Fields f(*body);
    const std::int64_t assetId = f.id("assetId", true, INT32_MAX, 0); // asset 0 is a valid asset id
    const std::int64_t amount = f.money("amount", true);
    if (f.ok() && amount <= 0)
    {
        f.fail("amount", "must be above zero");
    }
    if (!f.ok())
    {
        finish(ex, route, f.bad());
        return;
    }
    rest::RequestKey key;
    if (auto refused = readKey(*ex, true, key))
    {
        finish(ex, route, std::move(*refused));
        return;
    }
    const auto config = loadedConfig(ex, route);
    if (!config)
    {
        return;
    }
#ifndef GW_NC_NO_ASSET_CHECK
    const bool knownAsset = std::any_of(config->assets.begin(), config->assets.end(),
        [assetId](const wire::AssetConfig &a) { return a.assetId == assetId; });
    if (!knownAsset)
    {
        // the core's decoder drops a Deposit or Withdraw of an unknown asset without an answer
        finish(ex, route, validation("unknown asset", "assetId " + std::to_string(assetId) +
            " is not in the core's asset list (/api/v2/markets)"));
        return;
    }
#endif
    const auto cap = std::find_if(m_faucetCaps.begin(), m_faucetCaps.end(),
        [assetId](const auto &c) { return c.first == assetId; });
    if (cap == m_faucetCaps.end() || amount > cap->second)
    {
        api::ErrorSpec s;
        s.status = 422;
        s.code = api::code::REJECTED;
        s.text = "above the faucet cap";
        s.detail = cap == m_faucetCaps.end() ? "asset " + std::to_string(assetId) + " has no GATEWAY_FAUCET_MAX_AMOUNT entry"
            : "at most " + rest::formatMoney(cap->second) + " of asset " + std::to_string(assetId) + " per request";
        finish(ex, route, api::error(std::move(s)));
        return;
    }
    wire::MoneyCommand c;
    c.assetId = static_cast<std::int32_t>(assetId);
    c.amount = amount;
    c.requestIdHigh = key.high;
    c.requestIdLow = key.low;
    withPrincipal(ex, route, [this, ex, c, isDeposit, route](const Principal &p) mutable {
        if (!takePrincipal(ex, route, p.userId, 1))
        {
            return;
        }
        const TakeResult t = m_faucetBuckets.take(std::to_string(p.userId), m_deps.nowMs());
        if (!t.ok)
        {
            api::ErrorSpec s;
            s.status = 429;
            s.code = api::code::RATE_LIMITED;
            s.text = "too many requests";
            s.detail = "the faucet rate (GATEWAY_FAUCET_RATE) is spent";
            s.retryAfterMs = std::max<std::int64_t>(t.retryAfterMs, 1);
            finish(ex, route, api::error(std::move(s)));
            return;
        }
        c.userId = p.userId;
        const std::int64_t userId = p.userId;
        session::Command command = isDeposit ? session::Command{ session::Deposit{ c } } : session::Command{ session::Withdraw{ c } };
        ask(ex, route, true, std::move(command), [this, ex, route, userId](const session::Outcome &o) {
            dropCache(userId);
            const wire::MoneyAck *ack = nullptr;
            if (const auto *d = std::get_if<session::DepositAckReply>(&o.reply))
            {
                ack = &d->ack;
            }
            else if (const auto *w = std::get_if<session::WithdrawAckReply>(&o.reply))
            {
                ack = &w->ack;
            }
            if (session::OutcomeKind::REPLY != o.kind || nullptr == ack)
            {
                finish(ex, route, failure(o, Asked::KEYED_FAUCET));
                return;
            }
            finish(ex, route, api::jsonBody(200, "{\"assetId\":" + idJson(ack->assetId) + ",\"amount\":" +
                moneyJson(ack->amount) + ",\"available\":" + moneyJson(ack->newAvailable) + ",\"duplicate\":" +
                (ack->duplicate ? "true" : "false") + "}"));
        });
    });
}

// Register funding: one deposit per capped asset the config lists, each its cap, with the derived requestId; `done`
// gets true when every one was acked (a duplicate ack included).
void GatewayApi::fundRegistration(std::int64_t userId, std::function<void(bool)> done)
{
    const auto config = m_deps.config();
    std::vector<std::pair<std::int32_t, std::int64_t>> plan;
    bool complete = !m_faucetCaps.empty();
    for (const auto &[assetId, amount] : m_faucetCaps)
    {
        const bool listed = config && std::any_of(config->assets.begin(), config->assets.end(),
            [assetId = assetId](const wire::AssetConfig &a) { return a.assetId == assetId; });
        if (listed)
        {
            plan.emplace_back(assetId, amount);
        }
        else
        {
            complete = false; // never sent: the core would drop it unanswered
        }
    }
    if (plan.empty())
    {
        done(false);
        return;
    }
    struct Funding
    {
        std::mutex mutex;
        std::size_t pending = 0;
        bool all = true;
    };
    auto state = std::make_shared<Funding>();
    state->pending = plan.size();
    state->all = complete;
    for (const auto &[assetId, amount] : plan)
    {
        fundOne(userId, assetId, amount, 1, [state, done](bool acked) {
            bool last = false;
            bool all = false;
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->all = state->all && acked;
                last = 0 == --state->pending;
                all = state->all;
            }
            if (last)
            {
                done(all);
            }
        });
    }
}

void GatewayApi::fundOne(std::int64_t userId, std::int32_t assetId, std::int64_t amount, int attempt,
    std::function<void(bool)> done)
{
    wire::MoneyCommand c;
    c.userId = userId;
    c.assetId = assetId;
    c.amount = amount;
#ifdef GW_NC_RANDOM_FUNDING_ID
    // test-only control: a fresh random requestId per attempt instead of the derived one
    const std::string r = crypto::randomBytes(16);
    std::memcpy(&c.requestIdHigh, r.data(), 8);
    std::memcpy(&c.requestIdLow, r.data() + 8, 8);
#else
    rest::fundingRequestId(userId, assetId, c.requestIdHigh, c.requestIdLow);
#endif
    if (0 == c.requestIdHigh && 0 == c.requestIdLow)
    {
        done(false); // a 0/0 digest: not a key, never sent
        return;
    }
    m_fundingDeposits.fetch_add(1, std::memory_order_relaxed);
    const bool queued = m_deps.askTreasury(session::Command{ session::Deposit{ c } },
        [this, userId, assetId, amount, attempt, done](const session::Outcome &o) {
            if (session::OutcomeKind::REPLY == o.kind && std::holds_alternative<session::DepositAckReply>(o.reply))
            {
                done(true);
                return;
            }
            if (session::OutcomeKind::OUTCOME_UNKNOWN == o.kind && attempt < 2)
            {
                // The ack may be lost: once more inside this register request, with the same derived id, but only while
                // the core still remembers that id. The second attempt reaches the core up to two request timeouts
                // after the first was offered, so the window must be longer than that; otherwise a re-send of an
                // applied deposit would credit twice, and funded stays false instead.
                const auto config = m_deps.config();
                const std::int64_t window = config ? config->idempotencyWindowMs() : 0;
                if (reofferFitsWindow(window, m_s.requestTimeoutMs))
                {
                    fundOne(userId, assetId, amount, attempt + 1, done);
                    return;
                }
                frontLog(FrontLevel::WARN, "register funding of user %lld asset %d: outcome unknown and the idempotence "
                    "window (%lld ms) is not above twice GATEWAY_REQUEST_TIMEOUT_MS (%lld ms): not re-sent",
                    static_cast<long long>(userId), assetId, static_cast<long long>(window),
                    static_cast<long long>(m_s.requestTimeoutMs));
                done(false);
                return;
            }
            frontLog(FrontLevel::WARN, "register funding of user %lld asset %d not acked (%s): funded false",
                static_cast<long long>(userId), assetId, session::nameOf(o.kind));
            done(false);
        });
    if (!queued)
    {
        done(false);
    }
}

}}
