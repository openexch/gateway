#include "GatewayApi.h"

#include <algorithm>
#include <charconv>
#include <chrono>

#include <boost/json.hpp>

#include "gateway/common/Crypto.h"
#include "gateway/common/FrontLog.h"
#include "gateway/common/Password.h"
#include "gateway/common/Rest.h"

namespace openexchange { namespace gateway {

namespace json = boost::json;

namespace {

constexpr const char *API = "/api/v2";

std::int64_t steadyMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool validUsername(const std::string &u)
{
    if (u.size() < 3 || u.size() > 20)
    {
        return false;
    }
    for (const char c : u)
    {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || '_' == c))
        {
            return false;
        }
    }
    return true;
}

std::size_t codePoints(const std::string &utf8)
{
    std::size_t n = 0;
    for (const char c : utf8)
    {
        n += (static_cast<unsigned char>(c) & 0xC0) != 0x80 ? 1 : 0;
    }
    return n;
}

ApiResponse unavailable(const char *code, std::string text, std::int64_t retryAfterMs = 1000)
{
    api::ErrorSpec s;
    s.status = 503;
    s.code = code;
    s.text = std::move(text);
    s.retryAfterMs = retryAfterMs;
    return api::error(std::move(s));
}

ApiResponse storeFailure(StoreStatus st)
{
    return StoreStatus::UNAVAILABLE == st ? unavailable(api::code::AUTH_UNAVAILABLE, "the users store cannot answer")
                                          : api::error(500, api::code::INTERNAL, "internal error");
}

std::string userJson(std::int64_t userId, const std::optional<std::string> &username)
{
    return "\"userId\":" + api::quote(std::to_string(userId)) + ",\"username\":" +
        (username ? api::quote(*username) : std::string("null"));
}

// The answer for a core outcome that is not the reply the caller wanted.
ApiResponse coreFailure(const session::Outcome &o)
{
    switch (o.kind)
    {
        case session::OutcomeKind::OUTCOME_UNKNOWN:
            return api::error(504, api::code::OUTCOME_UNKNOWN, "the core did not answer in time",
                "nothing was written; retry the request");
        case session::OutcomeKind::CORE_UNAVAILABLE:
            return unavailable(api::code::CORE_UNAVAILABLE, "the core session is not available");
        case session::OutcomeKind::OVERLOADED:
            return unavailable(api::code::OVERLOADED, "the gateway is overloaded");
        case session::OutcomeKind::INTERNAL:
            return api::error(500, api::code::INTERNAL, "internal error");
        case session::OutcomeKind::REPLY:
            break;
    }
    if (const auto *reject = std::get_if<wire::OrderReject>(&o.reply))
    {
        const char *reason = wire::money::CoreRejectReason::c_str(reject->reason);
        if (wire::money::CoreRejectReason::UNAUTHORIZED_CLASS == reject->reason)
        {
            frontLog(FrontLevel::CRITICAL, "the core refused this gateway's session class for a balances read "
                "(UNAUTHORIZED_CLASS): a deployment error");
            api::ErrorSpec s;
            s.status = 403;
            s.code = api::code::FORBIDDEN;
            s.text = "the gateway's core credential may not do this";
            s.reason = reason;
            return api::error(std::move(s));
        }
        frontLog(FrontLevel::ERROR, "unexpected core reject %s for a balances read", reason);
        api::ErrorSpec s;
        s.status = 500;
        s.code = api::code::INTERNAL;
        s.text = "internal error";
        s.reason = reason;
        return api::error(std::move(s));
    }
    frontLog(FrontLevel::ERROR, "unexpected core reply (template %u) for a balances read",
        static_cast<unsigned>(o.templateId));
    return api::error(500, api::code::INTERNAL, "internal error");
}

}

namespace {

std::int64_t steadyNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}

GatewayApi::~GatewayApi()
{
    stopHistoryProbe();
}

GatewayApi::GatewayApi(ApiDeps deps) :
    m_deps(std::move(deps)),
    m_s(*m_deps.settings),
    m_sources(m_s.trustedProxies),
    m_registerBucket(m_s.registerRate, m_s.registerBurst),
    m_loginBucket(m_s.loginRate, m_s.loginBurst),
    m_sourceBuckets(m_s.authSourceRate, m_s.authSourceBurst),
    m_lookupBuckets(m_s.tokenLookupRate, m_s.tokenLookupBurst),
    m_principalBuckets(m_s.principalRate, m_s.principalBurst),
    m_faucetBuckets(m_s.faucetRate, std::max<std::int64_t>(1, m_s.faucetRate.count))
{
    if (!m_deps.nowMs)
    {
        m_deps.nowMs = &steadyMs;
    }
    if (!m_deps.config)
    {
        auto none = std::make_shared<const session::CoreConfig>();
        m_deps.config = [none] { return none; };
    }
    if (!m_deps.registered)
    {
        m_deps.registered = [] { return false; };
    }
    for (const FaucetCap &cap : m_s.faucetMaxAmount)
    {
        // the settings reader refused every cap rest::parseMoney does not read, so a miss here is a defect
        const auto units = rest::parseMoney(cap.amount);
        if (!units)
        {
            frontLog(FrontLevel::CRITICAL, "faucet: the cap '%s' of asset %lld passed the settings reader but does not "
                "parse (a defect): the asset has no faucet", cap.amount.c_str(), static_cast<long long>(cap.assetId));
            continue;
        }
        m_faucetCaps.emplace_back(static_cast<std::int32_t>(cap.assetId), *units);
    }
}

namespace {

// the order routes of the latency metrics: their index, or -1
int orderRouteOf(std::string_view route) noexcept
{
    if ("orders_create" == route)
    {
        return 0;
    }
    if ("orders_cancel" == route)
    {
        return 1;
    }
    if ("orders_amend" == route)
    {
        return 2;
    }
    return -1;
}

constexpr const char *ORDER_ROUTE_LABEL[] = { "create", "cancel", "amend" };
constexpr const char *ORDER_STAGE_LABEL[] = { "queue", "core", "local" };

}

void GatewayApi::finish(const Ex &ex, const char *route, ApiResponse response)
{
    if (const int which = orderRouteOf(route); which >= 0)
    {
        recordOrder(static_cast<std::size_t>(which), ex->receivedNs(), nullptr, steadyNs());
    }
    respondCounted(ex, route, std::move(response));
}

void GatewayApi::finishCore(const Ex &ex, const char *route, OrderRoute which, const session::Outcome &o,
    ApiResponse response)
{
    recordOrder(static_cast<std::size_t>(which), ex->receivedNs(), &o, steadyNs());
    respondCounted(ex, route, std::move(response));
}

// receivedNs (the I/O thread, once the request was read), Outcome::submittedNs (the duty thread, the command taken into
// the core session) and completedNs (the duty thread, the reply read or the request given up) and nowNs come from the
// same steady clock. Stages only for a core reply: a command the session refused or never answered has no core span.
void GatewayApi::recordOrder(std::size_t which, std::int64_t receivedNs, const session::Outcome *o, std::int64_t nowNs)
{
    const std::int64_t total = nowNs - receivedNs;
    if (total < 0)
    {
        m_latencyAnomalies.fetch_add(1, std::memory_order_relaxed);
    }
    else
    {
        m_orderTotal[which][nullptr == o ? 1 : 0].record(total, nowNs);
    }
    if (nullptr == o || session::OutcomeKind::REPLY != o->kind)
    {
        return;
    }
    if (0 == o->submittedNs || receivedNs > o->submittedNs || o->submittedNs > o->completedNs ||
        o->completedNs > nowNs)
    {
        m_latencyAnomalies.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    m_orderStage[which][0].record(o->submittedNs - receivedNs, nowNs);
    m_orderStage[which][1].record(o->completedNs - o->submittedNs, nowNs);
    m_orderStage[which][2].record(nowNs - o->completedNs, nowNs);
}

void GatewayApi::respondCounted(const Ex &ex, const char *route, ApiResponse response)
{
    if (const auto origin = ex->header("Origin"))
    {
        for (const std::string &allowed : m_s.corsOrigins)
        {
            if (*origin == allowed)
            {
                response.headers.emplace_back("Access-Control-Allow-Origin", allowed);
                // a browser script reads only the safelisted headers unless told: Retry-After of every 429 and 503
                response.headers.emplace_back("Access-Control-Expose-Headers", "Retry-After");
                response.headers.emplace_back("Vary", "Origin");
                break;
            }
        }
    }
    if (401 == response.status)
    {
        response.headers.emplace_back("WWW-Authenticate", "Bearer");
    }
    {
        std::lock_guard<std::mutex> lock(m_countMutex);
        m_requests[{ route, response.status }]++;
    }
    ex->respond(std::move(response));
}

void GatewayApi::preflight(const Ex &ex)
{
    const auto origin = ex->header("Origin");
    for (const std::string &allowed : m_s.corsOrigins)
    {
        if (origin && *origin == allowed)
        {
            ApiResponse r = api::noContent();
            r.headers.emplace_back("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
            r.headers.emplace_back("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key, X-API-Key");
            r.headers.emplace_back("Access-Control-Max-Age", "600");
            finish(ex, "preflight", std::move(r));
            return;
        }
    }
    finish(ex, "preflight", api::error(403, api::code::FORBIDDEN, "origin not allowed",
        "the Origin is not listed in GATEWAY_CORS_ORIGINS"));
}

void GatewayApi::handle(Ex ex)
{
    const std::string_view path = ex->path();
    const std::string_view method = ex->method();
    if ("OPTIONS" == method)
    {
        preflight(ex);
        return;
    }
    if (0 != path.rfind(API, 0))
    {
        finish(ex, "unknown", api::error(404, api::code::NOT_FOUND, "no such route",
            std::string(path.substr(0, 64)) + " is not served; the API is under /api/v2"));
        return;
    }
    struct Route
    {
        const char *path;
        const char *method;
        void (GatewayApi::*fn)(const Ex &);
        const char *name;
    };
    static const Route routes[] = {
        { "/api/v2/auth/register", "POST", &GatewayApi::registerUser, "auth_register" },
        { "/api/v2/auth/login", "POST", &GatewayApi::login, "auth_login" },
        { "/api/v2/auth/logout", "POST", &GatewayApi::logout, "auth_logout" },
        { "/api/v2/auth/me", "GET", &GatewayApi::me, "auth_me" },
        { "/api/v2/markets", "GET", &GatewayApi::markets, "markets" },
        { "/api/v2/status", "GET", &GatewayApi::status, "status" },
        { "/api/v2/orders", "POST", &GatewayApi::createOrder, "orders_create" },
        { "/api/v2/orders", "GET", &GatewayApi::listOrders, "orders_list" },
        { "/api/v2/orders/cancel", "POST", &GatewayApi::bulkCancel, "orders_bulk_cancel" },
        { "/api/v2/orders/history", "GET", &GatewayApi::ordersHistory, "orders_history" },
        { "/api/v2/fills", "GET", &GatewayApi::fills, "fills" },
        { "/api/v2/balances", "GET", &GatewayApi::balances, "balances" },
        { "/api/v2/balances/movements", "GET", &GatewayApi::balanceMovements, "balances_movements" },
        { "/api/v2/balances/deposit", "POST", &GatewayApi::deposit, "balances_deposit" },
        { "/api/v2/balances/withdraw", "POST", &GatewayApi::withdraw, "balances_withdraw" },
    };
    std::string allowed;
    const char *matchedName = nullptr;
    for (const Route &r : routes)
    {
        if (path == r.path)
        {
            if (method == r.method)
            {
                (this->*r.fn)(ex);
                return;
            }
            allowed += (allowed.empty() ? "" : ", ") + std::string(r.method);
            matchedName = r.name;
        }
    }
    // /api/v2/orders/{orderId}: GET, PUT, DELETE; /api/v2/orders/{orderId}/trail: GET
    static constexpr std::string_view ORDER_PREFIX = "/api/v2/orders/";
    static constexpr std::string_view TRAIL_SUFFIX = "/trail";
    if (nullptr == matchedName && 0 == path.rfind(ORDER_PREFIX, 0))
    {
        std::string_view idText = path.substr(ORDER_PREFIX.size());
        const bool trail = idText.size() > TRAIL_SUFFIX.size() &&
            0 == idText.compare(idText.size() - TRAIL_SUFFIX.size(), TRAIL_SUFFIX.size(), TRAIL_SUFFIX);
        if (trail)
        {
            idText.remove_suffix(TRAIL_SUFFIX.size());
        }
        const char *name = trail ? "orders_trail" : "GET" == method ? "orders_get" : "PUT" == method ? "orders_amend"
            : "DELETE" == method ? "orders_cancel" : "orders_id";
        std::int64_t orderId = 0;
        bool good = !idText.empty() && '0' != idText[0];
        for (const char c : idText)
        {
            good = good && c >= '0' && c <= '9';
        }
        if (good)
        {
            const auto [end, ec] = std::from_chars(idText.data(), idText.data() + idText.size(), orderId);
            good = std::errc() == ec && end == idText.data() + idText.size(); // overflow refused
        }
        if (!good || orderId <= 0)
        {
            finish(ex, name, api::error(400, api::code::VALIDATION, "invalid orderId",
                "the path's orderId must be a positive decimal integer"));
            return;
        }
        if (trail)
        {
            if ("GET" == method)
            {
                orderTrail(ex, orderId);
                return;
            }
            ApiResponse resp = api::error(405, api::code::METHOD_NOT_ALLOWED, "method not allowed",
                "/api/v2/orders/{orderId}/trail takes GET");
            resp.headers.emplace_back("Allow", "GET");
            finish(ex, name, std::move(resp));
        }
        else if ("GET" == method)
        {
            getOrder(ex, orderId);
        }
        else if ("PUT" == method)
        {
            amendOrder(ex, orderId);
        }
        else if ("DELETE" == method)
        {
            cancelOrder(ex, orderId);
        }
        else
        {
            ApiResponse resp = api::error(405, api::code::METHOD_NOT_ALLOWED, "method not allowed",
                "/api/v2/orders/{orderId} takes GET, PUT or DELETE");
            resp.headers.emplace_back("Allow", "GET, PUT, DELETE");
            finish(ex, name, std::move(resp));
        }
        return;
    }
    if (nullptr != matchedName)
    {
        ApiResponse resp = api::error(405, api::code::METHOD_NOT_ALLOWED, "method not allowed",
            std::string(path) + " takes " + allowed);
        resp.headers.emplace_back("Allow", allowed);
        finish(ex, matchedName, std::move(resp));
        return;
    }
    finish(ex, "unknown", api::error(404, api::code::NOT_FOUND, "no such route", std::string(path.substr(0, 64))));
}

void GatewayApi::withPrincipal(const Ex &ex, const char *route, std::function<void(const Principal &)> next)
{
    if (ex->headerCount("Authorization") > 1 || ex->headerCount("X-API-Key") > 1)
    {
        finish(ex, route, api::error(401, api::code::UNAUTHORIZED, "unauthorized", "a credential header is repeated"));
        return;
    }
    CredentialHeaders h;
    h.authorization = ex->header("Authorization");
    h.apiKey = ex->header("X-API-Key");
    h.target = ex->target();
    h.lookupBudget = [this, &ex] { return m_lookupBuckets.take(sourceKey(ex), m_deps.nowMs()); };
    m_deps.auth->resolve(h, [this, ex, route, next = std::move(next)](AuthResult r) {
        switch (r.outcome)
        {
            case AuthOutcome::OK:
                next(r.principal);
                return;
            case AuthOutcome::UNAUTHORIZED:
                finish(ex, route, api::error(401, api::code::UNAUTHORIZED, "unauthorized", r.detail));
                return;
            case AuthOutcome::UNAVAILABLE:
                finish(ex, route, unavailable(api::code::AUTH_UNAVAILABLE, "the credential cannot be checked now"));
                return;
            case AuthOutcome::BUSY:
                finish(ex, route, unavailable(api::code::AUTH_BUSY, "the credential cannot be checked now"));
                return;
            case AuthOutcome::RATE_LIMITED:
            {
                api::ErrorSpec s;
                s.status = 429;
                s.code = api::code::RATE_LIMITED;
                s.text = "too many requests";
                s.retryAfterMs = r.retryAfterMs;
                finish(ex, route, api::error(std::move(s)));
                return;
            }
        }
    });
}

std::string GatewayApi::sourceKey(const Ex &ex) const
{
    std::vector<std::string_view> forwarded;
    if (m_sources.enabled())
    {
        forwarded = ex->headers(m_s.clientIpHeader);
    }
    return budgetKey(m_sources.resolve(ex->peer(), forwarded), static_cast<int>(m_s.sourceV6PrefixBits));
}

bool GatewayApi::takeAuthBudget(const Ex &ex, const char *route, RateBucket &global)
{
    const std::int64_t now = m_deps.nowMs();
    const std::string source = sourceKey(ex);
    TakeResult t = m_sourceBuckets.take(source, now);
    if (t.ok)
    {
        t = global.take(now);
        if (!t.ok)
        {
            m_sourceBuckets.refund(source, now); // the request is refused: it costs the source nothing
        }
    }
    if (t.ok)
    {
        return true;
    }
    m_authRateLimited.fetch_add(1, std::memory_order_relaxed);
    api::ErrorSpec s;
    s.status = 429;
    s.code = api::code::RATE_LIMITED;
    s.text = "too many requests";
    s.retryAfterMs = t.retryAfterMs;
    finish(ex, route, api::error(std::move(s)));
    return false;
}

bool GatewayApi::credentials(const Ex &ex, const char *route, std::string &username, std::string &password)
{
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return false;
    }
    const auto body = api::readBody(*ex, { "username", "password" }, bad);
    if (!body)
    {
        finish(ex, route, std::move(bad));
        return false;
    }
    const json::value *u = body->if_contains("username");
    const json::value *p = body->if_contains("password");
    if (nullptr == u || !u->is_string() || !validUsername(std::string(u->as_string())))
    {
        finish(ex, route, api::error(400, api::code::VALIDATION, "invalid username",
            "username must be a string of 3 to 20 characters a-z, 0-9 or _"));
        return false;
    }
    if (nullptr == p || !p->is_string() || codePoints(std::string(p->as_string())) < 6)
    {
        finish(ex, route, api::error(400, api::code::VALIDATION, "invalid password",
            "password must be a string of 6 or more characters"));
        return false;
    }
    username = std::string(u->as_string());
    password = std::string(p->as_string());
    return true;
}

void GatewayApi::runHash(const Ex &ex, const char *route, std::function<void()> job)
{
#ifdef GW_NC_HASH_ON_IO
    (void)route;
    ex->runOnIo(std::move(job)); // control only: PBKDF2 on the I/O strand
#else
    if (!m_deps.hashPool->trySubmit([job = std::move(job)](int) { job(); }))
    {
        m_hashBusy.fetch_add(1, std::memory_order_relaxed);
        finish(ex, route, unavailable(api::code::AUTH_BUSY, "too many logins and registrations at once"));
    }
#endif
}

void GatewayApi::runStore(const Ex &ex, const char *route, std::function<void(int)> job)
{
    if (!m_deps.storePool->trySubmit(std::move(job)))
    {
        finish(ex, route, unavailable(api::code::AUTH_BUSY, "the users store queue is full"));
    }
}

void GatewayApi::registerUser(const Ex &ex)
{
    static const char *route = "auth_register";
    if (0 == (m_deps.auth->modes() & auth_mode::DEMO))
    {
        finish(ex, route, api::error(404, api::code::FEATURE_DISABLED, "registration is not enabled",
            "register and login need demo in GATEWAY_AUTH_MODES"));
        return;
    }
    std::string username;
    std::string password;
    if (!credentials(ex, route, username, password) || !takeAuthBudget(ex, route, m_registerBucket))
    {
        return;
    }
    const std::uint32_t iterations = static_cast<std::uint32_t>(m_s.pbkdf2Iterations);
    runHash(ex, route, [this, ex, username, password, iterations] {
        const std::string hash = password::hash(password, iterations);
        runStore(ex, route, [this, ex, username, hash](int worker) {
            std::int64_t userId = 0;
            const StoreStatus st = m_deps.store->reserveUserId(worker, username, userId);
            if (StoreStatus::CONFLICT == st)
            {
                finish(ex, route, api::error(409, api::code::CONFLICT, "username taken"));
                return;
            }
            if (StoreStatus::OK != st)
            {
                finish(ex, route, storeFailure(st));
                return;
            }
            registerReserved(ex, username, hash, userId, worker);
        });
    });
}

void GatewayApi::registerReserved(const Ex &ex, const std::string &username, const std::string &passwordHash,
    std::int64_t userId, int worker)
{
    static const char *route = "auth_register";
#ifdef GW_NC_NO_IDENTITY_CHECK
    registerInsert(ex, username, passwordHash, userId, worker); // control only: no core question
    return;
#else
    (void)worker;
#endif
    wire::RequestUserBalances q;
    q.userId = userId;
    const bool queued = m_deps.askCore(session::Command{ q },
        [this, ex, username, passwordHash, userId](const session::Outcome &o) {
            const auto *balances = std::get_if<session::BalancesReply>(&o.reply);
            if (session::OutcomeKind::REPLY != o.kind || nullptr == balances)
            {
                finish(ex, route, coreFailure(o));
                return;
            }
            if (balances->head.userId != userId)
            {
                frontLog(FrontLevel::ERROR, "identity check: asked for user %lld, the core answered for %lld",
                    static_cast<long long>(userId), static_cast<long long>(balances->head.userId));
                finish(ex, route, api::error(500, api::code::INTERNAL, "internal error"));
                return;
            }
            if (balances->head.known)
            {
                m_userIdConflicts.fetch_add(1, std::memory_order_relaxed);
                frontLog(FrontLevel::CRITICAL, "identity check: the users database handed out userId %lld, which the "
                    "core already knows: no user row written, nothing funded. The users database is behind the core "
                    "(reset or restored): set demo_user_seq above the highest userId the core knows before registration "
                    "reopens", static_cast<long long>(userId));
                finish(ex, route, unavailable(api::code::IDENTITY_STORE_INCONSISTENT,
                    "registration is unavailable: the users store is behind the core", 60'000));
                return;
            }
            runStore(ex, route, [this, ex, username, passwordHash, userId](int w) {
                registerInsert(ex, username, passwordHash, userId, w);
            });
        });
    if (!queued)
    {
        finish(ex, route, unavailable(api::code::OVERLOADED, "the gateway is overloaded"));
    }
}

void GatewayApi::registerInsert(const Ex &ex, const std::string &username, const std::string &passwordHash,
    std::int64_t userId, int worker)
{
    static const char *route = "auth_register";
    // on store worker `worker`
    const std::string token = newDemoToken();
    const crypto::Digest digest = crypto::sha256(token);
    const std::string tokenHash(crypto::view(digest));
    const StoreStatus st = m_deps.store->insertUser(worker, userId, username, passwordHash, tokenHash);
    if (StoreStatus::CONFLICT == st)
    {
        finish(ex, route, api::error(409, api::code::CONFLICT, "username taken"));
        return;
    }
    if (StoreStatus::OK != st)
    {
        finish(ex, route, storeFailure(st));
        return;
    }
    m_deps.auth->remember(tokenHash, userId, username);
    const std::string head = "{" + userJson(userId, username) + ",\"token\":" + api::quote(token) + ",\"funded\":";
    if (!m_deps.askTreasury)
    {
        finish(ex, route, api::jsonBody(201, head + "false}")); // the treasury lane is off: nothing is funded
        return;
    }
    fundRegistration(userId, [this, ex, head](bool funded) {
        finish(ex, route, api::jsonBody(201, head + (funded ? "true}" : "false}")));
    });
}

void GatewayApi::login(const Ex &ex)
{
    static const char *route = "auth_login";
    if (0 == (m_deps.auth->modes() & auth_mode::DEMO))
    {
        finish(ex, route, api::error(404, api::code::FEATURE_DISABLED, "login is not enabled",
            "register and login need demo in GATEWAY_AUTH_MODES"));
        return;
    }
    std::string username;
    std::string password;
    if (!credentials(ex, route, username, password) || !takeAuthBudget(ex, route, m_loginBucket))
    {
        return;
    }
    const std::uint32_t iterations = static_cast<std::uint32_t>(m_s.pbkdf2Iterations);
    runStore(ex, route, [this, ex, username, password, iterations](int worker) {
        auto user = std::make_shared<StoredUser>();
        const StoreStatus st = m_deps.store->findByUsername(worker, username, *user);
        if (StoreStatus::OK != st && StoreStatus::NOT_FOUND != st)
        {
            finish(ex, route, storeFailure(st));
            return;
        }
        const bool found = StoreStatus::OK == st;
        runHash(ex, route, [this, ex, user, password, found, iterations] {
            // an unknown username costs one PBKDF2 too, so the answer time does not tell which usernames exist
            const bool good = found ? password::verify(password, user->passwordHash)
                                    : (password::hash(password, iterations), false);
            if (!good)
            {
                finish(ex, route, api::error(401, api::code::UNAUTHORIZED, "invalid username or password"));
                return;
            }
            runStore(ex, route, [this, ex, user](int w) {
                const std::string token = newDemoToken();
                const crypto::Digest digest = crypto::sha256(token);
                const std::string tokenHash(crypto::view(digest));
                const StoreStatus set = m_deps.store->setToken(w, user->userId, tokenHash);
                if (StoreStatus::OK != set)
                {
                    finish(ex, route, StoreStatus::NOT_FOUND == set
                            ? api::error(401, api::code::UNAUTHORIZED, "invalid username or password")
                            : storeFailure(set));
                    return;
                }
                if (user->tokenHash)
                {
                    m_deps.auth->forget(*user->tokenHash);
                }
                m_deps.auth->remember(tokenHash, user->userId, user->username);
                finish(ex, route, api::jsonBody(200, "{" + userJson(user->userId, user->username) + ",\"token\":" +
                    api::quote(token) + "}"));
            });
        });
    });
}

void GatewayApi::logout(const Ex &ex)
{
    static const char *route = "auth_logout";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    if (!ex->body().empty())
    {
        finish(ex, route, api::error(400, api::code::VALIDATION, "logout takes no body"));
        return;
    }
    withPrincipal(ex, route, [this, ex](const Principal &p) {
        if (PrincipalMode::DEMO != p.mode)
        {
            finish(ex, route, api::error(404, api::code::FEATURE_DISABLED, "logout applies to demo tokens only",
                std::string("this credential is ") + nameOf(p.mode) + "; it is not stored by the gateway"));
            return;
        }
        const std::int64_t userId = p.userId;
        const std::string tokenHash = p.tokenHash;
        runStore(ex, route, [this, ex, userId, tokenHash](int worker) {
            const StoreStatus st = m_deps.store->clearToken(worker, userId, tokenHash);
            if (StoreStatus::OK != st && StoreStatus::NOT_FOUND != st)
            {
                finish(ex, route, storeFailure(st));
                return;
            }
            m_deps.auth->forget(tokenHash);
            finish(ex, route, api::noContent());
        });
    });
}

void GatewayApi::me(const Ex &ex)
{
    static const char *route = "auth_me";
    ApiResponse bad;
    if (!api::readQuery(*ex, {}, bad))
    {
        finish(ex, route, std::move(bad));
        return;
    }
    withPrincipal(ex, route, [this, ex](const Principal &p) {
        finish(ex, route, api::jsonBody(200, "{" + userJson(p.userId, p.username) + "}"));
    });
}

std::string GatewayApi::healthJson() const
{
    std::string out = ",\"auth\":{\"modes\":";
    const unsigned m = m_deps.auth->modes();
    std::string modes;
    for (const auto &[bit, name] : { std::pair<unsigned, const char *>{ auth_mode::DEMO, "demo" },
             { auth_mode::API_KEY, "api-key" }, { auth_mode::JWT, "jwt" }, { auth_mode::DEV, "dev" } })
    {
        if (0 != (m & bit))
        {
            modes += (modes.empty() ? "" : ",") + api::quote(name);
        }
    }
    out += "[" + modes + "]";
    if (nullptr != m_deps.hashPool)
    {
        out += ",\"hashQueue\":" + std::to_string(m_deps.hashPool->queued()) + ",\"hashQueueCapacity\":" +
            std::to_string(m_deps.hashPool->capacity());
    }
    if (nullptr != m_deps.store)
    {
        out += ",\"usersStore\":" + api::quote(m_deps.store->state()) + ",\"storeQueue\":" +
            std::to_string(m_deps.storePool->queued());
    }
    out += ",\"userIdConflicts\":" + std::to_string(userIdConflicts()) + "}";
    out += ",\"history\":{\"state\":" + api::quote(nullptr == m_deps.history ? std::string("off (no GATEWAY_PG_URL)")
        : m_deps.history->state());
    if (nullptr != m_deps.history)
    {
        const HistoryHealth h = historyHealth();
        const bool known = h.answered && HistoryCause::NONE == h.cause;
        const bool stale = !known || h.ageMs > m_s.historyStaleMs;
        const std::int64_t agoMs = h.answered ? (steadyNs() - h.atNs) / 1'000'000 : -1;
        out += ",\"ageMs\":" + (h.answered && (known || HistoryCause::STALE == h.cause || HistoryCause::EPOCH == h.cause)
                ? std::to_string(h.ageMs) : std::string("null")) +
            ",\"stale\":" + (stale ? "true" : "false") + ",\"cause\":" +
            (!h.answered ? std::string("\"unprobed\"") : HistoryCause::NONE == h.cause ? std::string("null")
                : api::quote(groupOf(h.cause))) +
            ",\"eventSeq\":" + (known ? std::to_string(h.eventSeq) : std::string("null")) + ",\"probedAgoMs\":" +
            (agoMs >= 0 ? std::to_string(agoMs) : std::string("null"));
    }
    if (nullptr != m_deps.historyPool)
    {
        out += ",\"queue\":" + std::to_string(m_deps.historyPool->queued()) + ",\"queueCapacity\":" +
            std::to_string(m_deps.historyPool->capacity());
    }
    out += "}";
    return out;
}

std::string GatewayApi::metrics(const std::string &p) const
{
    std::string out;
    out += "# HELP " + p + "requests_total Public API answers by route and HTTP status.\n# TYPE " + p +
        "requests_total counter\n";
    {
        std::lock_guard<std::mutex> lock(m_countMutex);
        for (const auto &[key, n] : m_requests)
        {
            out += p + "requests_total{route=\"" + key.first + "\",code=\"" + std::to_string(key.second) + "\"} " +
                std::to_string(n) + "\n";
        }
    }
    out += "# HELP " + p + "user_id_conflict_total Registrations refused because the core already knew the new userId.\n"
        "# TYPE " + p + "user_id_conflict_total counter\n" + p + "user_id_conflict_total " +
        std::to_string(userIdConflicts()) + "\n";
    out += "# HELP " + p + "auth_rate_limited_total Register and login requests refused by their rate budgets.\n"
        "# TYPE " + p + "auth_rate_limited_total counter\n" + p + "auth_rate_limited_total " +
        std::to_string(m_authRateLimited.load(std::memory_order_relaxed)) + "\n";
    if (nullptr != m_deps.hashPool)
    {
        out += "# HELP " + p + "hash_queue Password hashing jobs waiting.\n# TYPE " + p + "hash_queue gauge\n" + p +
            "hash_queue " + std::to_string(m_deps.hashPool->queued()) + "\n";
        out += "# HELP " + p + "hash_busy_total Logins and registrations refused because the hash queue was full.\n"
            "# TYPE " + p + "hash_busy_total counter\n" + p + "hash_busy_total " +
            std::to_string(m_hashBusy.load(std::memory_order_relaxed)) + "\n";
    }
    if (nullptr != m_deps.storePool)
    {
        out += "# HELP " + p + "pg_auth_queue Users store calls waiting.\n# TYPE " + p + "pg_auth_queue gauge\n" + p +
            "pg_auth_queue " + std::to_string(m_deps.storePool->queued()) + "\n";
        out += "# HELP " + p + "pg_auth_refused_total Users store calls refused because the queue was full.\n"
            "# TYPE " + p + "pg_auth_refused_total counter\n" + p + "pg_auth_refused_total " +
            std::to_string(m_deps.storePool->refused()) + "\n";
    }
    out += "# HELP " + p + "history_reads_total History reads by query (orders, fills, order, trail), result (ok, not_found, "
        "busy, unavailable) and the cause (busy: none for a full queue, expired for a read that waited past its "
        "deadline; unavailable: unconfigured, absent, unreachable, timeout, no_heartbeat, bad_row, error, lagging, epoch, "
        "stale).\n# TYPE " + p + "history_reads_total counter\n";
    {
        std::lock_guard<std::mutex> lock(m_countMutex);
        for (const auto &[key, n] : m_historyReads)
        {
            const std::size_t bar = key.second.find('|');
            const std::string result = key.second.substr(0, bar);
            const std::string cause = std::string::npos == bar ? "none" : key.second.substr(bar + 1);
            out += p + "history_reads_total{query=\"" + key.first + "\",result=\"" + result + "\",cause=\"" + cause +
                "\"} " + std::to_string(n) + "\n";
        }
    }
    if (nullptr != m_deps.historyPool)
    {
        out += "# HELP " + p + "pg_history_queue History reads waiting for a history connection.\n# TYPE " + p +
            "pg_history_queue gauge\n" + p + "pg_history_queue " + std::to_string(m_deps.historyPool->queued()) + "\n";
        out += "# HELP " + p + "pg_history_refused_total History reads refused because the queue was full (503 "
            "HISTORY_BUSY).\n# TYPE " + p + "pg_history_refused_total counter\n" + p + "pg_history_refused_total " +
            std::to_string(m_deps.historyPool->refused()) + "\n";
    }
    if (nullptr != m_deps.history)
    {
        const HistoryHealth h = historyHealth();
        const bool known = h.answered && HistoryCause::NONE == h.cause;
        out += "# HELP " + p + "history_age_ms The history projection's age at the last history probe (-1: no heartbeat "
            "this gateway can serve: no probe yet, PostgreSQL down, no heartbeat, another genesis, or above "
            "GATEWAY_HISTORY_MAX_AGE_MS).\n# TYPE " + p + "history_age_ms gauge\n" + p + "history_age_ms " +
            (known ? std::to_string(h.ageMs) : std::string("-1")) + "\n";
    }
    out += "# HELP " + p + "token_lookup_refused_total Bearer tokens the cache did not hold, refused before the users "
        "store was asked (per-source lookup budget or the lookup share of the store queue).\n# TYPE " + p +
        "token_lookup_refused_total counter\n" + p + "token_lookup_refused_total " +
        std::to_string(m_deps.auth->unknownLookupsRefused()) + "\n";
    out += "# HELP " + p + "principal_rate_limited_total Requests refused by the principal's request budget.\n# TYPE " + p +
        "principal_rate_limited_total counter\n" + p + "principal_rate_limited_total " +
        std::to_string(m_principalLimited.load(std::memory_order_relaxed)) + "\n";
    out += "# HELP " + p + "read_cache_hits_total Open-orders and balances reads answered from the read cache.\n# TYPE " +
        p + "read_cache_hits_total counter\n" + p + "read_cache_hits_total " +
        std::to_string(m_cacheHits.load(std::memory_order_relaxed)) + "\n";
    out += "# HELP " + p + "cancel_reoffered_not_open_total Unkeyed cancels offered more than once and answered NOT_OPEN "
        "with reoffered true.\n# TYPE " + p + "cancel_reoffered_not_open_total counter\n" + p +
        "cancel_reoffered_not_open_total " + std::to_string(m_cancelReoffered.load(std::memory_order_relaxed)) + "\n";
    out += "# HELP " + p + "register_funding_deposits_total Register-funding deposits sent (re-offers inside one register "
        "included).\n# TYPE " + p + "register_funding_deposits_total counter\n" + p + "register_funding_deposits_total " +
        std::to_string(m_fundingDeposits.load(std::memory_order_relaxed)) + "\n";
    const std::int64_t now = steadyNs();
    const std::string total = p + "order_latency_seconds";
    out += "# HELP " + total + " Order requests (route create, cancel, amend) from the request read to the answer. "
        "outcome core: handed to the core session, every outcome (reply, 504, 503, 500); outcome local: refused before "
        "the core (validation, auth, rate limit, full command ring).\n# TYPE " + total + " histogram\n";
    for (std::size_t r = 0; r < ORDER_ROUTES; ++r)
    {
        for (std::size_t k = 0; k < 2; ++k)
        {
            m_orderTotal[r][k].render(out, total, std::string("route=\"") + ORDER_ROUTE_LABEL[r] + "\",outcome=\"" +
                (0 == k ? "core" : "local") + "\"");
        }
    }
    const std::string totalMax = p + "order_latency_window_max_seconds";
    out += "# HELP " + totalMax + " The largest order_latency_seconds of the last 10 s (any number of scrapers; never "
        "reset by a read).\n# TYPE " + totalMax + " gauge\n";
    for (std::size_t r = 0; r < ORDER_ROUTES; ++r)
    {
        for (std::size_t k = 0; k < 2; ++k)
        {
            m_orderTotal[r][k].renderWindowMax(out, totalMax, std::string("route=\"") + ORDER_ROUTE_LABEL[r] +
                "\",outcome=\"" + (0 == k ? "core" : "local") + "\"", now);
        }
    }
    const std::string stage = p + "order_stage_seconds";
    out += "# HELP " + stage + " Order requests the core replied to, by stage: queue from the request read to the "
        "command taken into the core session (validation, auth, the command ring), core from there to the reply "
        "(offers and re-offers included), local from the reply to the answer.\n# TYPE " + stage + " histogram\n";
    for (std::size_t r = 0; r < ORDER_ROUTES; ++r)
    {
        for (std::size_t k = 0; k < 3; ++k)
        {
            m_orderStage[r][k].render(out, stage, std::string("route=\"") + ORDER_ROUTE_LABEL[r] + "\",stage=\"" +
                ORDER_STAGE_LABEL[k] + "\"");
        }
    }
    const std::string stageMax = p + "order_stage_window_max_seconds";
    out += "# HELP " + stageMax + " The largest order_stage_seconds of the last 10 s.\n# TYPE " + stageMax + " gauge\n";
    for (std::size_t r = 0; r < ORDER_ROUTES; ++r)
    {
        for (std::size_t k = 0; k < 3; ++k)
        {
            m_orderStage[r][k].renderWindowMax(out, stageMax, std::string("route=\"") + ORDER_ROUTE_LABEL[r] +
                "\",stage=\"" + ORDER_STAGE_LABEL[k] + "\"", now);
        }
    }
    out += "# HELP " + p + "order_latency_anomalies_total Order answers whose timestamps were out of order (no stage "
        "recorded; a defect).\n# TYPE " + p + "order_latency_anomalies_total counter\n" + p +
        "order_latency_anomalies_total " + std::to_string(m_latencyAnomalies.load(std::memory_order_relaxed)) + "\n";
    out += "# HELP " + p + "token_stale_served_total Demo tokens served from the cache while the users store could not "
        "answer.\n# TYPE " + p + "token_stale_served_total counter\n" + p + "token_stale_served_total " +
        std::to_string(m_deps.auth->staleServed()) + "\n";
    return out;
}

}}
