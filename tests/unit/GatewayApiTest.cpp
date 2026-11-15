// The public listener and the auth routes in process (T0): the real HttpServer and GatewayApi on loopback, the users
// store in memory, the core replaced by a function answering the identity question from a pool thread (as the duty
// bridge's completion thread would). No request names a user; a backend failure is never a client error; every
// credential mode at the edge; the unauthenticated budget (rates, the hash queue, no PBKDF2 on an I/O thread); the
// identity check before a user row is written.
#include <pthread.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

#include <boost/json.hpp>

#include "Check.h"
#include "HttpClient.h"
#include "MemoryStore.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/HttpServer.h"
#include "gateway/common/Jwt.h"
#include "gateway/common/Password.h"
#include "GatewayApi.h"

using namespace openexchange::gateway;
using gwtest::httpCall;
using gwtest::HttpResult;
using gwtest::MemoryStore;
namespace json = boost::json;

namespace {

const std::string JWT_SECRET = "test-only-hs256-secret-0123456789abcdef";
const std::string API_KEY = "test-key-aaaaaaaaaaaaaaaa";
const std::vector<std::pair<std::string, std::string>> NO_HEADERS;

enum class CoreMode
{
    ANSWER,       // balances reply, known = the user is in `known`
    RING_FULL,    // askCore refuses (the command ring is full)
    UNAVAILABLE,  // CORE_UNAVAILABLE
    UNKNOWN,      // OUTCOME_UNKNOWN
    WRONG_CLASS   // an OrderReject UNAUTHORIZED_CLASS
};

struct Rig
{
    GatewaySettings g;
    MemoryStore store;
    std::unique_ptr<WorkPool> storePool;
    std::unique_ptr<WorkPool> hashPool;
    std::unique_ptr<WorkPool> corePool;
    std::unique_ptr<Authenticator> auth;
    std::unique_ptr<GatewayApi> api;
    std::unique_ptr<HttpServer> server;
    std::mutex coreMutex;
    std::set<std::int64_t> known;
    std::vector<std::int64_t> asked;
    std::atomic<CoreMode> coreMode{ CoreMode::ANSWER };
    std::uint16_t port = 0;

    Rig()
    {
        g.common.authModes = auth_mode::DEMO | auth_mode::API_KEY | auth_mode::JWT | auth_mode::DEV;
        g.common.ioThreads = 2;
        g.pbkdf2Iterations = 1000;
        g.registerRate = Rate{ 1000, 1000, "1000/s" };
        g.registerBurst = 1000;
        g.loginRate = Rate{ 1000, 1000, "1000/s" };
        g.loginBurst = 1000;
        g.authSourceRate = Rate{ 1000, 1000, "1000/s" };
        g.authSourceBurst = 1000;
        g.tokenLookupRate = Rate{ 1000, 1000, "1000/s" };
        g.tokenLookupBurst = 1000;
        g.tokenLookupInflight = 32;
        g.corsOrigins = { "https://app.example.test" };
    }

    void start()
    {
        storePool = std::make_unique<WorkPool>("gw-pg-auth", static_cast<int>(g.pgAuthPool), static_cast<std::size_t>(g.pgQueue));
        hashPool = std::make_unique<WorkPool>("gw-hash", static_cast<int>(g.hashThreads), static_cast<std::size_t>(g.hashQueue));
        corePool = std::make_unique<WorkPool>("t-core", 1, 1024);
        AuthMaterial m;
        m.apiKeys.add(API_KEY, 4242);
        m.jwtSecret = JWT_SECRET;
        AuthenticatorSettings as;
        as.modes = g.common.authModes;
        as.unknownLookupInflight = g.tokenLookupInflight;
        auth = std::make_unique<Authenticator>(as, std::move(m), &store, storePool.get());
        ApiDeps d;
        d.settings = &g;
        d.auth = auth.get();
        d.store = &store;
        d.storePool = storePool.get();
        d.hashPool = hashPool.get();
        d.askCore = [this](session::Command c, std::function<void(const session::Outcome &)> done) {
            if (CoreMode::RING_FULL == coreMode.load())
            {
                return false;
            }
            const std::int64_t user = std::get<wire::RequestUserBalances>(c).userId;
            return corePool->trySubmit([this, user, done](int) {
                session::Outcome o;
                o.templateId = wire::tid::REQUEST_USER_BALANCES;
                switch (coreMode.load())
                {
                    case CoreMode::UNAVAILABLE: o.kind = session::OutcomeKind::CORE_UNAVAILABLE; break;
                    case CoreMode::UNKNOWN: o.kind = session::OutcomeKind::OUTCOME_UNKNOWN; break;
                    case CoreMode::WRONG_CLASS:
                    {
                        wire::OrderReject r;
                        r.reason = wire::money::CoreRejectReason::UNAUTHORIZED_CLASS;
                        o.reply = r;
                        break;
                    }
                    default:
                    {
                        session::BalancesReply b;
                        b.head.userId = user;
                        std::lock_guard<std::mutex> l(coreMutex);
                        asked.push_back(user);
                        b.head.known = known.count(user) > 0;
                        o.reply = b;
                    }
                }
                done(o);
            });
        };
        api = std::make_unique<GatewayApi>(std::move(d));
        HttpServerSettings hs;
        hs.host = "127.0.0.1";
        hs.port = 0;
        hs.ioThreads = static_cast<int>(g.common.ioThreads);
        hs.maxBodyBytes = static_cast<std::size_t>(g.maxBodyBytes);
        hs.maxHeaderBytes = static_cast<std::size_t>(g.maxHeaderBytes);
        hs.idleTimeoutMs = g.httpIdleTimeoutMs;
        hs.maxConnections = g.common.maxConnections;
        server = std::make_unique<HttpServer>(hs, [this](std::shared_ptr<HttpExchange> ex) { api->handle(std::move(ex)); });
        std::string error;
        CHECK_MSG(server->bind(error), error);
        storePool->start();
        hashPool->start();
        corePool->start();
        server->start();
        port = server->boundPort();
    }

    ~Rig()
    {
        if (server)
        {
            hashPool->stop(5000);
            storePool->stop(5000);
            corePool->stop(5000);
            server->stop(5000);
            server.reset();
        }
    }

    HttpResult post(const std::string &path, const std::string &body,
        const std::vector<std::pair<std::string, std::string>> &headers = NO_HEADERS)
    {
        return httpCall(port, "POST", path, body, headers);
    }
    HttpResult get(const std::string &path, const std::vector<std::pair<std::string, std::string>> &headers = NO_HEADERS)
    {
        return httpCall(port, "GET", path, "", headers);
    }
    std::size_t rows()
    {
        std::lock_guard<std::mutex> l(store.mutex);
        return store.users.size();
    }
};

json::object body(const HttpResult &r)
{
    boost::system::error_code ec;
    json::value v = json::parse(r.body, ec);
    return !ec && v.is_object() ? v.as_object() : json::object{};
}

std::string str(const json::object &o, const char *key)
{
    const json::value *v = o.if_contains(key);
    return nullptr != v && v->is_string() ? std::string(v->as_string()) : std::string("<missing>");
}

std::string codeOf(const HttpResult &r)
{
    return str(body(r), "code");
}

std::string creds(const std::string &u, const std::string &p)
{
    return R"({"username":")" + u + R"(","password":")" + p + R"("})";
}

std::vector<std::pair<std::string, std::string>> bearer(const std::string &token)
{
    return { { "Authorization", "Bearer " + token } };
}

std::string what(const HttpResult &r)
{
    return std::to_string(r.status) + " " + r.body + r.error;
}

std::mutex g_hashThreadsMutex;
std::set<std::string> g_hashThreads;
void recordHashThread()
{
    char name[16] = {};
    pthread_getname_np(pthread_self(), name, sizeof(name));
    std::lock_guard<std::mutex> l(g_hashThreadsMutex);
    g_hashThreads.insert(name);
}

}

TEST(api_register_login_me_logout)
{
    Rig rig;
    rig.start();
    HttpResult r = rig.post("/api/v2/auth/register", creds("alice", "secret-1"));
    CHECK_MSG(201 == r.status, what(r));
    json::object b = body(r);
    CHECK(str(b, "userId") == "100000" && str(b, "username") == "alice");
    CHECK(b.contains("funded") && b.at("funded").is_bool() && !b.at("funded").as_bool());
    const std::string token = str(b, "token");
    CHECK(43 == token.size());
    CHECK(rig.asked == std::vector<std::int64_t>{ 100000 }); // the core was asked before the row was written
    {
        std::lock_guard<std::mutex> l(rig.store.mutex);
        const StoredUser &u = rig.store.users.at(100000);
        CHECK(u.tokenHash && *u.tokenHash == std::string(crypto::view(crypto::sha256(token)))); // the store holds a hash
        CHECK(u.tokenHash && *u.tokenHash != token);
        CHECK(password::verify("secret-1", u.passwordHash));
    }
    r = rig.get("/api/v2/auth/me", bearer(token));
    CHECK_MSG(200 == r.status && r.body == R"({"userId":"100000","username":"alice"})", what(r));

    r = rig.post("/api/v2/auth/login", creds("alice", "wrong-pass"));
    CHECK_MSG(401 == r.status && "UNAUTHORIZED" == codeOf(r), what(r));
    CHECK(r.headers["www-authenticate"] == "Bearer");
    r = rig.post("/api/v2/auth/login", creds("nobody", "secret-1"));
    CHECK_MSG(401 == r.status, what(r));
    r = rig.post("/api/v2/auth/login", creds("alice", "secret-1"));
    CHECK_MSG(200 == r.status, what(r));
    const std::string token2 = str(body(r), "token");
    CHECK(token2 != token && 43 == token2.size());
    CHECK(str(body(r), "userId") == "100000");
    // the login rotated the token: the old one is gone at once on this gateway
    r = rig.get("/api/v2/auth/me", bearer(token));
    CHECK_MSG(401 == r.status, what(r));
    r = rig.get("/api/v2/auth/me", bearer(token2));
    CHECK_MSG(200 == r.status, what(r));
    r = rig.post("/api/v2/auth/logout", "", bearer(token2));
    CHECK_MSG(204 == r.status && r.body.empty(), what(r));
    r = rig.get("/api/v2/auth/me", bearer(token2));
    CHECK_MSG(401 == r.status, what(r));
    r = rig.post("/api/v2/auth/logout", "", bearer(token2));
    CHECK_MSG(401 == r.status, what(r));
    {
        std::lock_guard<std::mutex> l(rig.store.mutex);
        CHECK(!rig.store.users.at(100000).tokenHash);
    }
    // a second user gets the next id
    r = rig.post("/api/v2/auth/register", creds("bob_2", "pässwörd"));
    CHECK_MSG(201 == r.status && str(body(r), "userId") == "100001", what(r));
    r = rig.post("/api/v2/auth/register", creds("alice", "secret-2"));
    CHECK_MSG(409 == r.status && "CONFLICT" == codeOf(r), what(r));
}

TEST(api_requests_never_name_a_user)
{
    Rig rig;
    rig.start();
    const std::vector<std::pair<std::string, std::string>> key = { { "X-API-Key", API_KEY } };
    struct Case
    {
        const char *method;
        const char *target;
        std::string body;
        const char *needle;
    };
    const std::vector<Case> cases = {
        { "POST", "/api/v2/auth/register", R"({"username":"carol","password":"secret-1","userId":"5"})", "userId" },
        { "POST", "/api/v2/auth/register", R"({"username":"carol","password":"secret-1","x":{"user_id":5}})", "user_id" },
        { "POST", "/api/v2/auth/login", R"({"username":"carol","password":"secret-1","meta":[{"userId":1}]})", "userId" },
        { "GET", "/api/v2/auth/me?userId=5", "", "userId" },
        { "GET", "/api/v2/auth/me?user%49d=5", "", "userId" },
        { "GET", "/api/v2/auth/me?token=abc", "", "token" },
        { "GET", "/api/v2/auth/me?access_token=abc", "", "access_token" },
        { "POST", "/api/v2/auth/register", R"({"username":"carol","password":"secret-1","role":"admin"})", "role" },
        { "POST", "/api/v2/auth/register", R"({"username":"Carol","password":"secret-1"})", "username" },
        { "POST", "/api/v2/auth/register", R"({"username":"carol","password":"12345"})", "password" },
        { "POST", "/api/v2/auth/register", R"({"username":"carol","password":123456})", "password" },
        { "POST", "/api/v2/auth/register", R"(["carol"])", "object" },
        { "POST", "/api/v2/auth/register", R"({"username":"carol",)", "JSON" },
    };
    for (const Case &c : cases)
    {
        const HttpResult r = httpCall(rig.port, c.method, c.target, c.body, key);
        CHECK_MSG(400 == r.status && "VALIDATION" == codeOf(r) && r.body.find(c.needle) != std::string::npos,
            std::string(c.target) + " " + c.body + " -> " + what(r));
    }
    const HttpResult text = httpCall(rig.port, "POST", "/api/v2/auth/register", creds("carol", "secret-1"), {},
        "text/plain");
    CHECK_MSG(400 == text.status && text.body.find("Content-Type") != std::string::npos, what(text));
    CHECK(0 == rig.rows());
    CHECK(rig.asked.empty());
}

TEST(api_identity_check)
{
    Rig rig;
    rig.start();
    rig.known.insert(100000); // the users database was reset behind a core that knows 100000
    HttpResult r = rig.post("/api/v2/auth/register", creds("alice", "secret-1"));
    CHECK_MSG(0 == rig.rows(), "new user inherited an existing account");
    CHECK_MSG(503 == r.status && "IDENTITY_STORE_INCONSISTENT" == codeOf(r), what(r));
    CHECK(r.headers.count("retry-after"));
    CHECK(1 == rig.api->userIdConflicts());
    CHECK(rig.api->metrics("gateway_").find("gateway_user_id_conflict_total 1\n") != std::string::npos);
    CHECK(0 == rig.auth->cachedTokens());
    // the next id is unknown to the core: registration proceeds with it
    r = rig.post("/api/v2/auth/register", creds("alice", "secret-1"));
    CHECK_MSG(201 == r.status && str(body(r), "userId") == "100001", what(r));
    CHECK(rig.asked == (std::vector<std::int64_t>{ 100000, 100001 }));
}

TEST(api_core_outcomes_never_write)
{
    Rig rig;
    rig.start();
    const std::vector<std::tuple<CoreMode, int, std::string>> cases = {
        { CoreMode::RING_FULL, 503, "OVERLOADED" },
        { CoreMode::UNAVAILABLE, 503, "CORE_UNAVAILABLE" },
        { CoreMode::UNKNOWN, 504, "OUTCOME_UNKNOWN" },
        { CoreMode::WRONG_CLASS, 403, "FORBIDDEN" },
    };
    int i = 0;
    for (const auto &[mode, status, code] : cases)
    {
        rig.coreMode = mode;
        const HttpResult r = rig.post("/api/v2/auth/register", creds("user_" + std::to_string(i++), "secret-1"));
        CHECK_MSG(status == r.status && code == codeOf(r), code + " -> " + what(r));
        CHECK(0 == rig.rows());
    }
}

TEST(api_backend_failures_are_never_client_errors)
{
    Rig rig;
    rig.start();
    HttpResult r = rig.post("/api/v2/auth/register", creds("alice", "secret-1"));
    const std::string token = str(body(r), "token");
    rig.store.down = true;
    for (const HttpResult &x : { rig.post("/api/v2/auth/register", creds("bob", "secret-1")),
             rig.post("/api/v2/auth/login", creds("alice", "secret-1")),
             rig.get("/api/v2/auth/me", bearer(crypto::base64UrlEncode(crypto::randomBytes(32)))) })
    {
        CHECK_MSG(503 == x.status && "AUTH_UNAVAILABLE" == codeOf(x) && x.headers.count("retry-after"), what(x));
    }
    // a cached token keeps working (inside the TTL), an api key never needs the store
    r = rig.get("/api/v2/auth/me", bearer(token));
    CHECK_MSG(200 == r.status, what(r));
    r = rig.get("/api/v2/auth/me", { { "X-API-Key", API_KEY } });
    CHECK_MSG(200 == r.status && r.body == R"({"userId":"4242","username":null})", what(r));
}

TEST(api_credentials_at_the_edge)
{
    Rig rig;
    rig.start();
    const std::string token = jwt::signHs256(R"({"alg":"HS256"})", R"({"sub":"777","exp":4000000000})", JWT_SECRET);
    HttpResult r = rig.get("/api/v2/auth/me", bearer(token));
    CHECK_MSG(200 == r.status && r.body == R"({"userId":"777","username":null})", what(r));
    r = rig.get("/api/v2/auth/me", bearer("dev:31"));
    CHECK_MSG(200 == r.status && r.body == R"({"userId":"31","username":null})", what(r));
    r = rig.get("/api/v2/auth/me", { { "Authorization", "Bearer dev:31" }, { "Authorization", "Bearer dev:32" } });
    CHECK_MSG(401 == r.status, what(r));
    r = rig.get("/api/v2/auth/me", { { "Authorization", "Bearer dev:31" }, { "X-API-Key", API_KEY } });
    CHECK_MSG(401 == r.status, what(r));
    r = rig.get("/api/v2/auth/me");
    CHECK_MSG(401 == r.status && "UNAUTHORIZED" == codeOf(r), what(r));
    r = rig.post("/api/v2/auth/logout", "", { { "X-API-Key", API_KEY } });
    CHECK_MSG(404 == r.status && "FEATURE_DISABLED" == codeOf(r), what(r));
    // demo off: register and login are not served
    Rig noDemo;
    noDemo.g.common.authModes = auth_mode::API_KEY;
    noDemo.start();
    r = noDemo.post("/api/v2/auth/register", creds("alice", "secret-1"));
    CHECK_MSG(404 == r.status && "FEATURE_DISABLED" == codeOf(r), what(r));
    r = noDemo.post("/api/v2/auth/login", creds("alice", "secret-1"));
    CHECK_MSG(404 == r.status && "FEATURE_DISABLED" == codeOf(r), what(r));
}

TEST(api_auth_rates)
{
    Rig rig;
    rig.g.registerRate = Rate{ 1, 60'000, "1/min" };
    rig.g.registerBurst = 2;
    rig.g.authSourceRate = Rate{ 1, 60'000, "1/min" };
    rig.g.authSourceBurst = 4;
    rig.g.trustedProxies = { "10.0.0.0/8" }; // the loopback peer is not a trusted proxy
    rig.start();
    const std::vector<std::pair<std::string, std::string>> forged = { { "X-Forwarded-For", "198.51.100.1" } };
    CHECK(201 == rig.post("/api/v2/auth/register", creds("u_one", "secret-1"), forged).status);
    CHECK(201 == rig.post("/api/v2/auth/register", creds("u_two", "secret-1")).status);
    HttpResult r = rig.post("/api/v2/auth/register", creds("u_three", "secret-1"));
    CHECK_MSG(429 == r.status && "RATE_LIMITED" == codeOf(r) && r.headers.count("retry-after"), what(r));
    CHECK(r.body.find("\"keyFinal\":false") != std::string::npos);
    // the per-source budget is shared by register and login: the peer has spent 2 of 4 (a forged header did not help,
    // and the globally refused register was given back)
    CHECK(200 == rig.post("/api/v2/auth/login", creds("u_one", "secret-1"), forged).status);
    CHECK(200 == rig.post("/api/v2/auth/login", creds("u_one", "secret-1")).status);
    r = rig.post("/api/v2/auth/login", creds("u_one", "secret-1"));
    CHECK_MSG(429 == r.status && "RATE_LIMITED" == codeOf(r), what(r));

    // behind a trusted proxy (the loopback peer): each client address has its own source budget
    Rig proxied;
    proxied.g.authSourceRate = Rate{ 1, 60'000, "1/min" };
    proxied.g.authSourceBurst = 1;
    proxied.g.trustedProxies = { "127.0.0.1" };
    proxied.start();
    CHECK(201 == proxied.post("/api/v2/auth/register", creds("p_one", "secret-1"), { { "X-Forwarded-For", "198.51.100.1" } }).status);
    CHECK(201 == proxied.post("/api/v2/auth/register", creds("p_two", "secret-1"), { { "X-Forwarded-For", "198.51.100.2" } }).status);
    r = proxied.post("/api/v2/auth/register", creds("p_three", "secret-1"), { { "X-Forwarded-For", "198.51.100.1" } });
    CHECK_MSG(429 == r.status, what(r));

    // a request the global bucket refuses costs its source nothing
    Rig global;
    global.g.registerRate = Rate{ 1, 60'000, "1/min" };
    global.g.registerBurst = 1;
    global.g.authSourceRate = Rate{ 1, 60'000, "1/min" };
    global.g.authSourceBurst = 2;
    global.start();
    CHECK(201 == global.post("/api/v2/auth/register", creds("g_one", "secret-1")).status);
    r = global.post("/api/v2/auth/register", creds("g_two", "secret-1"));
    CHECK_MSG(429 == r.status, what(r));
    r = global.post("/api/v2/auth/login", creds("g_one", "secret-1"));
    CHECK_MSG(200 == r.status, "the globally refused register spent the source's token: " + what(r));
}

TEST(api_unauthenticated_budget)
{
    // PBKDF2 sized so one hash takes about 150 ms on this machine
    const auto t0 = std::chrono::steady_clock::now();
    password::hash("calibrate", 100'000);
    const double perIteration = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count() / 100'000;
    const std::int64_t iterations = std::max<std::int64_t>(10'000, static_cast<std::int64_t>(150.0 / perIteration));
    Rig rig;
    rig.g.common.ioThreads = 1;
    rig.g.hashThreads = 1;
    rig.g.hashQueue = 2;
    rig.g.pbkdf2Iterations = iterations;
    rig.start();
    CHECK(201 == rig.post("/api/v2/auth/register", creds("burst", "secret-1")).status);
    {
        std::lock_guard<std::mutex> l(g_hashThreadsMutex);
        g_hashThreads.clear();
    }
    password::setHashObserver(&recordHashThread);
    std::atomic<int> busy{ 0 };
    std::atomic<int> ok{ 0 };
    std::vector<std::thread> burst;
    for (int i = 0; i < 12; ++i)
    {
        burst.emplace_back([&] {
            const HttpResult r = rig.post("/api/v2/auth/login", creds("burst", "secret-1"));
            if (503 == r.status && "AUTH_BUSY" == codeOf(r))
            {
                busy++;
            }
            else if (200 == r.status)
            {
                ok++;
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    double worst = 0;
    for (int i = 0; i < 20; ++i)
    {
        const HttpResult r = rig.get("/api/v2/auth/me", { { "X-API-Key", API_KEY } });
        CHECK_MSG(200 == r.status, what(r));
        worst = std::max(worst, r.ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    for (auto &t : burst)
    {
        t.join();
    }
    password::setHashObserver(nullptr);
    std::printf("  login burst: %d ok, %d AUTH_BUSY; authenticated request worst %.1f ms (one PBKDF2 %.0f ms)\n", ok.load(),
        busy.load(), worst, perIteration * static_cast<double>(iterations));
    CHECK_MSG(worst < 100.0, "order request delayed by login burst (worst " + std::to_string(worst) + " ms)");
    CHECK_MSG(busy.load() >= 1, "no AUTH_BUSY under a login burst above the hash queue");
    CHECK(ok.load() >= 1);
    std::lock_guard<std::mutex> l(g_hashThreadsMutex);
    for (const std::string &name : g_hashThreads)
    {
        CHECK_MSG(0 == name.rfind("gw-hash", 0), "PBKDF2 ran on thread " + name);
    }
}

TEST(api_unknown_token_flood_leaves_login_working)
{
    // Unauthenticated clients sending random bearer tokens: each one the cache does not hold costs a users-store
    // lookup. Those lookups must not take the store queue that login and register share.
    Rig rig;
    rig.g.pgAuthPool = 1;
    rig.g.pgQueue = 8;
    rig.g.tokenLookupInflight = 4; // half of the queue, as the settings default it
    rig.start();
    CHECK(201 == rig.post("/api/v2/auth/register", creds("victim", "secret-1")).status);
    rig.store.tokenDelayMs = 50;
    std::atomic<bool> stop{ false };
    std::atomic<int> flood{ 0 };
    std::vector<std::thread> attackers;
    for (int i = 0; i < 12; ++i)
    {
        attackers.emplace_back([&] {
            // paced, so the run does not use up the loopback's ephemeral ports (every call is a new connection)
            while (!stop.load() && flood.load() < 4000)
            {
                rig.get("/api/v2/auth/me", bearer(newDemoToken()));
                flood++;
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    int loginOk = 0;
    std::string failures;
    for (int i = 0; i < 5; ++i)
    {
        const HttpResult r = rig.post("/api/v2/auth/login", creds("victim", "secret-1"));
        if (200 == r.status)
        {
            loginOk++;
        }
        else
        {
            failures += " [" + what(r) + "]";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    stop = true;
    for (auto &t : attackers)
    {
        t.join();
    }
    std::printf("  unknown-token flood: %d requests; login %d/5 ok\n", flood.load(), loginOk);
    CHECK_MSG(5 == loginOk, "login starved by an unknown-token flood:" + failures);
    CHECK(rig.auth->unknownLookupsRefused() > 0);

    // one source: its lookup budget is spent after the burst, 429 with Retry-After, the store not asked
    Rig single;
    single.g.tokenLookupRate = Rate{ 1, 60'000, "1/min" };
    single.g.tokenLookupBurst = 3;
    single.start();
    for (int i = 0; i < 3; ++i)
    {
        const HttpResult r = single.get("/api/v2/auth/me", bearer(newDemoToken()));
        CHECK_MSG(401 == r.status, what(r));
    }
    const int calls = single.store.calls.load();
    HttpResult r = single.get("/api/v2/auth/me", bearer(newDemoToken()));
    CHECK_MSG(429 == r.status && "RATE_LIMITED" == codeOf(r) && r.headers.count("retry-after"), what(r));
    CHECK(single.store.calls.load() == calls);
    // a token this gateway knows is not charged: register and use it with the budget spent
    r = single.post("/api/v2/auth/register", creds("known_one", "secret-1"));
    CHECK_MSG(201 == r.status, what(r));
    r = single.get("/api/v2/auth/me", bearer(str(body(r), "token")));
    CHECK_MSG(200 == r.status, what(r));
    // login and register keep their own budget
    r = single.post("/api/v2/auth/login", creds("known_one", "secret-1"));
    CHECK_MSG(200 == r.status, what(r));
}

TEST(api_http_bounds_routes_and_cors)
{
    Rig rig;
    rig.g.maxBodyBytes = 256;
    rig.g.maxHeaderBytes = 1024;
    rig.start();
    HttpResult r = rig.post("/api/v2/auth/register", R"({"username":"alice","password":")" + std::string(300, 'x') + "\"}");
    CHECK_MSG(413 == r.status && "VALIDATION" == codeOf(r), what(r));
    r = rig.get("/api/v2/auth/me", { { "X-Filler", std::string(2000, 'y') } });
    CHECK_MSG(431 == r.status, what(r));
    r = rig.get("/api/v1/orders", { { "X-API-Key", API_KEY } });
    CHECK_MSG(404 == r.status && "NOT_FOUND" == codeOf(r), what(r));
    r = rig.get("/api/v2/nothing-here");
    CHECK_MSG(404 == r.status, what(r));
    r = rig.get("/api/v2/auth/register");
    CHECK_MSG(405 == r.status && r.headers["allow"] == "POST", what(r));
    r = httpCall(rig.port, "OPTIONS", "/api/v2/auth/login", "", { { "Origin", "https://app.example.test" } });
    CHECK_MSG(204 == r.status && r.headers["access-control-allow-origin"] == "https://app.example.test" &&
            r.headers["access-control-allow-headers"].find("Idempotency-Key") != std::string::npos,
        what(r));
    r = httpCall(rig.port, "OPTIONS", "/api/v2/auth/login", "", { { "Origin", "https://evil.example.test" } });
    CHECK_MSG(403 == r.status && !r.headers.count("access-control-allow-origin"), what(r));
    r = rig.get("/api/v2/auth/me", { { "X-API-Key", API_KEY }, { "Origin", "https://app.example.test" } });
    CHECK(200 == r.status && r.headers["access-control-allow-origin"] == "https://app.example.test");
    CHECK(r.headers["cache-control"] == "no-store");
    r = rig.get("/api/v2/auth/me", { { "X-API-Key", API_KEY }, { "Origin", "https://evil.example.test" } });
    CHECK(200 == r.status && !r.headers.count("access-control-allow-origin"));
}
