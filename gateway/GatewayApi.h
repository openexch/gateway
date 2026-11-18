#ifndef GATEWAY_GATEWAY_API_H
#define GATEWAY_GATEWAY_API_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <optional>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gateway/common/Api.h"
#include "gateway/common/Authenticator.h"
#include "gateway/common/History.h"
#include "gateway/common/HttpServer.h"
#include "gateway/common/RateLimit.h"
#include "gateway/common/SessionParts.h"
#include "gateway/common/Settings.h"
#include "gateway/common/UserStore.h"
#include "gateway/common/WorkPool.h"

// GatewayApi - the routes of oe-gateway's public listener, called on the I/O threads (HttpServer): the auth routes
// (this file's first part, GatewayApi.cpp) and the order, balance, market, status and treasury routes
// (GatewayRoutes.cpp, described there), with the shared rules every route follows.
//
//   POST /api/v2/auth/register  {username, password}  201 {userId, username, token, funded}   (demo mode)
//   POST /api/v2/auth/login     {username, password}  200 {userId, username, token}           (demo mode)
//   POST /api/v2/auth/logout    demo token            204 (the stored token hash is cleared)
//   GET  /api/v2/auth/me        any credential        200 {userId, username}
// username is [a-z0-9_]{3,20}; password is 6 or more characters. Without demo mode register and login answer 404
// FEATURE_DISABLED, and logout does too for a principal of another mode (only a demo token is stored).
//
// Threads. Nothing here blocks an I/O thread: PBKDF2 runs on the hash pool (full queue: 503 AUTH_BUSY), every users
// store call on the store pool (full queue: 503 AUTH_BUSY; store down: 503 AUTH_UNAVAILABLE), and the core is reached
// only through `askCore` (the duty bridge; a full command ring is 503 OVERLOADED). Answers are sent from whichever
// thread finishes the work (HttpExchange::respond posts to the connection's strand).
//
// Unauthenticated budget: register and login each take a token of their global bucket
// (GATEWAY_REGISTER_RATE + _BURST, GATEWAY_LOGIN_RATE + _BURST) and of the per-source bucket they share
// (GATEWAY_AUTH_SOURCE_RATE + _BURST); an empty bucket is 429 RATE_LIMITED with Retry-After. The source is the socket
// peer, or the client-IP header's right-most untrusted address when the peer is a trusted proxy (RateLimit.h); an IPv6
// source is keyed by its GATEWAY_SOURCE_IPV6_PREFIX. A source token spent on a request the global bucket then refuses
// is given back. A bearer token the cache does not hold takes a token of the per-source lookup bucket
// (GATEWAY_TOKEN_LOOKUP_RATE + _BURST) before the users store is asked, and at most GATEWAY_TOKEN_LOOKUP_INFLIGHT such
// lookups wait at once (Authenticator.h): random tokens cannot fill the store queue that login and register use.
//
// Identity check. The users table mints the userId (demo_user_seq). Before a new row is written the gateway
// asks the core for that user's balances: `known` true means the core already holds an account under the id (the users
// database was reset or restored behind a continuing core). Then nothing is written and nothing is funded: the answer
// is 503 IDENTITY_STORE_INCONSISTENT, a CRITICAL line is logged and gateway_user_id_conflict_total rises. Only
// `known` false lets the row be inserted. The id comes from nextval outside any transaction, so each refused attempt
// still uses up one sequence value: repeated attempts walk the sequence past the ids the core knows and the refusals
// stop on their own. Nobody gets an old account either way, but the counter is the signal that the runbook step
// (set demo_user_seq above the highest userId the core knows) was skipped: alert on any rise.
//
// CORS: an Origin listed in GATEWAY_CORS_ORIGINS gets Access-Control-Allow-Origin on every answer; a preflight
// (OPTIONS) from a listed origin is 204 with the allowed methods and headers, from any other origin 403.
//
// Test-only controls (never in a published build): GW_NC_HASH_ON_IO runs PBKDF2 on the I/O strand of the request,
// GW_NC_NO_IDENTITY_CHECK skips the core question; tests/CMakeLists.txt names the failure text each must produce.

namespace openexchange { namespace gateway {

struct ApiDeps
{
    const GatewaySettings *settings = nullptr;
    Authenticator *auth = nullptr;
    UserStore *store = nullptr;     // null without demo mode
    WorkPool *storePool = nullptr;  // null without demo mode
    WorkPool *hashPool = nullptr;   // null without demo mode
    HistoryStore *history = nullptr; // null without GATEWAY_PG_URL: every history read is 503 HISTORY_UNAVAILABLE
    WorkPool *historyPool = nullptr; // gw-pg-hist: GATEWAY_PG_HISTORY_POOL workers over its own GATEWAY_PG_QUEUE
    // Offers one command to the GATEWAY session; false when the command ring is full (nothing queued, `done` never runs).
    std::function<bool(session::Command, std::function<void(const session::Outcome &)>)> askCore;
    // The same for the TREASURY session (Deposit, Withdraw); empty when the treasury lane is off.
    std::function<bool(session::Command, std::function<void(const session::Outcome &)>)> askTreasury;
    // The core config the GATEWAY session holds (never null; loaded() false until a recorded CoreConfig was read).
    std::function<std::shared_ptr<const session::CoreConfig>()> config;
    // The GATEWAY session is registered (the AdminAck arrived).
    std::function<bool()> registered;
    std::function<std::int64_t()> nowMs; // monotonic; the steady clock when empty
};

// The idempotence window against the request deadline. A request still unanswered at GATEWAY_REQUEST_TIMEOUT_MS is
// OUTCOME_UNKNOWN; offered again with the same id (register funding's second attempt, a client retry with its key) it
// reaches the core up to two request timeouts after the first offer, and replays the stored decision only while the
// core still remembers that id (idempotencyWindowMs, the request TTL of the core's limits). So the window must be above
// twice the timeout.
inline bool reofferFitsWindow(std::int64_t idempotencyWindowMs, std::int64_t requestTimeoutMs) noexcept
{
    return idempotencyWindowMs / 2 > requestTimeoutMs;
}

// The window is the core's setting and the gateway learns it from the config read, after it started: a window too
// short for the deadline is a CRITICAL log line, not an exit. check() gives that line the first time it sees a loaded
// config version whose window does not fit, and nothing for the same version again (the duty thread calls it every
// cycle).
class ReofferWindowCheck
{
public:
    explicit ReofferWindowCheck(std::int64_t requestTimeoutMs) noexcept : m_requestTimeoutMs(requestTimeoutMs) {}

    std::optional<std::string> check(const session::CoreConfig &config)
    {
        if (!config.loaded() || config.head.configVersion == m_checkedVersion)
        {
            return std::nullopt;
        }
        m_checkedVersion = config.head.configVersion;
        if (reofferFitsWindow(config.idempotencyWindowMs(), m_requestTimeoutMs))
        {
            return std::nullopt;
        }
        return "core config version " + std::to_string(config.head.configVersion) + ": idempotencyWindowMs " +
            std::to_string(config.idempotencyWindowMs()) + " is not above twice GATEWAY_REQUEST_TIMEOUT_MS (" +
            std::to_string(m_requestTimeoutMs) + " ms): a re-offer can reach the core after it forgot the request id";
    }

private:
    std::int64_t m_requestTimeoutMs;
    std::uint32_t m_checkedVersion = 0; // 0: none checked (a loaded config has version 1 or above)
};

class GatewayApi
{
public:
    explicit GatewayApi(ApiDeps deps);
    ~GatewayApi();

    void handle(std::shared_ptr<HttpExchange> ex);

    // The history probe behind /health: every GATEWAY_HISTORY_PROBE_MS one heartbeat read on the history pool, through
    // the checks of a history read (History.h). /health shows history {ageMs, stale, cause, eventSeq, probedAgoMs} from
    // the last probe, and the metric gateway_history_age_ms. A probe that has not answered for two intervals and the
    // read deadline reads cause "unprobed", stale true: /health never shows fresh history without a probe. /ready does
    // not depend on it (the order path does not depend on PostgreSQL). Without GATEWAY_PG_URL nothing is started.
    // Test-only control (never in a published build): GW_NC_NO_HISTORY_PROBE starts no probe and shows what the last
    // successful history read found instead.
    void startHistoryProbe();
    void stopHistoryProbe();
    void probeHistoryOnce(); // one probe submitted now (the loop's step)

    std::string healthJson() const;
    std::string metrics(const std::string &prefix) const;
    std::uint64_t userIdConflicts() const noexcept { return m_userIdConflicts.load(std::memory_order_relaxed); }

private:
    using Ex = std::shared_ptr<HttpExchange>;

    void finish(const Ex &ex, const char *route, ApiResponse response);
    void preflight(const Ex &ex);
    void withPrincipal(const Ex &ex, const char *route, std::function<void(const Principal &)> next);
    bool takeAuthBudget(const Ex &ex, const char *route, RateBucket &global);
    std::string sourceKey(const Ex &ex) const;
    bool credentials(const Ex &ex, const char *route, std::string &username, std::string &password);
    void runHash(const Ex &ex, const char *route, std::function<void()> job);
    void runStore(const Ex &ex, const char *route, std::function<void(int)> job);

    // ---- GatewayRoutes.cpp
    struct OpenOrders;
    enum class Asked : std::uint8_t
    {
        KEYED,          // a keyed command: a retry with the same key replays the decision
        KEYED_FAUCET,   // a keyed deposit or withdraw: as KEYED, and the retry also takes a faucet token
        UNKEYED_CANCEL, // an unkeyed cancel
        READ            // a query: nothing changes
    };
    bool takePrincipal(const Ex &ex, const char *route, std::int64_t userId, std::int64_t cost);
    bool ask(const Ex &ex, const char *route, bool treasury, session::Command command,
        std::function<void(const session::Outcome &)> done);
    ApiResponse failure(const session::Outcome &o, Asked asked) const;
    std::shared_ptr<const session::CoreConfig> loadedConfig(const Ex &ex, const char *route);
    void readOpenOrders(std::int64_t userId, std::shared_ptr<OpenOrders> state,
        std::function<void(std::shared_ptr<OpenOrders>)> done);
    std::optional<std::string> cached(std::int64_t userId, const std::string &key);
    void cache(std::int64_t userId, const std::string &key, const std::string &body);
    void dropCache(std::int64_t userId);
    void fundRegistration(std::int64_t userId, std::function<void(bool)> done);
    void fundOne(std::int64_t userId, std::int32_t assetId, std::int64_t amount, int attempt, std::function<void(bool)> done);

    void markets(const Ex &ex);
    void status(const Ex &ex);
    void createOrder(const Ex &ex);
    void listOrders(const Ex &ex);
    void getOrder(const Ex &ex, std::int64_t orderId);
    void amendOrder(const Ex &ex, std::int64_t orderId);
    void cancelOrder(const Ex &ex, std::int64_t orderId);
    void bulkCancel(const Ex &ex);
    void balances(const Ex &ex);
    void deposit(const Ex &ex);
    void withdraw(const Ex &ex);
    void moneyCommand(const Ex &ex, bool deposit);
    void ordersHistory(const Ex &ex);
    void fills(const Ex &ex);
    void orderTrail(const Ex &ex, std::int64_t orderId);
    // orders, fills, or the trail of `orderId` (GET /orders/{orderId}/trail: the caller's own order only)
    void historyPage(const Ex &ex, history::Kind kind, std::int64_t orderId = 0);
    void orderFromHistory(const Ex &ex, std::int64_t userId, std::int64_t orderId);
    // Runs `job` on the history pool with the read's one deadline (steady clock ns, stamped now:
    // GATEWAY_PG_STATEMENT_TIMEOUT_MS); false (and 503 HISTORY_BUSY answered) when its queue is full. A job that
    // starts after its deadline is not run: 503 HISTORY_BUSY (its client has waited out the read's time in the queue).
    bool runHistory(const Ex &ex, const char *route, const char *query, std::function<void(int, std::int64_t)> job);
    ApiResponse historyUnavailable(HistoryCause cause, const char *query);
    HistoryExpect historyExpect() const;
    std::string asOfJson(std::int64_t ageMs, std::int64_t eventSeq) const;
    void countHistory(const char *query, const char *result);

    // ---- GatewayApi.cpp
    void registerUser(const Ex &ex);
    void registerReserved(const Ex &ex, const std::string &username, const std::string &passwordHash, std::int64_t userId,
        int worker);
    void registerInsert(const Ex &ex, const std::string &username, const std::string &passwordHash, std::int64_t userId,
        int worker);
    void login(const Ex &ex);
    void logout(const Ex &ex);
    void me(const Ex &ex);

    ApiDeps m_deps;
    const GatewaySettings &m_s;
    SourceAddress m_sources;
    RateBucket m_registerBucket;
    RateBucket m_loginBucket;
    KeyedRateLimiter m_sourceBuckets;
    KeyedRateLimiter m_lookupBuckets; // per source: store lookups of tokens the cache does not hold
    KeyedRateLimiter m_principalBuckets; // per principal: every core-backed route (GATEWAY_PRINCIPAL_RATE + _BURST)
    KeyedRateLimiter m_faucetBuckets;    // per principal: deposit and withdraw (GATEWAY_FAUCET_RATE; burst: one period's count)
    std::vector<std::pair<std::int32_t, std::int64_t>> m_faucetCaps; // assetId -> cap in units
    struct CacheEntry
    {
        std::int64_t atMs = 0;
        std::string body;
    };
    std::mutex m_cacheMutex;
    std::map<std::pair<std::int64_t, std::string>, CacheEntry> m_readCache; // (userId, read) -> last answer
    std::size_t m_cachePuts = 0;
    std::atomic<std::uint64_t> m_principalLimited{ 0 };
    std::atomic<std::uint64_t> m_cacheHits{ 0 };
    std::atomic<std::uint64_t> m_cancelReoffered{ 0 };
    std::atomic<std::uint64_t> m_fundingDeposits{ 0 };
    std::atomic<std::uint64_t> m_userIdConflicts{ 0 };
    std::atomic<std::uint64_t> m_authRateLimited{ 0 };
    std::atomic<std::uint64_t> m_hashBusy{ 0 };
    struct HistoryHealth
    {
        bool answered = false;          // a probe (or, in the control build, a read) has answered
        std::int64_t atNs = 0;          // steady clock of that answer
        HistoryCause cause = HistoryCause::NONE;
        std::int64_t ageMs = 0;
        std::int64_t eventSeq = 0;
    };
    void noteHistory(HistoryCause cause, std::int64_t ageMs, std::int64_t eventSeq);
    HistoryHealth historyHealth() const;
    mutable std::mutex m_historyHealthMutex;
    HistoryHealth m_historyHealth;
    std::atomic<bool> m_probeInFlight{ false };
    std::mutex m_probeMutex;
    std::condition_variable m_probeWake;
    bool m_probeStop = false;
    std::thread m_probeThread;
    mutable std::mutex m_countMutex;
    std::map<std::pair<std::string, unsigned>, std::uint64_t> m_requests; // (route, status) -> count
    std::map<std::pair<std::string, std::string>, std::uint64_t> m_historyReads; // (query, result) -> count
};

}}

#endif
