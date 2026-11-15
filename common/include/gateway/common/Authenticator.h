#ifndef GATEWAY_COMMON_AUTHENTICATOR_H
#define GATEWAY_COMMON_AUTHENTICATOR_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "gateway/common/AuthMaterial.h"
#include "gateway/common/RateLimit.h"
#include "gateway/common/UserStore.h"
#include "gateway/common/WorkPool.h"

// Authenticator - the principal resolver both binaries share: it turns the credential of a request into a
// userId, and nothing else decides the userId. It reads only the Authorization and X-API-Key headers, never the
// URL.
//
//   X-API-Key: <key>            api-key mode: the key file's userId (constant-time compare)
//   Authorization: Bearer dev:<userId>   dev mode (only with GATEWAY_ALLOW_DEV_AUTH=1, refused at boot otherwise)
//   Authorization: Bearer <a.b.c>        jwt mode: an HS256 JWT (Jwt.h), sub = userId, exp required
//   Authorization: Bearer <token>        demo mode: an opaque token, looked up by SHA-256 in the users table
// A credential of a mode that is not on, both headers at once, another scheme, or an empty token is 401: one
// credential, one meaning. A JWT-shaped token is judged as a JWT only (no fall-through to demo).
//
// Demo tokens. A lookup result is cached by token hash for GATEWAY_TOKEN_CACHE_TTL_MS. Past that the store is asked
// again on a store worker (never on the calling I/O thread); when the store cannot answer (UNAVAILABLE, or its queue
// is full) an entry that was valid is still served until it is GATEWAY_TOKEN_STALE_MS old, then the answer is 503
// AUTH_UNAVAILABLE (store down) or AUTH_BUSY (queue full), never 401. A token the store does not know is 401 and
// leaves the cache. Login and register put the new token in the cache; logout and a rotation remove the old one here.
// A logout or rotation through another gateway is seen here when the cached entry expires (at most the TTL). A lookup
// that read the store before a logout here finished is answered but not cached (a generation count, bumped by forget).
//
// Unknown tokens have their own budget. A token the cache does not hold costs a store lookup, and anyone can send
// one. So such a lookup first takes a token of the caller's budget (CredentialHeaders::lookupBudget, per source
// address in oe-gateway; refused: RATE_LIMITED), and at most `unknownLookupInflight` of them wait for the store at
// once (above: BUSY), which keeps the rest of the store queue for login, register, logout and cached-token refreshes.
// A cached token past its TTL is not charged: this gateway has seen it valid.

namespace openexchange { namespace gateway {

enum class PrincipalMode : std::uint8_t
{
    DEMO,
    API_KEY,
    JWT,
    DEV
};
const char *nameOf(PrincipalMode m) noexcept;

struct Principal
{
    std::int64_t userId = 0;
    PrincipalMode mode = PrincipalMode::DEMO;
    std::optional<std::string> username; // demo only
    std::string tokenHash;               // demo only: SHA-256 of the bearer token (32 bytes)
};

enum class AuthOutcome : std::uint8_t
{
    OK,
    UNAUTHORIZED, // 401
    UNAVAILABLE,  // 503 AUTH_UNAVAILABLE: the users store cannot answer and no cached entry is usable
    BUSY,         // 503 AUTH_BUSY: the store queue (or the unknown-token share of it) is full, no cached entry usable
    RATE_LIMITED  // 429 RATE_LIMITED: the caller's budget for unknown-token lookups is spent
};

struct AuthResult
{
    AuthOutcome outcome = AuthOutcome::UNAUTHORIZED;
    Principal principal;
    std::string detail; // for the log and the error body (never the credential itself)
    std::int64_t retryAfterMs = 0; // RATE_LIMITED: time until the budget has a token again
};

struct CredentialHeaders
{
    std::optional<std::string_view> authorization;
    std::optional<std::string_view> apiKey;
    std::string_view target; // read only by the test-only control GW_NC_QUERY_TOKEN
    // Called (on the calling thread, at most once) only when a demo token is not in the cache and the store must be
    // asked; a refused take is RATE_LIMITED. Empty: no per-caller budget.
    std::function<TakeResult()> lookupBudget;
};

struct AuthenticatorSettings
{
    unsigned modes = 0;                 // auth_mode bits
    std::int64_t tokenCacheTtlMs = 5000;
    std::int64_t tokenStaleMs = 600'000;
    std::int64_t unknownLookupInflight = 0; // store lookups of uncached tokens at once; 0 = no bound
};

class TokenCache
{
public:
    struct Entry
    {
        std::int64_t userId = 0;
        std::string username;
        std::int64_t verifiedMs = 0; // when the store last confirmed it
    };
    std::optional<Entry> get(const std::string &tokenHash);
    void put(const std::string &tokenHash, Entry entry, std::int64_t staleMs);
    // put, unless a revoke ran since `generation` was read (a lookup racing a logout must not bring the token back)
    void putIfUnchanged(const std::string &tokenHash, Entry entry, std::int64_t staleMs, std::uint64_t generation);
    void erase(const std::string &tokenHash);  // the store does not know it
    void revoke(const std::string &tokenHash); // logout or rotation here: erase, and bump the generation
    std::uint64_t generation();
    std::size_t size();

private:
    std::mutex m_mutex;
    std::unordered_map<std::string, Entry> m_entries;
    std::size_t m_puts = 0;
    std::uint64_t m_generation = 0;
};

class Authenticator
{
public:
    using Done = std::function<void(AuthResult)>;
    using MonotonicMs = std::function<std::int64_t()>;
    using EpochSeconds = std::function<std::int64_t()>;

    // `store` and `storePool` may be null when demo mode is off.
    Authenticator(AuthenticatorSettings settings, AuthMaterial material, UserStore *store, WorkPool *storePool,
        MonotonicMs monotonicMs = {}, EpochSeconds epochSeconds = {});

    // Calls `done` exactly once: at once for every mode but a demo lookup, from a store worker for that one.
    void resolve(const CredentialHeaders &headers, Done done);

    // Register and login: the new token is valid now. Logout and rotation: the old one is gone.
    void remember(const std::string &tokenHash, std::int64_t userId, const std::string &username);
    void forget(const std::string &tokenHash);

    std::int64_t nowMs() const { return m_monotonicMs(); }
    unsigned modes() const noexcept { return m_settings.modes; }
    std::uint64_t staleServed() const noexcept { return m_staleServed.load(std::memory_order_relaxed); }
    std::uint64_t storeLookups() const noexcept { return m_lookups.load(std::memory_order_relaxed); }
    std::uint64_t unknownLookupsRefused() const noexcept { return m_unknownRefused.load(std::memory_order_relaxed); }
    std::size_t cachedTokens() { return m_cache.size(); }

private:
    void resolveDemo(const std::string &token, const std::function<TakeResult()> &budget, Done done);
    bool serveStale(const std::optional<TokenCache::Entry> &e, const std::string &hash, Done &done);

    AuthenticatorSettings m_settings;
    AuthMaterial m_material;
    UserStore *m_store;
    WorkPool *m_pool;
    MonotonicMs m_monotonicMs;
    EpochSeconds m_epochSeconds;
    TokenCache m_cache;
    std::atomic<std::uint64_t> m_staleServed{ 0 };
    std::atomic<std::uint64_t> m_lookups{ 0 };
    std::atomic<std::int64_t> m_unknownInflight{ 0 };
    std::atomic<std::uint64_t> m_unknownRefused{ 0 };
};

// A new demo token: 32 random bytes, unpadded base64url (43 characters, no '.').
std::string newDemoToken();

}}

#endif
