#include "gateway/common/Authenticator.h"

#include <chrono>
#include <memory>

#include "gateway/common/Crypto.h"
#include "gateway/common/Jwt.h"
#include "gateway/common/SettingsReader.h"

namespace openexchange { namespace gateway {

namespace {

std::int64_t steadyMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::int64_t wallSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

bool iequals(std::string_view a, std::string_view b)
{
    if (a.size() != b.size())
    {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i)
    {
        if ((a[i] | 0x20) != (b[i] | 0x20))
        {
            return false;
        }
    }
    return true;
}

bool visibleToken(std::string_view t)
{
    if (t.empty())
    {
        return false;
    }
    for (const char c : t)
    {
        if (c <= 0x20 || c >= 0x7f)
        {
            return false;
        }
    }
    return true;
}

AuthResult unauthorized(std::string detail)
{
    AuthResult r;
    r.outcome = AuthOutcome::UNAUTHORIZED;
    r.detail = std::move(detail);
    return r;
}

AuthResult ok(std::int64_t userId, PrincipalMode mode)
{
    AuthResult r;
    r.outcome = AuthOutcome::OK;
    r.principal.userId = userId;
    r.principal.mode = mode;
    return r;
}

}

const char *nameOf(PrincipalMode m) noexcept
{
    switch (m)
    {
        case PrincipalMode::DEMO: return "demo";
        case PrincipalMode::API_KEY: return "api-key";
        case PrincipalMode::JWT: return "jwt";
        case PrincipalMode::DEV: return "dev";
    }
    return "?";
}

std::optional<TokenCache::Entry> TokenCache::get(const std::string &tokenHash)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto it = m_entries.find(tokenHash);
    if (it == m_entries.end())
    {
        return std::nullopt;
    }
    return it->second;
}

void TokenCache::put(const std::string &tokenHash, Entry entry, std::int64_t staleMs)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    const std::int64_t now = entry.verifiedMs;
    m_entries[tokenHash] = std::move(entry);
    // entries past the stale bound can never be served again: drop them now and then (no count bound)
    if (++m_puts % 256 == 0)
    {
        for (auto it = m_entries.begin(); it != m_entries.end();)
        {
            it = now - it->second.verifiedMs > staleMs ? m_entries.erase(it) : std::next(it);
        }
    }
}

void TokenCache::putIfUnchanged(const std::string &tokenHash, Entry entry, std::int64_t staleMs, std::uint64_t generation)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (generation != m_generation)
    {
        return;
    }
    const std::int64_t now = entry.verifiedMs;
    m_entries[tokenHash] = std::move(entry);
    if (++m_puts % 256 == 0)
    {
        for (auto it = m_entries.begin(); it != m_entries.end();)
        {
            it = now - it->second.verifiedMs > staleMs ? m_entries.erase(it) : std::next(it);
        }
    }
}

void TokenCache::erase(const std::string &tokenHash)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_entries.erase(tokenHash);
}

void TokenCache::revoke(const std::string &tokenHash)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_entries.erase(tokenHash);
    ++m_generation;
}

std::uint64_t TokenCache::generation()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_generation;
}

std::size_t TokenCache::size()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_entries.size();
}

std::string newDemoToken()
{
    return crypto::base64UrlEncode(crypto::randomBytes(32));
}

Authenticator::Authenticator(AuthenticatorSettings settings, AuthMaterial material, UserStore *store, WorkPool *storePool,
    MonotonicMs monotonicMs, EpochSeconds epochSeconds) :
    m_settings(settings),
    m_material(std::move(material)),
    m_store(store),
    m_pool(storePool),
    m_monotonicMs(monotonicMs ? std::move(monotonicMs) : MonotonicMs(&steadyMs)),
    m_epochSeconds(epochSeconds ? std::move(epochSeconds) : EpochSeconds(&wallSeconds))
{
}

void Authenticator::resolve(const CredentialHeaders &h, Done done)
{
    std::optional<std::string_view> authorization = h.authorization;
#ifdef GW_NC_QUERY_TOKEN
    // Control only: a parser that takes the token from the URL (its test must catch it).
    if (!authorization && !h.apiKey)
    {
        const std::size_t at = h.target.find("token=");
        if (std::string_view::npos != at)
        {
            static thread_local std::string fromUrl;
            fromUrl = "Bearer " + std::string(h.target.substr(at + 6, h.target.find('&', at) - at - 6));
            authorization = fromUrl;
        }
    }
#endif
    if (authorization && h.apiKey)
    {
        done(unauthorized("two credentials (Authorization and X-API-Key): send one"));
        return;
    }
    if (h.apiKey)
    {
        if (0 == (m_settings.modes & auth_mode::API_KEY))
        {
            done(unauthorized("api-key authentication is not enabled"));
            return;
        }
        const auto user = m_material.apiKeys.find(*h.apiKey);
        if (!user)
        {
            done(unauthorized("unknown API key"));
            return;
        }
        done(ok(*user, PrincipalMode::API_KEY));
        return;
    }
    if (!authorization)
    {
        done(unauthorized("no credential (Authorization: Bearer <token> or X-API-Key)"));
        return;
    }
    const std::string_view value = *authorization;
    if (value.size() < 7 || !iequals(value.substr(0, 7), "Bearer "))
    {
        done(unauthorized("the Authorization scheme must be Bearer"));
        return;
    }
    const std::string_view token = value.substr(7);
    if (!visibleToken(token))
    {
        done(unauthorized("empty or malformed bearer token"));
        return;
    }
    if (0 == token.rfind("dev:", 0))
    {
        if (0 == (m_settings.modes & auth_mode::DEV))
        {
            done(unauthorized("dev authentication is not enabled"));
            return;
        }
        std::int64_t userId = 0;
        if (!parsePositiveDecimal(std::string(token.substr(4)), userId))
        {
            done(unauthorized("dev:<userId> needs a positive integer"));
            return;
        }
        done(ok(userId, PrincipalMode::DEV));
        return;
    }
    if (std::string_view::npos != token.find('.'))
    {
        if (0 == (m_settings.modes & auth_mode::JWT))
        {
            done(unauthorized("jwt authentication is not enabled"));
            return;
        }
        const jwt::Result r = jwt::verifyHs256(token, m_material.jwtSecret, m_epochSeconds());
        if (jwt::Verdict::OK != r.verdict)
        {
            done(unauthorized(std::string("jwt refused: ") + jwt::nameOf(r.verdict)));
            return;
        }
        done(ok(r.userId, PrincipalMode::JWT));
        return;
    }
    if (0 == (m_settings.modes & auth_mode::DEMO) || nullptr == m_store || nullptr == m_pool)
    {
        done(unauthorized("unknown bearer token"));
        return;
    }
    resolveDemo(std::string(token), h.lookupBudget, std::move(done));
}

bool Authenticator::serveStale(const std::optional<TokenCache::Entry> &e, const std::string &hash, Done &done)
{
    if (!e || m_monotonicMs() - e->verifiedMs > m_settings.tokenStaleMs)
    {
        return false;
    }
    m_staleServed.fetch_add(1, std::memory_order_relaxed);
    AuthResult r = ok(e->userId, PrincipalMode::DEMO);
    r.principal.username = e->username;
    r.principal.tokenHash = hash;
    r.detail = "served from the token cache while the users store cannot answer";
    done(std::move(r));
    return true;
}

void Authenticator::resolveDemo(const std::string &token, const std::function<TakeResult()> &budget, Done done)
{
    const crypto::Digest digest = crypto::sha256(token);
    std::string hash(crypto::view(digest));
    const std::optional<TokenCache::Entry> cached = m_cache.get(hash);
    if (cached && m_monotonicMs() - cached->verifiedMs < m_settings.tokenCacheTtlMs)
    {
        AuthResult r = ok(cached->userId, PrincipalMode::DEMO);
        r.principal.username = cached->username;
        r.principal.tokenHash = hash;
        done(std::move(r));
        return;
    }
    const bool unknown = !cached;
    if (unknown)
    {
        const std::int64_t cap = m_settings.unknownLookupInflight;
        if (cap > 0 && m_unknownInflight.fetch_add(1, std::memory_order_acq_rel) >= cap)
        {
            m_unknownInflight.fetch_sub(1, std::memory_order_acq_rel);
            m_unknownRefused.fetch_add(1, std::memory_order_relaxed);
            AuthResult r;
            r.outcome = AuthOutcome::BUSY;
            r.detail = "too many unknown-token lookups at once";
            done(std::move(r));
            return;
        }
        if (budget)
        {
            const TakeResult t = budget();
            if (!t.ok)
            {
                if (cap > 0)
                {
                    m_unknownInflight.fetch_sub(1, std::memory_order_acq_rel);
                }
                m_unknownRefused.fetch_add(1, std::memory_order_relaxed);
                AuthResult r;
                r.outcome = AuthOutcome::RATE_LIMITED;
                r.retryAfterMs = t.retryAfterMs;
                r.detail = "too many lookups of unknown tokens from this source";
                done(std::move(r));
                return;
            }
        }
    }
    const bool counted = unknown && m_settings.unknownLookupInflight > 0;
    const std::uint64_t generation = m_cache.generation();
    auto shared = std::make_shared<Done>(std::move(done));
    const bool taken = m_pool->trySubmit([this, hash, cached, shared, counted, generation](int worker) {
        struct Release
        {
            std::atomic<std::int64_t> *inflight;
            ~Release()
            {
                if (nullptr != inflight)
                {
                    inflight->fetch_sub(1, std::memory_order_acq_rel);
                }
            }
        } release{ counted ? &m_unknownInflight : nullptr };
        m_lookups.fetch_add(1, std::memory_order_relaxed);
        StoredUser user;
        const StoreStatus st = m_store->findByToken(worker, hash, user);
        if (StoreStatus::OK == st)
        {
            m_cache.putIfUnchanged(hash, TokenCache::Entry{ user.userId, user.username, m_monotonicMs() },
                m_settings.tokenStaleMs, generation);
            AuthResult r = ok(user.userId, PrincipalMode::DEMO);
            r.principal.username = user.username;
            r.principal.tokenHash = hash;
            (*shared)(std::move(r));
            return;
        }
        if (StoreStatus::NOT_FOUND == st)
        {
            m_cache.erase(hash);
            (*shared)(unauthorized("unknown or expired bearer token"));
            return;
        }
        if (!serveStale(cached, hash, *shared))
        {
            AuthResult r;
            r.outcome = AuthOutcome::UNAVAILABLE;
            r.detail = "the users store cannot answer";
            (*shared)(std::move(r));
        }
    });
    if (!taken && counted)
    {
        m_unknownInflight.fetch_sub(1, std::memory_order_acq_rel);
    }
    if (!taken && !serveStale(cached, hash, *shared))
    {
        AuthResult r;
        r.outcome = AuthOutcome::BUSY;
        r.detail = "the users store queue is full";
        (*shared)(std::move(r));
    }
}

void Authenticator::remember(const std::string &tokenHash, std::int64_t userId, const std::string &username)
{
    m_cache.put(tokenHash, TokenCache::Entry{ userId, username, m_monotonicMs() }, m_settings.tokenStaleMs);
}

void Authenticator::forget(const std::string &tokenHash)
{
    m_cache.revoke(tokenHash);
}

}}
