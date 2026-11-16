#include "gateway/common/PgUserStore.h"

#include <chrono>
#include <cstring>
#include <map>

#include "gateway/common/Crypto.h"
#include "gateway/common/FrontLog.h"

namespace openexchange { namespace gateway {

namespace {

// pg_advisory_lock key of the migration step: the eight bytes "gwschema" as a big-endian int64
constexpr const char *MIGRATION_LOCK_KEY = "7455554578925579617";

using pg::Exec;
using pg::nowNs;
using pg::Result;

std::string unhex(const char *text)
{
    std::string out;
    const std::size_t n = std::strlen(text);
    auto v = [](char c) { return c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10; };
    for (std::size_t i = 0; i + 1 < n; i += 2)
    {
        out += static_cast<char>((v(text[i]) << 4) | v(text[i + 1]));
    }
    return out;
}

}

const char *nameOf(StoreStatus s) noexcept
{
    switch (s)
    {
        case StoreStatus::OK: return "OK";
        case StoreStatus::NOT_FOUND: return "NOT_FOUND";
        case StoreStatus::CONFLICT: return "CONFLICT";
        case StoreStatus::UNAVAILABLE: return "UNAVAILABLE";
    }
    return "?";
}

PgUserStore::PgUserStore(PgSettings settings, std::vector<Migration> migrations) :
    m_settings(std::move(settings)), m_migrations(std::move(migrations))
{
    for (int i = 0; i < m_settings.workers; ++i)
    {
        m_conns.push_back(std::make_unique<PgConn>());
    }
}

PgUserStore::~PgUserStore() = default;

void PgUserStore::note(const char *what, const std::string &detail)
{
    m_failures.fetch_add(1, std::memory_order_relaxed);
    m_degraded.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        if (!m_mismatch.load())
        {
            m_state = std::string(what) + ": " + detail;
        }
    }
    std::uint64_t held = 0;
    if (!m_warns.take(m_settings.retryMs * 1'000'000, held))
    {
        return;
    }
    if (0 == held)
    {
        frontLog(FrontLevel::WARN, "users store: %s: %s", what, detail.c_str());
    }
    else
    {
        frontLog(FrontLevel::WARN, "users store: %s: %s (%llu similar lines held back)", what, detail.c_str(),
            static_cast<unsigned long long>(held));
    }
}

std::string PgUserStore::state() const
{
    std::lock_guard<std::mutex> lock(m_stateMutex);
    return m_state;
}

PgConn *PgUserStore::ready(int worker)
{
    if (m_mismatch.load(std::memory_order_acquire) || worker < 0 || worker >= static_cast<int>(m_conns.size()))
    {
        return nullptr;
    }
    PgConn &c = *m_conns[static_cast<std::size_t>(worker)];
    if (!c.ok())
    {
        if (c.counted)
        {
            c.counted = false;
            m_connected.fetch_sub(1, std::memory_order_relaxed);
        }
        if (nowNs() < m_retryAtNs.load(std::memory_order_acquire))
        {
            m_failures.fetch_add(1, std::memory_order_relaxed);
            return nullptr; // a connect failed moments ago: fail fast until the retry time
        }
        m_connectAttempts.fetch_add(1, std::memory_order_relaxed);
        std::string error;
        if (!c.connect(m_settings.url, m_settings.statementTimeoutMs, error))
        {
            m_retryAtNs.store(nowNs() + m_settings.retryMs * 1'000'000, std::memory_order_release);
            note("connect failed", error);
            return nullptr;
        }
        c.counted = true;
        m_connected.fetch_add(1, std::memory_order_relaxed);
    }
    if (m_schemaVerified.load(std::memory_order_acquire))
    {
        if (m_degraded.exchange(false, std::memory_order_acq_rel))
        {
            std::lock_guard<std::mutex> lock(m_stateMutex);
            m_state = "ready";
        }
        return &c;
    }
    const std::int64_t t = m_settings.statementTimeoutMs;
    Result r;
    std::string error;
    auto fail = [&](const char *what) -> PgConn * {
        note(what, error);
        if (c.ok() && m_settings.migrate)
        {
            Result ignored;
            std::string why;
            c.exec("ROLLBACK", {}, t, ignored, why, true);
            c.exec(std::string("SELECT pg_advisory_unlock(") + MIGRATION_LOCK_KEY + ")", {}, t, ignored, why, false);
        }
        return nullptr;
    };
    if (m_settings.migrate)
    {
        if (Exec::OK != c.exec(std::string("SELECT pg_advisory_lock(") + MIGRATION_LOCK_KEY + ")", {}, t, r, error, false))
        {
            return fail("schema lock");
        }
        if (Exec::OK != c.exec("CREATE TABLE IF NOT EXISTS gateway_schema_migrations (version INT PRIMARY KEY, "
                               "name TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
                            {}, t, r, error, false))
        {
            return fail("schema table");
        }
    }
    else
    {
        if (Exec::OK != c.exec("SELECT to_regclass('gateway_schema_migrations') IS NOT NULL", {}, t, r, error, false))
        {
            return fail("schema check");
        }
        if (0 != std::strcmp("t", PQgetvalue(r.get(), 0, 0)))
        {
            error = "gateway_schema_migrations does not exist (oe-gateway applies it)";
            return fail("schema not applied");
        }
    }
    if (Exec::OK != c.exec("SELECT version, sha256 FROM gateway_schema_migrations ORDER BY version", {}, t, r, error, false))
    {
        return fail("schema read");
    }
    std::map<int, std::string> applied;
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        applied[std::atoi(PQgetvalue(r.get(), i, 0))] = PQgetvalue(r.get(), i, 1);
    }
    std::string mismatch;
    for (const auto &[version, sha] : applied)
    {
        const Migration *known = nullptr;
        for (const Migration &m : m_migrations)
        {
            known = m.version == version ? &m : known;
        }
        if (nullptr == known)
        {
            mismatch = "version " + std::to_string(version) + " is recorded but unknown to this build";
        }
        else if (sha != known->sha256)
        {
            mismatch = "version " + std::to_string(version) + " (" + known->name + ") is recorded with sha256 " + sha +
                ", this build carries " + known->sha256;
        }
    }
    if (!mismatch.empty())
    {
        if (!m_mismatch.exchange(true))
        {
            {
                std::lock_guard<std::mutex> lock(m_stateMutex);
                m_state = "schema mismatch: " + mismatch;
            }
            frontLog(FrontLevel::CRITICAL, "users store: schema mismatch: %s; every auth call answers 503 until the "
                "database and the build agree", mismatch.c_str());
        }
        error = mismatch;
        fail("schema mismatch");
        return nullptr;
    }
    for (const Migration &m : m_migrations)
    {
        if (applied.count(m.version))
        {
            continue;
        }
        if (!m_settings.migrate)
        {
            error = "version " + std::to_string(m.version) + " (" + m.name + ") is not applied";
            return fail("schema not applied");
        }
        const std::string script = std::string("BEGIN;\n") + m.sql +
            "\nINSERT INTO gateway_schema_migrations (version, name, sha256) VALUES (" + std::to_string(m.version) +
            ", '" + m.name + "', '" + m.sha256 + "');\nCOMMIT;";
        if (Exec::OK != c.exec(script, {}, t, r, error, true))
        {
            return fail("migration failed");
        }
        frontLog(FrontLevel::INFO, "users store: applied migration V%03d %s (sha256 %s)", m.version, m.name, m.sha256);
    }
    if (m_settings.migrate)
    {
        c.exec(std::string("SELECT pg_advisory_unlock(") + MIGRATION_LOCK_KEY + ")", {}, t, r, error, false);
    }
    m_schemaVerified.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        m_state = "ready";
    }
    return &c;
}

StoreStatus PgUserStore::reserveUserId(int worker, const std::string &username, std::int64_t &userId)
{
    PgConn *c = ready(worker);
    if (nullptr == c)
    {
        return StoreStatus::UNAVAILABLE;
    }
    Result r;
    std::string error;
    if (Exec::OK != c->exec("SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE username = $1) THEN NULL "
                            "ELSE nextval('demo_user_seq') END",
                        { username }, m_settings.statementTimeoutMs, r, error, false))
    {
        note("reserve user id", error);
        return StoreStatus::UNAVAILABLE;
    }
    if (PQgetisnull(r.get(), 0, 0))
    {
        return StoreStatus::CONFLICT;
    }
    userId = std::atoll(PQgetvalue(r.get(), 0, 0));
    return StoreStatus::OK;
}

StoreStatus PgUserStore::insertUser(int worker, std::int64_t userId, const std::string &username,
    const std::string &passwordHash, const std::string &tokenHash)
{
    PgConn *c = ready(worker);
    if (nullptr == c)
    {
        return StoreStatus::UNAVAILABLE;
    }
    Result r;
    std::string error;
    const Exec e = c->exec("INSERT INTO users (user_id, username, password_hash, token_hash, token_issued_at) "
                           "VALUES ($1, $2, $3, decode($4, 'hex'), now())",
        { std::to_string(userId), username, passwordHash, crypto::hex(tokenHash) }, m_settings.statementTimeoutMs, r,
        error, false);
    if (Exec::UNIQUE == e)
    {
        return StoreStatus::CONFLICT;
    }
    if (Exec::OK != e)
    {
        note("insert user", error);
        return StoreStatus::UNAVAILABLE;
    }
    return StoreStatus::OK;
}

namespace {

void readUser(PGresult *r, StoredUser &out)
{
    out.userId = std::atoll(PQgetvalue(r, 0, 0));
    out.username = PQgetvalue(r, 0, 1);
    out.passwordHash = PQgetvalue(r, 0, 2);
    if (PQgetisnull(r, 0, 3))
    {
        out.tokenHash.reset();
    }
    else
    {
        out.tokenHash = unhex(PQgetvalue(r, 0, 3));
    }
}

}

StoreStatus PgUserStore::findByUsername(int worker, const std::string &username, StoredUser &out)
{
    PgConn *c = ready(worker);
    if (nullptr == c)
    {
        return StoreStatus::UNAVAILABLE;
    }
    Result r;
    std::string error;
    if (Exec::OK != c->exec("SELECT user_id, username, password_hash, encode(token_hash, 'hex') FROM users "
                            "WHERE username = $1",
                        { username }, m_settings.statementTimeoutMs, r, error, false))
    {
        note("find by username", error);
        return StoreStatus::UNAVAILABLE;
    }
    if (0 == PQntuples(r.get()))
    {
        return StoreStatus::NOT_FOUND;
    }
    readUser(r.get(), out);
    return StoreStatus::OK;
}

StoreStatus PgUserStore::findByToken(int worker, const std::string &tokenHash, StoredUser &out)
{
    PgConn *c = ready(worker);
    if (nullptr == c)
    {
        return StoreStatus::UNAVAILABLE;
    }
    Result r;
    std::string error;
    if (Exec::OK != c->exec("SELECT user_id, username, password_hash, encode(token_hash, 'hex') FROM users "
                            "WHERE token_hash = decode($1, 'hex')",
                        { crypto::hex(tokenHash) }, m_settings.statementTimeoutMs, r, error, false))
    {
        note("find by token", error);
        return StoreStatus::UNAVAILABLE;
    }
    if (0 == PQntuples(r.get()))
    {
        return StoreStatus::NOT_FOUND;
    }
    readUser(r.get(), out);
    return StoreStatus::OK;
}

StoreStatus PgUserStore::setToken(int worker, std::int64_t userId, const std::string &tokenHash)
{
    PgConn *c = ready(worker);
    if (nullptr == c)
    {
        return StoreStatus::UNAVAILABLE;
    }
    Result r;
    std::string error;
    const Exec e = c->exec("UPDATE users SET token_hash = decode($2, 'hex'), token_issued_at = now() WHERE user_id = $1",
        { std::to_string(userId), crypto::hex(tokenHash) }, m_settings.statementTimeoutMs, r, error, false);
    if (Exec::UNIQUE == e)
    {
        return StoreStatus::CONFLICT;
    }
    if (Exec::OK != e)
    {
        note("set token", error);
        return StoreStatus::UNAVAILABLE;
    }
    return 0 == std::strcmp("1", PQcmdTuples(r.get())) ? StoreStatus::OK : StoreStatus::NOT_FOUND;
}

StoreStatus PgUserStore::clearToken(int worker, std::int64_t userId, const std::string &tokenHash)
{
    PgConn *c = ready(worker);
    if (nullptr == c)
    {
        return StoreStatus::UNAVAILABLE;
    }
    Result r;
    std::string error;
    if (Exec::OK != c->exec("UPDATE users SET token_hash = NULL, token_issued_at = NULL "
                            "WHERE user_id = $1 AND token_hash = decode($2, 'hex')",
                        { std::to_string(userId), crypto::hex(tokenHash) }, m_settings.statementTimeoutMs, r, error,
                        false))
    {
        note("clear token", error);
        return StoreStatus::UNAVAILABLE;
    }
    return 0 == std::strcmp("1", PQcmdTuples(r.get())) ? StoreStatus::OK : StoreStatus::NOT_FOUND;
}

}}
