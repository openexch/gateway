#include "gateway/common/PgUserStore.h"

#include <poll.h>

#include <chrono>
#include <cstring>
#include <map>

#include <libpq-fe.h>

#include "gateway/common/Crypto.h"
#include "gateway/common/FrontLog.h"

namespace openexchange { namespace gateway {

namespace {

// pg_advisory_lock key of the migration step: the eight bytes "gwschema" as a big-endian int64
constexpr const char *MIGRATION_LOCK_KEY = "7455554578925579617";

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct ResultDeleter
{
    void operator()(PGresult *r) const noexcept { PQclear(r); }
};
using Result = std::unique_ptr<PGresult, ResultDeleter>;

enum class Exec : std::uint8_t
{
    OK,
    UNIQUE,     // SQLSTATE 23505
    FAILED      // any other error, a lost connection or the deadline
};

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

class PgUserStore::Conn
{
public:
    ~Conn() { close(); }

    void close()
    {
        if (nullptr != pg)
        {
            PQfinish(pg);
            pg = nullptr;
        }
    }

    bool ok() const { return nullptr != pg && CONNECTION_OK == PQstatus(pg); }

    // Waits until the socket is readable (or writable) or the deadline passes.
    bool waitSocket(bool forWrite, std::int64_t deadlineNs)
    {
        const std::int64_t left = deadlineNs - nowNs();
        if (left <= 0)
        {
            return false;
        }
        pollfd p{};
        p.fd = PQsocket(pg);
        p.events = forWrite ? POLLOUT : POLLIN;
        const int ms = static_cast<int>((left + 999'999) / 1'000'000);
        const int r = ::poll(&p, 1, ms);
        return r > 0;
    }

    bool connect(const std::string &url, std::int64_t timeoutMs, std::string &error)
    {
        close();
        const std::int64_t deadline = nowNs() + timeoutMs * 1'000'000LL;
        const char *keys[] = { "dbname", nullptr };
        const char *values[] = { url.c_str(), nullptr };
        pg = PQconnectStartParams(keys, values, 1);
        if (nullptr == pg || CONNECTION_BAD == PQstatus(pg))
        {
            error = nullptr == pg ? "out of memory" : PQerrorMessage(pg);
            close();
            return false;
        }
        PostgresPollingStatusType st = PGRES_POLLING_WRITING;
        while (PGRES_POLLING_OK != st)
        {
            if (PGRES_POLLING_FAILED == st)
            {
                error = PQerrorMessage(pg);
                close();
                return false;
            }
            if (!waitSocket(PGRES_POLLING_WRITING == st, deadline))
            {
                error = "connect deadline (" + std::to_string(timeoutMs) + " ms) passed";
                close();
                return false;
            }
            st = PQconnectPoll(pg);
        }
        if (0 != PQsetnonblocking(pg, 1))
        {
            error = PQerrorMessage(pg);
            close();
            return false;
        }
        Result r;
        std::string why;
        if (Exec::OK != exec("SET statement_timeout = " + std::to_string(timeoutMs), {}, timeoutMs, r, why, false))
        {
            error = "SET statement_timeout: " + why;
            close();
            return false;
        }
        return true;
    }

    // One statement with text parameters (or, `simple`, a script of statements without parameters). The last result
    // is kept in `out`; any error result makes the call fail with its message.
    Exec exec(const std::string &sql, const std::vector<std::string> &params, std::int64_t timeoutMs, Result &out,
        std::string &error, bool simple)
    {
        const std::int64_t deadline = nowNs() + timeoutMs * 1'000'000LL;
        std::vector<const char *> values;
        for (const std::string &p : params)
        {
            values.push_back(p.c_str());
        }
        const int sent = simple ? PQsendQuery(pg, sql.c_str())
                                : PQsendQueryParams(pg, sql.c_str(), static_cast<int>(values.size()), nullptr,
                                      values.data(), nullptr, nullptr, 0);
        if (1 != sent)
        {
            error = PQerrorMessage(pg);
            return Exec::FAILED;
        }
        for (;;)
        {
            const int f = PQflush(pg);
            if (0 == f)
            {
                break;
            }
            if (f < 0 || !waitSocket(true, deadline))
            {
                error = f < 0 ? PQerrorMessage(pg) : "statement deadline passed while sending";
                close();
                return Exec::FAILED;
            }
        }
        Exec verdict = Exec::OK;
        out.reset();
        for (;;)
        {
            while (PQisBusy(pg))
            {
                if (!waitSocket(false, deadline))
                {
                    error = "statement deadline (" + std::to_string(timeoutMs) + " ms) passed";
                    close(); // the server's statement_timeout ends the statement on its side
                    return Exec::FAILED;
                }
                if (1 != PQconsumeInput(pg))
                {
                    error = PQerrorMessage(pg);
                    close();
                    return Exec::FAILED;
                }
            }
            PGresult *raw = PQgetResult(pg);
            if (nullptr == raw)
            {
                break;
            }
            Result r(raw);
            const ExecStatusType st = PQresultStatus(raw);
            if (PGRES_FATAL_ERROR == st || PGRES_BAD_RESPONSE == st || PGRES_NONFATAL_ERROR == st)
            {
                if (Exec::OK == verdict)
                {
                    const char *state = PQresultErrorField(raw, PG_DIAG_SQLSTATE);
                    verdict = nullptr != state && 0 == std::strcmp(state, "23505") ? Exec::UNIQUE : Exec::FAILED;
                    error = PQresultErrorMessage(raw);
                    while (!error.empty() && ('\n' == error.back() || ' ' == error.back()))
                    {
                        error.pop_back();
                    }
                }
            }
            out = std::move(r);
        }
        if (!ok())
        {
            close();
            return Exec::FAILED;
        }
        return verdict;
    }

    PGconn *pg = nullptr;
    bool counted = false;
};

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
        m_conns.push_back(std::make_unique<Conn>());
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
    const std::int64_t now = nowNs();
    std::int64_t last = m_lastWarnNs.load(std::memory_order_acquire);
    if ((0 == last || now - last >= m_settings.retryMs * 1'000'000) &&
        m_lastWarnNs.compare_exchange_strong(last, now, std::memory_order_acq_rel))
    {
        const std::uint64_t held = m_warnsHeld.exchange(0, std::memory_order_acq_rel);
        if (0 == held)
        {
            frontLog(FrontLevel::WARN, "users store: %s: %s", what, detail.c_str());
        }
        else
        {
            frontLog(FrontLevel::WARN, "users store: %s: %s (%llu similar lines held back)", what, detail.c_str(),
                static_cast<unsigned long long>(held));
        }
        return;
    }
    m_warnsHeld.fetch_add(1, std::memory_order_relaxed);
}

std::string PgUserStore::state() const
{
    std::lock_guard<std::mutex> lock(m_stateMutex);
    return m_state;
}

PgUserStore::Conn *PgUserStore::ready(int worker)
{
    if (m_mismatch.load(std::memory_order_acquire) || worker < 0 || worker >= static_cast<int>(m_conns.size()))
    {
        return nullptr;
    }
    Conn &c = *m_conns[static_cast<std::size_t>(worker)];
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
    auto fail = [&](const char *what) -> Conn * {
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
    Conn *c = ready(worker);
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
    Conn *c = ready(worker);
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
    Conn *c = ready(worker);
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
    Conn *c = ready(worker);
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
    Conn *c = ready(worker);
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
    Conn *c = ready(worker);
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
