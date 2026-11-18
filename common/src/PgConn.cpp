#include "gateway/common/PgConn.h"

#include <poll.h>

#include <algorithm>
#include <chrono>
#include <cstring>

namespace openexchange { namespace gateway {

namespace pg {

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool WarnLimiter::take(std::int64_t intervalNs, std::uint64_t &held)
{
    const std::int64_t now = nowNs();
    std::int64_t last = m_lastNs.load(std::memory_order_acquire);
    if ((0 == last || now - last >= intervalNs) && m_lastNs.compare_exchange_strong(last, now, std::memory_order_acq_rel))
    {
        held = m_held.exchange(0, std::memory_order_acq_rel);
        return true;
    }
    m_held.fetch_add(1, std::memory_order_relaxed);
    return false;
}

}

using pg::Exec;
using pg::nowNs;
using pg::Result;

void PgConn::close()
{
    if (nullptr != m_pg)
    {
        PQfinish(m_pg);
        m_pg = nullptr;
    }
}

// Waits until the socket is readable (or writable) or the deadline passes.
bool PgConn::waitSocket(bool forWrite, std::int64_t deadlineNs)
{
    const std::int64_t left = deadlineNs - nowNs();
    if (left <= 0)
    {
        return false;
    }
    pollfd p{};
    p.fd = PQsocket(m_pg);
    p.events = forWrite ? POLLOUT : POLLIN;
    const int ms = static_cast<int>((left + 999'999) / 1'000'000);
    const int r = ::poll(&p, 1, ms);
    return r > 0;
}

bool PgConn::connect(const std::string &url, std::int64_t timeoutMs, std::string &error,
    const std::vector<std::string> &setup, std::int64_t budgetMs,
    const std::vector<std::pair<std::string, std::string>> &options)
{
    close();
    m_sqlState.clear();
    m_deadline = false;
    const std::int64_t budget = budgetMs > 0 ? budgetMs : timeoutMs;
    const std::int64_t deadline = nowNs() + budget * 1'000'000LL;
    // what is left of the budget for one setup statement (at least 1 ms: a passed deadline then fails it at once)
    auto left = [deadline] { return std::max<std::int64_t>(1, (deadline - nowNs()) / 1'000'000); };
    std::vector<const char *> keys{ "dbname" };
    std::vector<const char *> values{ url.c_str() };
    for (const auto &[key, value] : options)
    {
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }
    keys.push_back(nullptr);
    values.push_back(nullptr);
    m_pg = PQconnectStartParams(keys.data(), values.data(), 1);
    if (nullptr == m_pg || CONNECTION_BAD == PQstatus(m_pg))
    {
        error = nullptr == m_pg ? "out of memory" : PQerrorMessage(m_pg);
        close();
        return false;
    }
    PostgresPollingStatusType st = PGRES_POLLING_WRITING;
    while (PGRES_POLLING_OK != st)
    {
        if (PGRES_POLLING_FAILED == st)
        {
            error = PQerrorMessage(m_pg);
            close();
            return false;
        }
        if (!waitSocket(PGRES_POLLING_WRITING == st, deadline))
        {
            error = "connect deadline (" + std::to_string(budget) + " ms) passed";
            m_deadline = true;
            close();
            return false;
        }
        st = PQconnectPoll(m_pg);
    }
    if (0 != PQsetnonblocking(m_pg, 1))
    {
        error = PQerrorMessage(m_pg);
        close();
        return false;
    }
    Result r;
    std::string why;
    if (Exec::OK != exec("SET statement_timeout = " + std::to_string(timeoutMs), {}, left(), r, why, false))
    {
        error = "SET statement_timeout: " + why;
        close();
        return false;
    }
    for (const std::string &statement : setup)
    {
        if (Exec::OK != exec(statement, {}, left(), r, why, false))
        {
            error = statement + ": " + why;
            close();
            return false;
        }
    }
    return true;
}

Exec PgConn::exec(const std::string &sql, const std::vector<std::string> &params, std::int64_t timeoutMs, Result &out,
    std::string &error, bool simple)
{
    m_sqlState.clear();
    m_deadline = false;
    const std::int64_t deadline = nowNs() + timeoutMs * 1'000'000LL;
    std::vector<const char *> values;
    for (const std::string &p : params)
    {
        values.push_back(p.c_str());
    }
    const int sent = simple ? PQsendQuery(m_pg, sql.c_str())
                            : PQsendQueryParams(m_pg, sql.c_str(), static_cast<int>(values.size()), nullptr,
                                  values.data(), nullptr, nullptr, 0);
    if (1 != sent)
    {
        error = PQerrorMessage(m_pg);
        return Exec::FAILED;
    }
    for (;;)
    {
        const int f = PQflush(m_pg);
        if (0 == f)
        {
            break;
        }
        if (f < 0 || !waitSocket(true, deadline))
        {
            error = f < 0 ? PQerrorMessage(m_pg) : "statement deadline passed while sending";
            m_deadline = f >= 0;
            close();
            return Exec::FAILED;
        }
    }
    Exec verdict = Exec::OK;
    out.reset();
    for (;;)
    {
        while (PQisBusy(m_pg))
        {
            if (!waitSocket(false, deadline))
            {
                error = "statement deadline (" + std::to_string(timeoutMs) + " ms) passed";
                m_deadline = true;
                close(); // the server's statement_timeout ends the statement on its side
                return Exec::FAILED;
            }
            if (1 != PQconsumeInput(m_pg))
            {
                error = PQerrorMessage(m_pg);
                close();
                return Exec::FAILED;
            }
        }
        PGresult *raw = PQgetResult(m_pg);
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
                m_sqlState = nullptr == state ? "" : state;
                verdict = "23505" == m_sqlState ? Exec::UNIQUE : Exec::FAILED;
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

}}
