#include "gateway/common/PgHistoryStore.h"

#include "gateway/common/FrontLog.h"

namespace openexchange { namespace gateway {

using pg::Exec;
using pg::nowNs;
using pg::Result;

namespace {

history::Row rowOf(PGresult *r, int i)
{
    history::Row row;
    const int n = PQnfields(r);
    row.reserve(static_cast<std::size_t>(n));
    for (int f = 0; f < n; ++f)
    {
        row.push_back(PQgetisnull(r, i, f) ? std::nullopt : std::optional<std::string>(PQgetvalue(r, i, f)));
    }
    return row;
}

}

PgHistoryStore::PgHistoryStore(PgHistorySettings settings) : m_settings(std::move(settings))
{
    for (int i = 0; i < m_settings.workers; ++i)
    {
        m_conns.push_back(std::make_unique<PgConn>());
    }
}

PgHistoryStore::~PgHistoryStore() = default;

std::string PgHistoryStore::state() const
{
    std::lock_guard<std::mutex> lock(m_stateMutex);
    return m_state;
}

HistoryAnswer PgHistoryStore::unavailable(HistoryCause cause, const char *what, const std::string &detail)
{
    {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        m_state = std::string(nameOf(cause)) + ": " + what + ": " + detail;
    }
    std::uint64_t held = 0;
    if (m_warns.take(m_settings.retryMs * 1'000'000, held))
    {
        if (0 == held)
        {
            frontLog(FrontLevel::WARN, "%s: %s: %s: %s", m_settings.label.c_str(), nameOf(cause), what, detail.c_str());
        }
        else
        {
            frontLog(FrontLevel::WARN, "%s: %s: %s: %s (%llu similar lines held back)", m_settings.label.c_str(),
                nameOf(cause), what, detail.c_str(), static_cast<unsigned long long>(held));
        }
    }
    return HistoryAnswer{ HistoryStatus::UNAVAILABLE, cause };
}

// A failed statement: its cause from the SQLSTATE (or the lost connection, or the deadline).
HistoryAnswer PgHistoryStore::failed(PgConn &c, const char *what, const std::string &detail)
{
    const std::string &state = c.lastSqlState();
    HistoryCause cause = HistoryCause::ERROR;
    if ("42P01" == state || "42703" == state)
    {
        cause = HistoryCause::ABSENT; // undefined_table, undefined_column
    }
    else if ("57014" == state || c.deadlinePassed())
    {
        cause = HistoryCause::TIMEOUT; // query_canceled (statement_timeout), or the client deadline
    }
    else if (!c.ok() || 0 == state.compare(0, 2, "08") || "57P01" == state)
    {
        cause = HistoryCause::UNREACHABLE; // the connection is gone
    }
    return unavailable(cause, what, detail);
}

PgConn *PgHistoryStore::ready(int worker, std::int64_t deadlineNs, HistoryAnswer &why, bool &reused)
{
    reused = false;
    if (worker < 0 || worker >= static_cast<int>(m_conns.size()))
    {
        why = unavailable(HistoryCause::ERROR, "worker", "index " + std::to_string(worker) + " has no connection");
        return nullptr;
    }
    PgConn &c = *m_conns[static_cast<std::size_t>(worker)];
    if (c.ok())
    {
        reused = true;
        return &c;
    }
    if (c.counted)
    {
        c.counted = false;
        m_connected.fetch_sub(1, std::memory_order_relaxed);
    }
    if (nowNs() < m_retryAtNs.load(std::memory_order_acquire))
    {
        why = HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::UNREACHABLE }; // fail fast until the retry time
        return nullptr;
    }
    std::int64_t budgetMs = 0;
    if (!left(deadlineNs, "connect", budgetMs, why))
    {
        return nullptr;
    }
    std::string error;
    if (!c.connect(m_settings.url, m_settings.statementTimeoutMs, error, { "SET default_transaction_read_only = on" },
            budgetMs))
    {
        if (c.deadlinePassed() && 2 * budgetMs < m_settings.statementTimeoutMs)
        {
            // the read's own deadline (most of it spent in the queue) cut the connect short: no evidence that the
            // server is down, so no retry window for the other workers
            why = unavailable(HistoryCause::TIMEOUT, "connect", error);
            return nullptr;
        }
        m_retryAtNs.store(nowNs() + m_settings.retryMs * 1'000'000, std::memory_order_release);
        why = unavailable(HistoryCause::UNREACHABLE, "connect failed", error);
        return nullptr;
    }
    c.counted = true;
    m_connected.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        m_state = "connected";
    }
    return &c;
}

HistoryAnswer PgHistoryStore::attempt(int worker, std::int64_t deadlineNs,
    const std::function<HistoryAnswer(PgConn &)> &run)
{
    HistoryAnswer why;
    bool reused = false;
    PgConn *c = ready(worker, deadlineNs, why, reused);
    if (nullptr == c)
    {
        return why;
    }
    HistoryAnswer a = run(*c);
    // a reused connection the server dropped while it sat idle: the read once more on a fresh connection, inside the
    // same deadline (never for a deadline that passed, never twice)
    if (reused && HistoryStatus::UNAVAILABLE == a.status && HistoryCause::UNREACHABLE == a.cause && !c->ok() &&
        !c->deadlinePassed())
    {
        c = ready(worker, deadlineNs, why, reused);
        if (nullptr == c)
        {
            return why;
        }
        a = run(*c);
    }
    return a;
}

bool PgHistoryStore::left(std::int64_t deadlineNs, const char *what, std::int64_t &ms, HistoryAnswer &why)
{
    ms = (deadlineNs - nowNs()) / 1'000'000;
    if (ms > 0)
    {
        return true;
    }
    why = unavailable(HistoryCause::TIMEOUT, what, "the read's deadline passed before this step");
    return false;
}

bool PgHistoryStore::heartbeat(PgConn &c, std::int64_t deadlineNs, const HistoryExpect &expect, HistoryAsOf &out,
    HistoryAnswer &why)
{
    std::int64_t ms = 0;
    if (!left(deadlineNs, "heartbeat", ms, why))
    {
        return false;
    }
    Result r;
    std::string error;
    if (Exec::OK != c.exec(history::heartbeatStatement(), {}, ms, r, error))
    {
        why = failed(c, "heartbeat", error);
        return false;
    }
    if (1 != PQntuples(r.get()) || PQgetisnull(r.get(), 0, 0))
    {
        why = unavailable(HistoryCause::NO_HEARTBEAT, "heartbeat",
            "gateway_history_heartbeat_v1 holds no row: the projection has not reported");
        return false;
    }
    for (int col = 1; col <= 4; ++col)
    {
        if (PQgetisnull(r.get(), 0, col))
        {
            static const char *const names[] = { "", "applied_order_id", "applied_event_seq", "epoch", "epoch" };
            why = unavailable(HistoryCause::BAD_ROW, "heartbeat", std::string("column ") + names[col] + " is NULL");
            return false;
        }
    }
    out.ageMs = std::atoll(PQgetvalue(r.get(), 0, 0));
    out.appliedOrderId = std::atoll(PQgetvalue(r.get(), 0, 1));
    out.eventSeq = std::atoll(PQgetvalue(r.get(), 0, 2));
    out.epoch = std::atoll(PQgetvalue(r.get(), 0, 3));
    const std::int64_t highest = std::atoll(PQgetvalue(r.get(), 0, 4));
#ifndef GW_NC_NO_EPOCH_CHECK
    // history of another genesis is never served: the expected genesis comes from the cluster (the gateway's last
    // CoreConfigReport), never from the data; a gateway that has read no report cannot know, the same answer
    if (0 == expect.genesisId || out.epoch != expect.genesisId || highest != out.epoch)
    {
        why = unavailable(HistoryCause::EPOCH, "heartbeat", "the projection's epoch is " + std::to_string(out.epoch) +
            (highest != out.epoch ? " and " + std::to_string(highest) : std::string()) + ", the core's genesisId " +
            (0 == expect.genesisId ? std::string("not known yet (no CoreConfigReport read)")
                                   : std::to_string(expect.genesisId)));
        return false;
    }
#else
    (void)highest; // test build only: no epoch check
#endif
    if (expect.maxAgeMs > 0 && out.ageMs > expect.maxAgeMs)
    {
        // normal under a projector outage with refusal set: the cause is in the metrics, no WARN line per read
        why = HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::STALE };
        return false;
    }
    return true;
}

HistoryAnswer PgHistoryStore::page(int worker, const history::PageRequest &request, HistoryPage &out,
    std::int64_t deadlineNs, const HistoryExpect &expect)
{
    return attempt(worker, deadlineNs, [&](PgConn &c) { return pageOn(c, request, out, deadlineNs, expect); });
}

HistoryAnswer PgHistoryStore::probe(int worker, HistoryAsOf &out, std::int64_t deadlineNs, const HistoryExpect &expect)
{
    return attempt(worker, deadlineNs, [&](PgConn &c) {
        HistoryAnswer why;
        if (!heartbeat(c, deadlineNs, expect, out, why))
        {
            return why;
        }
        return HistoryAnswer{ HistoryStatus::OK, HistoryCause::NONE };
    });
}

HistoryAnswer PgHistoryStore::pageOn(PgConn &c, const history::PageRequest &request, HistoryPage &out,
    std::int64_t deadlineNs, const HistoryExpect &expect)
{
    HistoryAnswer why;
    HistoryAsOf asOf;
#ifndef GW_NC_ROWS_BEFORE_HEARTBEAT
    if (!heartbeat(c, deadlineNs, expect, asOf, why))
    {
        return why;
    }
#endif
    const char *what = history::Kind::ORDERS == request.kind ? "orders page"
        : history::Kind::FILLS == request.kind                ? "fills page"
                                                              : "trail page";
    std::int64_t ms = 0;
    if (!left(deadlineNs, what, ms, why))
    {
        return why;
    }
    const history::Statement s = history::pageStatement(request);
    Result r;
    std::string error;
    if (Exec::OK != c.exec(s.sql, s.params, ms, r, error))
    {
        return failed(c, what, error);
    }
#ifdef GW_NC_ROWS_BEFORE_HEARTBEAT
    if (!heartbeat(c, deadlineNs, expect, asOf, why)) // test build only: the heartbeat after the rows
    {
        return why;
    }
#endif
    out.ageMs = asOf.ageMs;
    out.eventSeq = asOf.eventSeq;
    const int rows = PQntuples(r.get());
    const std::size_t limit = static_cast<std::size_t>(request.limit);
    history::Key last;
    out.items.clear();
    for (int i = 0; i < rows && out.items.size() < limit; ++i)
    {
        std::string json;
        std::string problem;
        const history::Row row = rowOf(r.get(), i);
        const bool ok = history::Kind::ORDERS == request.kind ? history::orderJson(row, json, last, problem)
            : history::Kind::FILLS == request.kind            ? history::fillJson(row, json, last, problem)
                                                              : history::trailJson(row, json, last, problem);
        if (!ok)
        {
            return unavailable(HistoryCause::BAD_ROW, what, problem);
        }
        out.items.push_back(std::move(json));
    }
    out.nextCursor.reset();
    if (static_cast<std::size_t>(rows) > limit)
    {
        out.nextCursor = history::nextCursor(request, last, out.items.size());
    }
    if (history::Kind::TRAIL == request.kind && out.items.empty())
    {
        // no row of the caller on this page: a later page of the caller's own order is an empty page; no row of the
        // caller for this order at all is the rule of GET /orders/{orderId} (the heartbeat was read first)
        bool exists = false;
        if (request.after)
        {
            if (!left(deadlineNs, "trail lookup", ms, why))
            {
                return why;
            }
            const history::Statement e = history::trailExistsStatement(request.userId, request.filter.orderId);
            Result x;
            if (Exec::OK != c.exec(e.sql, e.params, ms, x, error))
            {
                return failed(c, "trail lookup", error);
            }
            exists = PQntuples(x.get()) > 0;
        }
        if (!exists)
        {
            {
                std::lock_guard<std::mutex> lock(m_stateMutex);
                m_state = "ready";
            }
            out.nextCursor.reset();
            return HistoryAnswer{ request.filter.orderId > asOf.appliedOrderId ? HistoryStatus::UNAVAILABLE
                                                                               : HistoryStatus::NOT_FOUND,
                request.filter.orderId > asOf.appliedOrderId ? HistoryCause::LAGGING : HistoryCause::NONE };
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        m_state = "ready";
    }
    return HistoryAnswer{ HistoryStatus::OK, HistoryCause::NONE };
}

HistoryAnswer PgHistoryStore::movements(int worker, const history::PageRequest &request, HistoryPage &out,
    std::int64_t deadlineNs)
{
    return attempt(worker, deadlineNs, [&](PgConn &c) { return movementsOn(c, request, out, deadlineNs); });
}

HistoryAnswer PgHistoryStore::movementsOn(PgConn &c, const history::PageRequest &request, HistoryPage &out,
    std::int64_t deadlineNs)
{
    HistoryAnswer why;
    std::int64_t ms = 0;
    if (history::Kind::MOVEMENTS != request.kind)
    {
        return unavailable(HistoryCause::ERROR, "movements page", "asked with another kind of page");
    }
    if (!left(deadlineNs, "movements page", ms, why))
    {
        return why;
    }
    const history::Statement s = history::pageStatement(request);
    Result r;
    std::string error;
    if (Exec::OK != c.exec(s.sql, s.params, ms, r, error))
    {
        return failed(c, "movements page", error);
    }
    out.ageMs = 0;
    out.eventSeq = 0;
    const int rows = PQntuples(r.get());
    const std::size_t limit = static_cast<std::size_t>(request.limit);
    history::Key last;
    out.items.clear();
    for (int i = 0; i < rows && out.items.size() < limit; ++i)
    {
        std::string json;
        std::string problem;
        if (!history::movementJson(rowOf(r.get(), i), request.assetCodes, json, last, problem))
        {
            return unavailable(HistoryCause::BAD_ROW, "movements page", problem);
        }
        out.items.push_back(std::move(json));
    }
    out.nextCursor.reset();
    if (static_cast<std::size_t>(rows) > limit)
    {
        out.nextCursor = history::nextCursor(request, last, out.items.size());
    }
    {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        m_state = "ready";
    }
    return HistoryAnswer{ HistoryStatus::OK, HistoryCause::NONE };
}

HistoryAnswer PgHistoryStore::order(int worker, std::int64_t userId, std::int64_t orderId, std::string &json,
    HistoryAsOf &asOf, std::int64_t deadlineNs, const HistoryExpect &expect)
{
    return attempt(worker, deadlineNs,
        [&](PgConn &c) { return orderOn(c, userId, orderId, json, asOf, deadlineNs, expect); });
}

HistoryAnswer PgHistoryStore::orderOn(PgConn &c, std::int64_t userId, std::int64_t orderId, std::string &json,
    HistoryAsOf &asOf, std::int64_t deadlineNs, const HistoryExpect &expect)
{
    HistoryAnswer why;
    asOf = HistoryAsOf{};
    // a projection that never reported, or one of another genesis, cannot say "no such order": that is 503, never 404
    if (!heartbeat(c, deadlineNs, expect, asOf, why))
    {
        return why;
    }
    const std::int64_t applied = asOf.appliedOrderId;
    std::int64_t ms = 0;
    if (!left(deadlineNs, "order lookup", ms, why))
    {
        return why;
    }
    const history::Statement s = history::orderStatement(userId, orderId);
    Result r;
    std::string error;
    if (Exec::OK != c.exec(s.sql, s.params, ms, r, error))
    {
        return failed(c, "order lookup", error);
    }
    if (0 == PQntuples(r.get()))
    {
        // the heartbeat was read first: every order at or below `applied` had its row when the lookup ran. Above it
        // the order may exist and not be projected yet (it left the core before the projection reached it): not 404.
        // Normal operation, so no WARN line; the cause is in the metrics.
        if (orderId > applied)
        {
            return HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::LAGGING };
        }
        return HistoryAnswer{ HistoryStatus::NOT_FOUND, HistoryCause::NONE };
    }
    history::Key key;
    std::string problem;
    if (!history::orderJson(rowOf(r.get(), 0), json, key, problem))
    {
        return unavailable(HistoryCause::BAD_ROW, "order lookup", problem);
    }
    return HistoryAnswer{ HistoryStatus::OK, HistoryCause::NONE };
}

}}
