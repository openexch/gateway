#ifndef GATEWAY_COMMON_PG_HISTORY_STORE_H
#define GATEWAY_COMMON_PG_HISTORY_STORE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "gateway/common/History.h"
#include "gateway/common/PgConn.h"

// PgHistoryStore - HistoryStore over libpq (PgConn), on connections of its own: one per history worker
// (GATEWAY_PG_HISTORY_POOL), never one of the users store's, so a history storm cannot take a connection from login
// or a token lookup. Each connection is read-only (default_transaction_read_only; the privilege that enforces it is
// the role's grant, see the DDL) and has the server statement_timeout GATEWAY_PG_STATEMENT_TIMEOUT_MS. Each call has
// one deadline for the whole read (the caller's, stamped when it was queued): connect, heartbeat and rows each get
// only what is left of it, so a read never takes longer than that deadline. After a failed connect no worker tries
// again for GATEWAY_PG_RETRY_MS (those calls are UNREACHABLE at once), unless the read's own deadline cut the connect
// short (it was given less than half of the statement timeout: TIMEOUT, no evidence the server is down). The WARN
// line of a failure is written at most once per that interval with the count held back; the cause goes to the log,
// never to the client. The relations are not checked at connect: a missing one is a statement error (ABSENT) on the
// call that meets it, so the projection can be created while the gateway runs.
//
// A connection the server dropped while it sat idle (a restart, an idle kill) is seen only by the next statement:
// when a call on a reused connection finds it lost (and not by the deadline), the read is run once more on a fresh
// connection inside the same deadline. Reads are idempotent and nothing of the failed one was answered.
//
// A page reads the heartbeat first, then the rows: the rows are at least as new as the age they are answered with, and
// every row of an event at or below the heartbeat's applied_event_seq is on the page (the projection commits that
// cursor in the same transaction as the rows it covers). The heartbeat is checked (History.h) before any row is read.
// A trail page with no row of the caller is NOT_FOUND or LAGGING by applied_order_id, read in that same heartbeat.
//
// Test-only controls (never in a published build): GW_NC_NO_EPOCH_CHECK skips the epoch check (history of another
// genesis is served); GW_NC_ROWS_BEFORE_HEARTBEAT reads the rows first and the heartbeat after them.

namespace openexchange { namespace gateway {

struct PgHistorySettings
{
    std::string url;
    int workers = 4;
    std::int64_t statementTimeoutMs = 2000;
    std::int64_t retryMs = 1000;
};

class PgHistoryStore final : public HistoryStore
{
public:
    explicit PgHistoryStore(PgHistorySettings settings);
    ~PgHistoryStore() override;

    HistoryAnswer page(int worker, const history::PageRequest &request, HistoryPage &out,
        std::int64_t deadlineNs, const HistoryExpect &expect) override;
    HistoryAnswer order(int worker, std::int64_t userId, std::int64_t orderId, std::string &json,
        HistoryAsOf &asOf, std::int64_t deadlineNs, const HistoryExpect &expect) override;
    HistoryAnswer probe(int worker, HistoryAsOf &out, std::int64_t deadlineNs, const HistoryExpect &expect) override;
    std::string state() const override;

    int connected() const noexcept { return m_connected.load(std::memory_order_relaxed); }

private:
    PgConn *ready(int worker, std::int64_t deadlineNs, HistoryAnswer &why, bool &reused);
    // `run` on the worker's connection, once more on a fresh one when a reused connection turns out lost
    HistoryAnswer attempt(int worker, std::int64_t deadlineNs, const std::function<HistoryAnswer(PgConn &)> &run);
    HistoryAnswer failed(PgConn &c, const char *what, const std::string &detail);
    HistoryAnswer unavailable(HistoryCause cause, const char *what, const std::string &detail);
    // what is left of the deadline in whole milliseconds; false (TIMEOUT in `why`) when nothing is
    bool left(std::int64_t deadlineNs, const char *what, std::int64_t &ms, HistoryAnswer &why);
    // the heartbeat read and checked (no row, epoch, maximum age), in that order
    bool heartbeat(PgConn &c, std::int64_t deadlineNs, const HistoryExpect &expect, HistoryAsOf &out,
        HistoryAnswer &why);
    HistoryAnswer pageOn(PgConn &c, const history::PageRequest &request, HistoryPage &out, std::int64_t deadlineNs,
        const HistoryExpect &expect);
    HistoryAnswer orderOn(PgConn &c, std::int64_t userId, std::int64_t orderId, std::string &json,
        HistoryAsOf &asOf, std::int64_t deadlineNs, const HistoryExpect &expect);

    PgHistorySettings m_settings;
    std::vector<std::unique_ptr<PgConn>> m_conns;
    std::atomic<int> m_connected{ 0 };
    std::atomic<std::int64_t> m_retryAtNs{ 0 };
    pg::WarnLimiter m_warns;
    mutable std::mutex m_stateMutex;
    std::string m_state = "not connected";
};

}}

#endif
