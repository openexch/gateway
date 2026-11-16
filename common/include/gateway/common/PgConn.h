#ifndef GATEWAY_COMMON_PG_CONN_H
#define GATEWAY_COMMON_PG_CONN_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <libpq-fe.h>

// PgConn - the one libpq connection the gateway's stores share (the users store, PgUserStore, and the history read
// layer, PgHistoryStore): non-blocking libpq with a client-side deadline on every connect and every statement, so a
// dead or partitioned server costs the calling worker at most that long. The server's statement_timeout is set to the
// same value at connect. A deadline passed or a lost connection closes the connection; the next call reconnects.
// Each worker of a store owns one PgConn: a PgConn is never shared between threads.

namespace openexchange { namespace gateway {

namespace pg {

std::int64_t nowNs();

struct ResultDeleter
{
    void operator()(PGresult *r) const noexcept { PQclear(r); }
};
using Result = std::unique_ptr<PGresult, ResultDeleter>;

enum class Exec : std::uint8_t
{
    OK,
    UNIQUE, // SQLSTATE 23505
    FAILED  // any other error, a lost connection or the deadline
};

// A WARN line at most once per interval, with the count of lines held back since the last one (lock-free; the
// stores' failure log).
class WarnLimiter
{
public:
    // True when this line is written now; `held` is then the count of lines held back before it.
    bool take(std::int64_t intervalNs, std::uint64_t &held);

private:
    std::atomic<std::int64_t> m_lastNs{ 0 };
    std::atomic<std::uint64_t> m_held{ 0 };
};

}

class PgConn
{
public:
    ~PgConn() { close(); }

    void close();
    bool ok() const { return nullptr != m_pg && CONNECTION_OK == PQstatus(m_pg); }

    // Connects, switches to non-blocking, sets statement_timeout to `timeoutMs`, then runs each of `setup` (fixed
    // statements, never user input), all inside `budgetMs` (0: `timeoutMs`; a caller with a deadline of its own passes
    // what is left of it). False with `error` set on any failure (the connection is closed).
    bool connect(const std::string &url, std::int64_t timeoutMs, std::string &error,
        const std::vector<std::string> &setup = {}, std::int64_t budgetMs = 0);

    // One statement with text parameters (or, `simple`, a script of statements without parameters). The last result
    // is kept in `out`; any error result makes the call fail with its message. After a failure lastSqlState() holds
    // the server's SQLSTATE ("" when the failure was not the server's answer) and deadlinePassed() whether the client
    // deadline ended it.
    pg::Exec exec(const std::string &sql, const std::vector<std::string> &params, std::int64_t timeoutMs,
        pg::Result &out, std::string &error, bool simple = false);

    const std::string &lastSqlState() const noexcept { return m_sqlState; }
    bool deadlinePassed() const noexcept { return m_deadline; }

    bool counted = false; // the owning store counts this connection as connected

private:
    bool waitSocket(bool forWrite, std::int64_t deadlineNs);

    PGconn *m_pg = nullptr;
    std::string m_sqlState;
    bool m_deadline = false;
};

}}

#endif
