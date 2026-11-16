#ifndef GATEWAY_COMMON_PG_USER_STORE_H
#define GATEWAY_COMMON_PG_USER_STORE_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "gateway/common/UserStore.h"

// PgUserStore - UserStore over libpq: one connection per store worker, opened on first use and re-opened after any
// connection failure. Every connect and every statement has a client-side deadline of GATEWAY_PG_STATEMENT_TIMEOUT_MS
// (the server's statement_timeout is set to the same value), so a dead or partitioned server costs a worker at most
// that long; a deadline passed is UNAVAILABLE and the connection is dropped. After a failed connect no worker tries
// again for GATEWAY_PG_RETRY_MS: calls in that window are UNAVAILABLE at once (a cached token is then served stale
// without waiting for a doomed connect), and the WARN line of a failure is written at most once per window, with the
// count of lines held back.
//
// Schema. The migrations embedded at build time (db/migration/V<n>__<name>.sql, each with its sha256) are recorded in
// gateway_schema_migrations. With `migrate` (oe-gateway) a connection that has not seen the schema verified applies
// every missing migration in order, each in its own transaction, under a session advisory lock (N gateways may start
// at once); without it (oe-stream) it only verifies. A recorded migration whose sha256 differs from the embedded one,
// or a recorded version this build does not know, is a schema mismatch: every call is UNAVAILABLE, logged CRITICAL
// once (FrontLog), and /health names it. Nothing is ever applied over a mismatch.

namespace openexchange { namespace gateway {

struct Migration
{
    int version;
    const char *name;
    const char *sql;
    const char *sha256;
};
// The embedded migrations, ascending (generated from db/migration at configure time).
const std::vector<Migration> &embeddedMigrations();

struct PgSettings
{
    std::string url;
    int workers = 2;
    std::int64_t statementTimeoutMs = 2000;
    bool migrate = true;
    std::int64_t retryMs = 1000; // no reconnect for this long after a failed connect; also the WARN interval
};

class PgUserStore final : public UserStore
{
public:
    explicit PgUserStore(PgSettings settings, std::vector<Migration> migrations = embeddedMigrations());
    ~PgUserStore() override;

    StoreStatus reserveUserId(int worker, const std::string &username, std::int64_t &userId) override;
    StoreStatus insertUser(int worker, std::int64_t userId, const std::string &username, const std::string &passwordHash,
        const std::string &tokenHash) override;
    StoreStatus findByUsername(int worker, const std::string &username, StoredUser &out) override;
    StoreStatus findByToken(int worker, const std::string &tokenHash, StoredUser &out) override;
    StoreStatus setToken(int worker, std::int64_t userId, const std::string &tokenHash) override;
    StoreStatus clearToken(int worker, std::int64_t userId, const std::string &tokenHash) override;
    std::string state() const override;

    int connected() const noexcept { return m_connected.load(std::memory_order_relaxed); }
    std::uint64_t failures() const noexcept { return m_failures.load(std::memory_order_relaxed); }
    std::uint64_t connectAttempts() const noexcept { return m_connectAttempts.load(std::memory_order_relaxed); }

    class Conn;

private:
    Conn *ready(int worker);
    void note(const char *what, const std::string &detail);

    PgSettings m_settings;
    std::vector<Migration> m_migrations;
    std::vector<std::unique_ptr<Conn>> m_conns;
    std::atomic<bool> m_schemaVerified{ false };
    std::atomic<bool> m_mismatch{ false };
    std::atomic<bool> m_degraded{ false };
    std::atomic<int> m_connected{ 0 };
    std::atomic<std::uint64_t> m_failures{ 0 };
    std::atomic<std::uint64_t> m_connectAttempts{ 0 };
    std::atomic<std::int64_t> m_retryAtNs{ 0 };      // no connect before this (steady clock)
    std::atomic<std::int64_t> m_lastWarnNs{ 0 };
    std::atomic<std::uint64_t> m_warnsHeld{ 0 };
    mutable std::mutex m_stateMutex;
    std::string m_state = "not connected";
};

}}

#endif
