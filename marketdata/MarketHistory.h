#ifndef GATEWAY_MARKETDATA_MARKET_HISTORY_H
#define GATEWAY_MARKETDATA_MARKET_HISTORY_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "gateway/common/PgConn.h"

#include "Candles.h"
#include "TradePath.h"

// MarketHistory - the PostgreSQL statements of oe-marketdata's market history (sql/market_history_v1.sql), over one
// PgConn owned by the writer thread (md-pg). Plain PostgreSQL, exact integers only.
//
// The start, on every new connection, in this order:
//   1. the legacy guard: a database with the timescaledb extension or a relation named `trades` is the legacy market
//      database: `this is the legacy market database`, exit 78, nothing written;
//   2. the writer lock: pg_try_advisory_lock(WRITER_LOCK_KEY), held by the connection for its life (a session lock). Held
//      by another session: `market history writer lock is held`, exit 75. The console's reset takes the same key with
//      pg_try_advisory_lock and refuses with `market history writer lock is held: reset refused` while this service
//      runs, so a running writer never meets a truncate under it;
//   3. the schema: none of md_state, md_trades, md_candles exists: the file is applied in one transaction under a
//      transaction-scoped advisory lock; all three exist with the file's columns: present; anything else (some of them,
//      other columns, another schema_version in md_state) is foreign: exit 78. md_state without its row (a reset
//      truncated it) gets the row again: an empty history; then, in the same transaction and on every start,
//      sql/market_history_retention_v1.sql (CREATE OR REPLACE of market_trades_prune_v1 and the grants of the role
//      oe_history_pruner): a database an older build wrote gets the function in place, nothing else changes. Its
//      failure is FOREIGN for a refusal of the database (the role absent and not creatable: exit 78), FAILED otherwise;
//   4. md_state is read.
// One transaction per batch (commitBatch): the trades, the candles they touch (an upsert by the merge rule of
// Candles.h) and the cursor move from the expected old value to the new one, with the epoch and the first record of an
// empty history. Zero rows on the cursor update rolls everything back: ANOTHER_WRITER (exit 75, `market history has
// another writer`). A trade id or journal sequence that is already stored is the same verdict: only a second writer
// (or a commit split) can have stored a record above this writer's cursor.
// Test-only control (never in a published build): MD_NC_COMMIT_SPLIT commits the cursor in a transaction of its own
// before the rows (a kill in between loses the rows).

namespace openexchange { namespace gateway { namespace marketdata {

// pg_try_advisory_lock key of the writer: the eight bytes "oemdwrtr" as a big-endian int64.
inline constexpr const char *WRITER_LOCK_KEY = "8026942189125923954";
inline constexpr const char *LEGACY_DATABASE_TEXT = "this is the legacy market database";
inline constexpr const char *WRITER_LOCK_HELD_TEXT = "market history writer lock is held";
inline constexpr const char *ANOTHER_WRITER_TEXT = "market history has another writer";
inline constexpr int MARKET_HISTORY_SCHEMA_VERSION = 1;
// The newest trades of every market the tape's retention delete never removes, however old
// (sql/market_history_retention_v1.sql, keep_per_market): MD_RECENT_TRADES (the cold start's read) may not exceed it.
inline constexpr std::int64_t MARKET_TAPE_KEEP_PER_MARKET = 1000;
const char *marketHistorySchemaSql() noexcept;
const char *marketHistorySchemaSha256() noexcept;

struct HistoryOptions
{
    std::string url;
    std::string user;
    std::string passwordFile; // read at every connect (a rotated password needs no restart)
    std::int64_t statementTimeoutMs = 3000;
};

// Connects with the writer's session settings and TCP keepalives. False with `error`.
bool connectHistory(PgConn &conn, const HistoryOptions &options, std::string &error);

enum class StartVerdict : std::uint8_t
{
    READY,        // the lock is held, the schema present, `state` read
    LEGACY,       // exit 78
    LOCK_HELD,    // exit 75
    FOREIGN,      // exit 78
    FAILED        // the connection or a statement failed: retry
};
const char *nameOf(StartVerdict v) noexcept;

// Steps 1 to 4 on a connected `conn`.
StartVerdict startHistory(PgConn &conn, std::int64_t timeoutMs, MdState &state, std::string &detail);

// md_state as it is now (no row: an empty history; the row is written again).
bool readHistoryState(PgConn &conn, std::int64_t timeoutMs, MdState &state, std::string &error);

enum class Commit : std::uint8_t
{
    COMMITTED,
    ANOTHER_WRITER, // rolled back: the cursor was not the expected one, or a record is stored already
    FAILED,         // rolled back (or the connection is gone before COMMIT): nothing committed
    UNCERTAIN       // the COMMIT was sent and its answer lost: read the cursor to learn the outcome
};
const char *nameOf(Commit c) noexcept;

// `beforeCommit` (test builds) runs after every statement of the batch and before COMMIT.
Commit commitBatch(PgConn &conn, std::int64_t timeoutMs, const CommitBatch &batch, std::int64_t arrivalMs,
    std::string &error, const std::function<void()> &beforeCommit = {});

// ---- reads (the start of the live state, and the market data routes)
bool readRecentTrades(PgConn &conn, std::int64_t timeoutMs, std::int32_t marketId, std::int64_t limit,
    std::vector<TradeRow> &out, std::string &error);
// The newest `limit` buckets of one market and interval, oldest first.
bool readCandles(PgConn &conn, std::int64_t timeoutMs, std::int32_t marketId, std::int32_t intervalS, std::int64_t limit,
    std::vector<CandleRow> &out, std::string &error);
// Every bucket of one market and interval starting at or after `fromMs`, oldest first.
bool readCandlesSince(PgConn &conn, std::int64_t timeoutMs, std::int32_t marketId, std::int32_t intervalS,
    std::int64_t fromMs, std::vector<CandleRow> &out, std::string &error);

// The live path's start (TradePath.h MarketSeed) for `markets`: the newest `recentTrades` trades and the 1m candles
// that are the newest `candleHistory` buckets or start at or after `nowMs` - 24 h - 1 min, each oldest first.
bool readSeed(PgConn &conn, std::int64_t timeoutMs, const std::vector<int> &markets, std::int64_t recentTrades,
    std::int64_t candleHistory, std::int64_t nowMs, MarketSeed &out, std::string &error);

}}}

#endif
