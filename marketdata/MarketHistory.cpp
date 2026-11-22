#include "MarketHistory.h"

#include <algorithm>
#include <fstream>
#include <sstream>

namespace openexchange { namespace gateway { namespace marketdata {

using pg::Exec;
using pg::Result;

namespace {

#include "gateway/market_history_schema.inc"

// pg_advisory_xact_lock key of the schema step: the eight bytes "oemdschm" as a big-endian int64.
constexpr const char *SCHEMA_LOCK_KEY = "8026942189057828973";

// The columns the file creates, in table and ordinal order (information_schema names): a database whose relations
// carry anything else is foreign.
constexpr const char *EXPECTED_COLUMNS =
    "md_candles.market_id:integer,md_candles.interval_s:integer,md_candles.bucket_start_ms:bigint,"
    "md_candles.open:bigint,md_candles.high:bigint,md_candles.low:bigint,md_candles.close:bigint,"
    "md_candles.volume_base:numeric,md_candles.volume_quote:numeric,md_candles.trade_count:bigint,"
    "md_candles.last_trade_id:bigint,"
    "md_state.singleton:boolean,md_state.schema_version:integer,md_state.me_epoch:bigint,"
    "md_state.last_journal_seq:bigint,md_state.first_journal_seq:bigint,"
    "md_trades.market_id:integer,md_trades.trade_id:bigint,md_trades.price:bigint,md_trades.quantity:bigint,"
    "md_trades.taker_is_buy:boolean,md_trades.executed_at_ms:bigint,md_trades.journal_seq:bigint";

void rollback(PgConn &conn, std::int64_t timeoutMs)
{
    if (!conn.ok())
    {
        return;
    }
    Result r;
    std::string ignored;
    if (Exec::OK != conn.exec("ROLLBACK", {}, timeoutMs, r, ignored, false))
    {
        conn.close();
    }
}

std::int64_t rowsOf(const Result &r)
{
    if (!r)
    {
        return 0;
    }
    const char *n = PQcmdTuples(r.get());
    return nullptr == n || '\0' == *n ? 0 : std::stoll(n);
}

template<typename T, typename F>
std::string arrayOf(const std::vector<T> &items, F field)
{
    std::string out = "{";
    for (std::size_t i = 0; i < items.size(); ++i)
    {
        if (i > 0)
        {
            out += ',';
        }
        out += field(items[i]);
    }
    out += '}';
    return out;
}

std::string n64(std::int64_t v)
{
    return std::to_string(v);
}

bool readPassword(const std::string &path, std::string &password, std::string &error)
{
    std::ifstream in(path);
    if (!in)
    {
        error = "MD_PG_PASSWORD_FILE " + path + " cannot be read";
        return false;
    }
    std::stringstream ss;
    ss << in.rdbuf();
    password = ss.str();
    while (!password.empty() && ('\n' == password.back() || '\r' == password.back()))
    {
        password.pop_back();
    }
    return true;
}

CandleRow candleOf(PGresult *r, int row)
{
    CandleRow c;
    c.marketId = std::stoi(PQgetvalue(r, row, 0));
    c.intervalS = std::stoi(PQgetvalue(r, row, 1));
    c.bucketStartMs = std::stoll(PQgetvalue(r, row, 2));
    c.open = std::stoll(PQgetvalue(r, row, 3));
    c.high = std::stoll(PQgetvalue(r, row, 4));
    c.low = std::stoll(PQgetvalue(r, row, 5));
    c.close = std::stoll(PQgetvalue(r, row, 6));
    parseDecimal(PQgetvalue(r, row, 7), c.volumeBase);
    parseDecimal(PQgetvalue(r, row, 8), c.volumeQuote);
    c.tradeCount = std::stoll(PQgetvalue(r, row, 9));
    c.lastTradeId = std::stoll(PQgetvalue(r, row, 10));
    return c;
}

constexpr const char *CANDLE_COLUMNS = "market_id, interval_s, bucket_start_ms, open, high, low, close, "
    "volume_base::text, volume_quote::text, trade_count, last_trade_id";

}

const char *marketHistorySchemaSql() noexcept
{
    return MARKET_HISTORY_SCHEMA_SQL;
}

const char *marketHistorySchemaSha256() noexcept
{
    return MARKET_HISTORY_SCHEMA_SHA256;
}

const char *nameOf(StartVerdict v) noexcept
{
    switch (v)
    {
        case StartVerdict::READY: return "ready";
        case StartVerdict::LEGACY: return "legacy";
        case StartVerdict::LOCK_HELD: return "lock-held";
        case StartVerdict::FOREIGN: return "foreign";
        case StartVerdict::FAILED: break;
    }
    return "failed";
}

const char *nameOf(Commit c) noexcept
{
    switch (c)
    {
        case Commit::COMMITTED: return "committed";
        case Commit::ANOTHER_WRITER: return "another-writer";
        case Commit::FAILED: return "failed";
        case Commit::UNCERTAIN: break;
    }
    return "uncertain";
}

bool connectHistory(PgConn &conn, const HistoryOptions &o, std::string &error)
{
    std::string password;
    if (!readPassword(o.passwordFile, password, error))
    {
        return false;
    }
    return conn.connect(o.url, o.statementTimeoutMs, error,
        { "SET idle_in_transaction_session_timeout = " + std::to_string(std::max<std::int64_t>(o.statementTimeoutMs * 3,
              1000)),
            "SET application_name = 'oe-marketdata'", "SET client_min_messages = warning" },
        0,
        { { "user", o.user }, { "password", password }, { "keepalives", "1" }, { "keepalives_idle", "5" },
            { "keepalives_interval", "2" }, { "keepalives_count", "3" } });
}

bool readHistoryState(PgConn &conn, std::int64_t timeoutMs, MdState &state, std::string &error)
{
    Result r;
    // no row: a reset truncated it; the row is written again (an empty history)
    if (Exec::OK != conn.exec("INSERT INTO md_state (singleton, schema_version) VALUES (TRUE, $1) ON CONFLICT "
            "(singleton) DO NOTHING", { std::to_string(MARKET_HISTORY_SCHEMA_VERSION) }, timeoutMs, r, error, false))
    {
        return false;
    }
    if (Exec::OK != conn.exec("SELECT me_epoch, last_journal_seq, first_journal_seq, schema_version FROM md_state",
            {}, timeoutMs, r, error, false))
    {
        return false;
    }
    if (1 != PQntuples(r.get()))
    {
        error = "md_state holds " + std::to_string(PQntuples(r.get())) + " rows";
        return false;
    }
    PGresult *raw = r.get();
    state.epoch = PQgetisnull(raw, 0, 0) ? std::nullopt : std::optional<std::int64_t>(std::stoll(PQgetvalue(raw, 0, 0)));
    state.lastSeq = std::stoll(PQgetvalue(raw, 0, 1));
    state.firstSeq = PQgetisnull(raw, 0, 2) ? std::nullopt
                                            : std::optional<std::int64_t>(std::stoll(PQgetvalue(raw, 0, 2)));
    return true;
}

StartVerdict startHistory(PgConn &conn, std::int64_t timeoutMs, MdState &state, std::string &detail)
{
    Result r;
    std::string error;
    // 1. the legacy guard
    if (Exec::OK != conn.exec("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') OR "
            "to_regclass('trades') IS NOT NULL", {}, timeoutMs, r, error, false))
    {
        detail = "legacy check: " + error;
        return StartVerdict::FAILED;
    }
    if (0 == std::string(PQgetvalue(r.get(), 0, 0)).compare("t"))
    {
        detail = std::string(LEGACY_DATABASE_TEXT) + " (the timescaledb extension or a relation named trades): "
            "oe-marketdata writes only to its own database";
        return StartVerdict::LEGACY;
    }
    // 2. the writer lock (a session lock: held while this connection lives)
#ifndef MD_NC_NO_WRITER_LOCK
    if (Exec::OK != conn.exec(std::string("SELECT pg_try_advisory_lock(") + WRITER_LOCK_KEY + ")", {}, timeoutMs, r,
            error, false))
    {
        detail = "writer lock: " + error;
        return StartVerdict::FAILED;
    }
    if (0 != std::string(PQgetvalue(r.get(), 0, 0)).compare("t"))
    {
        detail = std::string(WRITER_LOCK_HELD_TEXT) + " (advisory lock " + WRITER_LOCK_KEY + " is held by another "
            "session: another oe-marketdata, or a reset in progress)";
        return StartVerdict::LOCK_HELD;
    }
#endif
    // 3. the schema
    auto failed = [&](const char *step) {
        detail = std::string(step) + ": " + error;
        rollback(conn, timeoutMs);
        return StartVerdict::FAILED;
    };
    if (Exec::OK != conn.exec("BEGIN", {}, timeoutMs, r, error, false))
    {
        return failed("BEGIN");
    }
    if (Exec::OK != conn.exec(std::string("SELECT pg_advisory_xact_lock(") + SCHEMA_LOCK_KEY + ")", {}, timeoutMs, r,
            error, false))
    {
        return failed("schema lock");
    }
    if (Exec::OK != conn.exec("SELECT (to_regclass('md_state') IS NOT NULL)::int + (to_regclass('md_trades') IS NOT "
            "NULL)::int + (to_regclass('md_candles') IS NOT NULL)::int", {}, timeoutMs, r, error, false))
    {
        return failed("relations");
    }
    const int present = std::stoi(PQgetvalue(r.get(), 0, 0));
    bool applied = false;
    if (0 == present)
    {
        if (Exec::OK != conn.exec(MARKET_HISTORY_SCHEMA_SQL, {}, timeoutMs, r, error, true))
        {
            const bool foreign = 0 == conn.lastSqlState().rfind("42", 0);
            detail = "sql/market_history_v1.sql: " + error;
            rollback(conn, timeoutMs);
            return foreign ? StartVerdict::FOREIGN : StartVerdict::FAILED;
        }
        applied = true;
    }
    else if (3 != present)
    {
        rollback(conn, timeoutMs);
        detail = "the database holds " + std::to_string(present) + " of the three market history relations (md_state, "
            "md_trades, md_candles): not a market history this binary wrote";
        return StartVerdict::FOREIGN;
    }
    if (Exec::OK != conn.exec("SELECT string_agg(table_name || '.' || column_name || ':' || data_type, ',' ORDER BY "
            "table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = current_schema() AND "
            "table_name IN ('md_state', 'md_trades', 'md_candles')", {}, timeoutMs, r, error, false))
    {
        return failed("columns");
    }
    const std::string columns = PQgetvalue(r.get(), 0, 0);
    if (columns != EXPECTED_COLUMNS)
    {
        rollback(conn, timeoutMs);
        detail = "the market history relations carry other columns than sql/market_history_v1.sql: " + columns;
        return StartVerdict::FOREIGN;
    }
    if (Exec::OK != conn.exec("SELECT schema_version FROM md_state", {}, timeoutMs, r, error, false))
    {
        return failed("md_state");
    }
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        if (std::to_string(MARKET_HISTORY_SCHEMA_VERSION) != PQgetvalue(r.get(), i, 0))
        {
            detail = std::string("md_state records market history schema version ") + PQgetvalue(r.get(), i, 0) +
                "; this binary carries version " + std::to_string(MARKET_HISTORY_SCHEMA_VERSION) + " only";
            rollback(conn, timeoutMs);
            return StartVerdict::FOREIGN;
        }
    }
    // 4. md_state
    if (!readHistoryState(conn, timeoutMs, state, error))
    {
        return failed("md_state");
    }
    if (Exec::OK != conn.exec("COMMIT", {}, timeoutMs, r, error, false))
    {
        return failed("COMMIT");
    }
    detail = std::string(applied ? "schema version 1 applied" : "schema version 1 present") + " (sha256 " +
        MARKET_HISTORY_SCHEMA_SHA256 + ")";
    return StartVerdict::READY;
}

Commit commitBatch(PgConn &conn, std::int64_t timeoutMs, const CommitBatch &b, std::int64_t arrivalMs,
    std::string &error, const std::function<void()> &beforeCommit)
{
    Result r;
    auto abort = [&](Commit verdict) {
        rollback(conn, timeoutMs);
        return verdict;
    };
    auto stepFailed = [&](Exec e, const char *step) {
        error = std::string(step) + ": " + error;
        if (Exec::UNIQUE == e)
        {
            error = std::string(ANOTHER_WRITER_TEXT) + " (a record of this batch is stored already: " + error + ")";
            return abort(Commit::ANOTHER_WRITER);
        }
        return abort(Commit::FAILED);
    };
    const std::string cursorSql = "UPDATE md_state SET last_journal_seq = $1, me_epoch = COALESCE(me_epoch, $2), "
        "first_journal_seq = COALESCE(first_journal_seq, $3) WHERE singleton AND last_journal_seq = $4 AND "
        "(me_epoch IS NULL OR me_epoch = $2)";
    const std::vector<std::string> cursorParams = { n64(b.toSeq), n64(b.epoch), n64(b.firstSeq), n64(b.fromSeq) };
    Exec e = Exec::OK;
#ifdef MD_NC_COMMIT_SPLIT
    // test build only: the cursor commits in a transaction of its own, before the rows
    if (Exec::OK != (e = conn.exec("BEGIN", {}, timeoutMs, r, error, false)) ||
        Exec::OK != (e = conn.exec(cursorSql, cursorParams, timeoutMs, r, error, false)))
    {
        return stepFailed(e, "cursor");
    }
    if (1 != rowsOf(r))
    {
        error = std::string(ANOTHER_WRITER_TEXT) + " (md_state.last_journal_seq is not " + n64(b.fromSeq) + ")";
        return abort(Commit::ANOTHER_WRITER);
    }
    if (Exec::OK != (e = conn.exec("COMMIT", {}, timeoutMs, r, error, false)))
    {
        return stepFailed(e, "COMMIT");
    }
    if (beforeCommit)
    {
        beforeCommit();
    }
#endif
    if (Exec::OK != (e = conn.exec("BEGIN", {}, timeoutMs, r, error, false)))
    {
        return stepFailed(e, "BEGIN");
    }
    if (!b.trades.empty())
    {
        const std::vector<TradeRow> &t = b.trades;
        e = conn.exec("INSERT INTO md_trades (market_id, trade_id, price, quantity, taker_is_buy, executed_at_ms, "
            "journal_seq) SELECT * FROM unnest($1::int[], $2::bigint[], $3::bigint[], $4::bigint[], $5::boolean[], "
            "$6::bigint[], $7::bigint[])",
            { arrayOf(t, [](const TradeRow &x) { return std::to_string(x.marketId); }),
                arrayOf(t, [](const TradeRow &x) { return n64(x.tradeId); }),
                arrayOf(t, [](const TradeRow &x) { return n64(x.price); }),
                arrayOf(t, [](const TradeRow &x) { return n64(x.quantity); }),
                arrayOf(t, [](const TradeRow &x) { return std::string(x.takerIsBuy ? "t" : "f"); }),
                arrayOf(t, [](const TradeRow &x) { return n64(x.executedAtMs); }),
                arrayOf(t, [](const TradeRow &x) { return n64(x.journalSeq); }) },
            timeoutMs, r, error, false);
        if (Exec::OK != e)
        {
            return stepFailed(e, "md_trades");
        }
        std::vector<CandleRow> c;
        for (const auto &[key, row] : aggregate(t, arrivalMs))
        {
            c.push_back(row);
        }
        e = conn.exec("INSERT INTO md_candles (market_id, interval_s, bucket_start_ms, open, high, low, close, "
            "volume_base, volume_quote, trade_count, last_trade_id) SELECT * FROM unnest($1::int[], $2::int[], "
            "$3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], $7::bigint[], $8::numeric[], $9::numeric[], "
            "$10::bigint[], $11::bigint[]) ON CONFLICT (market_id, interval_s, bucket_start_ms) DO UPDATE SET "
            "high = GREATEST(md_candles.high, EXCLUDED.high), low = LEAST(md_candles.low, EXCLUDED.low), "
            "close = EXCLUDED.close, volume_base = md_candles.volume_base + EXCLUDED.volume_base, "
            "volume_quote = md_candles.volume_quote + EXCLUDED.volume_quote, "
            "trade_count = md_candles.trade_count + EXCLUDED.trade_count, last_trade_id = EXCLUDED.last_trade_id",
            { arrayOf(c, [](const CandleRow &x) { return std::to_string(x.marketId); }),
                arrayOf(c, [](const CandleRow &x) { return std::to_string(x.intervalS); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.bucketStartMs); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.open); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.high); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.low); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.close); }),
                arrayOf(c, [](const CandleRow &x) { return decimal(x.volumeBase); }),
                arrayOf(c, [](const CandleRow &x) { return decimal(x.volumeQuote); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.tradeCount); }),
                arrayOf(c, [](const CandleRow &x) { return n64(x.lastTradeId); }) },
            timeoutMs, r, error, false);
        if (Exec::OK != e)
        {
            return stepFailed(e, "md_candles");
        }
    }
#ifndef MD_NC_COMMIT_SPLIT
    if (Exec::OK != (e = conn.exec(cursorSql, cursorParams, timeoutMs, r, error, false)))
    {
        return stepFailed(e, "cursor");
    }
    if (1 != rowsOf(r))
    {
        error = std::string(ANOTHER_WRITER_TEXT) + " (md_state.last_journal_seq is not " + n64(b.fromSeq) +
            " or md_state.me_epoch is not " + n64(b.epoch) + ")";
        return abort(Commit::ANOTHER_WRITER);
    }
    if (beforeCommit)
    {
        beforeCommit();
    }
#endif
    if (Exec::OK != conn.exec("COMMIT", {}, timeoutMs, r, error, false))
    {
        if (!conn.ok() || conn.lastSqlState().empty())
        {
            conn.close();
            error = "COMMIT: " + error;
            return Commit::UNCERTAIN; // sent; its answer is lost
        }
        error = "COMMIT: " + error;
        return abort(Commit::FAILED);
    }
    return Commit::COMMITTED;
}

bool readRecentTrades(PgConn &conn, std::int64_t timeoutMs, std::int32_t marketId, std::int64_t limit,
    std::vector<TradeRow> &out, std::string &error)
{
    Result r;
    if (Exec::OK != conn.exec("SELECT market_id, trade_id, price, quantity, taker_is_buy, executed_at_ms, journal_seq "
            "FROM md_trades WHERE market_id = $1 ORDER BY journal_seq DESC LIMIT $2",
            { std::to_string(marketId), n64(limit) }, timeoutMs, r, error, false))
    {
        return false;
    }
    out.clear();
    for (int i = PQntuples(r.get()) - 1; i >= 0; --i)
    {
        TradeRow t;
        t.marketId = std::stoi(PQgetvalue(r.get(), i, 0));
        t.tradeId = std::stoll(PQgetvalue(r.get(), i, 1));
        t.price = std::stoll(PQgetvalue(r.get(), i, 2));
        t.quantity = std::stoll(PQgetvalue(r.get(), i, 3));
        t.takerIsBuy = 't' == PQgetvalue(r.get(), i, 4)[0];
        t.executedAtMs = std::stoll(PQgetvalue(r.get(), i, 5));
        t.journalSeq = std::stoll(PQgetvalue(r.get(), i, 6));
        out.push_back(t);
    }
    return true;
}

bool readCandles(PgConn &conn, std::int64_t timeoutMs, std::int32_t marketId, std::int32_t intervalS, std::int64_t limit,
    std::vector<CandleRow> &out, std::string &error)
{
    Result r;
    if (Exec::OK != conn.exec(std::string("SELECT ") + CANDLE_COLUMNS + " FROM md_candles WHERE market_id = $1 AND "
            "interval_s = $2 ORDER BY bucket_start_ms DESC LIMIT $3",
            { std::to_string(marketId), std::to_string(intervalS), n64(limit) }, timeoutMs, r, error, false))
    {
        return false;
    }
    out.clear();
    for (int i = PQntuples(r.get()) - 1; i >= 0; --i)
    {
        out.push_back(candleOf(r.get(), i));
    }
    return true;
}

bool readCandlesSince(PgConn &conn, std::int64_t timeoutMs, std::int32_t marketId, std::int32_t intervalS,
    std::int64_t fromMs, std::vector<CandleRow> &out, std::string &error)
{
    Result r;
    if (Exec::OK != conn.exec(std::string("SELECT ") + CANDLE_COLUMNS + " FROM md_candles WHERE market_id = $1 AND "
            "interval_s = $2 AND bucket_start_ms >= $3 ORDER BY bucket_start_ms",
            { std::to_string(marketId), std::to_string(intervalS), n64(fromMs) }, timeoutMs, r, error, false))
    {
        return false;
    }
    out.clear();
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        out.push_back(candleOf(r.get(), i));
    }
    return true;
}

}}}
