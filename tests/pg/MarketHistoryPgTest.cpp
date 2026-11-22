// oe-marketdata's market history against a real PostgreSQL (tests/pg/pg_it.py runs it against a throwaway container):
//   md_history_start         a fresh database gets the schema and an empty md_state; a second writer is refused with the
//                            lock's text while the first holds it, and gets it once the first is gone; the legacy
//                            database (a relation named trades) and a foreign one (some of the relations, other columns)
//                            are refused; a reset (TRUNCATE of the md_* relations) starts an empty history again
//   md_history_exactly_once  a journal set committed in batches with the writer killed between its
//                            statements and COMMIT, at a lost COMMIT answer and at a dead connection, twice each: md_trades
//                            equals the set by trade id, md_candles equals a recomputation, the cursor is the last record
//   md_history_another_writer  a cursor that is not the expected one, an epoch that is not the stored one, a record
//                            stored already: rolled back with `market history has another writer`
//   md_candles_batch_splits  one trade set in batches of 1, of 7, and once after a truncate and replay: equal
//                            md_candles rows and equal tickers at one fixed "as of" time; the 128-bit quote volume exact
//   md_writer_outage         the writer thread (md-pg) with the database frozen twice: history_writable clears, nothing
//                            pending is dropped, every record commits once after each return (recoveries >= 2)
// The control builds (tests/CMakeLists.txt) switch one rule off each and must fail with the named text.
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "Check.h"
#include "PgTestSupport.h"
#include "Candles.h"
#include "HistoryWriter.h"
#include "MarketHistory.h"
#include "TradePath.h"

using namespace openexchange::gateway;
using namespace openexchange::gateway::marketdata;

namespace {

constexpr std::int64_t T = 3000;
constexpr std::int64_t EPOCH = 77;

void sleepMs(std::int64_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

std::string passwordFile()
{
    const std::string url = gwtest::env("GW_TEST_PG_URL"); // postgresql://gw:<password>@host:port/db
    const std::size_t colon = url.find(':', std::string("postgresql://").size());
    const std::size_t at = url.find('@');
    const std::string path = "/tmp/md-pg-test-password-" + std::to_string(::getpid());
    std::ofstream(path) << url.substr(colon + 1, at - colon - 1) << "\n";
    return path;
}

HistoryOptions options(const std::string &url)
{
    static const std::string pw = passwordFile();
    HistoryOptions o;
    o.url = url;
    o.user = "gw";
    o.passwordFile = pw;
    o.statementTimeoutMs = T;
    return o;
}

bool open(PgConn &c, const std::string &url, MdState &state, StartVerdict &v, std::string &detail)
{
    std::string error;
    if (!connectHistory(c, options(url), error))
    {
        detail = error;
        v = StartVerdict::FAILED;
        return false;
    }
    v = startHistory(c, T, state, detail);
    return StartVerdict::READY == v;
}

// A journal: records 1..n, a trade every record but every fifth (a record 2..5 there).
std::vector<PendingEntry> journal(std::uint32_t seed, std::size_t n)
{
    std::mt19937 rng(seed);
    std::vector<PendingEntry> out;
    std::int64_t ts = 1'790'000'000'000;
    for (std::size_t i = 1; i <= n; ++i)
    {
        PendingEntry e;
        e.seq = static_cast<std::int64_t>(i);
        e.hasTrade = 0 != i % 5;
        if (e.hasTrade)
        {
            ts += static_cast<std::int64_t>(rng() % 30'000);
            e.trade = TradeRow{ 1 + static_cast<std::int32_t>(rng() % 3), 5000 + e.seq,
                100'000'000LL * (40'000 + static_cast<std::int64_t>(rng() % 900)),
                1'000'000LL * (1 + static_cast<std::int64_t>(rng() % 900)), 0 == rng() % 2, ts, e.seq };
        }
        out.push_back(e);
    }
    PendingEntry big; // the 128-bit quote volume
    big.seq = static_cast<std::int64_t>(n + 1);
    big.hasTrade = true;
    big.trade = TradeRow{ 2, 5000 + big.seq, 5'000'000'000'000LL, 10'000'000'000'000LL, true, ts + 1, big.seq };
    out.push_back(big);
    return out;
}

std::vector<TradeRow> tradesOf(const std::vector<PendingEntry> &j)
{
    std::vector<TradeRow> out;
    for (const PendingEntry &e : j)
    {
        if (e.hasTrade)
        {
            out.push_back(e.trade);
        }
    }
    return out;
}

// md_trades against the set: `duplicate trade <id>` / `missing trade <id>` / `trade <id> differs`.
void checkTrades(const std::string &url, const std::vector<TradeRow> &want)
{
    const std::string rows = gwtest::sql(url, "SELECT coalesce(string_agg(market_id || ':' || trade_id || ':' || price || "
        "':' || quantity || ':' || taker_is_buy::int || ':' || executed_at_ms || ':' || journal_seq, ',' ORDER BY "
        "journal_seq), '') FROM md_trades");
    std::map<std::int64_t, int> seen;
    std::map<std::int64_t, std::string> stored;
    std::size_t at = 0;
    while (at < rows.size())
    {
        std::size_t end = rows.find(',', at);
        end = std::string::npos == end ? rows.size() : end;
        const std::string one = rows.substr(at, end - at);
        const std::size_t c1 = one.find(':');
        const std::int64_t id = std::stoll(one.substr(c1 + 1, one.find(':', c1 + 1) - c1 - 1));
        seen[id]++;
        stored[id] = one;
        at = end + 1;
    }
    std::set<std::int64_t> wanted;
    for (const TradeRow &t : want)
    {
        wanted.insert(t.tradeId);
        const std::string expect = std::to_string(t.marketId) + ":" + std::to_string(t.tradeId) + ":" +
            std::to_string(t.price) + ":" + std::to_string(t.quantity) + ":" + (t.takerIsBuy ? "1" : "0") + ":" +
            std::to_string(t.executedAtMs) + ":" + std::to_string(t.journalSeq);
        if (0 == seen.count(t.tradeId))
        {
            gwtest::fail(__FILE__, __LINE__, "missing trade " + std::to_string(t.tradeId));
        }
        else if (stored[t.tradeId] != expect)
        {
            gwtest::fail(__FILE__, __LINE__, "trade " + std::to_string(t.tradeId) + " differs: " + stored[t.tradeId]);
        }
    }
    for (const auto &[id, n] : seen)
    {
        if (n > 1 || 0 == wanted.count(id))
        {
            gwtest::fail(__FILE__, __LINE__, "duplicate trade " + std::to_string(id));
        }
    }
}

std::string candleDump(const std::string &url)
{
    return gwtest::sql(url, "SELECT coalesce(string_agg(concat_ws(':', market_id, interval_s, bucket_start_ms, open, high, "
        "low, close, volume_base, volume_quote, trade_count, last_trade_id), ',' ORDER BY market_id, interval_s, "
        "bucket_start_ms), '') FROM md_candles");
}

std::string expectedDump(const std::vector<TradeRow> &set)
{
    std::string out;
    for (const auto &[key, c] : aggregate(set))
    {
        out += (out.empty() ? "" : ",") + std::to_string(c.marketId) + ":" + std::to_string(c.intervalS) + ":" +
            std::to_string(c.bucketStartMs) + ":" + std::to_string(c.open) + ":" + std::to_string(c.high) + ":" +
            std::to_string(c.low) + ":" + std::to_string(c.close) + ":" + decimal(c.volumeBase) + ":" +
            decimal(c.volumeQuote) + ":" + std::to_string(c.tradeCount) + ":" + std::to_string(c.lastTradeId);
    }
    return out;
}

// Commits `j` through the ledger in batches of `split`; `crash(batch index)` may break a batch (the hook runs before
// COMMIT); a broken batch is resolved the way md-pg does (read the cursor) and committed again.
int commitAll(const std::string &url, const std::vector<PendingEntry> &j, std::size_t split, std::int64_t arrivalBase,
    const std::function<void(int, PgConn &)> &crash = {})
{
    PgConn c;
    MdState st;
    StartVerdict v;
    std::string detail;
    CHECK_MSG(open(c, url, st, v, detail), detail);
    TradeLedger ledger(j.size() + 1);
    ledger.publishState(st);
    if (!st.epoch)
    {
        ledger.bind(EPOCH, j.front().seq);
    }
    std::size_t pushed = 0;
    for (const PendingEntry &e : j)
    {
        if (e.seq > st.lastSeq)
        {
            ledger.push(e);
            pushed++;
        }
    }
    int index = 0;
    int broken = 0;
    std::int64_t arrival = arrivalBase;
    CommitBatch b;
    while (ledger.nextBatch(split, b))
    {
        const int me = index++;
        std::function<void()> hook;
        if (crash)
        {
            hook = [&crash, &c, me] { crash(me, c); };
        }
        std::string error;
        const Commit r = commitBatch(c, T, b, arrival, error, hook);
        arrival += 61'000;
        if (Commit::COMMITTED == r)
        {
            ledger.committed(b);
            continue;
        }
        broken++;
        CHECK_MSG(Commit::ANOTHER_WRITER != r, error);
        if (Commit::ANOTHER_WRITER == r)
        {
            return broken;
        }
        // as md-pg: a new connection (the lock again), the cursor says what happened
        c.close();
        MdState now;
        for (int tries = 0; tries < 50 && !open(c, url, now, v, detail); ++tries)
        {
            sleepMs(100);
        }
        CHECK_MSG(StartVerdict::READY == v, detail);
        if (now.lastSeq == b.toSeq)
        {
            ledger.committed(b);
        }
        else
        {
            CHECK(now.lastSeq == b.fromSeq);
        }
    }
    CHECK(0 == ledger.pending());
    (void)pushed;
    return broken;
}

void terminateOthers(const std::string &db)
{
    gwtest::sql(gwtest::env("GW_TEST_PG_ADMIN_URL"), "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
        "WHERE datname = '" + db + "' AND pid <> pg_backend_pid()");
}

}

TEST(md_history_start)
{
    const std::string url = gwtest::freshDatabase("md_start");
    PgConn a;
    MdState st;
    StartVerdict v;
    std::string detail;
    CHECK_MSG(open(a, url, st, v, detail), detail);
    CHECK(std::string::npos != detail.find("applied"));
    CHECK(!st.epoch && 0 == st.lastSeq && !st.firstSeq);
    // a second writer while the first holds the lock
    PgConn b;
    const bool second = open(b, url, st, v, detail);
    std::printf("second writer: %s: %s\n", nameOf(v), detail.c_str());
    CHECK_MSG(!second && StartVerdict::LOCK_HELD == v, "two market history writers");
    CHECK(std::string::npos != detail.find(WRITER_LOCK_HELD_TEXT));
    // the console's reset takes the same key and is refused while the writer runs
    CHECK("f" == gwtest::sql(url, std::string("SELECT pg_try_advisory_lock(") + WRITER_LOCK_KEY + ")"));
    a.close();
    sleepMs(200);
    PgConn c;
    CHECK_MSG(open(c, url, st, v, detail), detail);
    CHECK(std::string::npos != detail.find("present"));
    c.close();
    // the legacy database
    const std::string legacy = gwtest::freshDatabase("md_legacy");
    gwtest::sql(legacy, "CREATE TABLE trades (id BIGINT)");
    PgConn d;
    CHECK(!open(d, legacy, st, v, detail) && StartVerdict::LEGACY == v);
    std::printf("legacy: %s\n", detail.c_str());
    CHECK(std::string::npos != detail.find(LEGACY_DATABASE_TEXT));
    CHECK("0" == gwtest::sql(legacy, "SELECT count(*) FROM pg_tables WHERE tablename LIKE 'md\\_%'"));
    // foreign: some of the relations, or other columns
    const std::string some = gwtest::freshDatabase("md_some");
    gwtest::sql(some, "CREATE TABLE md_trades (x INT)");
    PgConn e;
    CHECK(!open(e, some, st, v, detail) && StartVerdict::FOREIGN == v);
    std::printf("foreign: %s\n", detail.c_str());
    const std::string other = gwtest::freshDatabase("md_other");
    PgConn f;
    CHECK_MSG(open(f, other, st, v, detail), detail);
    f.close();
    gwtest::sql(other, "ALTER TABLE md_candles ADD COLUMN written_at TIMESTAMPTZ");
    CHECK(!open(f, other, st, v, detail) && StartVerdict::FOREIGN == v);
    std::printf("foreign: %s\n", detail.c_str());
    // a reset: every md_* relation truncated (the console, with the service stopped); md_state is written again
    const std::string reset = gwtest::freshDatabase("md_reset");
    commitAll(reset, journal(3, 40), 16, 0);
    CHECK("41" == gwtest::sql(reset, "SELECT last_journal_seq FROM md_state"));
    gwtest::sql(reset, "TRUNCATE md_trades, md_candles, md_state");
    PgConn g;
    CHECK_MSG(open(g, reset, st, v, detail), detail);
    CHECK(!st.epoch && 0 == st.lastSeq && !st.firstSeq);
    CHECK("1" == gwtest::sql(reset, "SELECT count(*) FROM md_state WHERE schema_version = 1"));
}

TEST(md_history_exactly_once)
{
    for (int round = 1; round <= 2; ++round)
    {
        const std::string db = "md_once_" + std::to_string(round);
        const std::string url = gwtest::freshDatabase(db);
        const std::vector<PendingEntry> j = journal(100 + static_cast<std::uint32_t>(round), 600);
        int kills = 0;
        // batch 2: the backend killed between the statements and COMMIT; batch 5: the connection closed there (a
        // process killed mid-transaction); batch 8: the server ends the session at the COMMIT
        const int broken = commitAll(url, j, 37, 0, [&](int index, PgConn &c) {
            if (2 == index || 9 == index)
            {
                kills++;
                terminateOthers(db);
            }
            else if (5 == index || 12 == index)
            {
                kills++;
                c.close();
            }
            else if (8 == index)
            {
                kills++;
                gwtest::sql(gwtest::env("GW_TEST_PG_ADMIN_URL"), "SELECT count(pg_terminate_backend(pid)) FROM "
                    "pg_stat_activity WHERE datname = '" + db + "' AND state = 'idle in transaction'");
            }
        });
        std::printf("round %d: %d kills, %d broken batches\n", round, kills, broken);
        CHECK_MSG(kills >= 2 && broken >= 2, "writer was never killed");
        checkTrades(url, tradesOf(j));
        CHECK(std::to_string(j.back().seq) == gwtest::sql(url, "SELECT last_journal_seq FROM md_state"));
        const bool candles = candleDump(url) == expectedDump(tradesOf(j));
        CHECK_MSG(candles, "candles differ from a recomputation from the trade set");
    }
}

TEST(md_history_another_writer)
{
    const std::string url = gwtest::freshDatabase("md_another");
    const std::vector<PendingEntry> j = journal(5, 30);
    commitAll(url, j, 10, 0);
    PgConn c;
    MdState st;
    StartVerdict v;
    std::string detail;
    CHECK_MSG(open(c, url, st, v, detail), detail);
    auto batch = [&](std::int64_t from, std::int64_t to, std::int64_t epoch, std::int64_t tradeId) {
        CommitBatch b;
        b.fromSeq = from;
        b.toSeq = to;
        b.epoch = epoch;
        b.firstSeq = 1;
        b.trades.push_back(TradeRow{ 1, tradeId, 1, 1, true, 1'790'000'000'000, to });
        b.entries = 1;
        return b;
    };
    std::string error;
    // the cursor is not the expected one
    CHECK(Commit::ANOTHER_WRITER == commitBatch(c, T, batch(5, 32, EPOCH, 99'001), 0, error));
    std::printf("stale cursor: %s\n", error.c_str());
    CHECK(0 == error.rfind(ANOTHER_WRITER_TEXT, 0));
    // another epoch
    CHECK(Commit::ANOTHER_WRITER == commitBatch(c, T, batch(31, 32, EPOCH + 1, 99'002), 0, error));
    // a trade id stored already
    CHECK(Commit::ANOTHER_WRITER == commitBatch(c, T, batch(31, 32, EPOCH, j.front().trade.tradeId), 0, error));
    std::printf("stored already: %s\n", error.c_str());
    CHECK("31" == gwtest::sql(url, "SELECT last_journal_seq FROM md_state"));
    checkTrades(url, tradesOf(j));
}

TEST(md_candles_batch_splits)
{
    const std::vector<PendingEntry> j = journal(17, 500);
    const std::vector<TradeRow> set = tradesOf(j);
    const std::string a = gwtest::freshDatabase("md_split_one");
    const std::string b = gwtest::freshDatabase("md_split_seven");
    const std::string c = gwtest::freshDatabase("md_split_replay");
    commitAll(a, j, 1, 1'800'000'000'000);
    commitAll(b, j, 7, 1'800'300'000'000);
    commitAll(c, j, 64, 1'800'600'000'000);
    const std::string first = candleDump(c);
    gwtest::sql(c, "TRUNCATE md_trades, md_candles, md_state");
    commitAll(c, j, 50, 1'800'900'000'000);
    const std::string da = candleDump(a);
    const std::string db = candleDump(b);
    const std::string dc = candleDump(c);
    const bool equal = !da.empty() && da == db && db == dc && dc == first;
    CHECK_MSG(equal, "candle bucket depends on arrival time");
    // the 128-bit quote volume, exact
    const TradeRow &big = set.back();
    const std::string quote = gwtest::sql(a, "SELECT volume_quote::text FROM md_candles WHERE market_id = " +
        std::to_string(big.marketId) + " AND interval_s = 60 AND bucket_start_ms = " +
        std::to_string(bucketStart(big.executedAtMs, 60)));
    Int128 stored = 0;
    parseDecimal(quote, stored);
    const bool exact = stored >= quoteOf(5'000'000'000'000LL, 10'000'000'000'000LL) &&
        decimal(quoteOf(5'000'000'000'000LL, 10'000'000'000'000LL)) == "50000000000000000000000000";
    CHECK_MSG(exact, "quote volume overflow: " + quote);
    // the tickers at one fixed "as of" time
    const std::int64_t asOf = big.executedAtMs + 10'000;
    for (std::int32_t market = 1; market <= 3; ++market)
    {
        std::vector<std::optional<Ticker>> t;
        for (const std::string &url : { a, b, c })
        {
            PgConn conn;
            MdState st;
            StartVerdict v;
            std::string detail;
            std::string error;
            // a reader only: the lock is released when this connection closes
            CHECK_MSG(open(conn, url, st, v, detail), detail);
            std::vector<CandleRow> minutes;
            CHECK_MSG(readCandlesSince(conn, T, market, 60, asOf - TICKER_WINDOW_MS, minutes, error), error);
            t.push_back(tickerOf(market, minutes, asOf));
        }
        CHECK(t[0].has_value() && t[0] == t[1] && t[1] == t[2]);
    }
    // the reads the live state and the routes start from
    PgConn conn;
    MdState st;
    StartVerdict v;
    std::string detail;
    std::string error;
    CHECK_MSG(open(conn, a, st, v, detail), detail);
    std::vector<TradeRow> recent;
    CHECK(readRecentTrades(conn, T, 2, 5, recent, error) && 5 == recent.size() && recent.back() == big);
    std::vector<CandleRow> last;
    CHECK(readCandles(conn, T, 2, 60, 3, last, error) && 3 == last.size() &&
        last[0].bucketStartMs < last[2].bucketStartMs);
}

TEST(md_writer_outage)
{
    const std::string url = gwtest::freshDatabase("md_outage");
    const std::vector<PendingEntry> j = journal(23, 900);
    TradeLedger ledger(4096);
    HistoryWriterSettings s;
    s.pg = options(url);
    s.pg.statementTimeoutMs = 1000;
    s.retryMs = 200;
    s.commitBatch = 64;
    s.commitIntervalMs = 20;
    HistoryWriter w(s, ledger);
    CHECK(0 == w.startCheck());
    CHECK(ledger.stateKnown());
    w.start();
    ledger.bind(EPOCH, 1);
    auto waitFor = [](const std::function<bool()> &cond, std::int64_t ms) {
        for (std::int64_t waited = 0; waited < ms; waited += 20)
        {
            if (cond())
            {
                return true;
            }
            sleepMs(20);
        }
        return cond();
    };
    std::size_t at = 0;
    auto pushUpTo = [&](std::size_t n) {
        for (; at < n && at < j.size(); ++at)
        {
            CHECK(ledger.push(j[at]));
        }
    };
    pushUpTo(200);
    CHECK(waitFor([&] { return ledger.committedSeq() == j[199].seq; }, 10'000));
    for (int outage = 1; outage <= 2; ++outage)
    {
        gwtest::run("GW_TEST_PG_PAUSE");
        pushUpTo(200 + static_cast<std::size_t>(outage) * 300);
        const bool down = waitFor([&] { return !ledger.writable.load(); }, 10'000);
        std::printf("outage %d: writable=%d pending=%zu committed=%lld\n", outage, ledger.writable.load() ? 1 : 0,
            ledger.pending(), static_cast<long long>(ledger.committedSeq()));
        CHECK_MSG(down, "market history was never down");
        CHECK(ledger.pending() > 0);
        gwtest::run("GW_TEST_PG_UNPAUSE");
        const std::int64_t last = j[std::min(at, j.size()) - 1].seq;
        CHECK(waitFor([&] { return ledger.committedSeq() == last && 0 == ledger.pending(); }, 30'000));
        CHECK(waitFor([&] { return ledger.writable.load(); }, 5000));
    }
    pushUpTo(j.size());
    CHECK(waitFor([&] { return ledger.committedSeq() == j.back().seq; }, 10'000));
    std::printf("recoveries=%llu failures=%llu commits=%llu\n",
        static_cast<unsigned long long>(ledger.recoveries.load()), static_cast<unsigned long long>(ledger.failures.load()),
        static_cast<unsigned long long>(ledger.commits.load()));
    CHECK(ledger.recoveries.load() >= 2);
    CHECK(0 == ledger.fatalExit.load());
    w.stop(3000);
    checkTrades(url, tradesOf(j));
    CHECK(candleDump(url) == expectedDump(tradesOf(j)));
}
