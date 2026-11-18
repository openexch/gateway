// oe-projector against a real PostgreSQL (tests/pg/pg_it.py runs it against a throwaway container): the writer's
// batches (HistoryWork) over the shipped lease and fence, driven the way pj-pg drives them.
//   apply_story                  create, partial fill, fill, cancel, amend, reject, a zero-trade FILL, a 64-trade batch:
//                                the rows equal the expected set, no fills row for tradeId 0, 128 fills for the batch
//                                with maker by liquidity, all in one transaction
//   fk_order_first_records       ACCEPTED, REJECTED and a FILL as the first record of an order: the order row exists
//                                before its fill, the foreign key never fires
//   crash_points                 before commit, after commit before ack, a connection lost at commit, every backend
//                                ended mid-batch, a statement timeout mid-batch; each twice: the cursor and the rows agree,
//                                no row twice, none missing; an uncertain commit is resolved by reading the cursor
//   overlap_compare              10 000 records read again: all compared equal, counted, rows unchanged; one differing
//                                byte: a conflict naming the sequence and both SHA-256 values, nothing written from it;
//                                a second source that agrees resumes; twice
//   one_mapping_from_postgres    the history JSON of each projected order and fill equals the stream's JSON of its last
//                                event (fills: maker is null live)
//   read_contract_compat         the gateway's history store (PgHistoryStore) over the projected views: orders page, one
//                                order, fills page, LAGGING, no heartbeat; the views (the trail too) have the read
//                                contract's columns and types (sql/history_read_contract_v1.sql)
//   reference_equivalence        the same logical rows in the reference tables and as projected records render the same
//                                JSON for every case
//   beat_rule                    the beat statement against what the transaction committed: a probe whose head is
//                                applied beats (beat_at = the database's clock less the time since the probe was sent);
//                                a head above the cursor, another genesis, a halt, a renewal, an older probe: no beat
//   rebuild_reads                the gateway's store reading during an in-place rebuild and the re-projection after it:
//                                503 cause heartbeat from the truncate until the first beat, never a page, never 404 for
//                                an order that existed; complete pages after the beat
//   reader_role_prepared         the schema applied by an owner without CREATEROLE: the reader role absent, a refusal
//                                naming it; created beforehand, the apply succeeds with the four grants
//   projection_dense             the story applied in batches of 16: every journalSeq from 1 to applied_seq has its row
//                                in oe_order_events (the check the fixture's dump makes)
//   projection_byte_equal        the same story projected into two databases, a moment apart: the canonical dumps of
//                                oe_order_events, oe_orders and oe_fills (every column, in key order) are equal
// The control builds (tests/CMakeLists.txt) switch one rule off each and must fail with the named text.
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <atomic>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "Check.h"
#include "PgTestSupport.h"
#include "gateway/common/History.h"
#include "gateway/common/PgHistoryStore.h"
#include "gateway/common/Rest.h"
#include "HistoryWork.h"
#include "JournalRecord.h"
#include "ProjectionStore.h"

using namespace openexchange::gateway;
using namespace openexchange::gateway::projector;
namespace money = wire::money;

namespace {

constexpr std::int64_t LEASE_MS = 600;
constexpr std::int64_t T = 2000; // statement timeout of the rig
constexpr std::int64_t EPOCH = 77;

void sleepMs(std::int64_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

JournalRecord rec(std::int64_t seq, std::int64_t orderId, money::EventKind::Value kind, money::OrderStatus::Value status)
{
    JournalRecord r;
    r.seq = seq;
    r.epoch = EPOCH;
    r.logPosition = 4096 + seq * 128;
    wire::OrderEvent &e = r.event;
    e.orderId = orderId;
    e.userId = 500 + orderId % 3;
    e.marketId = 1;
    e.eventKind = kind;
    e.status = status;
    e.side = 0 == orderId % 2 ? money::Side::BUY : money::Side::SELL;
    e.orderType = money::OrderType::LIMIT;
    e.timeInForce = money::TimeInForce::GTC;
    e.price = 100'00000000;
    e.quantity = 5'00000000;
    e.remainingQty = 5'00000000;
    e.holdAssetId = 2;
    e.holdAmount = 500'00000000;
    e.holdRemaining = 500'00000000;
    e.createdAtMs = 1'700'000'000'000 + orderId;
    e.updatedAtMs = 1'700'000'100'000 + seq;
    e.meReason = 255;
    return r;
}

JournalRecord fill(std::int64_t seq, std::int64_t orderId, std::int64_t tradeId, std::int64_t qty, std::int64_t filled,
    Liquidity liquidity)
{
    JournalRecord r = rec(seq, orderId, money::EventKind::FILL, filled >= 5'00000000 ? money::OrderStatus::FILLED
                                                                                   : money::OrderStatus::PARTIALLY_FILLED);
    r.event.tradeId = tradeId;
    r.event.lastPrice = 100'00000000;
    r.event.lastQty = qty;
    r.event.filledQty = filled;
    r.event.remainingQty = 5'00000000 - filled;
    r.event.holdRemaining = 500'00000000 - filled * 100;
    r.event.meOrderId = 9000 + orderId;
    r.liquidity = liquidity;
    r.meEpoch = 3;
    r.meJournalSeq = 70 + tradeId;
    r.meLogPosition = 65536 + tradeId;
    r.meTimestampMs = 1'700'000'200'000 + tradeId;
    return r;
}

// The records as the core journals them: bytes and back, so the rig writes what a replay delivers.
JournalRecord journaled(const JournalRecord &r)
{
    const Payload p = encodeRecord(r);
    JournalRecord out;
    std::string why;
    if (!decodeRecord(p.data(), p.size(), out, why))
    {
        gwtest::fail(__FILE__, __LINE__, "story record refused: " + why);
    }
    return out;
}

// The story of apply_story: 7 single orders and a 64-trade batch.
std::vector<JournalRecord> story()
{
    std::vector<JournalRecord> s;
    std::int64_t q = 0;
    s.push_back(rec(++q, 1, money::EventKind::ACCEPTED, money::OrderStatus::NEW));         // create
    s.push_back(rec(++q, 1, money::EventKind::ACK_NEW, money::OrderStatus::NEW));
    s.push_back(fill(++q, 1, 100, 2'00000000, 2'00000000, Liquidity::TAKER));               // partial fill
    s.push_back(fill(++q, 1, 101, 3'00000000, 5'00000000, Liquidity::MAKER));               // fill
    s.push_back(rec(++q, 2, money::EventKind::ACCEPTED, money::OrderStatus::NEW));
    JournalRecord c = rec(++q, 2, money::EventKind::CANCELLED, money::OrderStatus::CANCELLED); // cancel
    c.event.holdRemaining = 0;
    s.push_back(c);
    s.push_back(rec(++q, 3, money::EventKind::ACCEPTED, money::OrderStatus::NEW));
    JournalRecord a = rec(++q, 3, money::EventKind::REPLACED, money::OrderStatus::NEW);      // amend
    a.event.quantity = 4'00000000;
    a.event.remainingQty = 4'00000000;
    a.event.holdRemaining = 400'00000000;
    s.push_back(a);
    JournalRecord j = rec(++q, 4, money::EventKind::REJECTED, money::OrderStatus::REJECTED); // reject
    j.event.reason = money::CoreRejectReason::INSUFFICIENT_FUNDS;
    j.event.holdRemaining = 0;
    s.push_back(j);
    s.push_back(rec(++q, 5, money::EventKind::ACCEPTED, money::OrderStatus::NEW));
    JournalRecord z = rec(++q, 5, money::EventKind::FILL, money::OrderStatus::CANCELLED); // a FILL with tradeId 0
    s.push_back(z);
    // a 64-trade batch: taker 10 against makers 20..83, one core log position for all 128 FILL records
    s.push_back(rec(++q, 10, money::EventKind::ACCEPTED, money::OrderStatus::NEW));
    for (int k = 0; k < 64; ++k)
    {
        s.push_back(rec(++q, 20 + k, money::EventKind::ACCEPTED, money::OrderStatus::NEW));
    }
    const std::int64_t batchLog = 900'000;
    for (int k = 0; k < 64; ++k)
    {
        JournalRecord t = fill(++q, 10, 1000 + k, 1'000000, 1'000000 * (k + 1), Liquidity::TAKER);
        t.logPosition = batchLog;
        s.push_back(t);
        JournalRecord m = fill(++q, 20 + k, 1000 + k, 1'000000, 1'000000, Liquidity::MAKER);
        m.logPosition = batchLog;
        s.push_back(m);
    }
    for (auto &r : s)
    {
        r = journaled(r);
    }
    return s;
}

// A synthetic stream of N records over N / 2 orders (ACCEPTED then CANCELLED).
std::vector<JournalRecord> stream(std::int64_t n, std::int64_t firstOrder = 1)
{
    std::vector<JournalRecord> s;
    for (std::int64_t q = 1; q <= n; ++q)
    {
        const std::int64_t order = firstOrder + (q - 1) / 2;
        s.push_back(journaled(1 == q % 2 ? rec(q, order, money::EventKind::ACCEPTED, money::OrderStatus::NEW)
                                         : rec(q, order, money::EventKind::CANCELLED, money::OrderStatus::CANCELLED)));
    }
    return s;
}

// pj-pg's steps over one connection: the schema, the takeover, the first (load) transaction, the batches.
struct Rig
{
    explicit Rig(std::string databaseUrl, std::int64_t batchEvents = 100'000) :
        url(std::move(databaseUrl)), ex(1'000'000), lease(LEASE_MS),
        work(ex, HistoryWorkSettings{ batchEvents, 0, T }, stats)
    {
    }

    bool connect()
    {
        PgOptions o;
        o.url = url;
        o.statementTimeoutMs = T;
        std::string error;
        if (!connectProjection(conn, o, error))
        {
            gwtest::fail(__FILE__, __LINE__, "connect: " + error);
            return false;
        }
        return true;
    }

    // migrate, wait for the lease, load
    bool start(std::int64_t waitMs = 5000)
    {
        if (!connect())
        {
            return false;
        }
        std::string detail;
        const SchemaVerdict v = migrate(conn, T, detail);
        if (SchemaVerdict::FAILED == v || SchemaVerdict::FOREIGN == v)
        {
            gwtest::fail(__FILE__, __LINE__, "schema: " + detail);
            return false;
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(waitMs);
        ProjectionState st;
        std::string error;
        for (;;)
        {
            const WriterLease::Take t = lease.takeover(conn, T, st, error);
            if (WriterLease::Take::WRITER == t)
            {
                break;
            }
            if (std::chrono::steady_clock::now() > deadline)
            {
                gwtest::fail(__FILE__, __LINE__, "no takeover: " + error);
                return false;
            }
            sleepMs(50);
        }
        work.onWriter(st);
        return commit() == Hold::HELD && loadGeneration();
    }

    bool loadGeneration()
    {
        Results r = ex.collect();
        if (!r.reset)
        {
            gwtest::fail(__FILE__, __LINE__, "no generation after the load transaction");
            return false;
        }
        generation = r.reset->generation;
        state = r.reset->state;
        orphans = r.reset->replays;
        memo = r.reset->memo;
        return true;
    }

    void push(Item::Kind kind, const JournalRecord &r, std::size_t source = 0, const std::string &name = "10.0.0.1:8010")
    {
        Item i;
        i.kind = kind;
        i.generation = generation;
        i.source = source;
        i.sourceName = name;
        i.key.recordingId = 5;
        i.key.startTimestamp = 1234;
        i.key.mtuLength = 1408;
        i.key.termBufferLength = 65536;
        i.key.streamId = 4102;
        i.endPosition = r.seq * 256;
        i.record = r;
        ex.push(std::move(i));
    }
    void pushItem(Item i)
    {
        i.generation = generation;
        ex.push(std::move(i));
    }
    void epoch(std::int64_t e)
    {
        Item i;
        i.kind = Item::Kind::EPOCH;
        i.epoch = e;
        pushItem(i);
    }

    // one batch: due (takes what is queued), the fenced transaction, committed on HELD
    Hold commit(const std::function<void()> &beforeFence = {}, const std::function<void()> &beforeCommit = {},
        bool ack = true, bool *beat = nullptr)
    {
        if (!work.due(INT64_MAX / 2))
        {
            return Hold::HELD; // nothing to write
        }
        StateAdvance advance;
        std::string error;
        const Hold h = lease.commitFenced(conn, T,
            [&](PgConn &c, std::string &e) { return work.write(c, lease.token(), advance, e); }, advance, error,
            beforeFence, beforeCommit, beat);
        lastError = error;
        if (Hold::HELD == h)
        {
            batches++;
            if (ack)
            {
                work.committed(advance);
            }
        }
        return h;
    }

    // what pj-pg does after a failed statement: a new connection, the state read again (renew), onWriter
    bool reassert()
    {
        conn.close();
        for (int i = 0; i < 100 && !conn.ok(); ++i)
        {
            PgOptions o;
            o.url = url;
            o.statementTimeoutMs = T;
            std::string error;
            if (!connectProjection(conn, o, error))
            {
                sleepMs(100);
            }
        }
        ProjectionState st;
        std::string error;
        if (Hold::HELD != lease.renew(conn, T, st, error))
        {
            gwtest::fail(__FILE__, __LINE__, "renew: " + error);
            return false;
        }
        work.onWriter(st);
        return true;
    }

    void release() { lease.release(conn, T); }

    // an answered head probe (H, G) sent `agoMs` ago, as pj-duty hands it on
    void beat(std::int64_t head, std::int64_t genesis, std::int64_t agoMs = 0)
    {
        Item i;
        i.kind = Item::Kind::BEAT;
        i.head = head;
        i.epoch = genesis;
        i.sentAtNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count() - agoMs * 1'000'000;
        pushItem(i);
    }

    std::string url;
    PgConn conn;
    FollowExchange ex;
    WriterStats stats;
    WriterLease lease;
    HistoryWork work;
    std::uint64_t generation = 0;
    ProjectionState state;
    std::vector<ReplayRow> orphans;
    std::vector<MemoRow> memo;
    std::string lastError;
    int batches = 0;
};

std::string one(const std::string &url, const std::string &sql)
{
    return gwtest::sql(url, sql);
}

std::int64_t num(const std::string &url, const std::string &sql)
{
    const std::string v = one(url, sql);
    return v.empty() ? -1 : std::stoll(v);
}

// The cursor and the rows agree: every journalSeq 1..applied_seq is there, once, and nothing above it.
void checkDense(const std::string &url, std::int64_t expectApplied, const std::string &what)
{
    const std::int64_t applied = num(url, "SELECT applied_seq FROM oe_projection_state");
    const std::int64_t count = num(url, "SELECT count(*) FROM oe_order_events");
    const std::int64_t max = num(url, "SELECT coalesce(max(journal_seq), 0) FROM oe_order_events");
    if (applied > max)
    {
        CHECK_MSG(false, "cursor ahead of the rows (" + what + "): applied_seq " + std::to_string(applied) +
            ", rows up to " + std::to_string(max));
    }
    CHECK_MSG(count == applied && max == applied, what + ": applied_seq " + std::to_string(applied) + ", " +
        std::to_string(count) + " rows, max " + std::to_string(max));
    if (expectApplied >= 0)
    {
        CHECK_MSG(applied == expectApplied, what + ": applied_seq " + std::to_string(applied) + ", expected " +
            std::to_string(expectApplied));
    }
}

void terminateWriters()
{
    gwtest::sql(gwtest::env("GW_TEST_PG_ADMIN_URL"), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE "
        "application_name = 'oe-projector'");
}

void terminateAll()
{
    gwtest::sql(gwtest::env("GW_TEST_PG_ADMIN_URL"), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE "
        "pid <> pg_backend_pid() AND backend_type = 'client backend'");
}

std::string rowJson(const std::string &url, bool order, std::int64_t userId, std::int64_t id)
{
    history::Statement s = order ? history::orderStatement(userId, id)
                                 : history::pageStatement(history::PageRequest{ history::Kind::FILLS, userId,
                                       history::Filter{ 0, "", id }, std::nullopt, 100 });
    PgConn c;
    std::string error;
    if (!c.connect(url, T, error))
    {
        gwtest::fail(__FILE__, __LINE__, error);
        return "";
    }
    pg::Result r;
    if (pg::Exec::OK != c.exec(s.sql, s.params, T, r, error))
    {
        gwtest::fail(__FILE__, __LINE__, error);
        return "";
    }
    std::string out;
    for (int i = 0; i < PQntuples(r.get()); ++i)
    {
        history::Row row;
        for (int k = 0; k < PQnfields(r.get()); ++k)
        {
            row.push_back(PQgetisnull(r.get(), i, k) ? std::nullopt
                                                     : std::optional<std::string>(PQgetvalue(r.get(), i, k)));
        }
        std::string json;
        history::Key key;
        std::string problem;
        if (!(order ? history::orderJson(row, json, key, problem) : history::fillJson(row, json, key, problem)))
        {
            gwtest::fail(__FILE__, __LINE__, problem);
        }
        out += (out.empty() ? "" : "\n") + json;
    }
    return out;
}

std::string readFile(const std::string &path)
{
    std::ifstream in(path);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

// name:type of each column of a relation, in order
std::string shapeOf(const std::string &url, const std::string &relation)
{
    return one(url, "SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) FROM "
        "information_schema.columns WHERE table_name = '" + relation + "'");
}

}

TEST(apply_story)
{
    const std::string url = gwtest::freshDatabase("pj_apply");
    Rig rig(url);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    const std::vector<JournalRecord> s = story();
    for (const auto &r : s)
    {
        rig.push(Item::Kind::APPLY, r);
    }
    const int before = rig.batches;
    const Hold h = rig.commit();
    if (Hold::HELD != h)
    {
        if (std::string::npos != rig.lastError.find("oe_fills"))
        {
            CHECK_MSG(false, "fill row with tradeId 0 (" + rig.lastError + ")");
        }
        CHECK_MSG(false, "the story did not commit: " + rig.lastError);
        return;
    }
    CHECK_MSG(1 == rig.batches - before && !rig.ex.pending(), "one transaction");
    checkDense(url, static_cast<std::int64_t>(s.size()), "story");
    CHECK(EPOCH == num(url, "SELECT epoch FROM oe_projection_state"));
    CHECK(83 == num(url, "SELECT applied_order_id FROM oe_projection_state"));
    if (0 != num(url, "SELECT count(*) FROM oe_fills WHERE trade_id = 0 OR order_id = 5"))
    {
        CHECK_MSG(false, "fill row with tradeId 0");
    }
    CHECK(130 == num(url, "SELECT count(*) FROM oe_fills"));
    CHECK(128 == num(url, "SELECT count(*) FROM oe_fills WHERE trade_id >= 1000"));
    CHECK(64 == num(url, "SELECT count(*) FROM oe_fills WHERE trade_id >= 1000 AND maker"));
    CHECK(64 == num(url, "SELECT count(*) FROM oe_fills WHERE trade_id >= 1000 AND NOT maker AND order_id = 10"));
    CHECK(1 == num(url, "SELECT count(DISTINCT core_log_position) FROM oe_fills WHERE trade_id >= 1000"));
    CHECK(1 == num(url, "SELECT count(*) FROM oe_fills WHERE trade_id = 101 AND maker"));
    // every order holds the state of its last record
    std::map<std::int64_t, const JournalRecord *> last;
    for (const auto &r : s)
    {
        last[r.event.orderId] = &r;
    }
    CHECK(static_cast<std::int64_t>(last.size()) == num(url, "SELECT count(*) FROM oe_orders"));
    for (const auto &[order, r] : last)
    {
        const Columns c = orderColumns(*r);
        const std::string got = one(url, "SELECT concat_ws('|', order_id, user_id, coalesce(client_order_id::text, ''), "
            "market_id, side, type, time_in_force, coalesce(price::text, ''), coalesce(stop_price::text, ''), quantity, "
            "filled_quantity, remaining_quantity, status, coalesce(reason, ''), coalesce(me_reason::text, ''), "
            "coalesce(me_order_id::text, ''), hold_asset_id, hold_amount, hold_remaining, created_at_ms, updated_at_ms, "
            "last_event_seq, last_log_position) FROM oe_orders WHERE order_id = " + std::to_string(order));
        std::string want;
        for (std::size_t i = 0; i < c.size(); ++i)
        {
            want += (i ? "|" : "") + c[i].value_or("");
        }
        CHECK_MSG(got == want, "order " + std::to_string(order) + ": " + got + " != " + want);
    }
    CHECK(0 == num(url, "SELECT count(*) FROM oe_order_events e JOIN oe_orders o USING (order_id) WHERE "
        "e.journal_seq > o.last_event_seq"));
}

TEST(fk_order_first_records)
{
    struct Case
    {
        const char *name;
        std::vector<JournalRecord> records;
    };
    std::vector<Case> cases;
    cases.push_back({ "accepted first", { journaled(rec(1, 1, money::EventKind::ACCEPTED, money::OrderStatus::NEW)),
                                            journaled(fill(2, 1, 50, 1'00000000, 1'00000000, Liquidity::TAKER)) } });
    JournalRecord rej = rec(1, 1, money::EventKind::REJECTED, money::OrderStatus::REJECTED);
    rej.event.reason = money::CoreRejectReason::INVALID;
    cases.push_back({ "rejected first", { journaled(rej) } });
    // a FILL as the first record a source delivers after a memo start: its full state makes the order row first
    cases.push_back({ "fill first", { journaled(fill(1, 1, 51, 2'00000000, 2'00000000, Liquidity::MAKER)),
                                        journaled(fill(2, 1, 52, 3'00000000, 5'00000000, Liquidity::MAKER)) } });
    int n = 0;
    for (const Case &c : cases)
    {
        const std::string url = gwtest::freshDatabase("pj_fk_" + std::to_string(n++));
        Rig rig(url);
        CHECK(rig.start());
        rig.epoch(EPOCH);
        for (const auto &r : c.records)
        {
            rig.push(Item::Kind::APPLY, r);
        }
        if (Hold::HELD != rig.commit())
        {
            if (std::string::npos != rig.lastError.find("foreign key"))
            {
                CHECK_MSG(false, std::string("fill before its order (") + c.name + "): " + rig.lastError);
            }
            else
            {
                CHECK_MSG(false, std::string(c.name) + ": " + rig.lastError);
            }
            continue;
        }
        checkDense(url, static_cast<std::int64_t>(c.records.size()), c.name);
        CHECK(1 == num(url, "SELECT count(*) FROM oe_orders"));
    }
}

TEST(crash_points)
{
    const std::vector<JournalRecord> all = stream(40);
    auto run = [&](const std::string &name, int round) {
        const std::string url = gwtest::freshDatabase("pj_crash_" + name + "_" + std::to_string(round));
        std::int64_t pushed = 0;
        auto pushUpTo = [&](Rig &rig, std::int64_t to) {
            for (; pushed < to; ++pushed)
            {
                rig.push(Item::Kind::APPLY, all[static_cast<std::size_t>(pushed)]);
            }
        };
        {
            Rig rig(url);
            CHECK(rig.start());
            rig.epoch(EPOCH);
            pushUpTo(rig, 10);
            CHECK(Hold::HELD == rig.commit());
            checkDense(url, 10, name + ": first batch");
            pushUpTo(rig, 20);
            if ("before_commit" == name)
            {
                // the process dies between its fence and its COMMIT: the transaction never commits
                CHECK(Hold::FAILED == rig.commit({}, [] { terminateWriters(); }));
                checkDense(url, 10, name + ": after the crash");
                // a new process: takes the lease when it expires, reads the cursor, reads the same records again
            }
            else if ("after_commit_before_ack" == name)
            {
                CHECK(Hold::HELD == rig.commit({}, {}, false)); // committed, never acknowledged
                checkDense(url, 20, name + ": after the crash");
            }
            else if ("lost_at_commit" == name)
            {
                // the same process: the COMMIT's answer is lost; the cursor tells what happened
                CHECK(Hold::FAILED == rig.commit({}, [] { terminateWriters(); }));
                CHECK(rig.reassert());
                CHECK(Hold::HELD == rig.commit()); // not committed: the held batch is written again
                checkDense(url, 20, name + ": retried");
                pushUpTo(rig, 30);
                CHECK(Hold::HELD == rig.commit({}, {}, false)); // committed, the answer "lost"
                CHECK(rig.reassert()); // the cursor equals the batch's last sequence: it committed
                const Results r = rig.ex.collect();
                CHECK_MSG(30 == r.appliedSeq, "uncertain commit resolved to " + std::to_string(r.appliedSeq));
                CHECK(rig.stats.uncertainCommits.load() >= 2);
                rig.release();
                checkDense(url, 30, name + ": resolved");
                return;
            }
            else if ("backends_ended" == name)
            {
                CHECK(Hold::FAILED == rig.commit([] { terminateAll(); }));
                CHECK(rig.reassert());
                CHECK(Hold::HELD == rig.commit());
                rig.release();
                checkDense(url, 20, name + ": retried");
                return;
            }
            else if ("statement_timeout" == name)
            {
                std::thread thaw;
                CHECK(Hold::FAILED == rig.commit([&] {
                    gwtest::run("GW_TEST_PG_PAUSE");
                    thaw = std::thread([] {
                        sleepMs(T + 500);
                        gwtest::run("GW_TEST_PG_UNPAUSE");
                    });
                }));
                thaw.join();
                CHECK(rig.reassert());
                CHECK(Hold::HELD == rig.commit());
                rig.release();
                checkDense(url, 20, name + ": retried");
                return;
            }
        }
        // the restart: a new writer after the old lease expired
        Rig again(url);
        CHECK(again.start(LEASE_MS * 10));
        const std::int64_t applied = again.state.appliedSeq;
        // the source is read again from the memo's position or earlier: what is at or below the cursor is compared
        for (std::int64_t q = std::max<std::int64_t>(1, applied - 5); q <= applied; ++q)
        {
            again.push(Item::Kind::COMPARE, all[static_cast<std::size_t>(q - 1)]);
        }
        for (std::int64_t q = applied + 1; q <= 40; ++q)
        {
            again.push(Item::Kind::APPLY, all[static_cast<std::size_t>(q - 1)]);
        }
        CHECK(Hold::HELD == again.commit());
        const Results r = again.ex.collect();
        CHECK(r.conflicts.empty());
        again.release();
        checkDense(url, 40, name + ": after the restart");
    };
    for (const char *name : { "before_commit", "after_commit_before_ack", "lost_at_commit", "backends_ended",
             "statement_timeout" })
    {
        for (int round = 0; round < 2; ++round)
        {
            std::printf("  crash point %s, round %d\n", name, round + 1);
            run(name, round);
        }
    }
}

TEST(overlap_compare)
{
    const std::string url = gwtest::freshDatabase("pj_overlap");
    const std::int64_t N = 10'000;
    const std::vector<JournalRecord> all = stream(N + 20);
    {
        Rig rig(url, 256);
        CHECK(rig.start());
        rig.epoch(EPOCH);
        for (std::int64_t q = 1; q <= N; ++q)
        {
            rig.push(Item::Kind::APPLY, all[static_cast<std::size_t>(q - 1)]);
        }
        while (rig.ex.pending())
        {
            CHECK(Hold::HELD == rig.commit());
        }
        rig.release();
        checkDense(url, N, "10 000 applied");
    }
    const std::string before = one(url, "SELECT md5(string_agg(journal_seq || ':' || encode(payload, 'hex'), ',' ORDER "
        "BY journal_seq)) FROM oe_order_events");
    Rig rig(url, 256);
    CHECK(rig.start());
    // a restart without a memo reads the whole history again: every record compared
    for (std::int64_t q = 1; q <= N; ++q)
    {
        rig.push(Item::Kind::COMPARE, all[static_cast<std::size_t>(q - 1)]);
    }
    std::uint64_t verified = 0;
    std::uint64_t equal = 0;
    while (rig.ex.pending())
    {
        CHECK(Hold::HELD == rig.commit());
        const Results r = rig.ex.collect();
        verified += r.comparesVerified;
        equal += r.comparesEqual;
        CHECK(r.conflicts.empty());
    }
    CHECK_MSG(N == static_cast<std::int64_t>(verified) && verified == equal, "compared " + std::to_string(verified) +
        ", equal " + std::to_string(equal));
    CHECK(N == static_cast<std::int64_t>(rig.stats.skips.load()));
    CHECK(before == one(url, "SELECT md5(string_agg(journal_seq || ':' || encode(payload, 'hex'), ',' ORDER BY "
        "journal_seq)) FROM oe_order_events"));
    // one differing byte, entered and cleared twice
    std::int64_t next = N + 1;
    for (int round = 0; round < 2; ++round)
    {
        const std::int64_t at = 5000 + round;
        JournalRecord bad = all[static_cast<std::size_t>(at - 1)];
        bad.payload[100] ^= 0x01;
        rig.push(Item::Kind::COMPARE, bad, 1, "10.0.0.2:8010");
        CHECK(Hold::HELD == rig.commit());
        Results r = rig.ex.collect();
        if (r.conflicts.empty())
        {
            CHECK_MSG(false, "conflicting duplicate skipped");
            continue;
        }
        const Conflict &c = r.conflicts.front();
        CHECK(at == c.seq && "10.0.0.2:8010" == c.sourceName && c.storedSha256 != c.readSha256);
        CHECK(sha256Hex(all[static_cast<std::size_t>(at - 1)].payload.data(), RECORD_LENGTH) == c.storedSha256);
        CHECK(sha256Hex(bad.payload.data(), RECORD_LENGTH) == c.readSha256);
        // nothing written from it; a second source that agrees resumes
        CHECK(before == one(url, "SELECT md5(string_agg(journal_seq || ':' || encode(payload, 'hex'), ',' ORDER BY "
            "journal_seq)) FROM oe_order_events WHERE journal_seq <= 10000"));
        rig.push(Item::Kind::COMPARE, all[static_cast<std::size_t>(at - 1)], 2, "10.0.0.3:8010");
        rig.push(Item::Kind::APPLY, all[static_cast<std::size_t>(next - 1)], 2, "10.0.0.3:8010");
        CHECK(Hold::HELD == rig.commit());
        r = rig.ex.collect();
        CHECK(r.conflicts.empty() && 1 == r.comparesEqual && next == r.appliedSeq);
        next++;
    }
    rig.release();
    checkDense(url, N + 2, "after the conflicts");
    CHECK(2 == static_cast<std::int64_t>(rig.stats.payloadConflicts.load()));
    CHECK(2 == num(url, "SELECT count(*) FROM oe_projection_source_memo"));
}

// The writer applies only the record after its cursor: an APPLY past a gap (a later sequence, in its own batch or after
// an earlier APPLY of the same batch) fails the whole batch, nothing of it is written, and a generation starts from the
// database. pj-duty never queues one; this does not trust it to.
TEST(dense_apply)
{
    const std::string url = gwtest::freshDatabase("pj_dense");
    const std::vector<JournalRecord> all = stream(40);
    Rig rig(url, 256);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    for (std::int64_t q = 1; q <= 10; ++q)
    {
        rig.push(Item::Kind::APPLY, all[static_cast<std::size_t>(q - 1)]);
    }
    CHECK(Hold::HELD == rig.commit());
    checkDense(url, 10, "10 applied");
    const std::uint64_t generation = rig.generation;
    struct Shape
    {
        const char *what;
        std::vector<std::int64_t> seqs;
    };
    for (const Shape &shape : { Shape{ "a batch that starts past the cursor", { 12, 13 } },
             Shape{ "a gap inside a batch", { 11, 13 } }, Shape{ "a sequence applied twice", { 11, 11 } } })
    {
        for (std::int64_t q : shape.seqs)
        {
            rig.push(Item::Kind::APPLY, all[static_cast<std::size_t>(q - 1)]);
        }
        const Hold h = rig.commit();
        const std::int64_t above = num(url, "SELECT count(*) FROM oe_order_events WHERE journal_seq > 10");
        if (Hold::HELD == h || above != 0)
        {
            CHECK_MSG(false, std::string("applied a record past a gap (") + shape.what + "): " +
                std::to_string(above) + " rows above the cursor 10, error '" + rig.lastError + "'");
            return;
        }
        CHECK_MSG(std::string::npos != rig.lastError.find("not the next record"), shape.what + (": " + rig.lastError));
        checkDense(url, 10, shape.what);
        // pj-pg's path after a failed batch: the state read again, a new generation from the database
        CHECK(rig.reassert());
        CHECK(Hold::HELD == rig.commit());
        CHECK(rig.loadGeneration());
        CHECK(rig.generation > generation);
        CHECK(10 == rig.state.appliedSeq);
    }
    // the dense continuation is written
    for (std::int64_t q = 11; q <= 20; ++q)
    {
        rig.push(Item::Kind::APPLY, all[static_cast<std::size_t>(q - 1)]);
    }
    CHECK(Hold::HELD == rig.commit());
    rig.release();
    checkDense(url, 20, "the dense continuation");
}

TEST(one_mapping_from_postgres)
{
    const std::string url = gwtest::freshDatabase("pj_mapping");
    Rig rig(url);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    const std::vector<JournalRecord> s = story();
    for (const auto &r : s)
    {
        rig.push(Item::Kind::APPLY, r);
    }
    CHECK(Hold::HELD == rig.commit());
    rig.release();
    std::map<std::int64_t, const JournalRecord *> last;
    std::map<std::int64_t, std::vector<const JournalRecord *>> fills;
    for (const auto &r : s)
    {
        last[r.event.orderId] = &r;
        if (hasFill(r))
        {
            fills[r.event.orderId].push_back(&r);
        }
    }
    int compared = 0;
    for (const auto &[order, r] : last)
    {
        const std::string fromPg = rowJson(url, true, r->event.userId, order);
        const std::string live = rest::orderJson(r->event);
        compared++;
        if (fromPg != live)
        {
            CHECK_MSG(false, "history row renders differently from the stream:\n  row  " + fromPg + "\n  live " + live);
        }
    }
    for (const auto &[order, list] : fills)
    {
        // the fills page is newest first: (executed_at_ms, trade_id, order_id) descending
        std::vector<const JournalRecord *> sorted = list;
        std::sort(sorted.begin(), sorted.end(), [](auto *a, auto *b) {
            return std::make_pair(a->event.updatedAtMs, a->event.tradeId) > std::make_pair(b->event.updatedAtMs,
                b->event.tradeId);
        });
        std::string live;
        for (const JournalRecord *f : sorted)
        {
            std::string j = rest::fillJson(f->event);
            j.replace(j.find("\"maker\":null"), 12, std::string("\"maker\":") +
                (Liquidity::MAKER == f->liquidity ? "true" : "false"));
            live += (live.empty() ? "" : "\n") + j;
        }
        const std::string fromPg = rowJson(url, false, list.front()->event.userId, order);
        compared++;
        if (fromPg != live)
        {
            CHECK_MSG(false, "history row renders differently from the stream:\n  row  " + fromPg + "\n  live " + live);
        }
    }
    std::printf("  one mapping: %d orders and fill sets compared\n", compared);
    CHECK(compared > 0);
}

TEST(read_contract_compat)
{
    const std::string url = gwtest::freshDatabase("pj_compat");
    Rig rig(url);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    const std::vector<JournalRecord> s = story();
    for (const auto &r : s)
    {
        rig.push(Item::Kind::APPLY, r);
    }
    CHECK(Hold::HELD == rig.commit());
    rig.release();
#ifdef GW_NC_VIEW_REMAINING_FROM_QUANTITY
    // test build only: a view that maps remaining_quantity from quantity
    gwtest::sql(url, "CREATE OR REPLACE VIEW gateway_order_history_v1 AS SELECT order_id, user_id, client_order_id, "
        "market_id, side, type, time_in_force, price, stop_price, quantity, filled_quantity, quantity AS remaining_quantity, "
        "status, reason, me_reason, me_order_id, hold_asset_id, hold_amount, hold_remaining, created_at_ms, updated_at_ms "
        "FROM oe_orders");
#endif
#ifdef GW_NC_VIEW_COLUMN_RENAMED
    // test build only: a view column renamed
    gwtest::sql(url, "ALTER VIEW gateway_fills_v1 RENAME COLUMN maker TO is_maker");
#endif
    // the views against the reference DDL: names and types
    const std::string ref = gwtest::freshDatabase("pj_compat_ref");
    gwtest::sql(ref, readFile(GW_HISTORY_CONTRACT_SQL));
    for (const char *relation : { "gateway_order_history_v1", "gateway_fills_v1", "gateway_order_trail_v1" })
    {
        const std::string mine = shapeOf(url, relation);
        const std::string theirs = shapeOf(ref, relation);
        CHECK_MSG(mine == theirs, std::string(relation) + ":\n  projected " + mine + "\n  reference " + theirs);
    }
    const std::string hbMine = shapeOf(url, "gateway_history_heartbeat_v1");
    CHECK_MSG(std::string::npos != hbMine.find("beat_at:timestamp with time zone") &&
        std::string::npos != hbMine.find("applied_order_id:bigint") &&
        std::string::npos != hbMine.find("applied_event_seq:bigint") && std::string::npos != hbMine.find("epoch:bigint"),
        hbMine);
    // the expected JSON of the story, from its records (the stream's mapping)
    std::map<std::int64_t, const JournalRecord *> last;
    for (const auto &r : s)
    {
        last[r.event.orderId] = &r;
    }
    PgHistorySettings hs;
    hs.url = url;
    hs.workers = 1;
    PgHistoryStore store(hs);
    auto deadline = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                   .count() + 5'000'000'000LL;
    };
    int answers = 0;
    // no heartbeat yet: every read is unavailable (cause heartbeat)
    {
        HistoryPage page;
        const HistoryAnswer a = store.page(0, history::PageRequest{ history::Kind::ORDERS, 501, {}, std::nullopt, 50 }, page,
            deadline(), HistoryExpect{ EPOCH, 0 });
        CHECK_MSG(HistoryStatus::UNAVAILABLE == a.status && HistoryCause::NO_HEARTBEAT == a.cause, nameOf(a.cause));
        answers++;
    }
    gwtest::sql(url, "UPDATE oe_projection_state SET beat_at = clock_timestamp()"); // the beat is not this test's rule
    bool differs = false;
    for (std::int64_t user : { 500, 501, 502 })
    {
        std::vector<const JournalRecord *> mine;
        for (const auto &[order, r] : last)
        {
            if (r->event.userId == user)
            {
                mine.push_back(r);
            }
        }
        std::sort(mine.begin(), mine.end(), [](auto *a, auto *b) {
            return std::make_pair(a->event.updatedAtMs, a->event.orderId) > std::make_pair(b->event.updatedAtMs,
                b->event.orderId);
        });
        HistoryPage page;
        const HistoryAnswer a = store.page(0, history::PageRequest{ history::Kind::ORDERS, user, {}, std::nullopt, 500 },
            page, deadline(), HistoryExpect{ EPOCH, 0 });
        if (HistoryStatus::OK != a.status)
        {
            CHECK_MSG(false, std::string("projected history differs from the read contract: orders page ") +
                nameOf(a.cause) + " (the gateway's " + nameOf(a.cause) + ")");
            continue;
        }
        answers++;
        CHECK(page.items.size() == mine.size());
        for (std::size_t i = 0; i < std::min(page.items.size(), mine.size()); ++i)
        {
            differs = differs || page.items[i] != rest::orderJson(mine[i]->event);
        }
        for (const JournalRecord *r : mine)
        {
            std::string json;
            HistoryAsOf asOf;
            const HistoryAnswer o = store.order(0, user, r->event.orderId, json, asOf, deadline(),
                HistoryExpect{ EPOCH, 0 });
            answers++;
            differs = differs || HistoryStatus::OK != o.status || json != rest::orderJson(r->event);
        }
        HistoryPage fills;
        const HistoryAnswer f = store.page(0, history::PageRequest{ history::Kind::FILLS, user, {}, std::nullopt, 500 },
            fills, deadline(), HistoryExpect{ EPOCH, 0 });
        if (HistoryStatus::OK != f.status)
        {
            CHECK_MSG(false, std::string("projected history differs from the read contract: fills page ") +
                nameOf(f.cause) + " (the gateway's " + nameOf(f.cause) + ")");
            continue;
        }
        answers++;
        std::vector<const JournalRecord *> userFills;
        for (const auto &r : s)
        {
            if (hasFill(r) && r.event.userId == user)
            {
                userFills.push_back(&r);
            }
        }
        std::sort(userFills.begin(), userFills.end(), [](auto *a, auto *b) {
            return std::make_tuple(a->event.updatedAtMs, a->event.tradeId, a->event.orderId) >
                std::make_tuple(b->event.updatedAtMs, b->event.tradeId, b->event.orderId);
        });
        CHECK(fills.items.size() == userFills.size());
        for (std::size_t i = 0; i < std::min(fills.items.size(), userFills.size()); ++i)
        {
            std::string j = rest::fillJson(userFills[i]->event);
            j.replace(j.find("\"maker\":null"), 12, std::string("\"maker\":") +
                (Liquidity::MAKER == userFills[i]->liquidity ? "true" : "false"));
            differs = differs || fills.items[i] != j;
        }
    }
    if (differs)
    {
        CHECK_MSG(false, "projected history differs from the read contract");
    }
    // LAGGING: an order id above applied_order_id with no row
    std::string json;
    HistoryAsOf asOf;
    const HistoryAnswer lag = store.order(0, 501, 9999, json, asOf, deadline(), HistoryExpect{ EPOCH, 0 });
    CHECK_MSG(HistoryStatus::UNAVAILABLE == lag.status && HistoryCause::LAGGING == lag.cause, nameOf(lag.cause));
    const HistoryAnswer nf = store.order(0, 502, 1, json, asOf, deadline(), HistoryExpect{ EPOCH, 0 }); // another user's order: not found
    CHECK(HistoryStatus::NOT_FOUND == nf.status);
    answers += 2;
    std::printf("  compatibility: %d answers compared\n", answers);
    CHECK(answers > 0);
}

TEST(reference_equivalence)
{
    // the same logical rows: the reference tables seeded directly, the projection fed the records they come from
    const std::string ref = gwtest::freshDatabase("pj_equiv_ref");
    gwtest::sql(ref, readFile(GW_HISTORY_CONTRACT_SQL));
    const std::string url = gwtest::freshDatabase("pj_equiv");
    Rig rig(url);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    const std::vector<JournalRecord> s = story();
    for (const auto &r : s)
    {
        rig.push(Item::Kind::APPLY, r);
    }
    CHECK(Hold::HELD == rig.commit());
    rig.release();
    std::map<std::int64_t, const JournalRecord *> last;
    for (const auto &r : s)
    {
        last[r.event.orderId] = &r;
    }
    auto lit = [](const std::optional<std::string> &v, const char *cast) {
        return v ? "'" + *v + "'" + cast : std::string("NULL");
    };
    for (const auto &[order, r] : last)
    {
        Columns c = orderColumns(*r);
#ifdef GW_NC_SHIFTED_COLUMN
        std::swap(c[10], c[11]); // test build only: filled and remaining quantity shifted
#endif
        gwtest::sql(ref, "INSERT INTO gateway_order_history_v1 (order_id, user_id, client_order_id, market_id, side, type, "
            "time_in_force, price, stop_price, quantity, filled_quantity, remaining_quantity, status, reason, me_reason, "
            "me_order_id, hold_asset_id, hold_amount, hold_remaining, created_at_ms, updated_at_ms) VALUES (" +
            *c[0] + "," + *c[1] + "," + lit(c[2], "::uuid") + "," + *c[3] + ",'" + *c[4] + "','" + *c[5] + "','" + *c[6] +
            "'," + lit(c[7], "::bigint") + "," + lit(c[8], "::bigint") + "," + *c[9] + "," + *c[10] + "," + *c[11] + ",'" +
            *c[12] + "'," + lit(c[13], "") + "," + lit(c[14], "::smallint") + "," + lit(c[15], "::bigint") + "," + *c[16] +
            "," + *c[17] + "," + *c[18] + "," + *c[19] + "," + *c[20] + ")");
    }
    for (const auto &r : s)
    {
        if (hasFill(r))
        {
            const Columns c = fillColumns(r);
            gwtest::sql(ref, "INSERT INTO gateway_fills_v1 (trade_id, order_id, user_id, market_id, side, price, quantity, "
                "maker, executed_at_ms) VALUES (" + *c[0] + "," + *c[1] + "," + *c[2] + "," + *c[3] + ",'" + *c[4] + "'," +
                *c[5] + "," + *c[6] + "," + *c[7] + "," + *c[8] + ")");
        }
    }
    int cases = 0;
    for (const auto &[order, r] : last)
    {
        cases++;
        if (rowJson(ref, true, r->event.userId, order) != rowJson(url, true, r->event.userId, order) ||
            rowJson(ref, false, r->event.userId, order) != rowJson(url, false, r->event.userId, order))
        {
            CHECK_MSG(false, "projected history differs from the reference DDL (order " + std::to_string(order) + ")");
        }
    }
    std::printf("  equivalence: %d orders compared (the reference suite's own seeds are not records: not compared)\n",
        cases);
}

namespace {

std::string beatAt(const std::string &url)
{
    return one(url, "SELECT coalesce(beat_at::text, 'null') FROM oe_projection_state");
}

}

TEST(beat_rule)
{
    const std::string url = gwtest::freshDatabase("pj_beat");
    Rig rig(url);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    const std::vector<JournalRecord> s = story();
    for (const auto &r : s)
    {
        rig.push(Item::Kind::APPLY, r);
    }
    CHECK(Hold::HELD == rig.commit());
    const std::int64_t n = static_cast<std::int64_t>(s.size());
    CHECK_MSG("null" == beatAt(url), "a beat before any probe: " + beatAt(url));
    // the head applied, the genesis the projection's: a beat, as of the probe's send time on the database's clock
    bool wrote = false;
    rig.beat(n, EPOCH, 400);
    CHECK(Hold::HELD == rig.commit({}, {}, true, &wrote));
    const std::int64_t ageMs = num(url, "SELECT (extract(epoch FROM clock_timestamp() - beat_at) * 1000)::bigint FROM "
        "oe_projection_state");
    CHECK_MSG(wrote && ageMs >= 400 && ageMs < 3000, "beat at the head: written " + std::to_string(wrote) + ", age " +
        std::to_string(ageMs) + " ms");
    CHECK_MSG(n == num(url, "SELECT head_seq FROM oe_projection_state"), "head_seq " + beatAt(url));
    CHECK_MSG(1 == num(url, "SELECT count(*) FROM gateway_history_heartbeat_v1 WHERE applied_event_seq = " +
        std::to_string(n) + " AND epoch = " + std::to_string(EPOCH)), "the heartbeat view row");
    std::string last = beatAt(url);
    bool fresh = true;     // false once a beat is written without a fresh head
    bool sameGenesis = true;
    auto noBeat = [&](const char *what, bool &flag) {
        const std::string now = beatAt(url);
        if (now != last)
        {
            flag = false;
            CHECK_MSG(false, std::string(what) + ": beat_at moved from " + last + " to " + now);
        }
        last = now;
    };
    // a head above the cursor: no beat, whatever else holds
    rig.beat(n + 5, EPOCH);
    CHECK(Hold::HELD == rig.commit({}, {}, true, &wrote));
    noBeat("applied below head", fresh);
    // a renewal alone (an idle writer): no beat
    {
        ProjectionState st;
        std::string error;
        CHECK(Hold::HELD == rig.lease.renew(rig.conn, T, st, error));
    }
    noBeat("renewal without a probe", fresh);
    // an older probe than the last beat: beat_at never moves back
    rig.beat(n, EPOCH, 5000);
    CHECK(Hold::HELD == rig.commit());
    noBeat("older probe", fresh);
    // another genesis, its head reached (a projection of an old genesis is far above a new one's head)
    rig.beat(1, EPOCH + 1);
    CHECK(Hold::HELD == rig.commit());
    noBeat("probe of another genesis", sameGenesis);
    // the projection halted
    Item halt;
    halt.kind = Item::Kind::HALT;
    halt.halt = HaltState{ Halt::GAP, "test" };
    rig.pushItem(halt);
    CHECK(Hold::HELD == rig.commit());
    last = beatAt(url);
    rig.beat(n, EPOCH);
    CHECK(Hold::HELD == rig.commit());
    noBeat("halted", fresh);
    // halted EPOCH_MISMATCH, a probe of the new genesis whose head the old cursor is far above
    halt.halt = HaltState{ Halt::EPOCH_MISMATCH, "test" };
    rig.pushItem(halt);
    CHECK(Hold::HELD == rig.commit());
    last = beatAt(url);
    rig.beat(2, EPOCH + 1);
    CHECK(Hold::HELD == rig.commit());
    noBeat("EPOCH_MISMATCH", sameGenesis);
    // the halt cleared: the projection's own genesis beats again
    halt.halt = HaltState{};
    rig.pushItem(halt);
    CHECK(Hold::HELD == rig.commit());
    rig.beat(n, EPOCH);
    CHECK(Hold::HELD == rig.commit({}, {}, true, &wrote));
    CHECK_MSG(wrote && beatAt(url) != last, "no beat after the halt cleared");
    rig.release();
    if (!fresh)
    {
        CHECK_MSG(false, "beat without a fresh head");
    }
    if (!sameGenesis)
    {
        CHECK_MSG(false, "beat across genesis ids");
    }
}

TEST(rebuild_reads)
{
    const std::string url = gwtest::freshDatabase("pj_rebuild_reads");
    Rig rig(url);
    CHECK(rig.start());
    rig.epoch(EPOCH);
    const std::vector<JournalRecord> s = story();
    for (const auto &r : s)
    {
        rig.push(Item::Kind::APPLY, r);
    }
    rig.beat(static_cast<std::int64_t>(s.size()), EPOCH);
    CHECK(Hold::HELD == rig.commit());
    const std::int64_t user = 501;
    std::set<std::int64_t> mine;
    for (const auto &r : s)
    {
        if (r.event.userId == user)
        {
            mine.insert(r.event.orderId);
        }
    }
    auto mono = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count();
    };
    struct Read
    {
        std::int64_t startNs = 0;
        std::int64_t endNs = 0;
        bool page = true;
        HistoryAnswer answer;
        std::size_t items = 0;
        std::int64_t ageMs = 0;
    };
    std::vector<Read> reads;
    std::atomic<bool> reading{ true };
    std::thread reader([&] {
        PgHistorySettings hs;
        hs.url = url;
        hs.workers = 1;
        PgHistoryStore store(hs);
        std::size_t k = 0;
        while (reading.load())
        {
            Read r;
            r.startNs = mono();
            r.page = 0 == k % 2;
            if (r.page)
            {
                HistoryPage page;
                r.answer = store.page(0, history::PageRequest{ history::Kind::ORDERS, user, {}, std::nullopt, 500 }, page,
                    mono() + 5'000'000'000LL, HistoryExpect{ EPOCH, 0 });
                r.items = page.items.size();
                r.ageMs = page.ageMs;
            }
            else
            {
                std::string json;
                HistoryAsOf asOf;
                const auto it = std::next(mine.begin(), static_cast<long>(k / 2 % mine.size()));
                r.answer = store.order(0, user, *it, json, asOf, mono() + 5'000'000'000LL, HistoryExpect{ EPOCH, 0 });
            }
            r.endNs = mono();
            reads.push_back(r);
            k++;
            sleepMs(2);
        }
    });
    sleepMs(200);
    // the rebuild (its TRUNCATE waits behind the reads, bounded; run again until it commits)
    const std::int64_t rebuildStart = mono();
    Hold h = Hold::FAILED;
    for (int attempt = 0; attempt < 20 && Hold::HELD != h; ++attempt)
    {
        std::string error;
        h = rig.lease.rebuild(rig.conn, T, 1000, error);
        if (Hold::HELD != h)
        {
            std::printf("  rebuild attempt %d: %s\n", attempt, error.c_str());
            if (!rig.conn.ok())
            {
                rig.connect();
            }
        }
    }
    CHECK(Hold::HELD == h);
    const std::int64_t rebuildDone = mono();
    // the re-projection: the writer reads the reset row and starts over, in several batches, then beats at the head
    CHECK(rig.reassert());
    CHECK(Hold::HELD == rig.commit());
    CHECK(rig.loadGeneration());
    rig.epoch(EPOCH);
    CHECK(Hold::HELD == rig.commit());
    for (std::size_t i = 0; i < s.size(); ++i)
    {
        rig.push(Item::Kind::APPLY, s[i]);
        if (0 == i % 10)
        {
            CHECK(Hold::HELD == rig.commit());
            sleepMs(20);
        }
    }
    CHECK(Hold::HELD == rig.commit());
    sleepMs(100);
    const std::int64_t beatStart = mono();
    rig.beat(static_cast<std::int64_t>(s.size()), EPOCH);
    bool wrote = false;
    CHECK(Hold::HELD == rig.commit({}, {}, true, &wrote));
    CHECK(wrote);
    const std::int64_t beatDone = mono();
    sleepMs(200);
    reading = false;
    reader.join();
    rig.release();
    int during = 0;
    int after = 0;
    int servedEarly = 0;
    int otherCause = 0;
    int notFound = 0;
    int incompleteAfter = 0;
    for (const Read &r : reads)
    {
        if (r.startNs >= rebuildDone && r.endNs <= beatStart)
        {
            during++;
            if (HistoryStatus::NOT_FOUND == r.answer.status)
            {
                notFound++;
            }
            else if (HistoryStatus::OK == r.answer.status)
            {
                servedEarly++; // a page or an order of a projection that has not reached the head
            }
            else if (HistoryCause::NO_HEARTBEAT != r.answer.cause)
            {
                otherCause++;
            }
        }
        else if (r.startNs >= beatDone)
        {
            after++;
            const bool complete = HistoryStatus::OK == r.answer.status && (!r.page || r.items == mine.size());
            incompleteAfter += complete ? 0 : 1;
        }
        else if (r.endNs <= rebuildStart && r.page)
        {
            CHECK_MSG(HistoryStatus::OK == r.answer.status && r.items == mine.size(), "before the rebuild");
        }
    }
    std::printf("  rebuild: %d reads inside the rebuild, %d after the beat\n", during, after);
    CHECK_MSG(during >= 20 && after >= 10, "reads inside " + std::to_string(during) + ", after " + std::to_string(after));
    CHECK_MSG(0 == notFound, std::to_string(notFound) + " reads answered 404 for an order that existed");
    CHECK_MSG(0 == otherCause, std::to_string(otherCause) + " reads inside the rebuild answered 503 with a cause other "
        "than heartbeat");
    if (servedEarly > 0 || notFound > 0)
    {
        CHECK_MSG(false, "rebuilding history served as complete: " + std::to_string(servedEarly) + " reads answered "
            "with rows and " + std::to_string(notFound) + " with 404 between the truncate and the first beat");
    }
    CHECK_MSG(0 == incompleteAfter, std::to_string(incompleteAfter) + " reads after the beat were not complete");
}

// The schema applied by an owner role without CREATEROLE: with oe_history_reader absent the apply fails and names the
// role; with the role created beforehand by an administrator it succeeds and grants exactly the four read relations.
TEST(reader_role_prepared)
{
    const std::string admin = gwtest::env("GW_TEST_PG_ADMIN_URL");
    CHECK_MSG(gwtest::sql(admin, "SELECT count(*) FROM pg_roles WHERE rolname = 'oe_history_reader'") == "0",
        "oe_history_reader exists before the case: the case needs a server of its own");
    gwtest::sql(admin, "CREATE ROLE pj_owner LOGIN PASSWORD 'pj-owner' NOCREATEROLE NOSUPERUSER");
    auto ownedBy = [&](const std::string &name) {
        const std::string url = gwtest::freshDatabase(name);
        gwtest::sql(admin, "ALTER DATABASE " + name + " OWNER TO pj_owner");
        return "postgresql://pj_owner:pj-owner" + url.substr(url.find('@'));
    };
    auto apply = [&](const std::string &url, std::string &detail) {
        PgConn conn;
        PgOptions o;
        o.url = url;
        o.statementTimeoutMs = T;
        std::string error;
        if (!connectProjection(conn, o, error))
        {
            detail = "connect: " + error;
            return SchemaVerdict::FAILED;
        }
        return migrate(conn, T, detail);
    };
    {
        std::string detail;
        const SchemaVerdict v = apply(ownedBy("pj_role_absent"), detail);
        // a refusal (the process exits; no retry loop), not a transient failure
        CHECK_MSG(SchemaVerdict::FOREIGN == v, "the apply without CREATEROLE and without the role: " + detail);
        CHECK_MSG(std::string::npos != detail.find("oe_history_reader") &&
            std::string::npos != detail.find("CREATEROLE"), "the refusal does not name the role: " + detail);
        CHECK(gwtest::sql(admin, "SELECT count(*) FROM pg_roles WHERE rolname = 'oe_history_reader'") == "0");
        std::printf("  refused: %s\n", detail.c_str());
    }
    gwtest::sql(admin, "CREATE ROLE oe_history_reader NOLOGIN");
    const std::string url = ownedBy("pj_role_prepared");
    std::string detail;
    const SchemaVerdict v = apply(url, detail);
    CHECK_MSG(SchemaVerdict::APPLIED == v, "the apply with the role created beforehand: " + detail);
    const std::string base = gwtest::env("GW_TEST_PG_URL");
    const std::string superUrl = base.substr(0, base.rfind('/') + 1) + "pj_role_prepared";
    const std::string select = gwtest::sql(superUrl, "SELECT coalesce(string_agg(c.relname, ',' ORDER BY c.relname), "
        "'') FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND "
        "has_table_privilege('oe_history_reader', c.oid, 'SELECT')");
    CHECK_MSG(select == "gateway_fills_v1,gateway_history_heartbeat_v1,gateway_order_history_v1,gateway_order_trail_v1",
        "the reader can SELECT: " + select);
    const std::string other = gwtest::sql(superUrl, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = "
        "c.relnamespace WHERE n.nspname = 'public' AND (has_table_privilege('oe_history_reader', c.oid, 'INSERT') OR "
        "has_table_privilege('oe_history_reader', c.oid, 'UPDATE') OR has_table_privilege('oe_history_reader', c.oid, "
        "'DELETE') OR has_table_privilege('oe_history_reader', c.oid, 'TRUNCATE'))");
    CHECK_MSG(other == "0", "the reader can write " + other + " relations");
}

namespace {

// The story applied into an empty database in batches of 16 (many transactions), every batch committed.
bool projectStory(const std::string &url)
{
    Rig rig(url, 16);
    if (!rig.start())
    {
        return false;
    }
    rig.epoch(EPOCH);
    for (const auto &r : story())
    {
        rig.push(Item::Kind::APPLY, r);
    }
    while (rig.ex.pending())
    {
        if (Hold::HELD != rig.commit())
        {
            gwtest::fail(__FILE__, __LINE__, "a batch did not commit: " + rig.lastError);
            return false;
        }
    }
    rig.release();
    return true;
}

// SHA-256 of a relation's rows in key order, every column (`full`), or every column but projected_at
std::string dumpSha(const std::string &url, const std::string &table, const std::string &key, bool full)
{
    const std::string row = full ? "t::text" : "(to_jsonb(t) - 'projected_at')::text";
    return one(url, "SELECT encode(sha256(convert_to(coalesce(string_agg(" + row + ", E'\\n' ORDER BY " + key +
        "), ''), 'UTF8')), 'hex') FROM " + table + " t");
}

}

// What the fixture's dump checks: oe_order_events dense from 1 to the cursor. A record acknowledged and not written
// leaves a journalSeq without its row.
TEST(projection_dense)
{
    const std::string url = gwtest::freshDatabase("pj_proj_dense");
    CHECK(projectStory(url));
    const std::int64_t applied = num(url, "SELECT applied_seq FROM oe_projection_state");
    CHECK_MSG(applied == static_cast<std::int64_t>(story().size()), "applied_seq " + std::to_string(applied) +
        " after the story of " + std::to_string(story().size()));
    const std::string missing = one(url, "SELECT coalesce(string_agg(s::text, ',' ORDER BY s), '') FROM "
        "generate_series(1, (SELECT applied_seq FROM oe_projection_state)) s WHERE NOT EXISTS (SELECT 1 FROM "
        "oe_order_events e WHERE e.journal_seq = s)");
    if (!missing.empty())
    {
        CHECK_MSG(false, "journalSeq missing in projection: " + missing + " (applied_seq " + std::to_string(applied) +
            ")");
    }
    checkDense(url, static_cast<std::int64_t>(story().size()), "story in batches of 16");
}

// Two projections of one journal are byte equal (what the fixture hashes): no column depends on when it was written.
TEST(projection_byte_equal)
{
    const std::string a = gwtest::freshDatabase("pj_bytes_a");
    const std::string b = gwtest::freshDatabase("pj_bytes_b");
    CHECK(projectStory(a));
    sleepMs(50);
    CHECK(projectStory(b));
    struct Relation
    {
        const char *table;
        const char *key;
    };
    for (const Relation &r : { Relation{ "oe_order_events", "journal_seq" }, Relation{ "oe_orders", "order_id" },
             Relation{ "oe_fills", "trade_id, order_id" } })
    {
        const std::string fa = dumpSha(a, r.table, r.key, true);
        const std::string fb = dumpSha(b, r.table, r.key, true);
        if (fa != fb)
        {
            const bool onlyWallClock = dumpSha(a, r.table, r.key, false) == dumpSha(b, r.table, r.key, false);
            CHECK_MSG(false, std::string("projection dump differs: ") + r.table + " (" + fa + " != " + fb + ")" +
                (onlyWallClock ? "; without the column projected_at the dumps are equal" : ""));
        }
        else
        {
            std::printf("%s dump sha256 %s in both projections\n", r.table, fa.c_str());
        }
    }
}
