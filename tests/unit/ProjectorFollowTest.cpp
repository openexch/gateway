// oe-projector T0: the follow rules and the record mapping, pure (no Aeron, no PostgreSQL):
//   forward_rule_table               below, equal, next and above the cursor; memo and memo-free reads; BELOW_RANGE and
//                                    HOLE only when memo-free, each with its range
//   record_refuses_unnamed_enums     every enum field of the record and liquidity 3: refused, named, no row columns
//   record_round_trip                encodeRecord and decodeRecord are inverse; epoch 0 and seq 0 are refused
//   one_mapping_order_and_fill_json  the history reader's JSON of the projected row equals the stream's JSON of the event
//   source_choice_avoids_the_leader  followers first, a member of unknown role next, the leader last; a full budget skips
//   leader_waits_for_a_backing_off_follower   a follower only backing off holds the leader back; one left unreachable,
//                                    marked or with its budget used up does not
//   backoff_doubles_to_its_cap       the wait of a left source doubles up to the cap and resets on progress
//   behind_window                    BEHIND for a silent source and for one that does not close the lag; not for one
//                                    that only re-reads (compares) history the projection holds; not otherwise
//   halt_names_the_gap               GAP from the cursor + 1 to the smallest missing end; conflict and schema first
//   follow_fuzz_recovers             property: random sources with holes, late fixes, conflicts and writer failures;
//                                    the applied prefix is dense and equals the canonical bytes; recoveries > 0
// Each control build (tests/CMakeLists.txt) compiles one rule off and must fail its case with the named text.
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "com_openexchange_core_infrastructure_generated/OrderEvent.h"

#include "Check.h"
#include "gateway/common/History.h"
#include "gateway/common/Rest.h"
#include "FollowRules.h"
#include "JournalRecord.h"
#include "ProjectionStore.h"

using namespace openexchange::gateway;
using namespace openexchange::gateway::projector;
namespace money = wire::money;

namespace {

JournalRecord base(std::int64_t seq, std::int64_t orderId)
{
    JournalRecord r;
    r.seq = seq;
    r.epoch = 7;
    r.logPosition = 1000 + seq * 64;
    wire::OrderEvent &e = r.event;
    e.eventSeq = seq;
    e.logPosition = r.logPosition;
    e.orderId = orderId;
    e.userId = 42;
    e.marketId = 3;
    e.eventKind = money::EventKind::ACCEPTED;
    e.status = money::OrderStatus::NEW;
    e.reason = money::CoreRejectReason::NONE;
    e.side = money::Side::BUY;
    e.orderType = money::OrderType::LIMIT;
    e.timeInForce = money::TimeInForce::GTC;
    e.price = 101'00000000;
    e.quantity = 2'00000000;
    e.remainingQty = 2'00000000;
    e.holdAssetId = 2;
    e.holdAmount = 202'00000000;
    e.holdRemaining = 202'00000000;
    e.createdAtMs = 1'700'000'000'000 + orderId;
    e.updatedAtMs = e.createdAtMs + seq;
    e.meReason = 255;
    return r;
}

// One story of every record shape the mapping has a rule for.
std::vector<JournalRecord> story()
{
    std::vector<JournalRecord> out;
    JournalRecord a = base(1, 10);
    a.event.clientOrderIdHigh = 0x0123456789abcdefLL;
    a.event.clientOrderIdLow = 0x0fedcba987654321LL;
    out.push_back(a);
    JournalRecord f = base(2, 10);
    f.event.eventKind = money::EventKind::FILL;
    f.event.status = money::OrderStatus::PARTIALLY_FILLED;
    f.event.filledQty = 1'00000000;
    f.event.remainingQty = 1'00000000;
    f.event.lastPrice = 100'50000000;
    f.event.lastQty = 1'00000000;
    f.event.tradeId = 900;
    f.event.meOrderId = 5555;
    f.event.holdRemaining = 101'00000000;
    f.liquidity = Liquidity::MAKER;
    f.meEpoch = 7;
    f.meJournalSeq = 31;
    f.meLogPosition = 4096;
    f.meTimestampMs = 1'700'000'000'500;
    out.push_back(f);
    JournalRecord m = base(3, 11); // a market order: no price
    m.event.orderType = money::OrderType::MARKET;
    m.event.timeInForce = money::TimeInForce::IOC;
    m.event.price = 0;
    m.event.side = money::Side::SELL;
    out.push_back(m);
    JournalRecord s = base(4, 12); // a stop limit with a stop price
    s.event.orderType = money::OrderType::STOP_LIMIT;
    s.event.stopPrice = 99'00000000;
    s.event.status = money::OrderStatus::PENDING_TRIGGER;
    out.push_back(s);
    JournalRecord j = base(5, 13); // rejected with a reason, an ME reason
    j.event.eventKind = money::EventKind::REJECTED;
    j.event.status = money::OrderStatus::REJECTED;
    j.event.reason = money::CoreRejectReason::ME_REJECTED;
    j.event.meReason = 4;
    j.event.holdRemaining = 0;
    out.push_back(j);
    JournalRecord z = base(6, 11); // a FILL with tradeId 0 (no fills row)
    z.event.eventKind = money::EventKind::FILL;
    z.event.orderType = money::OrderType::MARKET;
    z.event.price = 0;
    z.event.status = money::OrderStatus::CANCELLED;
    out.push_back(z);
    return out;
}

history::Row orderRow(const JournalRecord &r)
{
    const Columns c = orderColumns(r);
    history::Row row;
    for (std::size_t i = 0; i < 21; ++i)
    {
        if (1 != i) // user_id is not a column of the order statement
        {
            row.push_back(c[i]);
        }
    }
    return row;
}

history::Row fillRow(const JournalRecord &r)
{
    const Columns c = fillColumns(r);
    // trade_id, order_id, market_id, side, price, quantity, maker, executed_at_ms
    return { c[0], c[1], c[3], c[4], c[5], c[6], std::string("true" == *c[7] ? "t" : "f"), c[8] };
}

}

TEST(forward_rule_table)
{
    CHECK(Forward::COMPARE == forwardRule(3, 6, true, false).kind);  // below
    CHECK(Forward::COMPARE == forwardRule(5, 6, false, false).kind); // equal to the cursor
    CHECK(Forward::APPLY == forwardRule(6, 6, true, false).kind);    // next
    CHECK(Forward::APPLY == forwardRule(6, 6, false, true).kind);
    const ForwardVerdict below = forwardRule(9, 6, true, true);
    CHECK(Forward::BELOW_RANGE == below.kind && 6 == below.from && 8 == below.to);
    const ForwardVerdict hole = forwardRule(9, 6, true, false);
    CHECK(Forward::HOLE == hole.kind && 6 == hole.from && 8 == hole.to);
    // a jump on a read from the memo is never believed: the source is read again from its chain start
    for (bool first : { true, false })
    {
        const ForwardVerdict memo = forwardRule(9, 6, false, first);
        if (Forward::RE_READ != memo.kind)
        {
            CHECK_MSG(false, "hole believed from a memo read");
        }
    }
}

TEST(record_round_trip)
{
    for (const JournalRecord &r : story())
    {
        const Payload p = encodeRecord(r);
        JournalRecord back;
        std::string why;
        CHECK_MSG(decodeRecord(p.data(), p.size(), back, why), why);
        CHECK(back.seq == r.seq && back.epoch == r.epoch && back.logPosition == r.logPosition);
        wire::OrderEvent expected = r.event;
        expected.eventSeq = r.seq;
        expected.logPosition = r.logPosition;
        CHECK(back.event == expected);
        CHECK(back.liquidity == r.liquidity && back.meEpoch == r.meEpoch && back.meJournalSeq == r.meJournalSeq);
        CHECK(back.meLogPosition == r.meLogPosition && back.meTimestampMs == r.meTimestampMs);
        CHECK(back.payload == p);
    }
    JournalRecord e = base(1, 10);
    e.epoch = 0;
    Payload p = encodeRecord(e);
    JournalRecord out;
    std::string why;
    CHECK(!decodeRecord(p.data(), p.size(), out, why) && std::string::npos != why.find("epoch 0"));
    e = base(1, 10);
    e.seq = 0;
    p = encodeRecord(e);
    CHECK(!decodeRecord(p.data(), p.size(), out, why) && std::string::npos != why.find("below 1"));
    p = encodeRecord(base(1, 10));
    p[4] = 4; // schema 4 (the ME journal)
    CHECK(!decodeRecord(p.data(), p.size(), out, why) && std::string::npos != why.find("schema 4"));
    CHECK(!decodeRecord(p.data(), 215, out, why));
}

TEST(record_refuses_unnamed_enums)
{
    // message offsets of the enum fields
    struct Bad
    {
        const char *field;
        std::size_t offset;
        std::uint8_t value;
    };
    auto at = [](std::size_t blockOffset) { return 32 + blockOffset - 16; }; // the OrderEvent body starts at 32
    const std::vector<Bad> bad = {
        { "eventKind", at(money::OrderEvent::eventKindEncodingOffset()), 37 },
        { "status", at(money::OrderEvent::statusEncodingOffset()), 40 },
        { "reason", at(money::OrderEvent::reasonEncodingOffset()), 200 },
        { "side", at(money::OrderEvent::sideEncodingOffset()), 9 },
        { "orderType", at(money::OrderEvent::orderTypeEncodingOffset()), 77 },
        { "timeInForce", at(money::OrderEvent::timeInForceEncodingOffset()), 66 },
        { "liquidity", 183, 3 },
    };
    for (const Bad &b : bad)
    {
        Payload p = encodeRecord(base(1, 10));
        p[b.offset] = b.value;
        JournalRecord out;
        std::string why;
        if (decodeRecord(p.data(), p.size(), out, why))
        {
            CHECK_MSG(false, std::string("row written with an unnamed enum value (") + b.field + ")");
            continue;
        }
        CHECK_MSG(std::string::npos != why.find(b.field), why);
    }
}

TEST(one_mapping_order_and_fill_json)
{
    for (const JournalRecord &r : story())
    {
        JournalRecord d;
        std::string why;
        const Payload p = encodeRecord(r);
        CHECK(decodeRecord(p.data(), p.size(), d, why));
        std::string fromRow;
        history::Key key;
        std::string problem;
        CHECK_MSG(history::orderJson(orderRow(d), fromRow, key, problem), problem);
        const std::string live = rest::orderJson(d.event);
        if (fromRow != live)
        {
            CHECK_MSG(false, "history row renders differently from the stream:\n  row  " + fromRow + "\n  live " + live);
        }
        if (hasFill(d))
        {
            std::string fill;
            CHECK_MSG(history::fillJson(fillRow(d), fill, key, problem), problem);
            std::string liveFill = rest::fillJson(d.event);
            const std::string maker = Liquidity::MAKER == d.liquidity ? "true" : "false";
            liveFill.replace(liveFill.find("\"maker\":null"), 12, "\"maker\":" + maker);
            if (fill != liveFill)
            {
                CHECK_MSG(false, "history row renders differently from the stream:\n  row  " + fill + "\n  live " +
                    liveFill);
            }
        }
    }
    std::vector<JournalRecord> s = story();
    CHECK(hasFill(s[1]) && !hasFill(s[5]) && !hasFill(s[0]));
}

TEST(source_choice_avoids_the_leader)
{
    std::vector<SourceView> v(3);
    v[0].role = NodeRole::LEADER;
    v[1].role = NodeRole::UNKNOWN;
    v[2].role = NodeRole::FOLLOWER;
    auto pick = chooseSource(v, 0, std::nullopt);
    if (!pick || 2 != *pick)
    {
        CHECK_MSG(false, "the leader's archive read while a follower could be");
    }
    v[2].blockedUntilNs = 100; // the follower backs off: a member of unknown role before the leader
    pick = chooseSource(v, 50, std::nullopt);
    CHECK(pick && 1 == *pick);
    v[1].mark = Mark::HOLE; // the leader only when nothing else can be read
    v[2].unreachable = true;
    pick = chooseSource(v, 50, std::nullopt);
    CHECK(pick && 0 == *pick);
    v[2].unreachable = false;
    CHECK(betterSourceAvailable(v, 200, 0)); // the follower is back: the leader is left for it
    CHECK(!betterSourceAvailable(v, 200, 2));
    // the reader replay budget: a full member is skipped unless it is the one attached
    v[2].replaysActive = 8;
    v[2].readerBudget = 8;
    CHECK(budgetFull(v[2]));
    pick = chooseSource(v, 200, std::nullopt);
    CHECK(pick && 0 == *pick);
    pick = chooseSource(v, 200, std::size_t{ 2 });
    CHECK(pick && 2 == *pick);
    v[2].replaysActive = -1; // the member does not count: unknown, not full
    CHECK(!budgetFull(v[2]));
    // a journal replica is never the leader
    std::vector<SourceView> r(2);
    r[0].role = NodeRole::LEADER;
    r[1].role = NodeRole::NOT_A_MEMBER;
    pick = chooseSource(r, 0, std::nullopt);
    CHECK(pick && 1 == *pick);
}

TEST(leader_waits_for_a_backing_off_follower)
{
    std::vector<SourceView> v(3);
    v[0].role = NodeRole::LEADER;
    v[1].role = NodeRole::FOLLOWER;
    v[2].role = NodeRole::FOLLOWER;
    // both followers back off (left BEHIND, say): reachable, so the leader is not read; the follower is waited for
    v[1].blockedUntilNs = 100;
    v[2].blockedUntilNs = 200;
    auto pick = chooseSource(v, 50, std::nullopt);
    if (pick && 0 == *pick)
    {
        CHECK_MSG(false, "the leader's archive read while a follower could be");
    }
    CHECK(!pick);
    CHECK(!betterSourceAvailable(v, 50, 0));
    pick = chooseSource(v, 150, std::nullopt); // the first back-off ended
    CHECK(pick && 1 == *pick);
    // one follower marked, the other only backing off: still waited for
    v[1].mark = Mark::PAYLOAD_CONFLICT;
    pick = chooseSource(v, 150, std::nullopt);
    CHECK(!pick);
    // the other follower left because its archive could not be reached: the leader may be read
    v[2].unreachable = true;
    pick = chooseSource(v, 150, std::nullopt);
    CHECK(pick && 0 == *pick);
    // its reader replay budget used up instead: the leader may be read too
    v[2].unreachable = false;
    v[2].replaysActive = 2;
    v[2].readerBudget = 2;
    pick = chooseSource(v, 150, std::nullopt);
    CHECK(pick && 0 == *pick);
    // a member of unknown role that only backs off holds the leader back as well
    std::vector<SourceView> u(2);
    u[0].role = NodeRole::LEADER;
    u[1].role = NodeRole::UNKNOWN;
    u[1].blockedUntilNs = 100;
    CHECK(!chooseSource(u, 50, std::nullopt));
}

TEST(backoff_doubles_to_its_cap)
{
    Backoff b(1000, 4000);
    const std::int64_t ms = 1'000'000;
    std::int64_t now = 0;
    std::vector<std::int64_t> waits;
    for (int i = 0; i < 5; ++i)
    {
        const std::int64_t until = b.leave(now);
        waits.push_back((until - now) / ms);
        now = until;
    }
    // requests to one source over a minute stay under the back-off's rate (a cap of 4 s: at most 15 + 1 a minute)
    if (waits != std::vector<std::int64_t>{ 1000, 2000, 4000, 4000, 4000 })
    {
        CHECK_MSG(false, "replay requests flood the source archive");
    }
    b.progress();
    CHECK(1000 * ms == b.leave(0));
}

// One candidate per answered probe while the cursor is below its head: a stall of an hour at one probe a second must
// not grow without bound. At the cap the oldest goes (the beat waits for a newer probe; none is ever made up).
TEST(beat_candidates_capped)
{
    std::deque<BeatCandidate> beats;
    std::size_t dropped = 0;
    for (std::int64_t i = 1; i <= 3600; ++i)
    {
        dropped += addBeatCandidate(beats, BeatCandidate{ 100 + i, 7, i * 1'000'000'000LL }, 4);
    }
    if (beats.size() > 4)
    {
        CHECK_MSG(false, "beat candidates grow without bound: " + std::to_string(beats.size()));
    }
    CHECK(3596 == dropped);
    CHECK(!beats.empty() && 3700 == beats.back().head && 3697 == beats.front().head);
    // the same head and genesis again: the last one is replaced by the newer send time, nothing is dropped
    CHECK(0 == addBeatCandidate(beats, BeatCandidate{ 3700, 7, 9 }, 4));
    CHECK(4 == beats.size() && 9 == beats.back().sentAtNs);
    // a cap of one keeps the newest only
    std::deque<BeatCandidate> one;
    addBeatCandidate(one, BeatCandidate{ 1, 7, 1 }, 1);
    CHECK(1 == addBeatCandidate(one, BeatCandidate{ 2, 7, 2 }, 1));
    CHECK(1 == one.size() && 2 == one.front().head);
}

TEST(behind_window)
{
    const std::int64_t ms = 1'000'000;
    // a silent source: the head stays above the cursor, nothing delivered
    BehindWindow w(100);
    w.restart(0);
    w.onProbe(50, 10, 10);
    w.onProbe(60, 10, 10);
    CHECK(!w.behind(50 * ms, 60, 10));
    CHECK(w.behind(100 * ms, 60, 10));
    // a slow source: it delivers, but the lag does not shrink
    w.restart(0);
    w.onProbe(50, 10, 10);
    w.onDelivered();
    w.onProbe(90, 30, 30);
    if (!w.behind(100 * ms, 90, 30))
    {
        CHECK_MSG(false, "attached to a lagging source while another was current");
    }
    // a source closing the lag is not behind; nor one at the head; nor one with no answered probe
    w.restart(0);
    w.onProbe(50, 10, 10);
    w.onDelivered();
    CHECK(!w.behind(100 * ms, 55, 50));
    w.restart(0);
    w.onProbe(10, 10, 10);
    CHECK(!w.behind(100 * ms, 10, 10));
    w.restart(0);
    CHECK(!w.behind(100 * ms, 60, 10));
    // a source re-reading the overlap from its chain start: the cursor stays at 100 while its compares are verified
    // and its read position climbs; over the window it is not BEHIND, over two windows neither
    w.restart(0);
    w.onProbe(250, 100, 0);
    w.onDelivered();
    w.onProbe(250, 100, 40);
    if (w.behind(100 * ms, 250, 40))
    {
        CHECK_MSG(false, "BEHIND left a source that was comparing the overlap");
    }
    w.onProbe(250, 100, 41);
    w.onDelivered();
    w.onProbe(250, 100, 90);
    CHECK(!w.behind(200 * ms, 250, 90));
}

TEST(halt_names_the_gap)
{
    std::vector<SourceView> v(2);
    v[0].mark = Mark::BELOW_RANGE;
    v[0].missingFrom = 11;
    v[0].missingTo = 40;
    v[1].mark = Mark::HOLE;
    v[1].missingFrom = 11;
    v[1].missingTo = 15;
    HaltState h = haltOf(v, 10);
    CHECK(Halt::GAP == h.halt && std::string::npos != h.detail.find("11..15"));
    v[1].mark = Mark::NONE; // one source can still be read: no halt
    CHECK(Halt::NONE == haltOf(v, 10).halt);
    v[1].mark = Mark::PAYLOAD_CONFLICT;
    CHECK(Halt::PAYLOAD_CONFLICT == haltOf(v, 10).halt);
    v[0].mark = Mark::SCHEMA;
    v[1].mark = Mark::MTU;
    CHECK(Halt::SCHEMA == haltOf(v, 10).halt);
    v[0].mark = Mark::MTU;
    CHECK(Halt::MTU == haltOf(v, 10).halt);
}

// The property: sources are lists of (seq, bytes); faults are holes, a start above 1, a bad copy, an unreachable
// period, and writer failures (a batch lost before its commit). Each fault may clear after 1..N iterations. The model
// drives the same rules as pj-duty (forwardRule, chooseSource, Backoff, haltOf) against an in-memory writer.
TEST(follow_fuzz_recovers)
{
    const char *never = std::getenv("GW_FUZZ_FAULTS_NEVER_CLEAR"); // the non-vacuity control
    const bool faultsClear = nullptr == never;
    const std::int64_t N = 60;
    auto canonical = [](std::int64_t seq) { return static_cast<int>(seq * 7919 % 251); };
    std::uint64_t recoveries = 0;
    std::uint64_t runs = 0;
    for (unsigned seed = 1; seed <= 200; ++seed)
    {
        std::mt19937 rng(seed);
        auto coin = [&](int pct) { return static_cast<int>(rng() % 100) < pct; };
        const std::size_t S = 2 + rng() % 2;
        struct Src
        {
            std::map<std::int64_t, int> records; // what the source holds now
            std::map<std::int64_t, int> fix;     // what a cleared fault restores
            int clearAt = -1;
            int downUntil = -1;
        };
        std::vector<Src> src(S);
        for (std::size_t s = 0; s < S; ++s)
        {
            for (std::int64_t q = 1; q <= N; ++q)
            {
                src[s].records[q] = canonical(q);
            }
            if (coin(40)) // an interior hole
            {
                const std::int64_t from = 5 + rng() % 40;
                for (std::int64_t q = from; q < from + 1 + static_cast<std::int64_t>(rng() % 5) && q <= N; ++q)
                {
                    src[s].fix[q] = src[s].records[q];
                    src[s].records.erase(q);
                }
            }
            if (coin(25)) // restored from a peer: starts above 1
            {
                for (std::int64_t q = 1; q <= 3; ++q)
                {
                    src[s].fix[q] = src[s].records[q];
                    src[s].records.erase(q);
                }
            }
            if (coin(20)) // a bad copy
            {
                const std::int64_t q = 1 + rng() % N;
                if (src[s].records.count(q))
                {
                    src[s].fix[q] = src[s].records[q];
                    src[s].records[q] = (src[s].records[q] + 1) % 251;
                }
            }
            if (!faultsClear) // the control: every source lacks the same range, for good
            {
                for (std::int64_t q = 20; q <= 22; ++q)
                {
                    src[s].records.erase(q);
                }
                src[s].fix.clear();
            }
            src[s].clearAt = static_cast<int>(1 + rng() % 40);
            if (coin(20))
            {
                src[s].downUntil = static_cast<int>(rng() % 30);
            }
        }
        // the writer: stored bytes by seq; the cursor
        std::map<std::int64_t, int> stored;
        std::int64_t applied = 0;
        std::vector<SourceView> views(S);
        std::vector<Backoff> backoffs(S, Backoff(1, 8));
        std::optional<std::size_t> cur;
        std::int64_t pos = 0; // the read position on the current source (a memo-free read from its start)
        bool sawFault = false;
        for (int it = 0; it < 400 && applied < N; ++it)
        {
            const std::int64_t now = it * 1'000'000LL;
            for (std::size_t s = 0; s < S; ++s)
            {
                if (faultsClear && it == src[s].clearAt)
                {
                    for (auto &[q, b] : src[s].fix)
                    {
                        src[s].records[q] = b;
                    }
                    src[s].fix.clear();
                }
            }
            if (!cur)
            {
                cur = chooseSource(views, now, std::nullopt);
                pos = 0;
                if (!cur)
                {
                    const HaltState h = haltOf(views, applied);
                    if (Halt::NONE != h.halt && it % 10 == 0)
                    {
                        sawFault = true;
                        for (auto &v : views)
                        {
                            v.mark = Mark::NONE; // the halt cycle: every source read again
                        }
                    }
                    continue;
                }
            }
            Src &s = src[*cur];
            if (it < s.downUntil) // the archive does not answer: left with a back-off
            {
                views[*cur].blockedUntilNs = backoffs[*cur].leave(now);
                cur.reset();
                sawFault = true;
                continue;
            }
            // one poll: up to 8 records from the read position
            std::int64_t next = applied + 1;
            bool first = 0 == pos;
            int taken = 0;
            std::vector<std::pair<std::int64_t, int>> batch;
            bool left = false;
            for (auto i = s.records.upper_bound(pos); i != s.records.end() && taken < 8; ++i, ++taken)
            {
                const ForwardVerdict v = forwardRule(i->first, next, true, first);
                first = false;
                pos = i->first;
                if (Forward::COMPARE == v.kind)
                {
                    const auto inBatch = std::find_if(batch.begin(), batch.end(), [&](auto &b) { return b.first == i->first; });
                    const bool equal = stored.count(i->first) ? stored[i->first] == i->second
                        : inBatch != batch.end() && inBatch->second == i->second;
                    if (!equal)
                    {
                        views[*cur].mark = Mark::PAYLOAD_CONFLICT;
                        left = true;
                        break;
                    }
                    continue;
                }
                if (Forward::APPLY == v.kind)
                {
                    if (i->second != canonical(i->first))
                    {
                        // a bad copy read first becomes the stored bytes; the model then detects it as the conflict of
                        // every agreeing source, which is the parked case: count it and stop this seed
                        sawFault = true;
                        left = true;
                        views.assign(S, SourceView{});
                        applied = N + 1; // excluded from the dense check below
                        break;
                    }
                    batch.emplace_back(i->first, i->second);
                    next++;
                    continue;
                }
                views[*cur].mark = Forward::HOLE == v.kind ? Mark::HOLE : Mark::BELOW_RANGE;
                views[*cur].missingFrom = v.from;
                views[*cur].missingTo = v.to;
                left = true;
                break;
            }
            if (applied > N)
            {
                break;
            }
            // the writer: a batch is lost before its commit 10% of the time (nothing applied, the read goes again)
            if (!batch.empty() && coin(10))
            {
                sawFault = true;
                cur.reset();
                continue;
            }
            for (auto &[q, b] : batch)
            {
                stored[q] = b;
                applied = q;
                backoffs[*cur].progress();
            }
            for (auto &v : views)
            {
                if ((Mark::HOLE == v.mark || Mark::BELOW_RANGE == v.mark) && v.missingTo <= applied)
                {
                    v.mark = Mark::NONE;
                }
            }
            if (left)
            {
                sawFault = true;
                views[*cur].blockedUntilNs = backoffs[*cur].leave(now);
                cur.reset();
            }
            else if (s.records.upper_bound(pos) == s.records.end() && applied < N)
            {
                cur.reset(); // the chain is exhausted here: choose again
            }
        }
        if (applied > N)
        {
            continue; // a bad copy applied first: not this property's case
        }
        runs++;
        // invariants: the applied prefix is dense and holds the canonical bytes
        std::int64_t q = 0;
        for (auto &[seq, b] : stored)
        {
            CHECK_MSG(seq == ++q, "applied prefix not dense at " + std::to_string(seq));
            CHECK_MSG(b == canonical(seq), "row of journalSeq " + std::to_string(seq) + " is not the canonical record");
        }
        CHECK(q == applied);
        if (sawFault && applied == N)
        {
            recoveries++;
        }
    }
    std::printf("  follow_fuzz_recovers: %llu seeds checked, %llu recovered to the head after a fault\n",
        static_cast<unsigned long long>(runs), static_cast<unsigned long long>(recoveries));
    if (0 == recoveries)
    {
        CHECK_MSG(false, "no recovery in the seed set");
    }
}
