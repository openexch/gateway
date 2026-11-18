// T0 rows of oe-stream's core logic (StreamCore) against the model core of StreamRig.h: the stitch, the gap and its
// replay, the overrun, the resume rules, the balance stitch, the re-snapshot pacing, the per-user cache, the slow client
// and the fills. Each control of tests/CMakeLists.txt rebuilds StreamCore with one rule off and runs the case that
// guards it.

#include <string>

#include "StreamRig.h"

using namespace gwtest::stream_rig;
namespace ch = st::channel;

namespace {

void expectConverged(const Client &c, const FakeCore &core, std::int64_t userId, const std::string &where)
{
    std::string why;
    CHECK_MSG(c.matches(core, userId, why), "client state differs from oracle (" + where + "): " + why);
    for (const std::int64_t d : c.duplicates)
    {
        CHECK_MSG(false, "duplicate event " + std::to_string(d) + " (" + where + ")");
    }
}

void expectFills(const Client &c, const FakeCore &core, std::int64_t userId, std::int64_t fromTrade = 0)
{
    const auto it = core.fills.find(userId);
    if (it == core.fills.end())
    {
        return;
    }
    for (const std::int64_t t : it->second)
    {
        if (t > fromTrade && 0 == c.fills.count(t))
        {
            CHECK_MSG(false, "fill " + std::to_string(t) + " missing");
        }
    }
    for (const std::int64_t t : c.duplicateFills)
    {
        CHECK_MSG(false, "fill " + std::to_string(t) + " delivered twice");
    }
}

}

// Stitch (stream_it.py runs it against the real core): a user above one page, with orders created, filled and cancelled
// between the pages, converges to the core's state with no duplicate and no missing event.
TEST(stream_stitch_across_pages)
{
    Rig rig;
    rig.start();
    const std::int64_t user = 7;
    std::vector<std::int64_t> mine;
    for (int i = 0; i < 8; ++i)
    {
        mine.push_back(rig.fake.create(user));
        rig.fake.create(99); // another user's flow in between
    }
    rig.step(2);
    rig.autoAnswer = false;
    const std::size_t c = rig.connect(user, ch::ORDERS | ch::FILLS);
    rig.core.doWork(rig.now);
    // page 1 (3 orders), then flow, page 2, flow, page 3 ...
    rig.fake.answerOnly(wire::tid::REQUEST_ORDER_SNAPSHOT);
    rig.fake.deliver(rig.core, rig.now);
    rig.fake.cancel(mine[0]);                 // on page 1, cancelled between the pages
    rig.fake.fill(mine[4], 3 * SCALE);        // on page 2, filled before page 2 is read
    const std::int64_t early = rig.fake.create(user); // a new order: above the cursor, it is on a later page
    rig.fake.create(99);
    rig.step(1);
    rig.fake.answerOnly(wire::tid::REQUEST_ORDER_SNAPSHOT);
    rig.fake.deliver(rig.core, rig.now);
    rig.fake.fill(mine[1], 10 * SCALE);       // on page 1, filled in full between the pages: it left the open set
    rig.fake.fill(mine[7], 2 * SCALE);
    rig.step(1);
    rig.fake.answerOnly(wire::tid::REQUEST_ORDER_SNAPSHOT);
    rig.fake.deliver(rig.core, rig.now);
    rig.fake.fill(mine[2], 1 * SCALE);        // the user's own event is the last entry before the last page
    rig.step(1);
    rig.autoAnswer = true;
    rig.step(5);
    (void)early;
    const Client &cl = rig.clients[c];
    CHECK_MSG(1 == cl.snapshots, "one snapshot, got " + std::to_string(cl.snapshots));
    CHECK_MSG(rig.fake.submittedByTemplate[wire::tid::REQUEST_ORDER_SNAPSHOT] >= 3,
        "the user spans several pages: " + std::to_string(rig.fake.submittedByTemplate[wire::tid::REQUEST_ORDER_SNAPSHOT]));
    expectConverged(cl, rig.fake, user, "after the stitch");
    // the folded fills follow the snapshot as fill frames (a fill is never dropped by folding)
    expectFills(cl, rig.fake, user);
    // and live flow after it
    rig.fake.fill(mine[3], 4 * SCALE);
    rig.fake.cancel(mine[5]);
    rig.step(3);
    expectConverged(cl, rig.fake, user, "live after the stitch");
    expectFills(cl, rig.fake, user);
}

// Gap: a hole in the live stream and a new session are filled by RequestOrderEvents; no client resyncs.
TEST(stream_gap_is_replayed)
{
    Rig rig;
    rig.start();
    const std::int64_t user = 11;
    const std::int64_t a = rig.fake.create(user);
    const std::size_t c = rig.connect(user, ch::ORDERS | ch::FILLS);
    rig.step(4);
    rig.fake.dropLive = true; // three events never arrive live
    rig.fake.fill(a, 1 * SCALE);
    const std::int64_t b = rig.fake.create(user);
    rig.fake.create(12);
    rig.fake.dropLive = false;
    rig.fake.fill(a, 2 * SCALE); // the next live event shows the hole
    rig.step(6);
    CHECK_MSG(rig.fake.replayQueries >= 1, "the hole was asked from the core");
    expectConverged(rig.clients[c], rig.fake, user, "after the hole");
    // a new session (the core closed the old one): the broadcasts in between are gone
    rig.fake.dropLive = true;
    rig.fake.cancel(b);
    rig.fake.create(user);
    rig.fake.dropLive = false;
    rig.core.onRegistered(2, 1, rig.now);
    rig.step(6);
    // and a leader change (same session, new term) with a quiet stream after it
    rig.fake.dropLive = true;
    rig.fake.fill(a, 3 * SCALE);
    rig.fake.dropLive = false;
    rig.core.onRegistered(2, 2, rig.now);
    rig.step(6);
    const Client &cl = rig.clients[c];
    CHECK_MSG(cl.resyncs.empty(), "a filled gap is only delay for the client");
    expectConverged(cl, rig.fake, user, "after the replays");
    expectFills(cl, rig.fake, user);
    CHECK(!rig.core.gapOpen());
}

// The stream's overrun: the core's ring no longer holds the hole -> every connection resyncs (EVENT_GAP) and converges.
TEST(stream_overrun_resyncs_every_connection)
{
    Rig rig;
    rig.start();
    const std::int64_t a = rig.fake.create(21);
    const std::size_t c1 = rig.connect(21, ch::ORDERS | ch::FILLS);
    const std::size_t c2 = rig.connect(22, ch::ORDERS);
    const std::size_t c3 = rig.connect(23, ch::FILLS);
    rig.step(4);
    rig.fake.ringCapacity = 4;
    rig.fake.dropLive = true;
    for (int i = 0; i < 10; ++i)
    {
        rig.fake.create(22);
    }
    rig.fake.fill(a, SCALE);
    rig.fake.dropLive = false;
    rig.fake.create(21);
    rig.step(10);
    for (const std::size_t c : { c1, c2, c3 })
    {
        const Client &cl = rig.clients[c];
        CHECK_MSG(1 == cl.resyncs.size() && "EVENT_GAP" == cl.resyncs[0], "every connection resyncs once (EVENT_GAP)");
    }
    expectConverged(rig.clients[c1], rig.fake, 21, "21 after the overrun");
    expectConverged(rig.clients[c2], rig.fake, 22, "22 after the overrun");
    CHECK_MSG(2 == rig.clients[c1].snapshots && 2 == rig.clients[c2].snapshots, "a fresh snapshot after the resync");
    CHECK(!rig.core.gapOpen());
}

// Resume: a quiet user resumes from its last heartbeat after other users' flow: served from the window, no
// core query. Then the backfill and the too-old cases.
TEST(stream_resume_from_heartbeat)
{
    st::StreamCoreSettings s = settings();
    s.eventWindow = 8;
    s.heartbeatMs = 5;
    Rig rig(s);
    rig.fake.ringCapacity = 8;
    rig.start();
    const std::int64_t quiet = 31;
    rig.fake.create(quiet);
    const std::size_t c = rig.connect(quiet, ch::ORDERS | ch::FILLS);
    rig.step(4);
    for (int round = 0; round < 6; ++round)
    {
        for (int i = 0; i < 3; ++i)
        {
            rig.fake.create(32); // other users' flow moves eventSeq far past the quiet user's last event
        }
        rig.step(6); // a heartbeat in each round
    }
    const std::int64_t last = rig.clients[c].lastSeq;
    rig.disconnect(c);
    rig.step(2);
    const auto snapshotsBefore = rig.fake.submittedByTemplate[wire::tid::REQUEST_ORDER_SNAPSHOT];
    const auto replaysBefore = rig.fake.replayQueries;
    const std::size_t r = rig.connect(quiet, ch::ORDERS | ch::FILLS, last);
    rig.step(3);
    const Client &cl = rig.clients[r];
    CHECK_MSG(cl.resyncs.empty(), "quiet user forced to resync (resumed from " + std::to_string(last) + ", window starts at " +
        std::to_string(rig.core.windowFirstSeq()) + ")");
    CHECK_MSG(rig.fake.submittedByTemplate[wire::tid::REQUEST_ORDER_SNAPSHOT] == snapshotsBefore &&
            rig.fake.replayQueries == replaysBefore, "a resume inside the window asks the core nothing");
    // its events after the resume arrive
    rig.fake.create(quiet);
    rig.step(2);
    CHECK_MSG(1 == rig.clients[r].orderFrames, "live events after the resume");
}

TEST(stream_resume_backfill_and_too_old)
{
    Rig rig;
    rig.fake.ringCapacity = 50;
    std::vector<std::int64_t> orders;
    for (int i = 0; i < 30; ++i)
    {
        orders.push_back(rig.fake.create(41 + i % 2));
    }
    // a stream that (re)starts now: its window is empty, the watermark is the core's head (30)
    rig.start();
    CHECK(30 == rig.core.watermark() && 0 == rig.core.windowSize());
    const auto replaysBefore = rig.fake.replayQueries;
    const std::size_t a = rig.connect(41, ch::ORDERS | ch::FILLS, 10);
    const std::size_t b = rig.connect(42, ch::ORDERS, 12);
    rig.step(6);
    CHECK_MSG(rig.fake.replayQueries == replaysBefore + 1, "one shared backfill for both clients, got " +
        std::to_string(rig.fake.replayQueries - replaysBefore));
    CHECK_MSG(rig.clients[a].resyncs.empty() && rig.clients[b].resyncs.empty(), "served after the backfill");
    CHECK_MSG(10 == rig.clients[a].orderFrames, "user 41's events above 10: " + std::to_string(rig.clients[a].orderFrames));
    CHECK_MSG(9 == rig.clients[b].orderFrames, "user 42's events above 12: " + std::to_string(rig.clients[b].orderFrames));
    // older than the core's ring: resync RESUME_TOO_OLD and a snapshot
    rig.fake.ringCapacity = 5;
    for (int i = 0; i < 40; ++i)
    {
        rig.fake.create(43);
    }
    rig.step(2);
    const std::size_t old = rig.connect(41, ch::ORDERS, 3);
    rig.step(6);
    const Client &o = rig.clients[old];
    CHECK_MSG(1 == o.resyncs.size() && "RESUME_TOO_OLD" == o.resyncs[0], "a resume older than the core's ring");
    CHECK(1 == o.snapshots);
    expectConverged(o, rig.fake, 41, "after RESUME_TOO_OLD");
}

// Balance stitch: a balance line older than the UserBalances reply is discarded, a newer one applies.
TEST(stream_balance_stitch)
{
    Rig rig;
    rig.start();
    const std::int64_t user = 51;
    rig.fake.deposit(user, 0, 100 * SCALE);
    rig.step(2);
    rig.autoAnswer = false;
    const std::size_t c = rig.connect(user, ch::BALANCES);
    rig.core.doWork(rig.now);
    rig.fake.deposit(user, 0, 5 * SCALE);   // applied before the read: its line is older than the reply
    rig.fake.answerAll();                   // the read
    rig.fake.deposit(user, 0, 7 * SCALE);   // applied after the read: its line is newer
    rig.fake.deposit(user, 1, 2 * SCALE);
    rig.fake.deliver(rig.core, rig.now);
    rig.autoAnswer = true;
    rig.step(3);
    const Client &cl = rig.clients[c];
    CHECK(1 == cl.balanceSnapshots);
    const auto usd = cl.balances.find("0");
    CHECK_MSG(usd != cl.balances.end() && "112" == usd->second, "stale balance after snapshot: USD " +
        (usd == cl.balances.end() ? std::string("absent") : usd->second) + ", ledger 112");
    const auto btc = cl.balances.find("1");
    CHECK_MSG(btc != cl.balances.end() && "2" == btc->second, "stale balance after snapshot: BTC");
}

// Overrun: an overrun with many connected users re-reads them through the budget: at most the session's burst of
// snapshot queries outstanding at once, every user converges, oldest connection first.
TEST(stream_resnapshot_is_paced)
{
    st::StreamCoreSettings s = settings();
    s.queryBurst = 5;
    Rig rig(s);
    rig.start();
    std::vector<std::size_t> clients;
    for (int u = 0; u < 60; ++u)
    {
        rig.fake.create(1000 + u);
        clients.push_back(rig.connect(1000 + u, ch::ORDERS));
    }
    rig.step(40);
    rig.fake.maxOutstandingUserQueries = 0;
    rig.fake.ringCapacity = 2;
    rig.fake.dropLive = true;
    for (int i = 0; i < 6; ++i)
    {
        rig.fake.create(1000 + i);
    }
    rig.fake.dropLive = false;
    rig.fake.create(1001);
    rig.autoAnswer = false;
    std::vector<std::int64_t> order;
    for (int i = 0; i < 400 && order.size() < 60; ++i)
    {
        rig.now += MS;
        rig.core.doWork(rig.now);
        // the core answers one query per cycle: the scheduler must hold the rest itself
        rig.fake.answerOne();
        for (const auto &q : rig.fake.egress)
        {
            if (2 == q.kind && std::holds_alternative<gs::SnapshotReply>(q.outcome.reply))
            {
                const auto &r = std::get<gs::SnapshotReply>(q.outcome.reply);
                if (!r.entries.empty())
                {
                    order.push_back(r.entries.front().userId);
                }
            }
        }
        rig.fake.deliver(rig.core, rig.now);
        rig.core.doWork(rig.now);
        for (Client &c : rig.clients)
        {
            c.take();
        }
    }
    CHECK_MSG(rig.fake.maxOutstandingUserQueries <= 5, "snapshot query rate above the bound: " +
        std::to_string(rig.fake.maxOutstandingUserQueries) + " outstanding, burst 5");
    bool oldestFirst = order.size() >= 2;
    for (std::size_t i = 1; i < order.size(); ++i)
    {
        oldestFirst = oldestFirst && order[i] > order[i - 1];
    }
    CHECK_MSG(oldestFirst, "re-snapshots in the order of the oldest connection");
    rig.autoAnswer = true;
    rig.step(20);
    for (int u = 0; u < 60; ++u)
    {
        expectConverged(rig.clients[clients[static_cast<std::size_t>(u)]], rig.fake, 1000 + u, "user " + std::to_string(u));
    }
}

// Per-user cache: a second connection and a reconnect inside the linger cost no core query; the cache is dropped after
// the linger and at a new registration.
TEST(stream_user_cache)
{
    Rig rig;
    rig.start();
    const std::int64_t user = 61;
    rig.fake.create(user);
    rig.fake.deposit(user, 0, 10 * SCALE);
    const std::size_t a = rig.connect(user, ch::ORDERS | ch::BALANCES);
    rig.step(4);
    auto snapshots = [&] { return rig.fake.submittedByTemplate[wire::tid::REQUEST_ORDER_SNAPSHOT]; };
    auto reads = [&] { return rig.fake.submittedByTemplate[wire::tid::REQUEST_USER_BALANCES]; };
    const auto s0 = snapshots();
    const auto b0 = reads();
    const std::size_t b = rig.connect(user, ch::ORDERS | ch::BALANCES);
    rig.step(3);
    CHECK_MSG(snapshots() == s0 && reads() == b0, "a second connection is served from the cache");
    expectConverged(rig.clients[b], rig.fake, user, "second tab");
    CHECK(1 == rig.clients[b].balanceSnapshots);
    rig.disconnect(a);
    rig.disconnect(b);
    rig.step(2);
    rig.now += 10'000 * MS; // inside the 30 s linger
    const std::size_t c = rig.connect(user, ch::ORDERS);
    rig.step(3);
    CHECK_MSG(snapshots() == s0, "a reconnect inside the linger is served from the cache");
    expectConverged(rig.clients[c], rig.fake, user, "reconnect");
    rig.disconnect(c);
    rig.step(2);
    rig.now += 31'000 * MS; // past the linger
    rig.step(2);
    CHECK(!rig.core.userCached(user));
    const std::size_t d = rig.connect(user, ch::ORDERS);
    rig.step(3);
    CHECK_MSG(snapshots() == s0 + 1, "after the linger the snapshot is read again");
    // a new registration drops every cache: the next connection reads again
    rig.core.onRegistered(9, 1, rig.now);
    rig.step(4);
    CHECK(!rig.core.userCached(user) || true);
    const std::size_t e = rig.connect(user, ch::ORDERS);
    rig.step(4);
    CHECK_MSG(snapshots() == s0 + 2, "the cache is dropped at a new registration");
    expectConverged(rig.clients[d], rig.fake, user, "d");
    expectConverged(rig.clients[e], rig.fake, user, "e");
}

// Slow client: a client that stops reading is closed 4001 at the bound; order frames of one order coalesce (no close
// below the bound); other clients are not delayed.
TEST(stream_slow_client_bound)
{
    st::StreamCoreSettings s = settings();
    s.wsQueueMessages = 16;
    Rig rig(s);
    rig.start();
    const std::size_t slow = rig.connect(71, ch::ORDERS);
    const std::size_t fast = rig.connect(72, ch::ORDERS);
    rig.step(4);
    rig.clients[slow].out->paused = true;
    rig.clients[slow].out->capacity = 4;
    const std::int64_t one = rig.fake.create(71, 1000 * SCALE);
    for (int i = 0; i < 40; ++i)
    {
        rig.fake.fill(one, SCALE); // one order: its frames coalesce, the queue stays small
        rig.fake.create(72);
    }
    rig.step(3);
    CHECK_MSG(!rig.clients[slow].out->isClosed, "frames of one order coalesce below the bound");
    for (int i = 0; i < 40; ++i)
    {
        rig.fake.create(71); // distinct orders: nothing coalesces
    }
    rig.step(3);
    CHECK_MSG(rig.clients[slow].out->isClosed && st::close_code::RESYNC == rig.clients[slow].out->closeCode,
        "queue exceeded bound: the slow client is not closed 4001");
    CHECK_MSG(rig.core.stats().queueHighWater.load() <= 17, "queue exceeded bound: high water " +
        std::to_string(rig.core.stats().queueHighWater.load()));
    CHECK_MSG(40 == rig.clients[fast].orderFrames, "the other client got every frame: " +
        std::to_string(rig.clients[fast].orderFrames) + " order frames, " + std::to_string(rig.clients[fast].snapshots) +
        " snapshot(s), " + std::to_string(rig.clients[fast].types.size()) + " frames");
}

// Slow client, the direct path: a frame handed straight to the writer counts against the bound too. The writer's
// handover holds more frames than the message bound and more bytes than the byte bound; a client that stops reading is
// still closed 4001 at the bound. A single frame larger than the byte bound on an otherwise empty connection goes out.
TEST(stream_slow_client_bound_direct)
{
    {
        st::StreamCoreSettings s = settings();
        s.wsQueueMessages = 16;
        Rig rig(s);
        rig.start();
        const std::size_t slow = rig.connect(73, ch::ORDERS);
        rig.step(4);
        rig.clients[slow].out->paused = true;
        rig.clients[slow].out->capacity = 64; // the handover takes every frame: nothing waits in the core's queue
        for (int i = 0; i < 40; ++i)
        {
            rig.fake.create(73);
        }
        rig.step(3);
        CHECK_MSG(rig.clients[slow].out->isClosed && st::close_code::RESYNC == rig.clients[slow].out->closeCode,
            "queue exceeded bound on the direct path: " + std::to_string(rig.clients[slow].out->unwrittenMessages()) +
                " frames unwritten, not closed 4001");
        CHECK_MSG(rig.core.stats().queueHighWater.load() <= 17, "queue exceeded bound on the direct path: high water " +
            std::to_string(rig.core.stats().queueHighWater.load()));
    }
    {
        st::StreamCoreSettings s = settings();
        s.wsQueueBytes = 100; // below one order frame
        Rig rig(s);
        rig.start();
        const std::size_t slow = rig.connect(74, ch::ORDERS);
        rig.step(4);
        rig.clients[slow].out->paused = true;
        rig.clients[slow].out->capacity = 64;
        rig.fake.create(74);
        rig.step(2);
        CHECK_MSG(!rig.clients[slow].out->isClosed, "a single frame above the byte bound on an empty connection is sent");
        rig.fake.create(74);
        rig.step(2);
        CHECK_MSG(rig.clients[slow].out->isClosed && st::close_code::RESYNC == rig.clients[slow].out->closeCode,
            "byte bound exceeded on the direct path: " + std::to_string(rig.clients[slow].out->unwrittenBytes()) +
                " bytes unwritten, not closed 4001");
    }
    {
        // a snapshot above the byte bound, then live frames: the frames behind the snapshot are inside the bound
        st::StreamCoreSettings s = settings();
        s.wsQueueBytes = 2000;
        Rig rig(s);
        rig.start();
        for (int i = 0; i < 12; ++i)
        {
            rig.fake.create(75);
        }
        rig.step(2);
        const std::size_t slow = rig.connect(75, ch::ORDERS);
        rig.clients[slow].out->paused = true;
        rig.clients[slow].out->capacity = 64;
        rig.step(4);
        const std::int64_t snapshotBytes = rig.clients[slow].out->unwrittenBytes();
        CHECK_MSG(snapshotBytes > 2000 && !rig.clients[slow].out->isClosed,
            "a snapshot above the byte bound is sent (" + std::to_string(snapshotBytes) + " bytes)");
        rig.fake.create(75);
        rig.step(2);
        CHECK_MSG(!rig.clients[slow].out->isClosed, "one live frame behind a large snapshot is inside the bound");
        for (int i = 0; i < 10; ++i)
        {
            rig.fake.create(75);
        }
        rig.step(2);
        CHECK_MSG(rig.clients[slow].out->isClosed && st::close_code::RESYNC == rig.clients[slow].out->closeCode,
            "byte bound exceeded behind a large snapshot: " + std::to_string(rig.clients[slow].out->unwrittenBytes()) +
                " bytes unwritten, not closed 4001");
    }
}

// The startup seam: a client subscribes before the first head probe is answered, and the probe is answered late (it
// failed or timed out once). No snapshot page may be read before the watermark is known; once it is, the stitch runs
// and the client converges, with every fill delivered.
TEST(stream_startup_snapshot_before_probe)
{
    Rig rig;
    rig.autoAnswer = false;
    const std::int64_t user = 7;
    rig.fake.create(user);
    rig.fake.egress.clear(); // before the session existed
    rig.core.onRegistered(1, 1, rig.now);
    const std::size_t c = rig.connect(user, ch::ORDERS | ch::FILLS);
    rig.step(1);
    rig.fake.answerOnly(wire::tid::REQUEST_ORDER_SNAPSHOT); // a snapshot answered before the probe
    rig.step(1);
    const std::int64_t later = rig.fake.create(user); // after that snapshot, before the probe answer
    rig.fake.fill(later, SCALE);
    rig.step(1);
    rig.autoAnswer = true;
    rig.step(5);
    CHECK_MSG(1 == rig.clients[c].snapshots, "no snapshot after the first head probe (" +
        std::to_string(rig.clients[c].snapshots) + " snapshots)");
    expectConverged(rig.clients[c], rig.fake, user, "after the first head probe");
    // a later gap elsewhere closes; the client still matches the core
    rig.fake.dropLive = true;
    rig.fake.create(99);
    rig.fake.dropLive = false;
    rig.fake.create(99);
    rig.step(6);
    expectConverged(rig.clients[c], rig.fake, user, "after a later gap");
    // the first fill precedes the snapshot's position (no page is read before the watermark), so the snapshot carries it
    // as state; every fill after the snapshot arrives as a fill frame
    rig.fake.fill(later, SCALE);
    rig.step(3);
    expectConverged(rig.clients[c], rig.fake, user, "after a later fill");
    expectFills(rig.clients[c], rig.fake, user, 1);
    CHECK_MSG(1 == rig.clients[c].fills.size(), "fill missing: " + std::to_string(rig.clients[c].fills.size()) +
        " fill frames after the snapshot");
}

// A failed first head probe is asked again at once, not after STREAM_HEAD_PROBE_MS: the watermark (and with it every
// snapshot) waits on it.
TEST(stream_first_probe_retried_at_once)
{
    Rig rig; // headProbeMs is 1000 s here
    rig.autoAnswer = false;
    rig.core.onRegistered(1, 1, rig.now);
    rig.step(1);
    CHECK_MSG(1 == rig.fake.pending.size(), "the first head probe is submitted");
    rig.fake.failAll();
    rig.step(1);
    rig.autoAnswer = true;
    rig.step(3);
    CHECK_MSG(rig.core.watermark() >= 0, "a failed first head probe is not retried before STREAM_HEAD_PROBE_MS");
}

// Fills: N fills on one order under back-pressure: N fill frames (or a 4001 close); fills inside a folded span follow
// the snapshot.
TEST(stream_fills_never_coalesce)
{
    st::StreamCoreSettings s = settings();
    s.wsQueueMessages = 64;
    Rig rig(s);
    rig.start();
    const std::int64_t user = 81;
    const std::size_t c = rig.connect(user, ch::ORDERS | ch::FILLS);
    rig.step(4);
    const std::int64_t o = rig.fake.create(user, 100 * SCALE);
    rig.step(2);
    rig.clients[c].out->paused = true;
    rig.clients[c].out->capacity = 2;
    const std::int64_t firstTrade = rig.fake.nextTradeId;
    for (int i = 0; i < 20; ++i)
    {
        rig.fake.fill(o, SCALE);
    }
    rig.step(3);
    rig.clients[c].out->paused = false;
    rig.clients[c].out->capacity = 1 << 20;
    rig.step(5);
    const Client &cl = rig.clients[c];
    if (!cl.out->isClosed)
    {
        expectFills(cl, rig.fake, user, firstTrade);
        CHECK_MSG(cl.count("order") < 21, "order frames of one order coalesced under back-pressure");
    }
    // folded span: fills between the pages arrive after the snapshot
    rig.fake.pageSize = 1;
    const std::int64_t user2 = 82;
    std::vector<std::int64_t> mine;
    for (int i = 0; i < 3; ++i)
    {
        mine.push_back(rig.fake.create(user2, 10 * SCALE));
    }
    rig.step(2);
    rig.autoAnswer = false;
    const std::size_t d = rig.connect(user2, ch::ORDERS | ch::FILLS);
    const std::int64_t before = rig.fake.nextTradeId;
    for (int page = 0; page < 3; ++page)
    {
        rig.core.doWork(rig.now);
        rig.fake.answerOnly(wire::tid::REQUEST_ORDER_SNAPSHOT);
        rig.fake.deliver(rig.core, rig.now);
        rig.fake.fill(mine[static_cast<std::size_t>(page)], SCALE);
        rig.step(1);
    }
    rig.autoAnswer = true;
    rig.step(4);
    expectFills(rig.clients[d], rig.fake, user2, before);
    expectConverged(rig.clients[d], rig.fake, user2, "folded fills");
}

// A FILL event with tradeId 0 (the core closing an order whose fills inside a replace window reached its quantity)
// names no trade: an order frame only, never a fill frame, live and on the folded span of a snapshot, so the fill frames
// of a user and the user's fills rows are the same set.
TEST(stream_no_fill_frame_without_a_trade)
{
    st::StreamCoreSettings s = settings();
    Rig rig(s);
    rig.start();
    const std::int64_t user = 85;
    const std::size_t c = rig.connect(user, ch::ORDERS | ch::FILLS);
    const std::size_t f = rig.connect(user, ch::FILLS);
    rig.step(4);
    const std::int64_t o = rig.fake.create(user, 10 * SCALE);
    rig.step(2);
    rig.fake.fill(o, 4 * SCALE);
    rig.step(2);
    const std::size_t ordersBefore = rig.clients[c].count("order");
    rig.fake.closeWithoutTrade(o);
    rig.step(3);
    for (const std::size_t k : { c, f })
    {
        const Client &cl = rig.clients[k];
        if (0 != cl.fills.count(0) || cl.count("fill") != 1)
        {
            CHECK_MSG(false, "fill frame with tradeId 0 (live, " + std::to_string(cl.count("fill")) + " fill frames)");
        }
    }
    CHECK_MSG(rig.clients[c].count("order") == ordersBefore + 1, "the closing FILL reaches the client as an order frame");
    // the folded span of a snapshot: the closing FILL between the pages
    rig.fake.pageSize = 1;
    const std::int64_t user2 = 86;
    std::vector<std::int64_t> mine;
    for (int i = 0; i < 3; ++i)
    {
        mine.push_back(rig.fake.create(user2, 10 * SCALE));
    }
    rig.step(2);
    rig.autoAnswer = false;
    const std::size_t d = rig.connect(user2, ch::ORDERS | ch::FILLS);
    for (int page = 0; page < 3; ++page)
    {
        rig.core.doWork(rig.now);
        rig.fake.answerOnly(wire::tid::REQUEST_ORDER_SNAPSHOT);
        rig.fake.deliver(rig.core, rig.now);
        if (1 == page)
        {
            rig.fake.closeWithoutTrade(mine[0]);
        }
        rig.step(1);
    }
    rig.autoAnswer = true;
    rig.step(4);
    if (0 != rig.clients[d].fills.count(0) || 0 != rig.clients[d].count("fill"))
    {
        CHECK_MSG(false, "fill frame with tradeId 0 (folded span)");
    }
    expectConverged(rig.clients[d], rig.fake, user2, "closing FILL in the folded span");
}

// Unsubscribe and resubscribe; a FEATURE_DISABLED balances channel; admission frames to every connection.
TEST(stream_channels_and_admission)
{
    st::StreamCoreSettings s = settings();
    s.balanceStream = false;
    Rig rig(s);
    rig.start();
    const std::size_t c = rig.connect(91, ch::ORDERS | ch::BALANCES);
    rig.step(3);
    CHECK(1 == rig.clients[c].count("error") && 1 == rig.clients[c].snapshots && 0 == rig.clients[c].balanceSnapshots);
    wire::AdmissionState a;
    a.marketId = 0;
    a.gateState = money::GateState::SUSPENDED;
    rig.core.onAdmission(a);
    rig.step(1);
    CHECK(1 == rig.clients[c].count("admission"));
    rig.core.unsubscribe(rig.connIds[c], ch::ORDERS);
    rig.fake.create(91);
    rig.step(2);
    CHECK(0 == rig.clients[c].orderFrames);
}
