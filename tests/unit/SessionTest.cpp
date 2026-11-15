// The core session wrapper (CoreSession.h) over the cluster-tools FakeTransport with a scripted core (SessionRig.h):
// the offer table, registered means acked, re-offer after a new leader, ack loss with the same key, the 504 and the
// late reply, the query budget, the config cache, the readiness conditions it feeds, the exact-version gate in front of
// the registrar, and the UNREGISTERED_SESSION path.
//
// Each rule has a test-only control build (GW_NC_*, tests/CMakeLists.txt) that switches it off; the case must then fail
// with the text named there. Random reply delays are seeded by GW_SESSION_SEED (default 1) and printed.
#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "Check.h"
#include "SessionRig.h"
#include "gateway/common/Readiness.h"

using namespace gwtest::session_rig;
namespace wire = openexchange::gateway::wire;
namespace gw = openexchange::gateway;

namespace {

std::uint64_t seed()
{
    static const std::uint64_t value = [] {
        const char *s = std::getenv("GW_SESSION_SEED");
        const std::uint64_t v = (nullptr == s || '\0' == *s) ? 1 : std::strtoull(s, nullptr, 10);
        std::printf("GW_SESSION_SEED=%" PRIu64 "\n", v);
        return v;
    }();
    return value;
}

const wire::OrderAck *ackOf(const gs::Outcome &o)
{
    return std::get_if<wire::OrderAck>(&o.reply);
}

std::uint64_t submitCreate(Rig &rig, std::int64_t key)
{
    const gs::SubmitResult r = rig.session.submit(gs::Command{ create(key) });
    CHECK(r.accepted);
    return r.ticket;
}

gw::Status statusOf(Rig &rig)
{
    gw::Status s;
    rig.session.fillStatus(s);
    s.driverConnected = true;
    s.publicListenerAccepting = true;
    return s;
}

std::vector<std::string> notReady(Rig &rig, std::int64_t nowNs, gw::GatewayReadyBounds bounds = {})
{
    return gw::failing(gw::gatewayReadyConditions(statusOf(rig), nowNs, bounds));
}

bool contains(const std::vector<std::string> &all, const std::string &one)
{
    return std::find(all.begin(), all.end(), one) != all.end();
}

}

// ---- registered means acked

TEST(session_registered_means_acked)
{
    Rig rig;
    rig.registerAcksToDrop = 1; // the first ack is lost: the registrar re-sends after registerResendMs (300)
    rig.connect(false);
    const std::uint64_t early = submitCreate(rig, 1);
    rig.cycle(100);
    CHECK(1 == rig.frames(80).size());
    CHECK_MSG(rig.frames(50).empty() && 0 == rig.commandsBeforeAck, "command offered before registration ack");
    CHECK(!rig.session.registrar().isRegistered());
    rig.cycle(300); // the re-send, then its ack
    CHECK(2 == rig.frames(80).size());
    CHECK(rig.session.registrar().isRegistered());
    CHECK(1 == rig.frames(50).size());
    CHECK(nullptr != rig.outcomeOf(early) && gs::OutcomeKind::REPLY == rig.outcomeOf(early)->kind);

    // the reset path twice in one run: each new leader clears "registered"; a command submitted before the new ack
    // waits for it, and the first ack of each round is lost again
    for (int round = 0; round < 2; ++round)
    {
        rig.registerAcksToDrop = 1;
        rig.newLeader(round + 1);
        rig.cycle(2);
        const std::uint64_t waiting = submitCreate(rig, 100 + round);
        rig.cycle(100);
        CHECK(!rig.session.registrar().isRegistered());
        CHECK_MSG(0 == rig.commandsBeforeAck, "command offered before registration ack");
        rig.cycle(400);
        CHECK(rig.session.registrar().isRegistered());
        CHECK(nullptr != rig.outcomeOf(waiting) && gs::OutcomeKind::REPLY == rig.outcomeOf(waiting)->kind);
    }
    CHECK_MSG(0 == rig.commandsBeforeAck && 0 == rig.session.counters().commandsOfferedUnregistered,
        "command offered before registration ack");
    CHECK(3 == rig.session.counters().registrations);
}

// ---- offer table

TEST(session_offer_table)
{
    Rig rig;
    rig.connect();
    auto pub = [&] { return &rig.fake.publications.at(static_cast<std::size_t>(rig.fake.publicationTo("10.0.0.1:9302"))); };

    // BACK_PRESSURED and ADMIN_ACTION: nothing sent, offered again next cycle
    for (std::int64_t code : { cl::OFFER_BACK_PRESSURED, cl::OFFER_ADMIN_ACTION })
    {
        const std::size_t before = rig.frames(50).size();
        pub()->scriptedResults.push_back(code);
        const std::uint64_t t = submitCreate(rig, 10 + code);
        rig.cycle(1);
        CHECK(before == rig.frames(50).size());
        CHECK(nullptr == rig.outcomeOf(t));
        rig.cycle(5);
        CHECK(before + 1 == rig.frames(50).size());
        CHECK(nullptr != rig.outcomeOf(t) && gs::OutcomeKind::REPLY == rig.outcomeOf(t)->kind);
    }
    CHECK(2 == rig.session.counters().offerBackPressured);

    // TRANSPORT_ERROR: not retried, INTERNAL, counted
    {
        const std::size_t before = rig.frames(50).size();
        pub()->scriptedResults.push_back(cl::OFFER_TRANSPORT_ERROR);
        const std::uint64_t t = submitCreate(rig, 30);
        rig.cycle(50);
        CHECK(before == rig.frames(50).size());
        CHECK(nullptr != rig.outcomeOf(t) && gs::OutcomeKind::INTERNAL == rig.outcomeOf(t)->kind);
        CHECK(1 == rig.session.counters().transportErrors);
    }

    // NOT_CONNECTED (an election): retried, never failed; the request is offered to the new leader inside its deadline
    {
        pub()->scriptedResults.push_back(cl::OFFER_NOT_CONNECTED);
        const std::uint64_t t = submitCreate(rig, 40);
        rig.cycle(3);
        CHECK_MSG(nullptr == rig.outcomeOf(t), "request failed during election");
        CHECK(1 == rig.session.counters().offerNotConnected);
        rig.cycle(500);
        CHECK_MSG(nullptr == rig.outcomeOf(t), "request failed during election");
        rig.newLeader(1);
        rig.cycle(20);
        const gs::Outcome *o = rig.outcomeOf(t);
        CHECK_MSG(nullptr != o && gs::OutcomeKind::REPLY == o->kind, "request failed during election");
        CHECK(nullptr != o && 1 == o->offers);
    }

    // CLOSED: the publication is gone; the client waits for the next leader, the request is offered there
    {
        auto *p = &rig.fake.publications.at(static_cast<std::size_t>(rig.fake.publicationTo("10.0.0.2:9302")));
        p->scriptedResults.push_back(cl::OFFER_PUBLICATION_CLOSED);
        const std::uint64_t t = submitCreate(rig, 50);
        rig.cycle(3);
        CHECK(nullptr == rig.outcomeOf(t));
        rig.newLeader(2);
        rig.cycle(20);
        CHECK(nullptr != rig.outcomeOf(t) && gs::OutcomeKind::REPLY == rig.outcomeOf(t)->kind);
    }

    // MAX_POSITION_EXCEEDED: the client closes the session; the session reconnects, registers, then offers
    {
        auto *p = &rig.fake.publications.at(static_cast<std::size_t>(rig.fake.publicationTo("10.0.0.3:9302")));
        p->scriptedResults.push_back(cl::OFFER_MAX_POSITION_EXCEEDED);
        const std::uint64_t t = submitCreate(rig, 60);
        rig.cycle(3);
        CHECK(cl::ClientState::CLOSED == rig.client.state());
        CHECK(nullptr == rig.outcomeOf(t));
        rig.cycle(150);
        rig.sessionId++;
        rig.image++;
        rig.fake.pushSessionEvent(rig.fake.lastConnectCorrelation(), rig.sessionId, rig.term, rig.leader,
            cc::EventCode::OK, "", rig.image);
        rig.cycle(20);
        CHECK(2 == rig.client.sessionEpoch());
        CHECK(nullptr != rig.outcomeOf(t) && gs::OutcomeKind::REPLY == rig.outcomeOf(t)->kind);
        CHECK(1 == rig.session.counters().reconnects);
    }
    CHECK(0 == rig.session.counters().lateReplies);
}

// ---- re-offer on a new leader

TEST(session_reoffers_after_a_new_leader)
{
    std::mt19937_64 random(seed());
    for (int variant = 0; variant < 2; ++variant) // 0: the new leader at once; 1: seconds later
    {
        Rig rig;
        rig.replyDelay = [&] { return 1 + static_cast<int>(random() % 5); };
        rig.connect();
        rig.muted = true; // the old leader takes the commands and dies before it answers
        const int K = 6;
        std::vector<std::uint64_t> tickets;
        for (int i = 0; i < K; ++i)
        {
            tickets.push_back(submitCreate(rig, 200 + i));
        }
        rig.cycle(3);
        const std::vector<std::int64_t> firstOrder = rig.createCorrelations;
        CHECK(K == static_cast<int>(firstOrder.size()));
        rig.dropHeld();
        rig.orders.clear(); // the old leader's work was lost with it (not in the log the new leader holds)
        rig.muted = false;
        if (1 == variant)
        {
            rig.cycle(3000);
        }
        rig.newLeader(2);
        rig.cycle(40);
        for (int i = 0; i < K; ++i)
        {
            const gs::Outcome *o = rig.outcomeOf(tickets[static_cast<std::size_t>(i)]);
            CHECK_MSG(nullptr != o && gs::OutcomeKind::REPLY == o->kind,
                "request " + std::to_string(i) + " unanswered after election");
            CHECK(nullptr == o || 2 == o->offers);
        }
        // offered again in the original order with the same correlation ids
        const std::vector<std::int64_t> all = rig.createCorrelations;
        CHECK(2 * K == static_cast<int>(all.size()));
        CHECK(std::vector<std::int64_t>(all.begin() + K, all.end()) == firstOrder);
        CHECK(K == static_cast<int>(rig.orders.size())); // one order per key
        CHECK(K == static_cast<int>(rig.outcomes.size())); // each answered once
        CHECK(K == static_cast<int>(rig.session.counters().reoffers));
    }
}

// ---- ack loss, same key

TEST(session_ack_loss_same_key)
{
    std::mt19937_64 random(seed() + 1);
    for (int run = 0; run < 20; ++run)
    {
        Rig rig;
        rig.connect();
        rig.dropCreateAcks = true; // the core applies the create; the ack is lost with the session
        const std::uint64_t t = submitCreate(rig, 300);
        rig.cycle(1 + static_cast<int>(random() % 8)); // the kill comes 1..8 duty cycles after the offer
        CHECK(1 == rig.orders.size());
        rig.dropCreateAcks = false;
        rig.sessionClosedByCluster();
        rig.cycle(30);
        const gs::Outcome *o = rig.outcomeOf(t);
        CHECK(nullptr != o && gs::OutcomeKind::REPLY == o->kind);
        const wire::OrderAck *ack = nullptr == o ? nullptr : ackOf(*o);
        CHECK(nullptr != ack && ack->duplicate && ack->orderId == rig.orders.begin()->second.orderId);
        CHECK_MSG(1 == rig.orders.size(), "second order for one Idempotency-Key");
        CHECK(nullptr == o || 2 == o->offers);
    }
}

// ---- 504 honesty and the late reply

TEST(session_504_and_the_late_reply)
{
    gs::SessionSettings settings = sessionSettings();
    settings.requestTimeoutMs = 400;
    Rig rig(settings);
    rig.connect();
    CHECK(900'000 == rig.session.config().idempotencyWindowMs()); // what the 504 body names, from CoreConfigReport

    rig.muted = true; // the egress is muted: the core applies, the answer does not come back in time
    const std::uint64_t first = submitCreate(rig, 400);
    rig.cycle(450);
    const gs::Outcome *o = rig.outcomeOf(first);
    CHECK(nullptr != o && gs::OutcomeKind::OUTCOME_UNKNOWN == o->kind);
    CHECK(1 == rig.session.counters().outcomeUnknown);
    CHECK(1 == rig.session.timeoutStreak());

    // the client retries with the same key; the first attempt's reply arrives while the retry is in flight
    const std::uint64_t retry = submitCreate(rig, 400);
    rig.cycle(2); // the retry is offered (its reply is held too)
    rig.releaseHeld();
    rig.muted = false;
    rig.cycle(10);
    const gs::Outcome *r = rig.outcomeOf(retry);
    CHECK(nullptr != r && gs::OutcomeKind::REPLY == r->kind);
    const wire::OrderAck *ack = nullptr == r ? nullptr : ackOf(*r);
    CHECK_MSG(nullptr != ack && ack->correlationId == r->correlationId, "reply delivered to the wrong request");
    CHECK(nullptr != ack && ack->duplicate);
    CHECK_MSG(1 == rig.session.counters().lateReplies, "reply delivered to the wrong request");
    CHECK(1 == rig.orders.size());
    CHECK(0 == rig.session.timeoutStreak()); // one reply clears the streak

    // a request never offered (no registered session) ends 503 CORE_UNAVAILABLE, never 504
    rig.registerAcksToDrop = 100;
    rig.newLeader(1);
    rig.cycle(2);
    const std::uint64_t unsent = submitCreate(rig, 401);
    rig.cycle(450);
    CHECK(nullptr != rig.outcomeOf(unsent) && gs::OutcomeKind::CORE_UNAVAILABLE == rig.outcomeOf(unsent)->kind);
    CHECK(0 == rig.session.timeoutStreak());
}

// ---- query budget

TEST(session_query_budget)
{
    gs::SessionSettings settings = sessionSettings();
    settings.queryRateCount = 10;
    settings.queryRatePeriodMs = 1000;
    settings.queryBurst = 5;
    settings.requestTimeoutMs = 10'000;
    Rig rig(settings);
    rig.connect(); // the config read took one token
    const std::size_t before = rig.queryTimesNs.size();
    const std::int64_t start = rig.fake.now;
    for (int i = 0; i < 200; ++i)
    {
        CHECK(rig.session.submit(gs::Command{ wire::RequestUserBalances{ 0, 1001 } }).accepted);
    }
    rig.cycle(5);
    // a command is never held behind waiting queries
    const std::uint64_t command = submitCreate(rig, 500);
    rig.cycle(3);
    CHECK(nullptr != rig.outcomeOf(command) && gs::OutcomeKind::REPLY == rig.outcomeOf(command)->kind);
    rig.cycle(10'100);
    const std::size_t seen = rig.queryTimesNs.size() - before;
    const double seconds = static_cast<double>(rig.fake.now - start) / 1e9;
    const double rate = static_cast<double>(seen) / seconds;
    std::printf("query budget: %zu queries reached the core in %.2f s (%.2f/s, bound %.1f/s)\n", seen, seconds, rate,
        10 * 1.2);
    CHECK_MSG(rate <= 10 * 1.2, "query rate above the bound");
    const std::size_t overloaded = rig.countOutcomes(gs::OutcomeKind::OVERLOADED);
    CHECK(overloaded > 0 && overloaded + seen == 200);
    CHECK(rig.session.counters().budgetWaitNs > 0);
    // every OVERLOADED query was never offered
    for (const gs::Outcome &o : rig.outcomes)
    {
        if (gs::OutcomeKind::OVERLOADED == o.kind)
        {
            CHECK(0 == o.offers && o.budgetWaitNs > 0);
        }
    }
}

// ---- config cache

TEST(session_config_cache)
{
    Rig rig;
    rig.globalGate = money::GateState::SUSPENDED;
    rig.connect();
    CHECK(1 == rig.frames(56).size());
    const gs::CoreConfig &c = rig.session.config();
    CHECK(c.loaded() && 3 == c.head.configVersion && 1 == c.markets.size() && 2 == c.assets.size());
    CHECK(!c.gateOpen());
    CHECK(rig.session.configSnapshot()->loaded());

    // AdmissionState moves the gate between reads; an older one is ignored
    rig.push(rig.admission(0, money::GateState::OPEN, 1100));
    rig.cycle(3);
    CHECK(rig.session.config().gateOpen());
    rig.push(rig.admission(0, money::GateState::SUSPENDED, 1050));
    rig.cycle(3);
    CHECK(rig.session.config().gateOpen());
    rig.push(rig.admission(1, money::GateState::SUSPENDED, 1200));
    rig.cycle(3);
    CHECK(money::GateState::SUSPENDED == rig.session.config().marketGate(1)->state);

    // ConfigApplied newer than the report: read again (twice in one run); not newer: no read
    for (int round = 0; round < 2; ++round)
    {
        rig.configVersion = 4 + static_cast<std::uint32_t>(round);
        rig.logPosition = 2000 + 1000 * round;
        rig.globalGate = money::GateState::OPEN;
        rig.push(rig.configApplied(rig.logPosition - 10));
        rig.cycle(5);
        CHECK(static_cast<std::size_t>(2 + round) == rig.frames(56).size());
        CHECK(rig.session.config().head.configVersion == rig.configVersion);
    }
    rig.push(rig.configApplied(100));
    rig.cycle(5);
    CHECK(3 == rig.frames(56).size());
    // the market gate moved at 1200 is older than the last report (3000): the report's state holds
    CHECK(money::GateState::OPEN == rig.session.config().marketGate(1)->state);

    // a report with no recorded CoreConfig is not "loaded"
    Rig fresh;
    fresh.configVersion = 0;
    fresh.connect();
    CHECK(fresh.session.config().reported && !fresh.session.config().loaded());
}

// An AdmissionState for a market this view does not hold yet (it arrives before the first report) is kept and applied
// when the report comes, if it is newer than the report; an older one is not.
TEST(session_admission_before_the_report)
{
    for (const bool newer : { true, false })
    {
        Rig rig;
        rig.muted = true; // the config report is held
        rig.connect();
        CHECK(!rig.session.config().reported);
        const std::int64_t at = newer ? rig.logPosition + 500 : rig.logPosition - 100;
        rig.fake.pushAppMessage(rig.sessionId, rig.admission(1, money::GateState::SUSPENDED, at), rig.image, rig.term);
        rig.cycle(3);
        rig.muted = false;
        rig.releaseHeld();
        rig.cycle(5);
        const gs::GateView *gate = rig.session.config().marketGate(1);
        CHECK(rig.session.config().loaded() && nullptr != gate);
        if (newer)
        {
            CHECK_MSG(nullptr != gate && money::GateState::SUSPENDED == gate->state && at == gate->logPosition,
                "admission before the report lost");
        }
        else
        {
            CHECK_MSG(nullptr != gate && money::GateState::OPEN == gate->state,
                "admission older than the report applied");
        }
    }
}

// ---- storage starvation after start

// Every connect attempt refused for storage space: not ready (storage_space) while it lasts, exit 70 once it lasted the
// runtime's bound. A failure for another reason, or a successful connect, ends the episode.
TEST(session_storage_starvation_exits_70)
{
    const char *starved = "insufficient usable storage for new log of length=4194304";
    const std::int64_t bound = 30'000; // cycles = ms: cluster_tools::runtime::STORAGE_STARVED_EXIT_NS

    // continuous: not ready at once, 70 at the bound and not before
    {
        Rig rig;
        rig.fake.subscriptionStatus = cl::Registration::FAILED;
        rig.fake.error = starved;
        CHECK(rig.session.start());
        rig.cycle(500);
        CHECK_MSG(contains(notReady(rig, rig.fake.now), "storage_space"), "storage starvation not read as not ready");
        rig.cycle(static_cast<int>(bound - 1000));
        CHECK(0 == rig.session.exitCode());
        rig.cycle(2000);
        CHECK_MSG(70 == rig.session.exitCode(), "storage starvation retried forever");
    }

    // interrupted by a failure of another reason, then by a successful connect: never 70
    {
        Rig rig;
        rig.fake.subscriptionStatus = cl::Registration::FAILED;
        rig.fake.error = starved;
        CHECK(rig.session.start());
        rig.cycle(static_cast<int>(bound * 2 / 3));
        CHECK(contains(notReady(rig, rig.fake.now), "storage_space"));
        rig.fake.error = "refused for another reason";
        rig.cycle(2000);
        CHECK(!contains(notReady(rig, rig.fake.now), "storage_space"));
        rig.fake.error = starved;
        rig.cycle(static_cast<int>(bound * 2 / 3));
        CHECK(0 == rig.session.exitCode() && contains(notReady(rig, rig.fake.now), "storage_space"));
        rig.fake.subscriptionStatus = cl::Registration::READY;
        for (int i = 0; i < 200 && !rig.client.isConnected(); ++i) // answer whichever attempt is waiting for its answer
        {
            rig.cycle(20);
            rig.fake.pushSessionEvent(rig.fake.lastConnectCorrelation(), rig.sessionId, rig.term, rig.leader,
                cc::EventCode::OK, "", rig.image);
            rig.cycle(5);
        }
        CHECK(rig.client.isConnected() && !contains(notReady(rig, rig.fake.now), "storage_space"));
        rig.cycle(static_cast<int>(bound));
        CHECK_MSG(0 == rig.session.exitCode(), "an interrupted storage starvation ended the process");
    }
}

// ---- readiness from the session

TEST(session_readiness_conditions)
{
    gs::SessionSettings settings = sessionSettings();
    settings.requestTimeoutMs = 200;
    Rig rig(settings);
    gw::GatewayReadyBounds bounds;
    bounds.readyStaleMs = 3000;
    bounds.timeoutStreak = 3;
    CHECK(contains(notReady(rig, rig.fake.now, bounds), "session_connected"));
    rig.globalGate = money::GateState::SUSPENDED;
    rig.connect();
    CHECK(contains(notReady(rig, rig.fake.now, bounds), "keepalive_fresh")); // the first keepalive is 1 s after connect
    rig.cycle(1000);
    for (int round = 0; round < 2; ++round) // every reset twice
    {
        const std::int64_t openAt = rig.logPosition + 100; // newer than the report the registration read
        // gate SUSPENDED: /ready fails gate_open only, /ready/session holds
        std::vector<std::string> f = notReady(rig, rig.fake.now, bounds);
        std::string names;
        for (const std::string &n : f)
        {
            names += n + " ";
        }
        CHECK_MSG(std::vector<std::string>{ "gate_open" } == f, "round " + std::to_string(round) + ": " + names);
        CHECK(gw::failing(gw::gatewaySessionConditions(statusOf(rig))).empty());
        rig.push(rig.admission(0, money::GateState::OPEN, openAt));
        rig.cycle(3);
        CHECK(notReady(rig, rig.fake.now, bounds).empty());

        // no successful keepalive offer inside the bound
        CHECK(contains(notReady(rig, rig.fake.now + 3100 * MS, bounds), "keepalive_fresh"));

        // a timeout streak: N deadlines missed in a row -> not ready until one reply
        rig.muted = true;
        for (int i = 0; i < 3; ++i)
        {
            submitCreate(rig, 600 + 10 * round + i);
        }
        rig.cycle(250);
        CHECK(contains(notReady(rig, rig.fake.now, bounds), "timeout_streak"));
        rig.dropHeld();
        rig.muted = false;
        submitCreate(rig, 650 + round);
        rig.cycle(5);
        CHECK(!contains(notReady(rig, rig.fake.now, bounds), "timeout_streak"));

        // no leader: the egress image is gone
        rig.fake.closedImages[rig.image] = true;
        rig.cycle(2);
        f = notReady(rig, rig.fake.now, bounds);
        CHECK(contains(f, "session_connected") && contains(f, "session_registered"));
        CHECK(!gw::failing(gw::gatewaySessionConditions(statusOf(rig))).empty());
        rig.cycle(2500); // no NewLeaderEvent within 2 s: the session closes; reconnect and register
        rig.sessionId++;
        rig.image++;
        rig.globalGate = money::GateState::SUSPENDED;
        rig.logPosition += 10'000; // the next report is newer than every gate state seen so far
        rig.fake.pushSessionEvent(rig.fake.lastConnectCorrelation(), rig.sessionId, rig.term, rig.leader,
            cc::EventCode::OK, "", rig.image);
        rig.cycle(1100); // the new session's first keepalive is 1 s after its connect
        CHECK(rig.session.registrar().isRegistered());
    }
}

// ---- the decoder gates the registrar on the exact money schema version

TEST(session_adminack_of_another_version_never_registers)
{
    Rig rig;
    rig.answerRegistrations = false;
    rig.connect(false);
    CHECK(1 == rig.frames(80).size());
    std::vector<std::uint8_t> ack = rig.adminAck(rig.session.registrar().correlationId(), money::AdminResult::ACCEPTED);
    ack[6] = 7; // header version 7
    ack[7] = 0;
    rig.fake.pushAppMessage(rig.sessionId, ack, rig.image, rig.term);
    rig.cycle(3);
    CHECK_MSG(!rig.session.registrar().isRegistered(), "registered on a v7 AdminAck");
    CHECK(1 == rig.session.decoder().counters().refused[static_cast<std::size_t>(wire::Refusal::WRONG_VERSION)]);
    rig.fake.pushAppMessage(rig.sessionId, rig.adminAck(rig.session.registrar().correlationId(),
        money::AdminResult::ACCEPTED), rig.image, rig.term);
    rig.cycle(3);
    CHECK(rig.session.registrar().isRegistered());
}

// ---- egress the session does not read: OrderFault is ignored and counted, a refused frame is logged rate limited

TEST(session_ignores_order_fault_and_rate_limits_refusals)
{
    Rig rig;
    rig.connect();
    rig.fake.pushAppMessage(rig.sessionId, rig.orderFault(rig.logPosition++), rig.image, rig.term);
    rig.fake.pushAppMessage(rig.sessionId, rig.orderFault(rig.logPosition++), rig.image, rig.term);
    rig.cycle(3);
    gw::Status status;
    rig.session.fillStatus(status);
    CHECK_MSG(0 == status.decodeRefused, "OrderFault counted as a refused frame");
    CHECK(2 == status.wireIgnored[0]);
    CHECK(0 == rig.session.counters().refusalWarnings);
    CHECK(rig.session.registrar().isRegistered());

    // 2500 refused frames (header version 7): three WARN lines, the 1st, the 1000th and the 2000th
    std::vector<std::uint8_t> v7 = rig.orderFault(1);
    v7[6] = 7;
    for (int n = 0; n < 2500; ++n)
    {
        rig.fake.pushAppMessage(rig.sessionId, v7, rig.image, rig.term);
        rig.cycle();
    }
    rig.cycle(5);
    rig.session.fillStatus(status);
    CHECK(2500 == status.decodeRefused);
    CHECK_MSG(3 == rig.session.counters().refusalWarnings,
        "refusal warnings " + std::to_string(rig.session.counters().refusalWarnings) + ", expected 3");
}

// ---- UNREGISTERED_SESSION is never an answer

TEST(session_unregistered_reject_registers_again)
{
    Rig rig;
    rig.connect();
    bool once = true;
    rig.rejectUnregistered = [&](std::int64_t) {
        const bool r = once;
        once = false;
        return r;
    };
    const std::size_t registrations = rig.frames(80).size();
    const std::uint64_t t = submitCreate(rig, 700);
    rig.cycle(20);
    const gs::Outcome *o = rig.outcomeOf(t);
    CHECK(nullptr != o && gs::OutcomeKind::REPLY == o->kind && nullptr != ackOf(*o));
    CHECK(nullptr == o || 2 == o->offers);
    CHECK(registrations + 1 == rig.frames(80).size());
    CHECK(1 == rig.session.counters().unregisteredRejects);
}

// ---- REJECTED registration is a configuration error

TEST(session_rejected_registration_is_exit_78)
{
    Rig rig;
    rig.registrationResult = money::AdminResult::REJECTED;
    rig.connect();
    CHECK(78 == rig.session.exitCode());
    const gs::SubmitResult r = rig.session.submit(gs::Command{ create(800) });
    CHECK(!r.accepted && gs::OutcomeKind::CORE_UNAVAILABLE == r.refusal);
}

// ---- the request table bound

TEST(session_full_table_is_overloaded)
{
    gs::SessionSettings settings = sessionSettings();
    settings.maxInflight = 3;
    Rig rig(settings);
    rig.answerQueries = false;
    rig.connect(); // the config read holds one slot until its deadline
    rig.muted = true;
    CHECK(rig.session.submit(gs::Command{ create(900) }).accepted);
    CHECK(rig.session.submit(gs::Command{ create(901) }).accepted);
    const gs::SubmitResult full = rig.session.submit(gs::Command{ create(902) });
    CHECK(!full.accepted && gs::OutcomeKind::OVERLOADED == full.refusal);
    CHECK(1 == rig.session.counters().refusedFull);
}
