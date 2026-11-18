// Negative and fuzz cases for the money-frame decoder: truncated frames, another schema id, any version but the exact
// one, wrong block lengths, unknown templates, trailing bytes and values the schema does not name are refused, by
// name and counted, never read and never a crash; a refused frame delivers nothing to the handler. Also the reason the
// decoder is the gate in front of the exported session registrar: the registrar alone reads an AdminAck at any header
// version. Ignored templates (OrderFault, the settlement broadcasts) are counted apart and pass the same structural
// checks; a handler that throws (std or not) never takes the process down; refusals are logged rate limited.
//
// The random leg is seeded and sized by the environment (GW_WIRE_FUZZ_SEED, GW_WIRE_FUZZ_ITERATIONS; defaults 1 and
// 200000); a failure prints the seed and the iteration.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "Check.h"
#include "WireTestValues.h"
#include "cluster_tools/client/CoreSessionRegistrar.h"
#include "com_openexchange_core_infrastructure_generated/AdmissionState.h"
#include "com_openexchange_core_infrastructure_generated/CoreConfigReport.h"
#include "com_openexchange_core_infrastructure_generated/OrderAck.h"
#include "com_openexchange_core_infrastructure_generated/OrderEventBatch.h"
#include "com_openexchange_core_infrastructure_generated/OrderReject.h"
#include "gateway/common/Wire.h"

namespace {
#include "gateway_frames.inc"
}

using namespace openexchange::gateway::wire;

namespace {

struct Golden
{
    const char *name;
    const std::vector<std::uint8_t> *frame;
};

// every decoded golden frame
const Golden REPLIES[] = {
    { "AdminAck", &JAVA_ADMIN_ACK_REGISTER },
    { "AdmissionState", &JAVA_ADMISSION_STATE },
    { "ConfigApplied", &JAVA_CONFIG_APPLIED },
    { "OrderAck", &JAVA_ORDER_ACK },
    { "OrderReject", &JAVA_ORDER_REJECT },
    { "OrderEvent", &JAVA_ORDER_EVENT },
    { "OrderEventBatch", &JAVA_ORDER_EVENT_BATCH },
    { "OrderEventBatch (empty)", &JAVA_ORDER_EVENT_BATCH_EMPTY },
    { "OrderSnapshotBatch", &JAVA_ORDER_SNAPSHOT_BATCH },
    { "OrderSnapshotEnd", &JAVA_ORDER_SNAPSHOT_END },
    { "OrderEventsPage", &JAVA_ORDER_EVENTS_PAGE },
    { "OrderEventsEnd", &JAVA_ORDER_EVENTS_END },
    { "UserBalances", &JAVA_USER_BALANCES },
    { "UserBalances (unknown)", &JAVA_USER_BALANCES_UNKNOWN },
    { "CoreConfigReport", &JAVA_CORE_CONFIG_REPORT },
    { "BalanceUpdate", &JAVA_BALANCE_UPDATE },
    { "BalanceUpdateBatch", &JAVA_BALANCE_UPDATE_BATCH },
    { "DepositAck", &JAVA_DEPOSIT_ACK },
    { "DepositReject", &JAVA_DEPOSIT_REJECT },
    { "WithdrawAck", &JAVA_WITHDRAW_ACK },
    { "WithdrawReject", &JAVA_WITHDRAW_REJECT },
};

// every ignored golden frame
const Golden IGNORED_FRAMES[] = {
    { "OrderFault", &JAVA_ORDER_FAULT },
    { "SettlementApplied", &JAVA_SETTLEMENT_APPLIED },
    { "SettleFault", &JAVA_SETTLE_FAULT },
    { "SettlementAppliedBatch", &JAVA_SETTLEMENT_APPLIED_BATCH },
};

// Counts every handler call; a refused frame must leave it at zero.
struct CallCounter : ReplyHandler
{
    int calls = 0;
    void onAdminAck(const AdminAck &) override { calls++; }
    void onAdmissionState(const AdmissionState &) override { calls++; }
    void onConfigApplied(const ConfigApplied &) override { calls++; }
    void onOrderAck(const OrderAck &) override { calls++; }
    void onOrderReject(const OrderReject &) override { calls++; }
    void onOrderEvent(const OrderEvent &) override { calls++; }
    void onOrderEventBatch(std::uint16_t) override { calls++; }
    void onOrderEventsPage(std::int64_t, std::uint16_t) override { calls++; }
    void onPageEvent(std::int64_t, const OrderEvent &) override { calls++; }
    void onOrderEventsEnd(const OrderEventsEnd &) override { calls++; }
    void onOrderSnapshotBatch(std::uint16_t) override { calls++; }
    void onOrderSnapshotEntry(const OrderSnapshotEntry &) override { calls++; }
    void onOrderSnapshotEnd(const OrderSnapshotEnd &) override { calls++; }
    void onUserBalances(const UserBalancesHead &) override { calls++; }
    void onUserBalanceLine(std::int64_t, const BalanceLine &) override { calls++; }
    void onCoreConfigReport(const CoreConfigHead &) override { calls++; }
    void onCoreConfigMarket(const MarketConfig &) override { calls++; }
    void onCoreConfigAsset(const AssetConfig &) override { calls++; }
    void onBalanceUpdate(const BalanceUpdate &) override { calls++; }
    void onBalanceUpdateBatch(std::uint16_t) override { calls++; }
    void onDepositAck(const MoneyAck &) override { calls++; }
    void onWithdrawAck(const MoneyAck &) override { calls++; }
    void onDepositReject(const DepositReject &) override { calls++; }
    void onWithdrawReject(const WithdrawReject &) override { calls++; }
};

void putU16(std::vector<std::uint8_t> &frame, std::size_t at, std::uint16_t value)
{
    frame[at] = static_cast<std::uint8_t>(value & 0xFF);
    frame[at + 1] = static_cast<std::uint8_t>(value >> 8);
}

std::uint16_t getU16(const std::vector<std::uint8_t> &frame, std::size_t at)
{
    return static_cast<std::uint16_t>(frame[at] | (frame[at + 1] << 8));
}

// One refusal: the expected reason, counted once, nothing delivered, describe() names it.
void expectRefused(const std::string &what, const std::vector<std::uint8_t> &frame, Refusal expected,
    const std::string &describeHas = "")
{
    Decoder decoder;
    CallCounter counter;
    const Refusal got = decoder.decode(frame.data(), frame.size(), counter);
    if (got != expected)
    {
        gwtest::fail(__FILE__, __LINE__,
            what + ": expected " + nameOf(expected) + ", got " + nameOf(got) +
                (Refusal::NONE == got ? std::string() : " (" + describe(decoder.lastRefusal()) + ")"));
        return;
    }
    CHECK_MSG(0 == counter.calls, what + ": a refused frame reached the handler");
    CHECK_MSG(0 == decoder.counters().accepted && 1 == decoder.counters().refusedTotal() &&
            1 == decoder.counters().refused[static_cast<std::size_t>(expected)],
        what + ": not counted once under " + nameOf(expected));
    const std::string line = describe(decoder.lastRefusal());
    CHECK_MSG(std::string::npos != line.find(nameOf(expected)), what + ": '" + line + "' does not name the reason");
    if (!describeHas.empty())
    {
        CHECK_MSG(std::string::npos != line.find(describeHas), what + ": '" + line + "' lacks '" + describeHas + "'");
    }
}

std::uint64_t envOr(const char *name, std::uint64_t fallback)
{
    const char *value = std::getenv(name);
    if (nullptr == value || '\0' == *value)
    {
        return fallback;
    }
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    if (nullptr == end || '\0' != *end)
    {
        std::fprintf(stderr, "%s='%s' is not a decimal number\n", name, value);
        std::exit(2);
    }
    return parsed;
}

}

TEST(wire_refuses_truncated_frames)
{
    for (const Golden &g : REPLIES)
    {
        for (std::size_t n = 0; n < g.frame->size(); ++n)
        {
            const std::vector<std::uint8_t> prefix(g.frame->begin(), g.frame->begin() + n);
            expectRefused(std::string(g.name) + " cut to " + std::to_string(n) + " bytes", prefix, Refusal::TRUNCATED);
        }
    }
    // a null payload is a zero-length frame
    Decoder decoder;
    ReplyHandler none;
    CHECK(Refusal::TRUNCATED == decoder.decode(nullptr, 24, none));
}

TEST(wire_refuses_another_schema_id)
{
    for (const Golden &g : REPLIES)
    {
        for (const std::uint16_t schema : { 0, 1, 3, 0xFFFF })
        {
            std::vector<std::uint8_t> frame = *g.frame;
            putU16(frame, 4, schema);
            expectRefused(std::string(g.name) + " schema " + std::to_string(schema), frame, Refusal::WRONG_SCHEMA,
                "schema " + std::to_string(schema));
        }
    }
}

TEST(wire_refuses_every_other_version_by_name)
{
    for (const Golden &g : REPLIES)
    {
        for (const std::uint16_t version : { 0, 5, 6, 7, 8, 10, 0xFFFF })
        {
            std::vector<std::uint8_t> frame = *g.frame;
            putU16(frame, 6, version);
            expectRefused(std::string(g.name) + " version " + std::to_string(version), frame, Refusal::WRONG_VERSION,
                "wrong_version (version " + std::to_string(version) + ", this build reads schema 2 version 9 only)");
        }
    }
}

TEST(wire_refuses_a_wrong_block_length)
{
    for (const Golden &g : REPLIES)
    {
        const std::uint16_t block = getU16(*g.frame, 0);
        for (const int delta : { -1, 1, 8 })
        {
            std::vector<std::uint8_t> frame = *g.frame;
            putU16(frame, 0, static_cast<std::uint16_t>(block + delta));
            expectRefused(std::string(g.name) + " block length " + std::to_string(block + delta), frame,
                Refusal::WRONG_BLOCK_LENGTH);
        }
        // a longer block that the frame really carries (the extension rule of a later version) is still refused: the
        // version is exact, so the block length is too
        std::vector<std::uint8_t> extended = *g.frame;
        putU16(extended, 0, static_cast<std::uint16_t>(block + 8));
        extended.insert(extended.begin() + 8 + block, 8, 0);
        expectRefused(std::string(g.name) + " extended block", extended, Refusal::WRONG_BLOCK_LENGTH);
    }
}

TEST(wire_refuses_unknown_templates)
{
    // ids outside the money schema, and money templates the gateway neither reads nor ignores (commands it sends, the
    // forwarder's and the bridge's traffic, frames of channels it does not subscribe to); OrderFault 113 is ignored
    for (const std::uint16_t id : { 0, 3, 7, 50, 54, 56, 60, 77, 80, 104, 105, 108, 114, 119, 0xFFFF })
    {
        std::vector<std::uint8_t> frame = JAVA_ADMIN_ACK_REGISTER;
        putU16(frame, 2, id);
        expectRefused("template " + std::to_string(id), frame, Refusal::UNKNOWN_TEMPLATE,
            "template " + std::to_string(id));
    }
}

TEST(wire_refuses_a_wrong_group_block_length)
{
    struct GroupAt
    {
        const char *name;
        const std::vector<std::uint8_t> *frame;
        const char *group;
    };
    const GroupAt groups[] = {
        { "OrderEventBatch", &JAVA_ORDER_EVENT_BATCH, "events" },
        { "OrderEventsPage", &JAVA_ORDER_EVENTS_PAGE, "events" },
        { "OrderSnapshotBatch", &JAVA_ORDER_SNAPSHOT_BATCH, "orders" },
        { "UserBalances", &JAVA_USER_BALANCES, "balances" },
        { "CoreConfigReport", &JAVA_CORE_CONFIG_REPORT, "markets" },
        { "BalanceUpdateBatch", &JAVA_BALANCE_UPDATE_BATCH, "updates" },
    };
    for (const GroupAt &g : groups)
    {
        const std::size_t at = 8 + getU16(*g.frame, 0);
        std::vector<std::uint8_t> frame = *g.frame;
        putU16(frame, at, static_cast<std::uint16_t>(getU16(frame, at) + 1));
        expectRefused(std::string(g.name) + " group block length", frame, Refusal::WRONG_GROUP_BLOCK_LENGTH, g.group);
        // a count larger than the frame carries
        std::vector<std::uint8_t> counted = *g.frame;
        putU16(counted, at + 2, static_cast<std::uint16_t>(getU16(counted, at + 2) + 1));
        expectRefused(std::string(g.name) + " group count + 1", counted, Refusal::TRUNCATED);
        putU16(counted, at + 2, 0xFFFF);
        expectRefused(std::string(g.name) + " group count 65535", counted, Refusal::TRUNCATED);
    }
    // the second group of CoreConfigReport
    const std::vector<std::uint8_t> &report = JAVA_CORE_CONFIG_REPORT;
    const std::size_t markets = 8 + getU16(report, 0);
    const std::size_t assets = markets + 4 + getU16(report, markets) * getU16(report, markets + 2);
    std::vector<std::uint8_t> frame = report;
    putU16(frame, assets, static_cast<std::uint16_t>(getU16(frame, assets) - 1));
    expectRefused("CoreConfigReport assets block length", frame, Refusal::WRONG_GROUP_BLOCK_LENGTH, "assets");
}

TEST(wire_refuses_trailing_bytes)
{
    for (const Golden &g : REPLIES)
    {
        std::vector<std::uint8_t> frame = *g.frame;
        frame.push_back(0);
        expectRefused(std::string(g.name) + " + 1 byte", frame, Refusal::TRAILING_BYTES);
    }
}

TEST(wire_refuses_values_the_schema_does_not_name)
{
    // OrderAck: status 10 (no such OrderStatus), status 255 (NULL_VALUE), duplicate 2 (BoolFlag)
    const std::size_t statusAt = 8 + money::OrderAck::statusEncodingOffset();
    const std::size_t duplicateAt = 8 + money::OrderAck::duplicateEncodingOffset();
    for (const std::uint8_t raw : { 10, 254, 255 })
    {
        std::vector<std::uint8_t> frame = JAVA_ORDER_ACK;
        frame[statusAt] = raw;
        expectRefused("OrderAck status " + std::to_string(raw), frame, Refusal::BAD_VALUE, "(status)");
    }
    std::vector<std::uint8_t> flag = JAVA_ORDER_ACK;
    flag[duplicateAt] = 2;
    expectRefused("OrderAck duplicate 2", flag, Refusal::BAD_VALUE, "(duplicate)");

    // AdmissionState: gateReasons with bit 9 (no such GateReasons choice)
    std::vector<std::uint8_t> reasons = JAVA_ADMISSION_STATE;
    const std::size_t reasonsAt = 8 + money::AdmissionState::gateReasonsEncodingOffset();
    reasons[reasonsAt + 1] |= 0x02;
    expectRefused("AdmissionState gateReasons bit 9", reasons, Refusal::BAD_VALUE, "(gateReasons)");

    // OrderReject reason 22 (a gap in CoreRejectReason)
    std::vector<std::uint8_t> reject = JAVA_ORDER_REJECT;
    reject[8 + money::OrderReject::reasonEncodingOffset()] = 22;
    expectRefused("OrderReject reason 22", reject, Refusal::BAD_VALUE, "(reason)");

    // AdminAck result 5
    std::vector<std::uint8_t> ack = JAVA_ADMIN_ACK_REGISTER;
    ack[8 + money::AdminAck::resultEncodingOffset()] = 5;
    expectRefused("AdminAck result 5", ack, Refusal::BAD_VALUE, "(result)");
}

TEST(wire_a_bad_value_in_the_last_entry_delivers_nothing)
{
    // the third event of the batch carries eventKind 12: the batch is refused whole, the first two never delivered
    std::vector<std::uint8_t> frame = JAVA_ORDER_EVENT_BATCH;
    const std::size_t entries = 8 + getU16(frame, 0) + 4;
    const std::size_t entryLength = getU16(frame, 8 + getU16(frame, 0));
    frame[entries + 2 * entryLength + money::OrderEventBatch::Events::eventKindEncodingOffset()] = 12;
    expectRefused("OrderEventBatch entry 3 eventKind 12", frame, Refusal::BAD_VALUE, "(eventKind)");

    // the second market of a config report: circuitBreaker 7
    std::vector<std::uint8_t> report = JAVA_CORE_CONFIG_REPORT;
    const std::size_t markets = 8 + getU16(report, 0) + 4;
    const std::size_t marketLength = getU16(report, 8 + getU16(report, 0));
    report[markets + marketLength + money::CoreConfigReport::Markets::circuitBreakerEncodingOffset()] = 7;
    expectRefused("CoreConfigReport market 2 circuitBreaker 7", report, Refusal::BAD_VALUE,
        "(markets.circuitBreaker)");
}

// An ignored template is ignored only as the exact frame the schema defines: every structural check still applies.
TEST(wire_an_ignored_template_is_still_checked)
{
    for (const Golden &g : IGNORED_FRAMES)
    {
        const std::string name = g.name;
        std::vector<std::uint8_t> v8 = *g.frame;
        putU16(v8, 6, 8);
        expectRefused(name + " v8", v8, Refusal::WRONG_VERSION,
            "template " + std::to_string(getU16(*g.frame, 2)) + " " + name);
        std::vector<std::uint8_t> v10 = *g.frame;
        putU16(v10, 6, 10);
        expectRefused(name + " v10", v10, Refusal::WRONG_VERSION);
        std::vector<std::uint8_t> schema = *g.frame;
        putU16(schema, 4, 1);
        expectRefused(name + " schema 1", schema, Refusal::WRONG_SCHEMA);
        std::vector<std::uint8_t> block = *g.frame;
        putU16(block, 0, static_cast<std::uint16_t>(getU16(block, 0) + 1));
        block.push_back(0);
        expectRefused(name + " block + 1", block, Refusal::WRONG_BLOCK_LENGTH);
        std::vector<std::uint8_t> shortFrame(g.frame->begin(), g.frame->end() - 1);
        expectRefused(name + " one byte short", shortFrame, Refusal::TRUNCATED);
        std::vector<std::uint8_t> trailing = *g.frame;
        trailing.push_back(0);
        expectRefused(name + " trailing byte", trailing, Refusal::TRAILING_BYTES);
    }
}

// The handler contract: a handler must not throw. One that does stops the frame's delivery at that call, whatever it
// threw (std::exception or not), and decode() returns handler_fault instead of letting it out of a noexcept function.
TEST(wire_a_throwing_handler_is_counted_never_a_crash)
{
    struct Thrower : CallCounter
    {
        int throwAt = 0;
        bool nonStd = false;
        void onOrderEvent(const OrderEvent &e) override
        {
            CallCounter::onOrderEvent(e);
            if (calls == throwAt)
            {
                if (nonStd)
                {
                    throw 42;
                }
                throw std::runtime_error("handler");
            }
        }
    };
    for (const bool nonStd : { false, true })
    {
        Decoder decoder;
        Thrower t;
        t.throwAt = 3; // the batch head, then events 1 and 2: the throw comes from the second event
        t.nonStd = nonStd;
        const Refusal r = decoder.decode(JAVA_ORDER_EVENT_BATCH.data(), JAVA_ORDER_EVENT_BATCH.size(), t);
        CHECK_MSG(Refusal::HANDLER_FAULT == r,
            std::string(nonStd ? "non-std" : "std") + " throw: got " + nameOf(r));
        CHECK(3 == t.calls); // delivery stopped at the throwing call; nothing after it
        CHECK(0 == decoder.counters().accepted);
        CHECK(1 == decoder.counters().refused[static_cast<std::size_t>(Refusal::HANDLER_FAULT)]);
        CHECK(std::string::npos != describe(decoder.lastRefusal()).find("handler_fault (handler threw)"));
        // the decoder is usable after it
        CallCounter fine;
        CHECK(Refusal::NONE == decoder.decode(JAVA_ORDER_EVENT_BATCH.data(), JAVA_ORDER_EVENT_BATCH.size(), fine));
        CHECK(4 == fine.calls);
    }
}

TEST(wire_refusals_are_logged_rate_limited)
{
    std::uint64_t logged = 0;
    for (std::uint64_t n = 1; n <= 2500; ++n)
    {
        logged += refusalLogged(n) ? 1 : 0;
    }
    CHECK(3 == logged); // the 1st, the 1000th and the 2000th
    CHECK(refusalLogged(1) && !refusalLogged(2) && !refusalLogged(999) && refusalLogged(1000) && refusalLogged(5000));
}

TEST(wire_counts_accepted_and_refused)
{
    Decoder decoder;
    CallCounter counter;
    std::uint64_t refused = 0;
    for (const Golden &g : IGNORED_FRAMES)
    {
        CHECK(Refusal::NONE == decoder.decode(g.frame->data(), g.frame->size(), counter));
    }
    CHECK(0 == counter.calls);
    CHECK(0 == decoder.counters().accepted && 0 == decoder.counters().refusedTotal());
    CHECK(sizeof(IGNORED_FRAMES) / sizeof(IGNORED_FRAMES[0]) == decoder.counters().ignoredTotal());
    for (const Golden &g : REPLIES)
    {
        CHECK(Refusal::NONE == decoder.decode(g.frame->data(), g.frame->size(), counter));
        std::vector<std::uint8_t> v7 = *g.frame;
        putU16(v7, 6, 7);
        CHECK(Refusal::WRONG_VERSION == decoder.decode(v7.data(), v7.size(), counter));
        refused++;
    }
    const std::uint64_t replies = sizeof(REPLIES) / sizeof(REPLIES[0]);
    CHECK(replies == decoder.counters().accepted);
    CHECK(refused == decoder.counters().refused[static_cast<std::size_t>(Refusal::WRONG_VERSION)]);
    CHECK(refused == decoder.counters().refusedTotal());
    CHECK(0 == decoder.counters().refused[static_cast<std::size_t>(Refusal::NONE)]);
}

// Contract: the exported registrar reads an AdminAck at any header version, so the owner puts the decoder in front of
// it. The first half documents that the registrar alone would take a v7 ack as a registration; the second that the
// decoder refuses the same bytes by name.
TEST(wire_gates_the_registrar_on_the_exact_version)
{
    namespace ct = cluster_tools::client;
    struct Client
    {
        std::int64_t nanoTime() const { return 1; }
        ct::ClientState state() const { return ct::ClientState::CONNECTED; }
        ct::CloseReason closeReason() const { return ct::CloseReason::NONE; }
        std::uint64_t sessionEpoch() const { return 1; }
        std::int64_t leadershipTermId() const { return 0; }
        bool isConnected() const { return true; }
        ct::OfferResult tryOffer(const std::uint8_t *, std::size_t)
        {
            ct::OfferResult r;
            r.status = ct::OfferStatus::OK;
            r.position = 64;
            return r;
        }
    } client;
    ct::RegistrarSettings settings;
    settings.sessionClass = ct::SessionClass::GATEWAY;
    settings.credentialClass = ct::SessionClass::GATEWAY;
    settings.channels = ct::core_channel::ACKS | ct::core_channel::CONTROL;
    settings.identity = 7;
    ct::CoreSessionRegistrar<Client> registrar(client, settings);
    registrar.doWork();
    CHECK(ct::RegistrationState::PENDING == registrar.state());

    std::vector<std::uint8_t> ack = JAVA_ADMIN_ACK_REGISTER; // AdminAck(80) ACCEPTED
    const std::uint64_t correlationId = static_cast<std::uint64_t>(registrar.correlationId());
    for (std::size_t b = 0; b < 8; ++b) // little endian, as the wire
    {
        ack[8 + money::AdminAck::correlationIdEncodingOffset() + b] = static_cast<std::uint8_t>(correlationId >> (8 * b));
    }
    putU16(ack, 6, 7);

    Decoder decoder;
    ReplyHandler none;
    CHECK(Refusal::WRONG_VERSION == decoder.decode(ack.data(), ack.size(), none));
    CHECK(ct::RegistrationState::PENDING == registrar.state()); // gated: never offered to the registrar

    ct::EgressMessage message;
    message.payload = ack.data();
    message.length = ack.size();
    const bool taken = registrar.onEgress(message);
    if (!(taken && ct::RegistrationState::REGISTERED == registrar.state()))
    {
        // the registrar learned the version gate: the gateway's own gate stays, this line says the note is stale
        std::printf("note: the exported registrar now refuses a v7 AdminAck itself\n");
    }
}

TEST(wire_fuzz_never_crashes_and_counts_everything)
{
    const std::uint64_t seed = envOr("GW_WIRE_FUZZ_SEED", 1);
    const std::uint64_t iterations = envOr("GW_WIRE_FUZZ_ITERATIONS", 200000);
    std::mt19937_64 rng(seed);
    Decoder decoder;
    CallCounter counter;
    std::uint64_t accepted = 0;
    // the seeds: every decoded and every ignored golden frame
    std::vector<const std::vector<std::uint8_t> *> seeds;
    for (const Golden &g : REPLIES)
    {
        seeds.push_back(g.frame);
    }
    for (const Golden &g : IGNORED_FRAMES)
    {
        seeds.push_back(g.frame);
    }
    const std::size_t n = seeds.size();
    for (std::uint64_t it = 0; it < iterations; ++it)
    {
        std::vector<std::uint8_t> frame;
        const std::uint64_t mode = rng() % 5;
        if (0 == mode)
        {
            // random bytes, random length (0..511)
            frame.resize(rng() % 512);
            for (std::uint8_t &b : frame)
            {
                b = static_cast<std::uint8_t>(rng());
            }
        }
        else
        {
            frame = *seeds[rng() % n];
            const std::uint64_t flips = 1 + rng() % 4;
            for (std::uint64_t f = 0; f < flips && !frame.empty(); ++f)
            {
                switch (mode)
                {
                    case 1: frame[rng() % frame.size()] = static_cast<std::uint8_t>(rng()); break;
                    case 2: frame[rng() % std::min<std::size_t>(frame.size(), 16)] ^= static_cast<std::uint8_t>(1u << (rng() % 8)); break;
                    case 3: frame.resize(rng() % (frame.size() + 64), static_cast<std::uint8_t>(rng())); break;
                    default: frame[rng() % frame.size()] ^= static_cast<std::uint8_t>(1u << (rng() % 8)); break;
                }
            }
        }
        // exact-length copy on the heap, so a read past the end is a read past the allocation
        std::vector<std::uint8_t> exact(frame);
        exact.shrink_to_fit();
        const Refusal r = decoder.decode(exact.empty() ? nullptr : exact.data(), exact.size(), counter);
        if (Refusal::NONE == r)
        {
            accepted++;
        }
        if (Refusal::DECODE_FAULT == r)
        {
            gwtest::fail(__FILE__, __LINE__,
                "seed " + std::to_string(seed) + " iteration " + std::to_string(it) + ": " +
                    describe(decoder.lastRefusal()));
            return;
        }
    }
    CHECK(accepted == decoder.counters().accepted + decoder.counters().ignoredTotal());
    CHECK(iterations == decoder.counters().accepted + decoder.counters().ignoredTotal() + decoder.counters().refusedTotal());
    CHECK(0 == decoder.counters().refused[static_cast<std::size_t>(Refusal::HANDLER_FAULT)]);
    std::printf("fuzz seed %llu: %llu frames, %llu accepted, %llu ignored, refused:",
        static_cast<unsigned long long>(seed), static_cast<unsigned long long>(iterations),
        static_cast<unsigned long long>(decoder.counters().accepted),
        static_cast<unsigned long long>(decoder.counters().ignoredTotal()));
    for (std::size_t k = 1; k < REFUSAL_COUNT; ++k)
    {
        std::printf(" %s=%llu", nameOf(static_cast<Refusal>(k)),
            static_cast<unsigned long long>(decoder.counters().refused[k]));
    }
    std::printf("\n");
}
