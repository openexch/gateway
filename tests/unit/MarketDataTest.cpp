// oe-marketdata T0: the settings (every required key, every invalid form, the expected market set), the book side
// over the engine's golden egress bytes (golden output frames, the chain rule, stale and heal), the fields the edge
// relay inspects, the frame shapes the simulator and the UI parse, the egress templates dropped unread, readiness on
// the expected set and the admin routes of the role.
//
// Golden output frames live in tests/golden/marketdata/<name>.json, one frame per file, byte for byte. GW_MD_GOLDEN_WRITE=1
// rewrites them from this build (review the diff before committing).
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <boost/json.hpp>

#include "Check.h"
#include "gateway/common/Admin.h"
#include "gateway/common/Readiness.h"

#include "BookCore.h"
#include "MarketDataSettings.h"

// the engines' golden corpus of the verified cluster-tools tree (GoldenMatch.java)
#include "golden_frames.inc"

using namespace openexchange::gateway;
using namespace openexchange::gateway::marketdata;
namespace rt = cluster_tools::runtime;
namespace json = boost::json;

namespace {

const std::string SECRET_HEX = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

rt::DirProbe probe()
{
    rt::DirProbe p;
    p.isDirectory = [](const std::string &d) { return "/run/oe" == d || "/dev/shm/aeron-host" == d; };
    p.isWritable = [](const std::string &d) { return "/run/oe" == d; };
    return p;
}

std::map<std::string, std::string> mdEnv()
{
    return {
        { "MD_DRIVER_MODE", "embedded" },
        { "MD_DRIVER_DIR", "/run/oe" },
        { "MD_INSTANCE_ID", "1" },
        { "MD_ME_CLUSTER_ADDRESSES", "10.0.0.1,10.0.0.2,10.0.0.3" },
        { "MD_ME_PORT_BASE", "9000" },
        { "CLUSTER_PORT_STRIDE", "100" },
        { "MD_ME_EGRESS_ENDPOINT", "10.0.0.9:9091" },
        { "ME_AUTH_CREDENTIAL", "11:GATEWAY:" + SECRET_HEX },
        { "MD_ME_JOURNAL_ARCHIVES", "10.0.0.2:8010,10.0.0.3:8010,10.0.0.1:8010" },
        { "MD_JOURNAL_CONTROL_STREAM", "10" },
        { "MD_JOURNAL_STREAM", "4100" },
        { "MD_JOURNAL_CHANNEL", "aeron:ipc" },
        { "MD_REPLAY_STREAM", "4300" },
        { "MD_JOURNAL_MTU", "8192" },
        { "MD_LOCAL_HOST", "10.0.0.9" },
        { "MD_PUBLIC_BIND", "0.0.0.0:8083" },
        { "MD_ADMIN_BIND", "127.0.0.1:9183" },
        { "MD_PG_URL", "postgresql://10.0.0.20/oe_marketdata" },
        { "MD_PG_USER", "oe_marketdata_writer" },
        { "MD_PG_PASSWORD_FILE", "/etc/oe/md-pg-password" },
        { "MD_CORS_ORIGINS", "none" },
        { "MD_EXPECTED_MARKETS", "1,2,3" },
    };
}

ParseResult<MarketDataSettings> md(const std::map<std::string, std::string> &env)
{
    return parseMarketDataSettings(MapEnvironment(env), probe());
}

bool hasLine(const std::vector<std::string> &errors, const std::string &prefix)
{
    for (const std::string &e : errors)
    {
        if (0 == e.rfind(prefix, 0))
        {
            return true;
        }
    }
    return false;
}

std::string joined(const std::vector<std::string> &errors)
{
    std::string out;
    for (const std::string &e : errors)
    {
        out += "\n    " + e;
    }
    return out;
}

const SettingRecord *recordOf(const std::vector<SettingRecord> &records, const std::string &name)
{
    for (const SettingRecord &r : records)
    {
        if (r.name == name)
        {
            return &r;
        }
    }
    return nullptr;
}

// ---- hand-built ME frames (order schema id 1 v12, the layouts the library's codec static_asserts)

template<typename T>
void put(std::vector<std::uint8_t> &b, std::size_t at, T v)
{
    std::memcpy(b.data() + at, &v, sizeof v);
}

struct L
{
    std::int64_t price;
    std::int64_t quantity;
    std::int32_t orders;
};

std::vector<std::uint8_t> snapshot(std::int32_t market, std::int64_t version, std::int64_t timestamp,
    const std::vector<L> &bids, const std::vector<L> &asks)
{
    std::vector<std::uint8_t> b(8 + 36 + 4 + 20 * bids.size() + 4 + 20 * asks.size(), 0);
    put<std::uint16_t>(b, 0, 36);
    put<std::uint16_t>(b, 2, 22);
    put<std::uint16_t>(b, 4, 1);
    put<std::uint16_t>(b, 6, 12);
    put<std::int32_t>(b, 8, market);
    put<std::int64_t>(b, 12, timestamp);
    put<std::int64_t>(b, 20, version - 2);
    put<std::int64_t>(b, 28, version - 1);
    put<std::int64_t>(b, 36, version);
    std::size_t at = 44;
    for (const auto *side : { &bids, &asks })
    {
        put<std::uint16_t>(b, at, 20);
        put<std::uint16_t>(b, at + 2, static_cast<std::uint16_t>(side->size()));
        at += 4;
        for (const L &l : *side)
        {
            put<std::int64_t>(b, at, l.price);
            put<std::int64_t>(b, at + 8, l.quantity);
            put<std::int32_t>(b, at + 16, l.orders);
            at += 20;
        }
    }
    return b;
}

struct C
{
    std::int64_t price;
    std::int64_t quantity;
    std::int32_t orders;
    std::uint8_t side;   // 0 BID, 1 ASK
    std::uint8_t update; // 0 NEW, 1 UPDATE, 2 DELETE
};

std::vector<std::uint8_t> delta(std::int32_t market, std::int64_t from, std::int64_t to, std::int64_t timestamp,
    const std::vector<C> &changes)
{
    std::vector<std::uint8_t> b(8 + 44 + 4 + 22 * changes.size(), 0);
    put<std::uint16_t>(b, 0, 44);
    put<std::uint16_t>(b, 2, 25);
    put<std::uint16_t>(b, 4, 1);
    put<std::uint16_t>(b, 6, 12);
    put<std::int32_t>(b, 8, market);
    put<std::int64_t>(b, 12, timestamp);
    put<std::int64_t>(b, 20, to);
    put<std::int64_t>(b, 28, to);
    put<std::int64_t>(b, 36, to);
    put<std::int64_t>(b, 44, from);
    std::size_t at = 52;
    put<std::uint16_t>(b, at, 22);
    put<std::uint16_t>(b, at + 2, static_cast<std::uint16_t>(changes.size()));
    at += 4;
    for (const C &c : changes)
    {
        put<std::int64_t>(b, at, c.price);
        put<std::int64_t>(b, at + 8, c.quantity);
        put<std::int32_t>(b, at + 16, c.orders);
        b[at + 20] = c.side;
        b[at + 21] = c.update;
        at += 22;
    }
    return b;
}

// The ladder JAVA_BOOK_DELTA_80 applies to: market 3 at its fromVersion, 20 bids 13.80..13.99 and 20 asks
// 15.80..15.99, the levels the delta deletes.
constexpr std::int64_t DELTA80_FROM = 0x0000'0200'0000'0001LL;
std::vector<std::uint8_t> delta80Base()
{
    std::vector<L> bids;
    std::vector<L> asks;
    for (int i = 0; i < 20; ++i)
    {
        bids.push_back({ 1'399'000'000LL - i * 1'000'000LL, 500'000'000LL + i, 2 + i });
        asks.push_back({ 1'580'000'000LL + i * 1'000'000LL, 700'000'000LL + i, 3 + i });
    }
    return snapshot(3, DELTA80_FROM, 1'700'000'000'444LL, bids, asks);
}

// ---- a client as the duty thread sees it

struct Captured
{
    std::string text;
    FrameType type;
};

class TestOutlet final : public Outlet
{
public:
    bool offer(const Frame &f) override
    {
        frames.push_back({ *f.text, f.type });
        return true;
    }
    void resyncBegins() override { resyncs++; }
    std::vector<Captured> frames;
    int resyncs = 0;
};

constexpr std::int64_t MS = 1'000'000LL;

struct Rig
{
    explicit Rig(std::vector<int> expected = { 1, 2, 3 }) :
        core(BookCoreSettings{ std::move(expected), MarketDataReadyBounds{ 10'000, 15'000 } }, stats,
            [] { return std::int64_t{ 1'700'000'009'999LL }; })
    {
    }
    std::shared_ptr<TestOutlet> client(std::uint64_t id, int market)
    {
        auto o = std::make_shared<TestOutlet>();
        core.open(id, o);
        if (0 != market)
        {
            core.subscribe(id, market, now);
        }
        return o;
    }
    void egress(const std::vector<std::uint8_t> &frame, std::int64_t frameTerm = 1, std::int64_t sessionTerm = 1)
    {
        core.onEgress(frame.data(), frame.size(), frameTerm, sessionTerm, now);
    }
    Status status() const
    {
        Status s;
        core.fillStatus(s);
        s.driverConnected = true;
        return s;
    }
    MdStats stats;
    BookCore core;
    std::int64_t now = 1'000 * MS;
};

std::string goldenDir()
{
    return GW_MD_GOLDEN_DIR;
}

// Compares a produced frame with tests/golden/marketdata/<name>.json (or writes it with GW_MD_GOLDEN_WRITE=1).
void golden(const std::string &name, const std::string &produced)
{
    const std::string path = goldenDir() + "/" + name + ".json";
    const char *write = std::getenv("GW_MD_GOLDEN_WRITE");
    if (nullptr != write && 0 == std::strcmp(write, "1"))
    {
        std::ofstream(path, std::ios::binary) << produced << "\n";
        return;
    }
    std::ifstream in(path, std::ios::binary);
    std::stringstream buffer;
    buffer << in.rdbuf();
    std::string want = buffer.str();
    if (!want.empty() && '\n' == want.back())
    {
        want.pop_back();
    }
    CHECK_MSG(in.is_open(), "golden file missing: " + path);
    CHECK_MSG(want == produced, "golden mismatch " + name + ":\n  want " + want + "\n  got  " + produced);
}

json::object parsed(const std::string &text)
{
    return json::parse(text).as_object();
}

// The chain a client keeps: a fresh snapshot sets its version, a stale snapshot ends the chain, a delta must start at
// the version held while the chain is live.
struct ChainModel
{
    bool live = false;
    std::int64_t version = 0;
    bool brokenServed = false;
    void see(const Captured &c)
    {
        if (FrameType::BOOK_SNAPSHOT == c.type)
        {
            const json::object o = parsed(c.text);
            live = nullptr == o.if_contains("stale");
            version = o.at("bookVersion").as_int64();
        }
        else if (FrameType::BOOK_DELTA == c.type)
        {
            const json::object o = parsed(c.text);
            if (!live || o.at("fromVersion").as_int64() != version)
            {
                brokenServed = true;
            }
            version = o.at("bookVersion").as_int64();
        }
    }
};

std::vector<Captured> since(const std::shared_ptr<TestOutlet> &o, std::size_t from)
{
    return std::vector<Captured>(o->frames.begin() + static_cast<std::ptrdiff_t>(from), o->frames.end());
}

// Every frame a rig produces over the golden inputs (the relay and the shape rows read these).
std::vector<Captured> sampleFrames()
{
    Rig rig;
    auto client = rig.client(1, 3);
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.egress(delta80Base());
    rig.egress(JAVA_BOOK_DELTA_80);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    rig.egress(JAVA_BOOK_SNAPSHOT_EMPTY);
    rig.core.ping(1);
    rig.core.badRequest(1, "unknown action 'x'");
    return client->frames;
}

}

// ---------------------------------------------------------------- settings

TEST(md_settings_defaults)
{
    const auto p = md(mdEnv());
    CHECK_MSG(p.ok(), joined(p.errors));
    const MarketDataSettings &s = p.settings;
    CHECK(s.common.driverMode == DriverMode::EMBEDDED && s.common.driverTimeoutMs == 10'000);
    CHECK(s.meIngress == "0=10.0.0.1:9002,1=10.0.0.2:9102,2=10.0.0.3:9202");
    CHECK((s.expectedMarkets == std::vector<int>{ 1, 2, 3 }));
    CHECK(s.egressStaleMs == 10'000 && s.bookStaleMs == 15'000 && s.historyStaleMs == 5000 && s.sendStallMs == 5000 &&
        s.sourceProbeMs == 10'000);
    CHECK(s.tradeFlushMs == 20 && s.tickersIntervalMs == 1000);
    CHECK(s.pendingTrades == 65'536 && s.commitBatch == 512 && s.commitIntervalMs == 50);
    CHECK(s.recentTrades == 50 && s.candleHistory == 200 && s.candlesLimitMax == 500);
    CHECK(s.wsMaxConnections == 4096 && s.wsQueueBytes == 524'288 && s.wsUnwritableCloseMs == 30'000 &&
        s.wsIdleMs == 60'000 && s.wsMaxFrame == 65'536);
    CHECK(s.corsOrigins.empty());
    const SettingRecord *queue = recordOf(p.records, "MD_WS_QUEUE_BYTES");
    CHECK(nullptr != queue && "524288" == queue->value && "default" == queue->source);
    const SettingRecord *cred = recordOf(p.records, "ME_AUTH_CREDENTIAL");
    CHECK(nullptr != cred && std::string::npos == cred->value.find(SECRET_HEX));
    const auto client = meClientSettings(s);
    CHECK(client.responseChannel == "aeron:udp?endpoint=10.0.0.9:9091");
    // a stride of 0 is one member per address: every member on the same port
    auto env = mdEnv();
    env["CLUSTER_PORT_STRIDE"] = "0";
    CHECK(md(env).settings.meIngress == "0=10.0.0.1:9002,1=10.0.0.2:9002,2=10.0.0.3:9002");
}

TEST(md_settings_required_keys)
{
    const std::vector<std::string> required = { "MD_DRIVER_MODE", "MD_DRIVER_DIR", "MD_INSTANCE_ID",
        "MD_ME_CLUSTER_ADDRESSES", "MD_ME_PORT_BASE", "CLUSTER_PORT_STRIDE", "MD_ME_EGRESS_ENDPOINT",
        "ME_AUTH_CREDENTIAL", "MD_ME_JOURNAL_ARCHIVES", "MD_JOURNAL_CONTROL_STREAM", "MD_JOURNAL_STREAM",
        "MD_JOURNAL_CHANNEL", "MD_REPLAY_STREAM", "MD_JOURNAL_MTU", "MD_LOCAL_HOST", "MD_PUBLIC_BIND", "MD_ADMIN_BIND",
        "MD_PG_URL", "MD_PG_USER", "MD_PG_PASSWORD_FILE", "MD_CORS_ORIGINS", "MD_EXPECTED_MARKETS" };
    for (const std::string &key : required)
    {
        auto env = mdEnv();
        env.erase(key);
        const auto p = md(env);
        if ("MD_PUBLIC_BIND" == key)
        {
            CHECK_MSG(!p.ok() && hasLine(p.errors, "missing setting: MD_PUBLIC_BIND"),
                "service started without MD_PUBLIC_BIND");
            continue;
        }
        CHECK_MSG(!p.ok() && hasLine(p.errors, "missing setting: " + key), key + ":" + joined(p.errors));
    }
    // external mode: AERON_DIR is required there
    auto env = mdEnv();
    env.erase("MD_DRIVER_DIR");
    env["MD_DRIVER_MODE"] = "external";
    const auto ext = md(env);
    CHECK_MSG(hasLine(ext.errors, "missing setting: AERON_DIR"), joined(ext.errors));
    // nothing at all: every required key in one pass, each named
    const auto none = md({});
    for (const char *key : { "MD_DRIVER_MODE", "MD_INSTANCE_ID", "MD_PUBLIC_BIND", "MD_ADMIN_BIND",
             "MD_EXPECTED_MARKETS", "ME_AUTH_CREDENTIAL", "MD_PG_URL", "MD_CORS_ORIGINS" })
    {
        CHECK_MSG(hasLine(none.errors, std::string("missing setting: ") + key), std::string(key) + ":" + joined(none.errors));
    }
    for (const std::string &e : none.errors)
    {
        CHECK_MSG(0 == e.rfind("missing setting: ", 0) || 0 == e.rfind("invalid setting: ", 0), e);
    }
}

TEST(md_settings_invalid_forms)
{
    const std::vector<std::pair<std::pair<std::string, std::string>, std::string>> cases = {
        { { "MD_EXPECTED_MARKETS", "1,6" }, "invalid setting: MD_EXPECTED_MARKETS" },
        { { "MD_EXPECTED_MARKETS", "0" }, "invalid setting: MD_EXPECTED_MARKETS" },
        { { "MD_EXPECTED_MARKETS", "1,1" }, "invalid setting: MD_EXPECTED_MARKETS" },
        { { "MD_EXPECTED_MARKETS", "1,,2" }, "invalid setting: MD_EXPECTED_MARKETS" },
        { { "MD_EXPECTED_MARKETS", "btc" }, "invalid setting: MD_EXPECTED_MARKETS" },
        { { "MD_DRIVER_TIMEOUT_MS", "2999" }, "invalid setting: MD_DRIVER_TIMEOUT_MS" },
        { { "MD_DRIVER_TIMEOUT_MS", "60001" }, "invalid setting: MD_DRIVER_TIMEOUT_MS" },
        { { "ME_AUTH_CREDENTIAL", "11:FORWARDER:" + SECRET_HEX }, "invalid setting: ME_AUTH_CREDENTIAL: ME credential "
            "class must be GATEWAY" },
        { { "ME_AUTH_CREDENTIAL", "11:GATEWAY:00" }, "invalid setting: ME_AUTH_CREDENTIAL" },
        { { "MD_PUBLIC_BIND", "0.0.0.0:0" }, "invalid setting: MD_PUBLIC_BIND" },
        { { "MD_ADMIN_BIND", "127.0.0.1:0" }, "invalid setting: MD_ADMIN_BIND" },
        { { "MD_ADMIN_BIND", "127.0.0.1:8083" }, "invalid setting: MD_ADMIN_BIND" },
        { { "MD_ME_EGRESS_ENDPOINT", "0.0.0.0:9091" }, "invalid setting: MD_ME_EGRESS_ENDPOINT" },
        { { "MD_ME_CLUSTER_ADDRESSES", "10.0.0.1,0.0.0.0" }, "invalid setting: MD_ME_CLUSTER_ADDRESSES" },
        { { "MD_ME_PORT_BASE", "x" }, "invalid setting: MD_ME_PORT_BASE" },
        { { "CLUSTER_PORT_STRIDE", "-1" }, "invalid setting: CLUSTER_PORT_STRIDE" },
        { { "MD_ME_INGRESS_CHANNEL", "aeron:udp?term-length=64k" }, "invalid setting: MD_ME_INGRESS_CHANNEL" },
        { { "MD_CORS_ORIGINS", "example.com" }, "invalid setting: MD_CORS_ORIGINS" },
        { { "MD_PG_URL", "mysql://x/y" }, "invalid setting: MD_PG_URL" },
        { { "MD_PG_PASSWORD_FILE", "relative/path" }, "invalid setting: MD_PG_PASSWORD_FILE" },
        { { "MD_WS_QUEUE_BYTES", "0" }, "invalid setting: MD_WS_QUEUE_BYTES" },
        { { "MD_BOOK_STALE_MS", "15s" }, "invalid setting: MD_BOOK_STALE_MS" },
        { { "MD_TYPO_SETTING", "1" }, "invalid setting: MD_TYPO_SETTING" },
        { { "MD_JOURNAL_MTU", "100" }, "invalid setting: MD_JOURNAL_MTU" },
        { { "AERON_DIR", "/dev/shm/aeron-host" }, "invalid setting: AERON_DIR" },
    };
    for (const auto &c : cases)
    {
        auto env = mdEnv();
        env[c.first.first] = c.first.second;
        const auto p = md(env);
        CHECK_MSG(!p.ok() && hasLine(p.errors, c.second), c.first.first + "=" + c.first.second + ":" + joined(p.errors));
    }
    auto env = mdEnv();
    env["MD_CORS_ORIGINS"] = "https://openexchange.dev,http://localhost:5173";
    const auto ok = md(env);
    CHECK_MSG(ok.ok() && 2 == ok.settings.corsOrigins.size(), joined(ok.errors));
}

// ---------------------------------------------------------------- the book

TEST(md_book_golden_frames)
{
    Rig rig;
    auto client = rig.client(1, 3);
    CHECK(3 == client->frames.size());
    golden("subscription_confirmed", client->frames[0].text);
    golden("book_snapshot_no_snapshot", client->frames[1].text);
    golden("error_history_unavailable", client->frames[2].text);

    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    CHECK(3 == client->frames.size()); // a heartbeat is no frame
    rig.egress(delta80Base());
    rig.egress(JAVA_BOOK_DELTA_80);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    rig.egress(JAVA_BOOK_SNAPSHOT_EMPTY);
    CHECK(7 == client->frames.size());
    if (7 == client->frames.size())
    {
        golden("book_snapshot_base", client->frames[3].text);
        golden("book_delta_80", client->frames[4].text);
        golden("book_snapshot_20x20", client->frames[5].text);
        golden("book_snapshot_empty", client->frames[6].text);
    }
    rig.core.ping(1);
    golden("pong", client->frames.back().text);
    rig.core.badRequest(1, "unknown action 'x'");
    golden("error_bad_request", client->frames.back().text);
    CHECK(1 == rig.stats.heartbeats.load() && 3 == rig.stats.snapshots.load() && 1 == rig.stats.deltas.load());
}

TEST(md_book_chain_break_marks_stale)
{
    Rig rig;
    auto client = rig.client(1, 3);
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    ChainModel chain;
    std::int64_t v = 100;
    rig.egress(snapshot(3, v, 1'700'000'000'000LL, { { 100'000'000, 1, 1 } }, { { 200'000'000, 1, 1 } }));
    for (int round = 0; round < 2; ++round)
    {
        const std::size_t before = client->frames.size();
        // a delta on the chain: sent
        rig.egress(delta(3, v, v + 1, 1'700'000'000'001LL, { { 100'000'000, 2, 2, 0, 1 } }));
        // a delta with a wrong fromVersion: the chain breaks, the book goes stale
        rig.egress(delta(3, v + 5, v + 6, 1'700'000'000'002LL, { { 100'000'000, 3, 3, 0, 1 } }));
        CHECK(MdBookState::BROKEN == rig.core.bookState(3, rig.now));
        // more deltas while broken: never sent
        rig.egress(delta(3, v + 6, v + 7, 1'700'000'000'003LL, { { 100'000'000, 4, 4, 0, 1 } }));
        rig.egress(delta(3, v + 7, v + 8, 1'700'000'000'004LL, { { 100'000'000, 5, 5, 0, 1 } }));
        const std::vector<Captured> got = since(client, before);
        bool staleSeen = false;
        for (const Captured &c : got)
        {
            staleSeen = staleSeen ||
                (FrameType::BOOK_SNAPSHOT == c.type && std::string::npos != c.text.find("\"stale\":true"));
        }
        CHECK_MSG(staleSeen, "no stale snapshot after the break, round " + std::to_string(round));
        // the next snapshot clears it
        v += 20;
        rig.egress(snapshot(3, v, 1'700'000'000'010LL, { { 100'000'000, 9, 1 } }, { { 200'000'000, 1, 1 } }));
        CHECK(MdBookState::FRESH == rig.core.bookState(3, rig.now));
        CHECK(FrameType::BOOK_SNAPSHOT == client->frames.back().type &&
            std::string::npos == client->frames.back().text.find("stale"));
    }
    for (const Captured &c : client->frames)
    {
        chain.see(c);
    }
    CHECK_MSG(!chain.brokenServed, "book served across a version break");
    CHECK(2 == rig.stats.breaks.load());
}

TEST(md_session_end_resets_books)
{
    Rig rig;
    auto client = rig.client(1, 3);
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    CHECK(MdBookState::FRESH == rig.core.bookState(3, rig.now));
    const std::size_t before = client->frames.size();
    rig.core.onSessionEnd(rig.now);
    const auto got = since(client, before);
    CHECK(1 == got.size() && std::string::npos != got[0].text.find("\"bids\":[],\"asks\":[]") &&
        std::string::npos != got[0].text.find("\"stale\":true"));
    CHECK(MdBookState::NO_SNAPSHOT == rig.core.bookState(3, rig.now));
    // a book frame of an ended term never reaches the ladder
    rig.core.onSessionUp(rig.now);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20, 4, 5);
    CHECK(1 == rig.stats.staleTerm.load() && MdBookState::NO_SNAPSHOT == rig.core.bookState(3, rig.now));
}

TEST(md_expected_set)
{
    Rig rig; // expected 1,2,3
    auto a = rig.client(1, 3);
    auto b = rig.client(2, 4); // refused: not served
    CHECK_MSG(!b->frames.empty() && std::string::npos != b->frames.back().text.find("UNKNOWN_MARKET"),
        "subscribe to market 4 was not refused");
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    const std::uint64_t snapshots = rig.stats.snapshots.load();
    rig.egress(snapshot(6, 50, 1'700'000'000'000LL, { { 100'000'000, 1, 1 } }, {}));
    rig.egress(snapshot(4, 50, 1'700'000'000'000LL, { { 100'000'000, 1, 1 } }, {}));
    CHECK(2 == rig.stats.foreignMarket.load());
    CHECK(snapshots == rig.stats.snapshots.load() && 0 == rig.stats.breaks.load());
    CHECK(cluster_tools::domain::forwarder::LadderState::EMPTY == rig.core.stitcher().ladder(4).state());
    bool served = false;
    for (const auto &o : { a, b })
    {
        for (const Captured &c : o->frames)
        {
            if (FrameType::BOOK_SNAPSHOT == c.type || FrameType::BOOK_DELTA == c.type)
            {
                const json::object f = parsed(c.text);
                const json::value &m = f.at("marketId");
                served = served || (m.is_int64() && (4 == m.as_int64() || 6 == m.as_int64()));
            }
        }
    }
    CHECK_MSG(!served, "book served for a market outside the expected set");
}

TEST(md_egress_templates_dropped_unread)
{
    Rig rig;
    auto client = rig.client(1, 3);
    const std::size_t before = client->frames.size();
    rig.egress(JAVA_TRADE_EXECUTION_BATCH_3);
    rig.egress(JAVA_ORDER_STATUS_BATCH);
    CHECK(1 == rig.stats.dropped[2].load()); // 26
    CHECK(1 == rig.stats.dropped[1].load()); // 24
    CHECK(client->frames.size() == before);
    CHECK(0 == rig.stats.snapshots.load() && 0 == rig.stats.deltas.load() && 0 == rig.stats.foreignMarket.load() &&
        0 == rig.stats.malformedNoMarket.load() && 0 == rig.stats.heartbeats.load());
    // a book frame of another order-schema version is refused undecoded
    auto v11 = JAVA_BOOK_SNAPSHOT_20x20;
    put<std::uint16_t>(v11, 6, 11);
    rig.egress(v11);
    CHECK(1 == rig.stats.schemaRefused.load() && 0 == rig.stats.snapshots.load());
}

TEST(md_subscribe_order_and_actions)
{
    Rig rig;
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    auto o = rig.client(1, 3);
    CHECK(3 == o->frames.size());
    if (3 == o->frames.size())
    {
        CHECK(FrameType::SUBSCRIPTION_CONFIRMED == o->frames[0].type && FrameType::BOOK_SNAPSHOT == o->frames[1].type &&
            FrameType::ERROR == o->frames[2].type);
        CHECK(std::string::npos == o->frames[1].text.find("stale"));
        CHECK(o->frames[2].text.find("\"parts\":[\"tape\",\"candles\",\"ticker\"]") != std::string::npos);
    }
    rig.core.refresh(1, 3, rig.now);
    CHECK(5 == o->frames.size() && FrameType::BOOK_SNAPSHOT == o->frames[3].type);
    rig.core.refresh(1, 2, rig.now);
    CHECK(std::string::npos != o->frames.back().text.find("NOT_SUBSCRIBED"));
    // subscribe replaces the market: frames of the old market stop
    rig.core.subscribe(1, 2, rig.now);
    const std::size_t before = o->frames.size();
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    CHECK(o->frames.size() == before);
    rig.core.resync(1, rig.now);
    CHECK(1 == o->resyncs && FrameType::BOOK_SNAPSHOT == o->frames[before].type);
    rig.core.gone(1);
    CHECK(0 == rig.core.connections());
}

// ---------------------------------------------------------------- readiness

TEST(md_readiness_expected_set)
{
    Rig rig;
    MarketDataReadyBounds bounds{ 10'000, 15'000 };
    rig.core.onSessionUp(rig.now);
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    // heartbeats only: me_session holds, book_fresh does not (no snapshot of any expected market)
    Status s = rig.status();
    CHECK(failing(marketDataSessionConditions(s, rig.now, bounds)).empty());
    CHECK_MSG((failing(marketDataBookConditions(s, rig.now, bounds)) == std::vector<std::string>{ "book_fresh" }),
        "ready with no market snapshot");
    // market 3 only: 1 and 2 are still no_snapshot
    rig.egress(JAVA_BOOK_SNAPSHOT_20x20);
    s = rig.status();
    CHECK(MdBookState::NO_SNAPSHOT == mdBookState(s.mdMarkets[0], s.mdLastHeartbeatNs, rig.now, bounds));
    CHECK(!failing(marketDataBookConditions(s, rig.now, bounds)).empty());
    rig.egress(snapshot(1, 10, 1, { { 100'000'000, 1, 1 } }, {}));
    rig.egress(snapshot(2, 0x77, 1, {}, {})); // a versioned empty book
    s = rig.status();
    CHECK(failing(marketDataBookConditions(s, rig.now, bounds)).empty());
    // later: books with levels and no frame turn stale; the empty one stays fresh while heartbeats arrive
    rig.now += 16'000 * MS;
    rig.egress(JAVA_CLUSTER_HEARTBEAT_V12);
    rig.core.doWork(rig.now);
    s = rig.status();
    CHECK(MdBookState::STALE == mdBookState(s.mdMarkets[0], s.mdLastHeartbeatNs, rig.now, bounds));
    CHECK(MdBookState::FRESH == mdBookState(s.mdMarkets[1], s.mdLastHeartbeatNs, rig.now, bounds));
    CHECK(2 == rig.stats.booksStale.load());
    // the full /ready names the trade path this build does not have
    const auto all = failing(marketDataReadyConditions(s, rig.now, bounds));
    CHECK((all == std::vector<std::string>{ "book_fresh", "journal_attached", "journal_fresh", "history_writable" }));
    // no heartbeat for the stale time: no me_session
    rig.now += 11'000 * MS;
    s = rig.status();
    CHECK(!failing(marketDataSessionConditions(s, rig.now, bounds)).empty());
}

TEST(md_admin_routes)
{
    AdminView v;
    v.role = Role::MARKETDATA;
    v.processName = "oe-marketdata";
    v.status.driverConnected = true;
    v.nowNs = 1000 * MS;
    v.watchdogMs = 5000;
    v.status.mdMarkets[0].expected = true;
    cluster_tools::runtime::HttpRequest r;
    r.method = "GET";
    r.path = "/ready/book";
    auto res = adminRoute(r, v);
    CHECK(503 == res.status && std::string::npos != res.body.find("\"me_session\",\"book_fresh\""));
    r.path = "/ready/session";
    res = adminRoute(r, v);
    CHECK(503 == res.status && std::string::npos != res.body.find("[\"me_session\"]"));
    r.path = "/metrics";
    res = adminRoute(r, v);
    CHECK(200 == res.status && std::string::npos != res.body.find("\nmd_ready 0\n") &&
        std::string::npos == res.body.find("wire_ignored_total"));
    r.path = "/health";
    res = adminRoute(r, v);
    CHECK(std::string::npos != res.body.find("\"books\":{\"1\":{\"state\":\"no_snapshot\""));
    CHECK(std::string::npos != res.body.find("\"halt\":null"));
}

// ---------------------------------------------------------------- relay compatibility and frame shapes

TEST(md_relay_fields)
{
    // the fields the edge relay inspects (match edge/market-relay protocol.ts): type a string; marketId, bookVersion,
    // fromVersion, timestamp JSON numbers; trades and tickers arrays; market and version absent
    for (const Captured &c : sampleFrames())
    {
        const json::object o = parsed(c.text);
        CHECK_MSG(o.contains("type") && o.at("type").is_string(), "relay field has the wrong JSON type: type");
        for (const char *key : { "marketId", "bookVersion", "fromVersion", "timestamp" })
        {
            if (o.contains(key))
            {
                CHECK_MSG(o.at(key).is_int64() || o.at(key).is_uint64(),
                    std::string("relay field has the wrong JSON type: ") + key);
            }
        }
        for (const char *key : { "trades", "tickers" })
        {
            if (o.contains(key))
            {
                CHECK_MSG(o.at(key).is_array(), std::string("relay field has the wrong JSON type: ") + key);
            }
        }
        CHECK_MSG(!o.contains("market") && !o.contains("version"), "a dropped relay field is sent: " + c.text);
        if (FrameType::BOOK_SNAPSHOT == c.type || FrameType::BOOK_DELTA == c.type)
        {
            CHECK_MSG(o.contains("marketId"), "relay field has the wrong JSON type: marketId");
            // the relay reads bookVersion first, then version, else 0 (feed.ts)
            const json::value *bv = o.if_contains("bookVersion");
            const json::value *ver = o.if_contains("version");
            const std::int64_t read = nullptr != bv && bv->is_int64() ? bv->as_int64()
                : nullptr != ver && ver->is_int64()                  ? ver->as_int64()
                                                                     : 0;
            const bool unversioned = std::string::npos != c.text.find("\"stale\":true") &&
                std::string::npos != c.text.find("\"bids\":[],\"asks\":[]");
            CHECK_MSG(unversioned || 0 != read, "relay reads book version 0");
        }
    }
}

TEST(md_frame_shapes)
{
    // the schema table of the market frames, every field with its JSON type; prices and quantities decimal strings
    const std::regex decimal("^-?[0-9]+(\\.[0-9]{1,8})?$");
    auto isNum = [](const json::object &o, const char *k) {
        return o.contains(k) && (o.at(k).is_int64() || o.at(k).is_uint64());
    };
    auto isDec = [&](const json::object &o, const char *k) {
        if (!o.contains(k) || !o.at(k).is_string())
        {
            return false;
        }
        const json::string &s = o.at(k).as_string();
        return std::regex_match(std::string(s.data(), s.size()), decimal);
    };
    std::set<std::string> types;
    for (const Captured &c : sampleFrames())
    {
        const json::object o = parsed(c.text);
        const std::string type(o.at("type").as_string().c_str());
        types.insert(type);
        if ("BOOK_SNAPSHOT" == type)
        {
            CHECK(isNum(o, "marketId") && isNum(o, "bookVersion") && isNum(o, "timestamp"));
            CHECK(o.contains("bids") && o.at("bids").is_array() && o.contains("asks") && o.at("asks").is_array());
            for (const char *side : { "bids", "asks" })
            {
                for (const json::value &l : o.at(side).as_array())
                {
                    const json::object &lo = l.as_object();
                    CHECK_MSG(isDec(lo, "price") && isDec(lo, "quantity"), "price is not a decimal string");
                    CHECK(isNum(lo, "orderCount"));
                }
            }
            if (o.contains("stale"))
            {
                CHECK(o.at("stale").is_bool() && o.at("stale").as_bool());
            }
        }
        else if ("BOOK_DELTA" == type)
        {
            CHECK(isNum(o, "marketId") && isNum(o, "bidVersion") && isNum(o, "askVersion") && isNum(o, "bookVersion") &&
                isNum(o, "fromVersion") && isNum(o, "timestamp"));
            // the simulator's change decoder (tools market-sim feed/ws.go wsChange): price, quantity, side, updateType
            bool decodable = o.contains("changes") && o.at("changes").is_array();
            for (const json::value &ch : o.at("changes").as_array())
            {
                const json::object &co = ch.as_object();
                CHECK_MSG(isDec(co, "price") && isDec(co, "quantity"), "price is not a decimal string");
                const bool side = co.contains("side") && co.at("side").is_string() &&
                    ("BID" == co.at("side").as_string() || "ASK" == co.at("side").as_string());
                const bool update = co.contains("updateType") && co.at("updateType").is_string() &&
                    ("NEW_LEVEL" == co.at("updateType").as_string() || "UPDATE_LEVEL" == co.at("updateType").as_string() ||
                        "DELETE_LEVEL" == co.at("updateType").as_string());
                decodable = decodable && side && update && isNum(co, "orderCount");
            }
            CHECK_MSG(decodable, "simulator cannot decode BOOK_DELTA");
        }
        else if ("SUBSCRIPTION_CONFIRMED" == type)
        {
            CHECK(o.contains("status") && o.at("status").is_string() && isNum(o, "marketId"));
        }
        else if ("PONG" == type)
        {
            CHECK(isNum(o, "timestamp"));
        }
        else if ("ERROR" == type)
        {
            CHECK(o.contains("code") && o.at("code").is_string() && o.contains("message") && o.at("message").is_string());
            if (o.contains("parts"))
            {
                CHECK(o.at("parts").is_array());
            }
        }
        else
        {
            CHECK_MSG(false, "unexpected frame type " + type);
        }
    }
    CHECK((types == std::set<std::string>{ "BOOK_SNAPSHOT", "BOOK_DELTA", "SUBSCRIPTION_CONFIRMED", "PONG", "ERROR" }));
}
