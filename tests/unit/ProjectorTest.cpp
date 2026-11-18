// oe-projector T0: its settings (required keys in one pass, defaults recorded and shown, the archive reader's rules
// under HISTORY_PROJECTOR_, the key file, the pairs, misspelt names refused), its readiness conditions and its admin
// rendering (a standby reads not ready and says so).
#include <unistd.h>

#include <cctype>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>

#include "Check.h"
#include "gateway/common/Admin.h"
#include "ProjectorApp.h"
#include "ProjectorSettings.h"

using namespace openexchange::gateway;
namespace rt = cluster_tools::runtime;

namespace {

const std::string SECRET_HEX = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

rt::DirProbe probe()
{
    rt::DirProbe p;
    p.isDirectory = [](const std::string &d) { return "/run/oe" == d || "/dev/shm/aeron-host" == d; };
    p.isWritable = [](const std::string &d) { return "/run/oe" == d; };
    return p;
}

std::string keyFile(const std::string &name, const std::string &content)
{
    const std::string path = "/tmp/gw-projector-test-" + std::to_string(::getpid()) + "-" + name;
    std::ofstream(path) << content;
    return path;
}

std::map<std::string, std::string> projectorEnv()
{
    return {
        { "HISTORY_PROJECTOR_DRIVER_MODE", "external" },
        { "AERON_DIR", "/dev/shm/aeron-host" },
        { "HISTORY_PROJECTOR_INSTANCE_ID", "3" },
        { "HISTORY_PROJECTOR_ADMIN_BIND", "127.0.0.1:9190" },
        { "HISTORY_PROJECTOR_JOURNAL_ARCHIVES", "10.0.0.2:8010,10.0.0.3:8010,10.0.0.1:8010" },
        { "HISTORY_PROJECTOR_JOURNAL_CONTROL_STREAM", "10" },
        { "HISTORY_PROJECTOR_JOURNAL_STREAM", "4102" },
        { "HISTORY_PROJECTOR_JOURNAL_CHANNEL", "aeron:ipc" },
        { "HISTORY_PROJECTOR_REPLAY_STREAM", "4202" },
        { "HISTORY_PROJECTOR_JOURNAL_MTU", "8192" },
        { "HISTORY_PROJECTOR_LOCAL_HOST", "10.0.0.9" },
        { "HISTORY_PROJECTOR_CLUSTER_MEMBERS", "0=10.0.0.1:9010,1=10.0.0.2:9010,2=10.0.0.3:9010" },
        { "HISTORY_PROJECTOR_CLUSTER_KEY_FILE", keyFile("gateway-key", "9:GATEWAY:" + SECRET_HEX + "\n") },
        { "HISTORY_PROJECTOR_PG_URL", "postgresql://projector:pw@10.0.0.20/history" },
    };
}

bool has(const std::vector<std::string> &errors, const std::string &needle)
{
    for (const std::string &e : errors)
    {
        if (std::string::npos != e.find(needle))
        {
            return true;
        }
    }
    std::fprintf(stderr, "    missing '%s' in:\n", needle.c_str());
    for (const std::string &e : errors)
    {
        std::fprintf(stderr, "      %s\n", e.c_str());
    }
    return false;
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

}

TEST(projector_settings_accepted_with_defaults)
{
    const auto parsed = parseProjectorSettings(MapEnvironment(projectorEnv()), probe());
    CHECK_MSG(parsed.ok(), parsed.errors.empty() ? "" : parsed.errors.front());
    const ProjectorSettings &s = parsed.settings;
    CHECK(DriverMode::EXTERNAL == s.common.driverMode);
    CHECK(3 == s.archive.archives.size());
    CHECK(4102 == s.archive.journalStreamId);
    CHECK(8192 == s.archive.journalMtu);
    CHECK(9 == s.clusterCredentials.keyId);
    CHECK(s.warnings.empty());
    CHECK(10'000 == s.leaseMs && 1000 == s.leasePollMs && 256 == s.batchEvents && 50 == s.batchMs);
    CHECK(4096 == s.queueEvents && 1000 == s.headProbeMs && 3000 == s.headFreshMs && 5000 == s.readyLagMs);
    CHECK(3000 == s.pgStatementTimeoutMs && 1000 == s.pgRetryMs && 10'000 == s.pgIdleInTxTimeoutMs);
    CHECK(5 == s.pgKeepaliveIdleS && 2 == s.pgKeepaliveIntervalS && 3 == s.pgKeepaliveCount);
    CHECK(5000 == s.archiveTimeoutMs && 1000 == s.relistMs && 5000 == s.sourceBehindMs);
    CHECK(1000 == s.sourceBackoffMs && 30'000 == s.sourceBackoffMaxMs && 5000 == s.gapRetryMs);
    // every setting in force is recorded, defaults included, the URL redacted
    for (const char *name : { "HISTORY_PROJECTOR_LEASE_MS", "HISTORY_PROJECTOR_BATCH_EVENTS", "HISTORY_PROJECTOR_QUEUE_EVENTS",
             "HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS", "HISTORY_PROJECTOR_SOURCE_BACKOFF_MAX_MS",
             "HISTORY_PROJECTOR_DUTY_WATCHDOG_MS", "HISTORY_PROJECTOR_DRIVER_TIMEOUT_MS",
             "HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS" })
    {
        const SettingRecord *r = recordOf(parsed.records, name);
        CHECK_MSG(nullptr != r && "default" == r->source, name);
    }
    const SettingRecord *url = recordOf(parsed.records, "HISTORY_PROJECTOR_PG_URL");
    CHECK(nullptr != url && std::string::npos == url->value.find("pw@"));
    const SettingRecord *archives = recordOf(parsed.records, "HISTORY_PROJECTOR_JOURNAL_ARCHIVES");
    CHECK(nullptr != archives && "10.0.0.2:8010,10.0.0.3:8010,10.0.0.1:8010" == archives->value);
}

TEST(projector_settings_required_in_one_pass)
{
    const auto parsed = parseProjectorSettings(MapEnvironment(), probe());
    for (const char *key : { "HISTORY_PROJECTOR_DRIVER_MODE", "HISTORY_PROJECTOR_INSTANCE_ID",
             "HISTORY_PROJECTOR_ADMIN_BIND", "HISTORY_PROJECTOR_JOURNAL_ARCHIVES", "HISTORY_PROJECTOR_JOURNAL_CONTROL_STREAM",
             "HISTORY_PROJECTOR_JOURNAL_STREAM", "HISTORY_PROJECTOR_JOURNAL_CHANNEL", "HISTORY_PROJECTOR_REPLAY_STREAM",
             "HISTORY_PROJECTOR_LOCAL_HOST", "HISTORY_PROJECTOR_JOURNAL_MTU", "HISTORY_PROJECTOR_CLUSTER_MEMBERS",
             "HISTORY_PROJECTOR_CLUSTER_KEY_FILE", "HISTORY_PROJECTOR_PG_URL" })
    {
        CHECK_MSG(has(parsed.errors, std::string(key) + " is required"), key);
    }
}

TEST(projector_settings_refusals)
{
    auto env = projectorEnv();
    env["HISTORY_PROJECTOR_LOCAL_HOST"] = "127.0.0.1";                  // loopback with remote sources
    env["HISTORY_PROJECTOR_JOURNAL_MTU"] = "100";                       // below the record's frame
    env["HISTORY_PROJECTOR_CLUSTER_KEY_FILE"] = keyFile("bridge-key", "9:BRIDGE:" + SECRET_HEX);
    env["HISTORY_PROJECTOR_HEAD_FRESH_MS"] = "1000";                    // not above the probe interval
    env["HISTORY_PROJECTOR_LEASE_POLL_MS"] = "10000";                   // not below the lease
    env["HISTORY_PROJECTOR_SOURCE_BACKOFF_MAX_MS"] = "500";             // below the back-off
    env["HISTORY_PROJECTOR_LEASE_SECONDS"] = "10";                      // misspelt
    env["HISTORY_PROJECTOR_PG_URL"] = "mysql://x";
    env["HISTORY_PROJECTOR_DRIVER_DIR"] = "/run/oe";                    // refused with external
    env["HISTORY_PROJECTOR_BATCH_MS"] = "0";
    const auto parsed = parseProjectorSettings(MapEnvironment(env), probe());
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_LOCAL_HOST 127.0.0.1 is loopback"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_JOURNAL_MTU must be a multiple of 32 in [248, 65504]"));
    CHECK(has(parsed.errors, "holds a BRIDGE key; the projector's head probe needs GATEWAY"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_HEAD_FRESH_MS (1000) must be above HISTORY_PROJECTOR_HEAD_PROBE_MS"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_LEASE_MS (10000) must be above HISTORY_PROJECTOR_LEASE_POLL_MS (10000)"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_SOURCE_BACKOFF_MAX_MS (500) must be at least"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_LEASE_SECONDS is not a setting of this process"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_PG_URL must be a postgresql://"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_DRIVER_DIR is set but HISTORY_PROJECTOR_DRIVER_MODE=external"));
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_BATCH_MS must be a positive integer"));
    CHECK(10 == parsed.errors.size());

    auto missing = projectorEnv();
    missing["HISTORY_PROJECTOR_CLUSTER_KEY_FILE"] = "/nonexistent/key";
    const auto noFile = parseProjectorSettings(MapEnvironment(missing), probe());
    CHECK(has(noFile.errors, "HISTORY_PROJECTOR_CLUSTER_KEY_FILE: cannot read /nonexistent/key"));
}

TEST(projector_settings_timeouts_against_the_lease)
{
    // a transaction a stopped writer left open would hold the state row past its lease
    auto env = projectorEnv();
    env["HISTORY_PROJECTOR_LEASE_MS"] = "3000";
    env["HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS"] = "900";
    env["HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS"] = "500";
    // (accepted with a WARN: the fence still holds, only the takeover's bound grows)
    auto parsed = parseProjectorSettings(MapEnvironment(env), probe());
    CHECK_MSG(parsed.ok(), parsed.errors.empty() ? "" : parsed.errors.front());
    CHECK(has(parsed.settings.warnings,
        "HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS (10000) is above HISTORY_PROJECTOR_LEASE_MS (3000)"));
    CHECK(1 == parsed.settings.warnings.size());
    env["HISTORY_PROJECTOR_PG_IDLE_IN_TX_TIMEOUT_MS"] = "3000";
    parsed = parseProjectorSettings(MapEnvironment(env), probe());
    CHECK_MSG(parsed.ok(), parsed.errors.empty() ? "" : parsed.errors.front());
    CHECK(parsed.settings.warnings.empty());

    // one slow statement would outlive a renewal: a third of the lease is the renewal interval
    env["HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS"] = "1000";
    parsed = parseProjectorSettings(MapEnvironment(env), probe());
    CHECK(has(parsed.errors, "HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS (1000) must be below a third of "
        "HISTORY_PROJECTOR_LEASE_MS (3000 / 3 = 1000"));
    CHECK(1 == parsed.errors.size());

    // the default lease with the old statement timeout default
    auto defaults = projectorEnv();
    defaults["HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS"] = "5000";
    CHECK(has(parseProjectorSettings(MapEnvironment(defaults), probe()).errors,
        "HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS (5000) must be below a third of HISTORY_PROJECTOR_LEASE_MS"));

    // the rebuild's lock wait only bounds anything below the statement timeout
    auto rebuild = projectorEnv();
    rebuild["HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS"] = "3000";
    CHECK(has(parseProjectorSettings(MapEnvironment(rebuild), probe()).errors,
        "HISTORY_PROJECTOR_PG_STATEMENT_TIMEOUT_MS (3000) must be above HISTORY_PROJECTOR_REBUILD_LOCK_TIMEOUT_MS (3000)"));
}

TEST(projector_writer_horizon)
{
    // pj-duty holds replays and the session only inside the horizon pj-pg last confirmed, even while pj-pg is blocked
    // in a statement
    ProjectorShared shared;
    CHECK(!writerNow(shared, 5));
    shared.writer.store(true);
    shared.writerUntilNs.store(1000);
    CHECK(writerNow(shared, 999));
    CHECK(!writerNow(shared, 1000));
    shared.writer.store(false);
    CHECK(!writerNow(shared, 10));
    // the lease less one renewal interval
    CHECK(6'666'666'667LL == writerHoldNs(10'000));
    CHECK(1'000'000'000LL == writerHoldNs(1500));
}

TEST(projector_lease_uses_the_database_clock_only)
{
    // every lease decision is on the database's clock: no SQL reads a transaction start time and no host time is sent
    std::ifstream in(GW_PROJECTION_STORE_SOURCE);
    CHECK(in.good());
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    for (char &ch : text)
    {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    CHECK(std::string::npos != text.find("lease_until = clock_timestamp() + $2::bigint"));
    for (const char *banned : { "now()", "current_timestamp", "transaction_timestamp", "statement_timestamp",
             "localtimestamp", "to_timestamp(", "::timestamptz" })
    {
        CHECK_MSG(std::string::npos == text.find(banned), banned);
    }
}

TEST(projector_settings_embedded_mode)
{
    auto env = projectorEnv();
    env.erase("AERON_DIR");
    env["HISTORY_PROJECTOR_DRIVER_MODE"] = "embedded";
    env["HISTORY_PROJECTOR_DRIVER_DIR"] = "/run/oe";
    const auto parsed = parseProjectorSettings(MapEnvironment(env), probe());
    CHECK_MSG(parsed.ok(), parsed.errors.empty() ? "" : parsed.errors.front());
    CHECK(DriverMode::EMBEDDED == parsed.settings.common.driverMode && "/run/oe" == parsed.settings.common.driverDir);
    env["AERON_DIR"] = "/dev/shm/aeron-host";
    CHECK(has(parseProjectorSettings(MapEnvironment(env), probe()).errors,
        "AERON_DIR is set but HISTORY_PROJECTOR_DRIVER_MODE=embedded"));
}

TEST(projector_readiness)
{
    Status s;
    s.driverConnected = true;
    const std::int64_t now = 100'000'000'000LL;
    ProjectorReadyBounds bounds;
    auto failingNow = [&] { return failing(projectorReadyConditions(s, now, bounds)); };
    CHECK((failingNow() == std::vector<std::string>{ "lease_held", "session_registered", "head_probe_fresh",
        "beat_fresh" }));
    s.leaseHeld = true;
    s.registered = true;
    s.lastHeadProbeNs = now - 2'000'000'000LL;  // 2 s ago, inside 3 s
    s.lastBeatNs = now - 1'000'000'000LL;       // 1 s ago, inside 5 s
    CHECK(failingNow().empty());
    s.halted = true;
    CHECK((failingNow() == std::vector<std::string>{ "not_halted" }));
    s.halted = false;
    s.lastBeatNs = now - 5'000'000'000LL;       // as old as the bound: not ready
    CHECK((failingNow() == std::vector<std::string>{ "beat_fresh" }));
    s.lastBeatNs = now;
    s.lastHeadProbeNs = now - 3'000'000'001LL;
    CHECK((failingNow() == std::vector<std::string>{ "head_probe_fresh" }));
}

TEST(projector_admin_standby_says_so)
{
    AdminView v;
    v.role = Role::PROJECTOR;
    v.processName = "oe-projector";
    v.watchdogMs = 5000;
    v.status.driverConnected = true;
    cluster_tools::runtime::HttpRequest request;
    request.method = "GET";
    request.path = "/ready";
    auto r = adminRoute(request, v);
    CHECK(503 == r.status);
    CHECK_MSG(0 == r.body.rfind("{\"ready\":false,\"role\":\"standby\",\"failing\":[\"lease_held\",", 0), r.body);
    v.status.leaseHeld = true;
    r = adminRoute(request, v);
    CHECK_MSG(std::string::npos != r.body.find("\"role\":\"writer\""), r.body);
    request.path = "/metrics";
    r = adminRoute(request, v);
    CHECK(200 == r.status && std::string::npos != r.body.find("\nprojector_ready 0\n") &&
        std::string::npos != r.body.find("projector_ready_condition{name=\"lease_held\"} 1"));
    request.path = "/ready/session";
    CHECK(404 == adminRoute(request, v).status);
    request.path = "/health";
    r = adminRoute(request, v);
    CHECK(200 == r.status && std::string::npos == r.body.find("\"treasuryLane\"") &&
        std::string::npos == r.body.find("\"events\""));
}
