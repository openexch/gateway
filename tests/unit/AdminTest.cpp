// Readiness conditions by name, and the admin endpoints rendered from a view: /ready and /ready/session list the
// failing conditions, /health follows the duty heartbeat against the watchdog bound and shows every setting,
// /metrics carries the prefix of the binary.
#include <string>
#include <vector>

#include "Check.h"
#include "gateway/common/Admin.h"
#include "gateway/common/Readiness.h"

using namespace openexchange::gateway;
namespace rt = cluster_tools::runtime;

namespace {

constexpr std::int64_t MS = 1'000'000LL;

std::vector<std::string> names(std::initializer_list<const char *> list)
{
    return std::vector<std::string>(list.begin(), list.end());
}

std::string joined(const std::vector<std::string> &v)
{
    std::string out;
    for (const std::string &s : v)
    {
        out += s + " ";
    }
    return out;
}

Status allGood(std::int64_t now)
{
    Status s;
    s.driverConnected = true;
    s.connected = true;
    s.registered = true;
    s.configLoaded = true;
    s.gateOpen = true;
    s.lastKeepaliveOfferNs = now - 100 * MS;
    s.timeoutStreak = 0;
    s.publicListenerAccepting = true;
    s.lastHeadProbeNs = now - 100 * MS;
    s.wsListenerAccepting = true;
    return s;
}

AdminView view(Role role, const std::vector<SettingRecord> *settings)
{
    AdminView v;
    v.role = role;
    v.processName = Role::GATEWAY == role ? "oe-gateway" : "oe-stream";
    v.version = "dev";
    v.build = "b";
    v.clusterTools = "c";
    v.moneySchemaVersion = 9;
    v.moneySchemaSha256 = "m";
    v.driverMode = "embedded";
    v.driverDir = "/dev/shm/oe-gateway-1";
    v.driverTimeoutMs = 10'000;
    v.nowNs = 1'000'000 * MS;
    v.heartbeatAgeMs = 1;
    v.watchdogMs = 5000;
    v.treasuryLane = "off";
    v.settings = settings;
    v.status.driverConnected = true; // past the driver wait, no session
    return v;
}

rt::HttpResponse get(const AdminView &v, const std::string &path, const std::string &method = "GET")
{
    rt::HttpRequest r;
    r.method = method;
    r.path = path;
    return adminRoute(r, v);
}

bool has(const std::string &text, const std::string &part)
{
    return std::string::npos != text.find(part);
}

}

TEST(gateway_without_a_session_fails_every_session_condition_by_name)
{
    const Status s; // what a process with no cluster session publishes
    const auto failingNow = failing(gatewayReadyConditions(s, 1'000 * MS, GatewayReadyBounds{}));
    CHECK_MSG(failingNow == names({ "driver_connected", "session_connected", "session_registered", "config_loaded",
                                 "gate_open", "keepalive_fresh", "public_listener" }),
        joined(failingNow));
    CHECK(failing(gatewaySessionConditions(s)) == names({ "driver_connected", "session_connected", "session_registered" }));
}

TEST(stream_without_a_session_fails_every_session_condition_by_name)
{
    const Status s;
    const auto failingNow = failing(streamReadyConditions(s, 1'000 * MS, StreamReadyBounds{}));
    CHECK_MSG(failingNow == names({ "driver_connected", "session_registered", "head_probe_fresh", "ws_listener" }),
        joined(failingNow));
}

TEST(each_gateway_condition_fails_alone)
{
    const std::int64_t now = 1'000'000 * MS;
    const GatewayReadyBounds bounds{ 3000, 5 };
    CHECK(failing(gatewayReadyConditions(allGood(now), now, bounds)).empty());
    struct Variant
    {
        const char *name;
        void (*apply)(Status &, std::int64_t);
    };
    const Variant variants[] = {
        { "driver_connected", [](Status &s, std::int64_t) { s.driverConnected = false; } },
        { "storage_space", [](Status &s, std::int64_t) { s.storageStarved = true; } },
        { "session_connected", [](Status &s, std::int64_t) { s.connected = false; } },
        { "session_registered", [](Status &s, std::int64_t) { s.registered = false; } },
        { "config_loaded", [](Status &s, std::int64_t) { s.configLoaded = false; } },
        { "gate_open", [](Status &s, std::int64_t) { s.gateOpen = false; } },
        { "keepalive_fresh", [](Status &s, std::int64_t n) { s.lastKeepaliveOfferNs = n - 3001 * MS; } },
        { "keepalive_fresh", [](Status &s, std::int64_t) { s.lastKeepaliveOfferNs = -1; } },
        { "timeout_streak", [](Status &s, std::int64_t) { s.timeoutStreak = 5; } },
        { "public_listener", [](Status &s, std::int64_t) { s.publicListenerAccepting = false; } },
    };
    for (const Variant &v : variants)
    {
        Status s = allGood(now);
        v.apply(s, now);
        const auto f = failing(gatewayReadyConditions(s, now, bounds));
        CHECK_MSG(f == names({ v.name }), std::string(v.name) + " -> " + joined(f));
    }
    // the stale bound is inclusive of exactly the bound, and a streak one below the bound holds
    Status edge = allGood(now);
    edge.lastKeepaliveOfferNs = now - 3000 * MS;
    edge.timeoutStreak = 4;
    CHECK(failing(gatewayReadyConditions(edge, now, bounds)).empty());
    // a closed gate does not fail /ready/session
    Status closed = allGood(now);
    closed.gateOpen = false;
    CHECK(failing(gatewaySessionConditions(closed)).empty());
}

TEST(each_stream_condition_fails_alone)
{
    const std::int64_t now = 1'000'000 * MS;
    const StreamReadyBounds bounds{ 15'000 };
    CHECK(failing(streamReadyConditions(allGood(now), now, bounds)).empty());
    Status gap = allGood(now);
    gap.gapOpen = true;
    CHECK(failing(streamReadyConditions(gap, now, bounds)) == names({ "event_gap" }));
    Status stale = allGood(now);
    stale.lastHeadProbeNs = now - 15'001 * MS;
    CHECK(failing(streamReadyConditions(stale, now, bounds)) == names({ "head_probe_fresh" }));
    Status noDriver = allGood(now);
    noDriver.driverConnected = false;
    CHECK(failing(streamReadyConditions(noDriver, now, bounds)) == names({ "driver_connected" }));
    Status unregistered = allGood(now);
    unregistered.registered = false;
    CHECK(failing(streamReadyConditions(unregistered, now, bounds)) == names({ "session_registered" }));
    // the gate is not a stream condition
    Status closed = allGood(now);
    closed.gateOpen = false;
    CHECK(failing(streamReadyConditions(closed, now, bounds)).empty());
}

TEST(ready_endpoints_answer_503_with_the_failing_names)
{
    const std::vector<SettingRecord> settings;
    const AdminView v = view(Role::GATEWAY, &settings);
    const auto ready = get(v, "/ready");
    CHECK(503 == ready.status);
    CHECK_MSG(has(ready.body, "\"failing\":[\"session_connected\",\"session_registered\",\"config_loaded\",\"gate_open\","
                              "\"keepalive_fresh\",\"public_listener\"]"),
        ready.body);
    const auto session = get(v, "/ready/session");
    CHECK(503 == session.status);
    CHECK_MSG(has(session.body, "[\"session_connected\",\"session_registered\"]"), session.body);

    AdminView good = v;
    good.status = allGood(v.nowNs);
    CHECK(200 == get(good, "/ready").status);
    CHECK(200 == get(good, "/ready/session").status);
    CHECK(200 == get(good, "/ready?verbose=1").status);
}

TEST(stream_has_no_session_route_and_its_own_conditions)
{
    const std::vector<SettingRecord> settings;
    const AdminView v = view(Role::STREAM, &settings);
    CHECK(404 == get(v, "/ready/session").status);
    const auto ready = get(v, "/ready");
    CHECK(503 == ready.status);
    CHECK_MSG(has(ready.body, "[\"session_registered\",\"head_probe_fresh\",\"ws_listener\"]"), ready.body);
}

TEST(unknown_paths_and_methods)
{
    const std::vector<SettingRecord> settings;
    const AdminView v = view(Role::GATEWAY, &settings);
    CHECK(404 == get(v, "/").status);
    CHECK(404 == get(v, "/api/v2/orders").status);
    CHECK(405 == get(v, "/health", "POST").status);
    CHECK(405 == get(v, "/ready", "DELETE").status);
}

TEST(health_follows_the_duty_heartbeat_and_shows_every_setting)
{
    const std::vector<SettingRecord> settings = { { "GATEWAY_MAX_INFLIGHT", "4096", "default" },
        { "CORE_AUTH_CREDENTIAL", "7:GATEWAY:(redacted)", "set" }, { "GATEWAY_PG_URL", "(unset)", "unset" },
        { "GATEWAY_CORS_ORIGINS", "https://a.example,\"quoted\"", "set" } };
    AdminView v = view(Role::GATEWAY, &settings);
    v.heartbeatAgeMs = 4999;
    const auto up = get(v, "/health");
    CHECK(200 == up.status);
    CHECK(has(up.body, "\"status\":\"UP\""));
    CHECK(has(up.body, "\"GATEWAY_MAX_INFLIGHT\":{\"value\":\"4096\",\"source\":\"default\"}"));
    CHECK(has(up.body, "\"CORE_AUTH_CREDENTIAL\":{\"value\":\"7:GATEWAY:(redacted)\",\"source\":\"set\"}"));
    CHECK_MSG(has(up.body, "https://a.example,\\\"quoted\\\""), up.body);
    CHECK(has(up.body, "\"driver\":{\"mode\":\"embedded\""));
    CHECK(has(up.body, "\"ready\":false"));
    CHECK(has(up.body, "\"treasuryLane\":\"off\""));
    CHECK(has(up.body, "\"session\":{\"connected\":false,\"registered\":false,\"epoch\":0,\"leader\":-1,\"keepaliveAgeMs\":null}"));
    v.heartbeatAgeMs = 5000;
    const auto stalled = get(v, "/health");
    CHECK(503 == stalled.status);
    CHECK(has(stalled.body, "\"status\":\"STALLED\""));
}

TEST(metrics_carry_the_binary_prefix_and_every_condition)
{
    const std::vector<SettingRecord> settings;
    const auto g = get(view(Role::GATEWAY, &settings), "/metrics");
    CHECK(200 == g.status);
    CHECK(has(g.body, "\ngateway_up 1\n"));
    CHECK(has(g.body, "\ngateway_driver_connected 1\n"));
    CHECK(has(g.body, "\ngateway_ready 0\n"));
    CHECK(has(g.body, "gateway_ready_condition{name=\"session_connected\"} 0\n"));
    CHECK(has(g.body, "gateway_ready_condition{name=\"timeout_streak\"} 1\n"));
    CHECK(has(g.body, "\ngateway_session_registered 0\n"));
    CHECK(has(g.body, "gateway_build_info{version=\"dev\""));
    CHECK(!has(g.body, "\nstream_"));
    // ignored egress templates are their own counter, by template id, apart from decode_refused_total
    AdminView ignoring = view(Role::GATEWAY, &settings);
    ignoring.status.decodeRefused = 2;
    ignoring.status.wireIgnored[0] = 5;
    const auto i = get(ignoring, "/metrics");
    CHECK(has(i.body, "\ngateway_decode_refused_total 2\n"));
    CHECK(has(i.body, "\n# TYPE gateway_wire_ignored_total counter\ngateway_wire_ignored_total{template=\"113\"} 5\n"));
    const auto s = get(view(Role::STREAM, &settings), "/metrics");
    CHECK(has(s.body, "\nstream_ready 0\n"));
    CHECK(has(s.body, "stream_ready_condition{name=\"event_gap\"} 1\n"));
    CHECK(!has(s.body, "\ngateway_"));
}

TEST(before_the_driver_connects_health_and_ready_answer_503)
{
    const std::vector<SettingRecord> settings = { { "GATEWAY_STATUS_INTERVAL_MS", "100", "default" } };
    for (const Role role : { Role::GATEWAY, Role::STREAM })
    {
        AdminView v = view(role, &settings);
        v.status = Status{};               // nothing published yet: the duty thread has not started
        v.heartbeatAgeMs = 1;              // even with a fresh heartbeat
        v.conductorHeartbeatAgeMs = -1;
        const auto health = get(v, "/health");
        CHECK(503 == health.status);
        CHECK_MSG(has(health.body, "\"status\":\"STARTING\""), health.body);
        CHECK_MSG(has(health.body, "\"connected\":false,\"conductorHeartbeatAgeMs\":-1"), health.body);
        CHECK(has(health.body, "\"driver_connected\":false"));
        CHECK(has(health.body, "\"GATEWAY_STATUS_INTERVAL_MS\":{\"value\":\"100\",\"source\":\"default\"}"));
        const auto ready = get(v, "/ready");
        CHECK(503 == ready.status);
        CHECK_MSG(has(ready.body, "\"failing\":[\"driver_connected\","), ready.body);
        const std::string p = Role::GATEWAY == role ? "gateway_" : "stream_";
        const auto metrics = get(v, "/metrics");
        CHECK(has(metrics.body, "\n" + p + "up 0\n") && has(metrics.body, "\n" + p + "driver_connected 0\n"));
        // the stall verdict needs a connected driver: STALLED only after the start
        v.heartbeatAgeMs = 6000;
        CHECK(has(get(v, "/health").body, "\"status\":\"STARTING\""));
        v.status.driverConnected = true;
        CHECK(has(get(v, "/health").body, "\"status\":\"STALLED\""));
    }
    const AdminView gwv = [&] {
        AdminView v = view(Role::GATEWAY, &settings);
        v.status = Status{};
        return v;
    }();
    const auto session = get(gwv, "/ready/session");
    CHECK_MSG(503 == session.status && has(session.body, "[\"driver_connected\",\"session_connected\",\"session_registered\"]"),
        session.body);
}

TEST(json_escape)
{
    CHECK("a\\\"b\\\\c\\n\\u0001" == jsonEscape(std::string("a\"b\\c\n\x01", 7)));
}
