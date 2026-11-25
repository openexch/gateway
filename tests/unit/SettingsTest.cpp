// Settings of both binaries: required keys, malformed values, every problem in one pass, defaults recorded, secrets
// redacted, cross-setting rules (driver mode keys, auth modes, treasury lane, binds), unknown names refused.
#include <algorithm>
#include <map>
#include <set>
#include <string>

#include "Check.h"
#include "gateway/common/Settings.h"

using namespace openexchange::gateway;
namespace rt = cluster_tools::runtime;

namespace {

const std::string SECRET_HEX = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

rt::DirProbe probe(std::set<std::string> dirs = { "/run/oe", "/dev/shm/aeron-host" },
    std::set<std::string> writable = { "/run/oe" })
{
    rt::DirProbe p;
    p.isDirectory = [dirs](const std::string &d) { return dirs.count(d) > 0; };
    p.isWritable = [writable](const std::string &d) { return writable.count(d) > 0; };
    return p;
}

std::map<std::string, std::string> gatewayEnv()
{
    return {
        { "GATEWAY_DRIVER_MODE", "embedded" },
        { "GATEWAY_DRIVER_DIR", "/run/oe" },
        { "GATEWAY_CORE_INGRESS", "0=10.0.0.1:9010,1=10.0.0.2:9010,2=10.0.0.3:9010" },
        { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=4m|mtu=8192" },
        { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp?endpoint=10.0.0.9:0|term-length=4m|mtu=8192" },
        { "CORE_AUTH_CREDENTIAL", "7:GATEWAY:" + SECRET_HEX },
        { "GATEWAY_INSTANCE_ID", "1" },
        { "GATEWAY_HTTP_BIND", "0.0.0.0:8080" },
        { "GATEWAY_ADMIN_BIND", "127.0.0.1:9180" },
        { "GATEWAY_AUTH_MODES", "api-key" },
        { "GATEWAY_API_KEYS_FILE", "/etc/oe/api-keys" },
    };
}

std::map<std::string, std::string> streamEnv()
{
    return {
        { "STREAM_DRIVER_MODE", "external" },
        { "AERON_DIR", "/dev/shm/aeron-host" },
        { "STREAM_CORE_INGRESS", "0=10.0.0.1:9010" },
        { "STREAM_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=4m|mtu=8192" },
        { "STREAM_CORE_RESPONSE_CHANNEL", "aeron:udp?endpoint=10.0.0.9:0|term-length=4m|mtu=8192" },
        { "CORE_AUTH_CREDENTIAL", "8:GATEWAY:" + SECRET_HEX },
        { "STREAM_INSTANCE_ID", "2" },
        { "STREAM_WS_BIND", "0.0.0.0:8081" },
        { "STREAM_ADMIN_BIND", "127.0.0.1:9181" },
        { "STREAM_AUTH_MODES", "api-key" },
        { "GATEWAY_API_KEYS_FILE", "/etc/oe/api-keys" },
    };
}

ParseResult<GatewaySettings> gw(const std::map<std::string, std::string> &env, const rt::DirProbe &p = probe())
{
    return parseGatewaySettings(MapEnvironment(env), p);
}

ParseResult<StreamSettings> st(const std::map<std::string, std::string> &env, const rt::DirProbe &p = probe())
{
    return parseStreamSettings(MapEnvironment(env), p);
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

// Exactly one problem, and it starts with `name` and contains `text`.
template<typename R>
void onlyProblem(const R &r, const std::string &name, const std::string &text = "")
{
    CHECK_MSG(1 == r.errors.size(), name + ": expected one problem, got" + joined(r.errors));
    if (!r.errors.empty())
    {
        CHECK_MSG(0 == r.errors[0].rfind(name, 0), "problem does not start with " + name + ":" + joined(r.errors));
        CHECK_MSG(std::string::npos != r.errors[0].find(text), "problem lacks '" + text + "':" + joined(r.errors));
    }
}

template<typename R>
bool mentions(const R &r, const std::string &text)
{
    return std::any_of(r.errors.begin(), r.errors.end(), [&](const std::string &e) { return std::string::npos != e.find(text); });
}

template<typename R>
const SettingRecord *recordOf(const R &r, const std::string &name)
{
    for (const SettingRecord &rec : r.records)
    {
        if (rec.name == name)
        {
            return &rec;
        }
    }
    return nullptr;
}

std::map<std::string, std::string> with(std::map<std::string, std::string> env,
    const std::map<std::string, std::string> &changes, const std::vector<std::string> &removed = {})
{
    for (const auto &kv : changes)
    {
        env[kv.first] = kv.second;
    }
    for (const std::string &k : removed)
    {
        env.erase(k);
    }
    return env;
}

}

TEST(gateway_valid_settings_parse)
{
    const auto r = gw(gatewayEnv());
    CHECK_MSG(r.ok(), joined(r.errors));
    CHECK(DriverMode::EMBEDDED == r.settings.common.driverMode);
    CHECK("/run/oe" == r.settings.common.driverDir);
    CHECK(10'000 == r.settings.common.driverTimeoutMs);
    CHECK(1 == r.settings.common.instanceId);
    CHECK(9180 == r.settings.common.adminBind.port);
    CHECK(auth_mode::API_KEY == r.settings.common.authModes);
    CHECK(4096 == r.settings.maxInflight);
    CHECK(50 == r.settings.principalRate.count && 1000 == r.settings.principalRate.periodMs);
    CHECK(10 == r.settings.authSourceRate.count && 60'000 == r.settings.authSourceRate.periodMs);
    CHECK(!r.settings.allowTreasuryLane);
}

TEST(stream_valid_settings_parse)
{
    const auto r = st(streamEnv());
    CHECK_MSG(r.ok(), joined(r.errors));
    CHECK(DriverMode::EXTERNAL == r.settings.common.driverMode);
    CHECK("/dev/shm/aeron-host" == r.settings.common.aeronDir);
    CHECK(2 == r.settings.common.instanceId);
    CHECK(r.settings.balanceStream);
    CHECK(65'536 == r.settings.eventWindow);
    CHECK(1'048'576 == r.settings.wsQueueBytes);
}

TEST(gateway_empty_environment_lists_every_required_key_in_one_pass)
{
    const auto r = gw({});
    const std::vector<std::string> required = { "GATEWAY_DRIVER_MODE", "GATEWAY_CORE_INGRESS",
        "GATEWAY_CORE_INGRESS_CHANNEL", "GATEWAY_CORE_RESPONSE_CHANNEL", "CORE_AUTH_CREDENTIAL", "GATEWAY_INSTANCE_ID",
        "GATEWAY_HTTP_BIND", "GATEWAY_ADMIN_BIND", "GATEWAY_AUTH_MODES" };
    CHECK_MSG(required.size() == r.errors.size(), joined(r.errors));
    for (const std::string &key : required)
    {
        CHECK_MSG(mentions(r, key + " is required"), key + " not named:" + joined(r.errors));
    }
}

TEST(stream_empty_environment_lists_every_required_key_in_one_pass)
{
    const auto r = st({});
    const std::vector<std::string> required = { "STREAM_DRIVER_MODE", "STREAM_CORE_INGRESS",
        "STREAM_CORE_INGRESS_CHANNEL", "STREAM_CORE_RESPONSE_CHANNEL", "CORE_AUTH_CREDENTIAL", "STREAM_INSTANCE_ID",
        "STREAM_WS_BIND", "STREAM_ADMIN_BIND", "STREAM_AUTH_MODES" };
    CHECK_MSG(required.size() == r.errors.size(), joined(r.errors));
    for (const std::string &key : required)
    {
        CHECK_MSG(mentions(r, key + " is required"), key + " not named:" + joined(r.errors));
    }
}

TEST(gateway_every_malformed_value_in_one_pass)
{
    const std::map<std::string, std::string> bad = {
        { "GATEWAY_INSTANCE_ID", "abc" },
        { "GATEWAY_MAX_INFLIGHT", "0" },
        { "GATEWAY_REQUEST_TIMEOUT_MS", "5s" },
        { "GATEWAY_IDLE", "spin" },
        { "GATEWAY_PRINCIPAL_RATE", "50" },
        { "GATEWAY_ALLOW_TREASURY_LANE", "yes" },
        { "GATEWAY_CORS_ORIGINS", "*" },
        { "GATEWAY_DRIVER_TIMEOUT_MS", "100" },
        { "GATEWAY_TRUSTED_PROXIES", "10.0.0.0/33" },
        { "GATEWAY_STREAM_BALANCE", "on" },
    };
    const auto r = gw(with(gatewayEnv(), bad));
    CHECK_MSG(bad.size() == r.errors.size(), joined(r.errors));
    for (const auto &kv : bad)
    {
        CHECK_MSG(mentions(r, kv.first), kv.first + " not named:" + joined(r.errors));
    }
}

TEST(stream_every_malformed_value_in_one_pass)
{
    const std::map<std::string, std::string> bad = {
        { "STREAM_INSTANCE_ID", "-3" },
        { "STREAM_BALANCE_STREAM", "true" },
        { "STREAM_EVENT_WINDOW", "64k" },
        { "STREAM_QUERY_RATE", "50/h" },
        { "STREAM_WS_QUEUE_BYTES", "1 MiB" },
        { "STREAM_IDLE", "" },
        { "STREAM_HEAD_PROBE", "5000" },
    };
    const auto r = st(with(streamEnv(), bad));
    CHECK_MSG(bad.size() == r.errors.size(), joined(r.errors));
    for (const auto &kv : bad)
    {
        CHECK_MSG(mentions(r, kv.first), kv.first + " not named:" + joined(r.errors));
    }
}

TEST(instance_id_must_be_a_positive_int64)
{
    for (const std::string bad : { "abc", "0", "-1", "+5", " 5", "5 ", "9223372036854775808", "1e3", "0x10" })
    {
        onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_INSTANCE_ID", bad } })), "GATEWAY_INSTANCE_ID", "positive integer");
    }
    const auto max = gw(with(gatewayEnv(), { { "GATEWAY_INSTANCE_ID", "9223372036854775807" } }));
    CHECK_MSG(max.ok(), joined(max.errors));
    CHECK(9223372036854775807LL == max.settings.common.instanceId);
}

TEST(channels_must_name_mtu_term_length_and_endpoint)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=4m" } })),
        "GATEWAY_CORE_INGRESS_CHANNEL", "missing: mtu");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?mtu=8192" } })),
        "GATEWAY_CORE_INGRESS_CHANNEL", "missing: term-length");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp" } })),
        "GATEWAY_CORE_INGRESS_CHANNEL", "missing: term-length, mtu");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL",
                    "aeron:udp?endpoint=10.0.0.1:9010|term-length=4m|mtu=8192" } })),
        "GATEWAY_CORE_INGRESS_CHANNEL", "must not carry endpoint=");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp?term-length=4m|mtu=8192" } })),
        "GATEWAY_CORE_RESPONSE_CHANNEL", "missing: endpoint");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_RESPONSE_CHANNEL",
                    "aeron:udp?endpoint=0.0.0.0:0|term-length=4m|mtu=8192" } })),
        "GATEWAY_CORE_RESPONSE_CHANNEL", "0.0.0.0");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:ipc?term-length=4m" } })),
        "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS", "10.0.0.1:9010" } })), "GATEWAY_CORE_INGRESS");
}

TEST(channel_mtu_and_term_length_values_are_validated)
{
    const auto ingress = [](const std::string &params) {
        return gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?" + params } }));
    };
    const auto response = [](const std::string &params) {
        return gw(with(gatewayEnv(), { { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp?endpoint=10.0.0.9:0|" + params } }));
    };
    // mtu: not a number, not a multiple of 32, out of range, not plain decimal
    for (const std::string bad : { "banana", "abc", "8191", "8200", "0", "16", "65536", "65535", "+8192", "-8192",
             "08192", "8192 ", "8k", "1e4" })
    {
        onlyProblem(ingress("term-length=4m|mtu=" + bad), "GATEWAY_CORE_INGRESS_CHANNEL: mtu", "'" + bad + "'");
        onlyProblem(response("term-length=4m|mtu=" + bad), "GATEWAY_CORE_RESPONSE_CHANNEL: mtu", "'" + bad + "'");
    }
    for (const std::string good : { "32", "1408", "8192", "65504" })
    {
        const auto r = ingress("term-length=4m|mtu=" + good);
        CHECK_MSG(r.ok(), good + joined(r.errors));
    }
    // term-length: not a number, not a power of two, out of range, a bad suffix
    for (const std::string bad : { "banana", "abc", "3m", "100k", "65535", "32k", "2g", "4096m", "0", "k", "4mb", "4 m",
             "-4m", "+4m", "04m", "4t", "1099511627776" })
    {
        onlyProblem(ingress("term-length=" + bad + "|mtu=8192"), "GATEWAY_CORE_INGRESS_CHANNEL: term-length",
            "'" + bad + "'");
        onlyProblem(response("term-length=" + bad + "|mtu=8192"), "GATEWAY_CORE_RESPONSE_CHANNEL: term-length",
            "'" + bad + "'");
    }
    // suffix forms, either case, and plain bytes
    for (const std::string good : { "64k", "64K", "4m", "4M", "16m", "1g", "1G", "1024m", "65536", "1073741824" })
    {
        const auto r = response("term-length=" + good + "|mtu=8192");
        CHECK_MSG(r.ok(), good + joined(r.errors));
    }
    // every bad value is its own problem, both channels in one pass
    const auto all = gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=3m|mtu=banana" },
        { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp?endpoint=10.0.0.9:0|term-length=abc|mtu=8200" } }));
    CHECK_MSG(4 == all.errors.size(), joined(all.errors));
    CHECK(mentions(all, "GATEWAY_CORE_INGRESS_CHANNEL: mtu") && mentions(all, "GATEWAY_CORE_INGRESS_CHANNEL: term-length"));
    CHECK(mentions(all, "GATEWAY_CORE_RESPONSE_CHANNEL: mtu") && mentions(all, "GATEWAY_CORE_RESPONSE_CHANNEL: term-length"));
    // a bad value and a missing key are both reported
    const auto both = gw(with(gatewayEnv(), { { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=3m" } }));
    CHECK_MSG(2 == both.errors.size() && mentions(both, "missing: mtu") && mentions(both, "term-length must be"),
        joined(both.errors));
    onlyProblem(st(with(streamEnv(), { { "STREAM_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=4m|mtu=9000" } })),
        "STREAM_CORE_INGRESS_CHANNEL: mtu");
}

TEST(credential_must_be_a_gateway_key)
{
    onlyProblem(gw(with(gatewayEnv(), { { "CORE_AUTH_CREDENTIAL", "7:FORWARDER:" + SECRET_HEX } })), "CORE_AUTH_CREDENTIAL",
        "needs GATEWAY");
    onlyProblem(gw(with(gatewayEnv(), { { "CORE_AUTH_CREDENTIAL", "7:GATEWAY:abc" } })), "CORE_AUTH_CREDENTIAL");
    onlyProblem(st(with(streamEnv(), { { "CORE_AUTH_CREDENTIAL", "8:TREASURY:" + SECRET_HEX } })), "CORE_AUTH_CREDENTIAL",
        "needs GATEWAY");
}

TEST(driver_mode_keys_are_strict_both_ways)
{
    onlyProblem(gw(with(gatewayEnv(), {}, { "GATEWAY_DRIVER_MODE", "GATEWAY_DRIVER_DIR" })), "GATEWAY_DRIVER_MODE",
        "required");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_MODE", "Embedded" } }, { "GATEWAY_DRIVER_DIR" })),
        "GATEWAY_DRIVER_MODE", "exactly embedded or external");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_DIR", "run/oe" } })), "GATEWAY_DRIVER_DIR", "absolute");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_DIR", "/nope" } })), "GATEWAY_DRIVER_DIR", "not an existing");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_DIR", "/dev/shm/aeron-host" } })), "GATEWAY_DRIVER_DIR",
        "not writable");
    onlyProblem(gw(with(gatewayEnv(), { { "AERON_DIR", "/dev/shm/aeron-host" } })), "AERON_DIR", "embedded");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_MODE", "external" } }, { "GATEWAY_DRIVER_DIR" })), "AERON_DIR",
        "required");
    // external with the embedded key still set: both named
    const auto both = gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_MODE", "external" }, { "AERON_DIR", "/dev/shm/aeron-host" } }));
    onlyProblem(both, "GATEWAY_DRIVER_DIR", "remove it");
    // external: a dir that does not exist (yet) is no driver, not a configuration problem (the connect exits 70);
    // only the form of the path is refused
    const auto missingDir = st(with(streamEnv(), { { "AERON_DIR", "/nope/aeron" } }));
    CHECK_MSG(missingDir.ok() && "/nope/aeron" == missingDir.settings.common.aeronDir, joined(missingDir.errors));
    onlyProblem(st(with(streamEnv(), { { "AERON_DIR", "dev/shm/aeron" } })), "AERON_DIR", "absolute");
    onlyProblem(st(with(streamEnv(), { { "AERON_DIR", "/dev/null" } })), "AERON_DIR", "not a directory");
    onlyProblem(st(with(streamEnv(), { { "AERON_DIR", "" } })), "AERON_DIR", "required");
    const auto ext = gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_MODE", "external" }, { "AERON_DIR", "/dev/shm/aeron-host" } },
        { "GATEWAY_DRIVER_DIR" }));
    CHECK_MSG(ext.ok(), joined(ext.errors));
    const auto timeout = gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_TIMEOUT_MS", "5000" } }));
    CHECK(timeout.ok() && 5000 == timeout.settings.common.driverTimeoutMs);
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_TIMEOUT_MS", "2999" } })), "GATEWAY_DRIVER_TIMEOUT_MS",
        "3000..60000");
}

TEST(auth_modes_and_the_keys_each_needs)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "api-key,dev" } })), "GATEWAY_AUTH_MODES",
        "GATEWAY_ALLOW_DEV_AUTH=1");
    const auto dev = gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "dev" }, { "GATEWAY_ALLOW_DEV_AUTH", "1" } }));
    CHECK_MSG(dev.ok(), joined(dev.errors));
    onlyProblem(gw(with(gatewayEnv(), {}, { "GATEWAY_API_KEYS_FILE" })), "GATEWAY_AUTH_MODES", "GATEWAY_API_KEYS_FILE");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "api-key,jwt" } })), "GATEWAY_AUTH_MODES",
        "GATEWAY_JWT_SECRET_FILE");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "demo,api-key" } })), "GATEWAY_AUTH_MODES",
        "GATEWAY_PG_URL");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "api-key,oauth" } })), "GATEWAY_AUTH_MODES", "unknown");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "api-key,api-key" } })), "GATEWAY_AUTH_MODES", "twice");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "api-key," } })), "GATEWAY_AUTH_MODES", "no empty item");
    onlyProblem(st(with(streamEnv(), { { "STREAM_AUTH_MODES", "demo" } })), "STREAM_AUTH_MODES", "GATEWAY_PG_URL");
    const auto demo = st(with(streamEnv(), { { "STREAM_AUTH_MODES", "demo" },
        { "GATEWAY_PG_URL", "postgresql://gw:pw@db:5432/gateway" } }));
    CHECK_MSG(demo.ok(), joined(demo.errors));
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_PG_URL", "mysql://x" } })), "GATEWAY_PG_URL");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_MONEY_PG_URL", "mysql://x" } })), "GATEWAY_MONEY_PG_URL");
    {
        // the money database is its own setting: never GATEWAY_PG_URL standing in for it, and secret in /health
        const auto r = gw(with(gatewayEnv(), { { "GATEWAY_PG_URL", "postgresql://gw:pw@db:5432/gateway" } }));
        CHECK(r.ok() && !r.settings.moneyPgUrl);
        const auto m = gw(with(gatewayEnv(), { { "GATEWAY_MONEY_PG_URL", "postgresql://gw:pw@money:5432/core" } }));
        CHECK(m.ok() && m.settings.moneyPgUrl && "postgresql://gw:pw@money:5432/core" == *m.settings.moneyPgUrl);
        const SettingRecord *rec = recordOf(m, "GATEWAY_MONEY_PG_URL");
        CHECK(nullptr != rec && std::string::npos == rec->value.find("pw@money"));
    }
}

TEST(treasury_lane_needs_flag_credential_and_caps_together)
{
    const std::string cred = "9:TREASURY:" + SECRET_HEX;
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_TREASURY_CREDENTIAL", cred } })), "GATEWAY_TREASURY_CREDENTIAL",
        "without GATEWAY_ALLOW_TREASURY_LANE=1");
    const auto flagOnly = gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" },
        { "GATEWAY_FAUCET_MAX_AMOUNT", "1:1000" } }));
    onlyProblem(flagOnly, "GATEWAY_ALLOW_TREASURY_LANE=1 needs GATEWAY_TREASURY_CREDENTIAL");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" }, { "GATEWAY_TREASURY_CREDENTIAL", cred } })),
        "GATEWAY_ALLOW_TREASURY_LANE=1 needs GATEWAY_FAUCET_MAX_AMOUNT");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" },
                    { "GATEWAY_TREASURY_CREDENTIAL", "9:GATEWAY:" + SECRET_HEX }, { "GATEWAY_FAUCET_MAX_AMOUNT", "1:5" } })),
        "GATEWAY_TREASURY_CREDENTIAL", "needs TREASURY");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" }, { "GATEWAY_TREASURY_CREDENTIAL", cred },
                    { "GATEWAY_FAUCET_MAX_AMOUNT", "1:5,1:6" } })),
        "GATEWAY_FAUCET_MAX_AMOUNT", "twice");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" }, { "GATEWAY_TREASURY_CREDENTIAL", cred },
                    { "GATEWAY_FAUCET_MAX_AMOUNT", "1:0.123456789" } })),
        "GATEWAY_FAUCET_MAX_AMOUNT");
    const auto on = gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" }, { "GATEWAY_TREASURY_CREDENTIAL", cred },
        { "GATEWAY_FAUCET_MAX_AMOUNT", "1:1000,2:0.5" } }));
    CHECK_MSG(on.ok(), joined(on.errors));
    CHECK(on.settings.allowTreasuryLane && on.settings.treasuryCredentials.has_value());
    CHECK(2 == on.settings.faucetMaxAmount.size());
    // asset 0 is a real asset id (the core's validAsset accepts it): a cap can name it
    const auto zero = gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" }, { "GATEWAY_TREASURY_CREDENTIAL", cred },
        { "GATEWAY_FAUCET_MAX_AMOUNT", "0:1000,1:5" } }));
    CHECK_MSG(zero.ok() && 2 == zero.settings.faucetMaxAmount.size() && 0 == zero.settings.faucetMaxAmount[0].assetId,
        "asset 0 refused in GATEWAY_FAUCET_MAX_AMOUNT: " + joined(zero.errors));
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ALLOW_TREASURY_LANE", "1" }, { "GATEWAY_TREASURY_CREDENTIAL", cred },
                    { "GATEWAY_FAUCET_MAX_AMOUNT", "-1:5" } })),
        "GATEWAY_FAUCET_MAX_AMOUNT");
}

TEST(faucet_cap_out_of_money_range_is_a_settings_problem)
{
    // the form is right but the amount is above INT64_MAX in units: the money parser the faucet route uses refuses it,
    // so the settings pass refuses it too (no cap is left out at run time)
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_FAUCET_MAX_AMOUNT", "1:99999999999999999999" } })),
        "GATEWAY_FAUCET_MAX_AMOUNT", "out of range");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_FAUCET_MAX_AMOUNT", "1:5,2:92233720368.54775808" } })),
        "GATEWAY_FAUCET_MAX_AMOUNT", "asset 2");
    const auto max = gw(with(gatewayEnv(), { { "GATEWAY_FAUCET_MAX_AMOUNT", "1:92233720368.54775807" } }));
    CHECK_MSG(max.ok() && 1 == max.settings.faucetMaxAmount.size(), joined(max.errors));
}

TEST(binds_admin_is_ipv4_and_never_the_public_listener)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "0.0.0.0:8080" } })), "GATEWAY_ADMIN_BIND",
        "same listener");
    // a wildcard admin bind takes the port on every address, the public 127.0.0.1 one included
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "0.0.0.0:8080" }, { "GATEWAY_HTTP_BIND", "127.0.0.1:8080" } })),
        "GATEWAY_ADMIN_BIND", "same listener");
    // localhost is 127.0.0.1, on either side
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "localhost:8080" }, { "GATEWAY_HTTP_BIND", "127.0.0.1:8080" } })),
        "GATEWAY_ADMIN_BIND", "same listener");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "127.0.0.1:8080" }, { "GATEWAY_HTTP_BIND", "localhost:8080" } })),
        "GATEWAY_ADMIN_BIND", "same listener");
    onlyProblem(st(with(streamEnv(), { { "STREAM_ADMIN_BIND", "0.0.0.0:8081" }, { "STREAM_WS_BIND", "[::1]:8081" } })),
        "STREAM_ADMIN_BIND", "same listener");
    // other ports, or two distinct non-wildcard hosts, are two listeners
    const auto apart = gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "0.0.0.0:9180" }, { "GATEWAY_HTTP_BIND", "127.0.0.1:8080" } }));
    CHECK_MSG(apart.ok(), joined(apart.errors));
    const auto hosts = gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "127.0.0.1:8080" }, { "GATEWAY_HTTP_BIND", "10.0.0.5:8080" } }));
    CHECK_MSG(hosts.ok(), joined(hosts.errors));
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "admin.local:9180" } })), "GATEWAY_ADMIN_BIND", "IPv4");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_ADMIN_BIND", "127.0.0.1:70000" } })), "GATEWAY_ADMIN_BIND", "0..65535");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_HTTP_BIND", "8080" } })), "GATEWAY_HTTP_BIND", "host:port");
    const auto v6 = gw(with(gatewayEnv(), { { "GATEWAY_HTTP_BIND", "[::]:8080" } }));
    CHECK_MSG(v6.ok(), joined(v6.errors));
}

TEST(set_but_empty_is_a_problem_not_the_default)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORS_ORIGINS", "" } })), "GATEWAY_CORS_ORIGINS", "set but empty");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_MAX_BODY_BYTES", "" } })), "GATEWAY_MAX_BODY_BYTES");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_IDLE", "" } })), "GATEWAY_IDLE", "set but empty");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_DRIVER_TIMEOUT_MS", "" } })), "GATEWAY_DRIVER_TIMEOUT_MS",
        "set but empty");
    const auto empty = st(with(streamEnv(), { { "STREAM_DRIVER_TIMEOUT_MS", "" } }));
    CHECK_MSG(!mentions(empty, "required"), joined(empty.errors));
}

TEST(status_interval_and_embedded_retry_are_settings)
{
    const auto d = gw(gatewayEnv());
    CHECK_MSG(d.ok(), joined(d.errors));
    CHECK(100 == d.settings.common.statusIntervalMs && 1000 == d.settings.common.embeddedRetryMs);
    const SettingRecord *interval = recordOf(d, "GATEWAY_STATUS_INTERVAL_MS");
    CHECK(nullptr != interval && "100" == interval->value && "default" == interval->source);
    const SettingRecord *retry = recordOf(d, "GATEWAY_EMBEDDED_RETRY_MS");
    CHECK(nullptr != retry && "1000" == retry->value && "default" == retry->source);

    const auto set = gw(with(gatewayEnv(), { { "GATEWAY_STATUS_INTERVAL_MS", "250" }, { "GATEWAY_EMBEDDED_RETRY_MS", "1500" } }));
    CHECK_MSG(set.ok(), joined(set.errors));
    CHECK(250 == set.settings.common.statusIntervalMs && 1500 == set.settings.common.embeddedRetryMs);
    const SettingRecord *setRetry = recordOf(set, "GATEWAY_EMBEDDED_RETRY_MS");
    CHECK(nullptr != setRetry && "1500" == setRetry->value && "set" == setRetry->source);

    for (const std::string bad : { "0", "-1", "abc", "100ms", "" })
    {
        onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_STATUS_INTERVAL_MS", bad } })), "GATEWAY_STATUS_INTERVAL_MS");
        onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_EMBEDDED_RETRY_MS", bad } })), "GATEWAY_EMBEDDED_RETRY_MS");
    }
    // stream (external): the interval is recorded; the embedded retry has no meaning there and is refused when set
    const auto s = st(streamEnv());
    const SettingRecord *streamInterval = recordOf(s, "STREAM_STATUS_INTERVAL_MS");
    CHECK(nullptr != streamInterval && "default" == streamInterval->source);
    onlyProblem(st(with(streamEnv(), { { "STREAM_EMBEDDED_RETRY_MS", "500" } })), "STREAM_EMBEDDED_RETRY_MS",
        "external");
}

TEST(unknown_names_with_the_own_prefix_are_refused)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_REQUEST_TIMEOUT", "100" } })), "GATEWAY_REQUEST_TIMEOUT",
        "not a setting");
    // the other binary's names and the shared GATEWAY_ auth keys are not the stream's problem
    const auto r = st(with(streamEnv(), { { "GATEWAY_MAX_INFLIGHT", "5" }, { "GATEWAY_SOMETHING", "x" } }));
    CHECK_MSG(r.ok(), joined(r.errors));
    onlyProblem(st(with(streamEnv(), { { "STREAM_WINDOW", "5" } })), "STREAM_WINDOW", "not a setting");
    const auto g = gw(with(gatewayEnv(), { { "STREAM_EVENT_WINDOW", "5" } }));
    CHECK_MSG(g.ok(), joined(g.errors));
}

TEST(token_stale_key_is_never_silently_ignored)
{
    // GATEWAY_TOKEN_CACHE_STALE_MS is a second spelling of GATEWAY_TOKEN_STALE_MS: both binaries refuse it by name
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_TOKEN_CACHE_STALE_MS", "1000" } })), "GATEWAY_TOKEN_CACHE_STALE_MS",
        "use GATEWAY_TOKEN_STALE_MS");
    onlyProblem(st(with(streamEnv(), { { "GATEWAY_TOKEN_CACHE_STALE_MS", "1000" } })), "GATEWAY_TOKEN_CACHE_STALE_MS",
        "use GATEWAY_TOKEN_STALE_MS");
    const auto ok = st(with(streamEnv(), { { "GATEWAY_TOKEN_STALE_MS", "1000" } }));
    CHECK_MSG(ok.ok() && 1000 == ok.settings.common.tokenStaleMs, joined(ok.errors));
}

TEST(cors_star_is_refused)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORS_ORIGINS", "https://a.example,*" } })), "GATEWAY_CORS_ORIGINS",
        "'*' is refused");
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_CORS_ORIGINS", "https://a.example/path" } })), "GATEWAY_CORS_ORIGINS");
    const auto ok = gw(with(gatewayEnv(), { { "GATEWAY_CORS_ORIGINS", "https://a.example,http://localhost:5173" } }));
    CHECK_MSG(ok.ok() && 2 == ok.settings.corsOrigins.size(), joined(ok.errors));
}

TEST(history_default_must_not_exceed_max)
{
    onlyProblem(gw(with(gatewayEnv(), { { "GATEWAY_HISTORY_LIMIT_DEFAULT", "2000" } })), "GATEWAY_HISTORY_LIMIT_DEFAULT");
    const auto ok = gw(with(gatewayEnv(), { { "GATEWAY_HISTORY_LIMIT_DEFAULT", "2000" },
        { "GATEWAY_HISTORY_LIMIT_MAX", "5000" } }));
    CHECK_MSG(ok.ok(), joined(ok.errors));
}

TEST(stream_timing_pairs_are_cross_checked)
{
    const auto d = st(streamEnv());
    CHECK_MSG(d.ok(), joined(d.errors));
    CHECK(d.settings.readyStaleMs > d.settings.headProbeMs && d.settings.wsPingMs < d.settings.wsIdleMs);
    onlyProblem(st(with(streamEnv(), { { "STREAM_READY_STALE_MS", "5000" } })), "STREAM_READY_STALE_MS",
        "above STREAM_HEAD_PROBE_MS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_HEAD_PROBE_MS", "20000" } })), "STREAM_READY_STALE_MS");
    const auto stale = st(with(streamEnv(), { { "STREAM_HEAD_PROBE_MS", "1000" },
        { "STREAM_READY_STALE_MS", "1001" } }));
    CHECK_MSG(stale.ok(), joined(stale.errors));
    onlyProblem(st(with(streamEnv(), { { "STREAM_WS_PING_MS", "60000" } })), "STREAM_WS_PING_MS",
        "below STREAM_WS_IDLE_MS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_WS_IDLE_MS", "10000" } })), "STREAM_WS_PING_MS");
    const auto ping = st(with(streamEnv(), { { "STREAM_WS_PING_MS", "59999" } }));
    CHECK_MSG(ping.ok(), joined(ping.errors));
}

TEST(every_setting_is_recorded_with_its_source)
{
    const auto r = gw(gatewayEnv());
    const std::vector<std::string> gatewayKeys = { "GATEWAY_DRIVER_MODE", "GATEWAY_DRIVER_DIR", "GATEWAY_DRIVER_TIMEOUT_MS",
        "GATEWAY_CORE_INGRESS", "GATEWAY_CORE_INGRESS_CHANNEL", "GATEWAY_CORE_RESPONSE_CHANNEL", "CORE_AUTH_CREDENTIAL",
        "GATEWAY_TREASURY_CREDENTIAL", "GATEWAY_ALLOW_TREASURY_LANE", "GATEWAY_INSTANCE_ID", "GATEWAY_HTTP_BIND",
        "GATEWAY_ADMIN_BIND", "GATEWAY_IO_THREADS", "GATEWAY_IDLE", "GATEWAY_COMMAND_RING", "GATEWAY_MAX_INFLIGHT",
        "GATEWAY_REQUEST_TIMEOUT_MS", "GATEWAY_MAX_BODY_BYTES", "GATEWAY_MAX_CONNECTIONS", "GATEWAY_PRINCIPAL_RATE",
        "GATEWAY_PRINCIPAL_BURST", "GATEWAY_QUERY_RATE", "GATEWAY_QUERY_BURST", "GATEWAY_QUERY_MIN_INTERVAL_MS",
        "GATEWAY_BULK_CANCEL_MAX", "GATEWAY_READY_STALE_MS", "GATEWAY_READY_TIMEOUT_STREAK", "GATEWAY_AUTH_MODES",
        "GATEWAY_ALLOW_DEV_AUTH", "GATEWAY_API_KEYS_FILE", "GATEWAY_JWT_SECRET_FILE", "GATEWAY_TOKEN_CACHE_TTL_MS",
        "GATEWAY_TOKEN_STALE_MS", "GATEWAY_HASH_THREADS", "GATEWAY_HASH_QUEUE", "GATEWAY_REGISTER_RATE",
        "GATEWAY_REGISTER_BURST", "GATEWAY_LOGIN_RATE", "GATEWAY_LOGIN_BURST", "GATEWAY_AUTH_SOURCE_RATE",
        "GATEWAY_TRUSTED_PROXIES", "GATEWAY_CLIENT_IP_HEADER", "GATEWAY_CORS_ORIGINS", "GATEWAY_PG_URL",
        "GATEWAY_PG_HISTORY_POOL", "GATEWAY_PG_AUTH_POOL", "GATEWAY_PG_STATEMENT_TIMEOUT_MS", "GATEWAY_PG_QUEUE",
        "GATEWAY_HISTORY_LIMIT_DEFAULT", "GATEWAY_HISTORY_LIMIT_MAX", "GATEWAY_HISTORY_RETRY_AFTER_MS",
        "GATEWAY_HISTORY_BUSY_RETRY_AFTER_MS", "GATEWAY_HISTORY_LAG_RETRY_AFTER_MS", "GATEWAY_FAUCET_MAX_AMOUNT", "GATEWAY_FAUCET_RATE",
        "GATEWAY_DUTY_WATCHDOG_MS", "GATEWAY_STOP_TIMEOUT_MS", "GATEWAY_STATUS_INTERVAL_MS", "GATEWAY_EMBEDDED_RETRY_MS",
        "GATEWAY_RECONNECT_DELAY_MS", "GATEWAY_REGISTER_RESEND_MS", "GATEWAY_TOKEN_LOOKUP_RATE", "GATEWAY_TOKEN_LOOKUP_BURST",
        "GATEWAY_TOKEN_LOOKUP_INFLIGHT", "GATEWAY_PG_RETRY_MS", "GATEWAY_SOURCE_IPV6_PREFIX", "GATEWAY_MONEY_PG_URL",
        "GATEWAY_PG_MONEY_POOL" };
    for (const std::string &key : gatewayKeys)
    {
        CHECK_MSG(nullptr != recordOf(r, key), key + " is not recorded");
    }
    const SettingRecord *inflight = recordOf(r, "GATEWAY_MAX_INFLIGHT");
    CHECK(nullptr != inflight && "4096" == inflight->value && "default" == inflight->source);
    const SettingRecord *mode = recordOf(r, "GATEWAY_DRIVER_MODE");
    CHECK(nullptr != mode && "embedded" == mode->value && "set" == mode->source);

    const auto s = st(streamEnv());
    for (const char *key : { "STREAM_DRIVER_MODE", "AERON_DIR", "STREAM_DRIVER_TIMEOUT_MS", "STREAM_INSTANCE_ID",
             "STREAM_WS_BIND", "STREAM_ADMIN_BIND", "STREAM_IO_THREADS", "STREAM_IDLE", "STREAM_COMMAND_RING",
             "STREAM_MAX_CONNECTIONS", "STREAM_AUTH_MODES", "STREAM_PG_AUTH_POOL", "STREAM_BALANCE_STREAM",
             "STREAM_EVENT_WINDOW", "STREAM_REPLAY_PAGE", "STREAM_GAP_BUFFER_EVENTS", "STREAM_QUERY_RATE",
             "STREAM_QUERY_BURST", "STREAM_USER_LINGER_MS", "STREAM_HEAD_PROBE_MS", "STREAM_READY_STALE_MS",
             "STREAM_HEARTBEAT_MS", "STREAM_WS_QUEUE_MESSAGES", "STREAM_WS_QUEUE_BYTES", "STREAM_WS_AUTH_TIMEOUT_MS",
             "STREAM_WS_PING_MS", "STREAM_WS_IDLE_MS", "STREAM_DUTY_WATCHDOG_MS", "STREAM_STOP_TIMEOUT_MS",
             "STREAM_RECONNECT_DELAY_MS", "STREAM_REGISTER_RESEND_MS", "STREAM_ANON_MAX_CONNECTIONS",
             "STREAM_ANON_WS_QUEUE_MESSAGES", "STREAM_ANON_WS_QUEUE_BYTES" })
    {
        CHECK_MSG(nullptr != recordOf(s, key), std::string(key) + " is not recorded");
    }
}

TEST(secrets_are_recorded_redacted)
{
    const auto r = gw(with(gatewayEnv(), { { "GATEWAY_AUTH_MODES", "demo" },
        { "GATEWAY_PG_URL", "postgresql://gw:hunter2@db:5432/gateway" } }));
    CHECK_MSG(r.ok(), joined(r.errors));
    for (const SettingRecord &rec : r.records)
    {
        CHECK_MSG(std::string::npos == rec.value.find(SECRET_HEX), rec.name + " leaks the credential secret");
        CHECK_MSG(std::string::npos == rec.value.find("hunter2"), rec.name + " leaks the PG password");
    }
    const SettingRecord *cred = recordOf(r, "CORE_AUTH_CREDENTIAL");
    CHECK(nullptr != cred && "7:GATEWAY:(redacted)" == cred->value);
}

TEST(rate_and_list_helpers)
{
    CHECK(parseRate("50/s").has_value() && 1000 == parseRate("50/s")->periodMs);
    CHECK(parseRate("10/min").has_value() && 60'000 == parseRate("10/min")->periodMs);
    for (const std::string bad : { "50", "0/s", "-1/s", "50/h", "/s", "50/", "5.5/s", "50 /s" })
    {
        CHECK_MSG(!parseRate(bad).has_value(), bad);
    }
    CHECK(splitList("a,b").has_value() && 2 == splitList("a,b")->size());
    CHECK(!splitList("a,,b").has_value());
    CHECK(!splitList("").has_value());
}

// The anonymous (engine status only) connections: a cap with a default, a share of STREAM_MAX_CONNECTIONS (at or above
// it the boot is refused), and the small outbound bound of such a connection.
TEST(stream_anonymous_settings)
{
    const auto d = st(streamEnv());
    CHECK_MSG(d.ok(), joined(d.errors));
    CHECK(500 == d.settings.anonMaxConnections);
    CHECK(8 == d.settings.anonQueueMessages);
    CHECK(65'536 == d.settings.anonQueueBytes);
    const auto r = st(with(streamEnv(), { { "STREAM_ANON_MAX_CONNECTIONS", "10" }, { "STREAM_ANON_WS_QUEUE_MESSAGES", "4" },
                                          { "STREAM_ANON_WS_QUEUE_BYTES", "4096" } }));
    CHECK_MSG(r.ok(), joined(r.errors));
    CHECK(10 == r.settings.anonMaxConnections && 4 == r.settings.anonQueueMessages && 4096 == r.settings.anonQueueBytes);
    onlyProblem(st(with(streamEnv(), { { "STREAM_ANON_MAX_CONNECTIONS", "0" } })), "STREAM_ANON_MAX_CONNECTIONS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_ANON_WS_QUEUE_MESSAGES", "x" } })), "STREAM_ANON_WS_QUEUE_MESSAGES");
    onlyProblem(st(with(streamEnv(), { { "STREAM_ANON_MAX_CONNECTIONS", "2000" } })), "STREAM_ANON_MAX_CONNECTIONS",
        "must be below STREAM_MAX_CONNECTIONS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_MAX_CONNECTIONS", "100" } })), "STREAM_ANON_MAX_CONNECTIONS",
        "must be below STREAM_MAX_CONNECTIONS (100)");
    const auto small = st(with(streamEnv(), { { "STREAM_MAX_CONNECTIONS", "100" }, { "STREAM_ANON_MAX_CONNECTIONS", "99" } }));
    CHECK_MSG(small.ok(), joined(small.errors));
}

// The engine status feed's settings: node endpoint lists (unset = no node polled, that block all null), the poll, its
// timeout, the rate cap and the body bound, each a setting with a default; a malformed list is named.
TEST(stream_engine_settings)
{
    const auto d = st(streamEnv());
    CHECK_MSG(d.ok(), joined(d.errors));
    CHECK(d.settings.coreNodeEndpoints.empty());
    CHECK(d.settings.meNodeEndpoints.empty());
    CHECK(1000 == d.settings.enginePollMs);
    CHECK(500 == d.settings.enginePollTimeoutMs);
    CHECK(1000 == d.settings.engineFrameMinMs);
    CHECK(1'048'576 == d.settings.engineMaxBodyBytes);
    const auto r = st(with(streamEnv(), { { "STREAM_CORE_NODE_ENDPOINTS", "0@10.0.0.1:9306,1@10.0.0.2:9306,2@node-c:9306" },
                                          { "STREAM_ME_NODE_ENDPOINTS", "0@10.0.1.1:9006" },
                                          { "STREAM_ENGINE_POLL_MS", "250" },
                                          { "STREAM_ENGINE_POLL_TIMEOUT_MS", "200" },
                                          { "STREAM_ENGINE_FRAME_MIN_MS", "2000" } }));
    CHECK_MSG(r.ok(), joined(r.errors));
    CHECK(3 == r.settings.coreNodeEndpoints.size());
    CHECK(1 == r.settings.meNodeEndpoints.size());
    if (3 == r.settings.coreNodeEndpoints.size())
    {
        CHECK(0 == r.settings.coreNodeEndpoints[0].memberId);
        CHECK("10.0.0.1" == r.settings.coreNodeEndpoints[0].host);
        CHECK(9306 == r.settings.coreNodeEndpoints[0].port);
        CHECK("node-c" == r.settings.coreNodeEndpoints[2].host);
        CHECK(2 == r.settings.coreNodeEndpoints[2].memberId);
    }
    CHECK(250 == r.settings.enginePollMs);
    CHECK(2000 == r.settings.engineFrameMinMs);
    for (const std::string bad : { "0@h:0", "0@h:65536", "x@h:1", "-1@h:1", "0@:9306", "0h:9306", "0@h", "0@h:1,", "0@h:1,0@g:2",
             "0@h:1,,1@g:2" })
    {
        onlyProblem(st(with(streamEnv(), { { "STREAM_CORE_NODE_ENDPOINTS", bad } })), "STREAM_CORE_NODE_ENDPOINTS");
    }
    onlyProblem(st(with(streamEnv(), { { "STREAM_ME_NODE_ENDPOINTS", "" } })), "STREAM_ME_NODE_ENDPOINTS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_ENGINE_POLL_MS", "0" } })), "STREAM_ENGINE_POLL_MS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_ENGINE_FRAME_MIN_MS", "-5" } })), "STREAM_ENGINE_FRAME_MIN_MS");
    // an IPv6 literal is written in brackets; the host is the address without them
    const auto v6 = st(with(streamEnv(), { { "STREAM_CORE_NODE_ENDPOINTS", "0@[::1]:9090,1@[fd00::2]:9091" } }));
    CHECK_MSG(v6.ok(), joined(v6.errors));
    if (2 == v6.settings.coreNodeEndpoints.size())
    {
        CHECK_MSG("::1" == v6.settings.coreNodeEndpoints[0].host,
            "IPv6 host kept its brackets: " + v6.settings.coreNodeEndpoints[0].host);
        CHECK(9090 == v6.settings.coreNodeEndpoints[0].port);
        CHECK("fd00::2" == v6.settings.coreNodeEndpoints[1].host);
    }
    for (const std::string bad : { "0@::1:9090", "0@[::1]9090", "0@[]:9090", "0@[::1:9090" })
    {
        onlyProblem(st(with(streamEnv(), { { "STREAM_CORE_NODE_ENDPOINTS", bad } })), "STREAM_CORE_NODE_ENDPOINTS");
    }
    // a request timeout at or above the poll interval: rounds would run back to back (refused, not silently accepted)
    onlyProblem(st(with(streamEnv(), { { "STREAM_ENGINE_POLL_MS", "250" } })), "STREAM_ENGINE_POLL_TIMEOUT_MS");
    onlyProblem(st(with(streamEnv(), { { "STREAM_ENGINE_POLL_MS", "500" }, { "STREAM_ENGINE_POLL_TIMEOUT_MS", "500" } })),
        "STREAM_ENGINE_POLL_TIMEOUT_MS");
}
