#include "gateway/common/Settings.h"

#include <arpa/inet.h>
#include <sys/stat.h>

#include <algorithm>
#include <charconv>
#include <set>

#include "cluster_tools/client/IngressEndpoints.h"
#include "cluster_tools/runtime/DriverTimeout.h"
#include "gateway/common/Rest.h"

// oe-gateway only (defaults from the settings table; none is a limit in code):
//   GATEWAY_TREASURY_CREDENTIAL unset (keyId:TREASURY:secretHex), GATEWAY_ALLOW_TREASURY_LANE 0: the lane is on only
//   with both; one without the other is refused. The lane also needs GATEWAY_FAUCET_MAX_AMOUNT (its caps live in this
//   process). GATEWAY_MAX_INFLIGHT 4096, GATEWAY_REQUEST_TIMEOUT_MS 5000, GATEWAY_MAX_BODY_BYTES 65536,
//   GATEWAY_PRINCIPAL_RATE 50/s + _BURST 100, GATEWAY_QUERY_RATE 100/s + _BURST 200, GATEWAY_QUERY_MIN_INTERVAL_MS 250,
//   GATEWAY_BULK_CANCEL_MAX 200, GATEWAY_READY_STALE_MS 3000, GATEWAY_READY_TIMEOUT_STREAK 5, GATEWAY_HASH_THREADS 1,
//   GATEWAY_HASH_QUEUE 32, GATEWAY_REGISTER_RATE 1/s + GATEWAY_REGISTER_BURST 5, GATEWAY_LOGIN_RATE 10/s +
//   GATEWAY_LOGIN_BURST 20, GATEWAY_AUTH_SOURCE_RATE 10/min + GATEWAY_AUTH_SOURCE_BURST 10 (per source address,
//   register and login together), GATEWAY_SOURCE_IPV6_PREFIX 64 (an IPv6 source's budget is shared by its /64; 1 to
//   128, the length of an address), GATEWAY_PBKDF2_ITERATIONS 600000 (OWASP; new password hashes; a stored hash keeps its own
//   count; at most 2^31-1, the PBKDF2 interface's bound), GATEWAY_MAX_HEADER_BYTES 8192 (the request header block,
//   above it 431), GATEWAY_HTTP_IDLE_TIMEOUT_MS 60000 (a connection silent this long, inside or between requests, is
//   closed), GATEWAY_TRUSTED_PROXIES none (IP or CIDR list),
//   GATEWAY_CLIENT_IP_HEADER X-Forwarded-For, GATEWAY_CORS_ORIGINS none (explicit http(s) origins; '*' refused),
//   GATEWAY_PG_HISTORY_POOL 4, GATEWAY_PG_AUTH_POOL 2, GATEWAY_PG_STATEMENT_TIMEOUT_MS 2000, GATEWAY_PG_QUEUE 64,
//   GATEWAY_PG_RETRY_MS 1000 (after a failed connect, calls fail at once for this long; also the WARN interval),
//   GATEWAY_TOKEN_LOOKUP_RATE 60/min + GATEWAY_TOKEN_LOOKUP_BURST 20 (per source address: store lookups of bearer
//   tokens the cache does not hold), GATEWAY_TOKEN_LOOKUP_INFLIGHT half of GATEWAY_PG_QUEUE (such lookups waiting at
//   once; with demo it must stay below GATEWAY_PG_QUEUE, the correctness reason being that the rest of the queue is
//   what login, register and logout are guaranteed),
//   GATEWAY_HISTORY_LIMIT_DEFAULT 100 (<= _MAX), GATEWAY_HISTORY_LIMIT_MAX 1000 (a larger limit is 400),
//   GATEWAY_HISTORY_RETRY_AFTER_MS 60000 (Retry-After of 503 HISTORY_UNAVAILABLE), GATEWAY_HISTORY_BUSY_RETRY_AFTER_MS
//   1000 (Retry-After of 503 HISTORY_BUSY), GATEWAY_HISTORY_LAG_RETRY_AFTER_MS 1000 (Retry-After of the 503
//   HISTORY_UNAVAILABLE of GET /orders/{orderId} for an id above the projection's applied_order_id: not projected yet).
//   History is read when GATEWAY_PG_URL is set, on its own pool (GATEWAY_PG_HISTORY_POOL connections over a queue of
//   its own, sized GATEWAY_PG_QUEUE like the auth pool's), never the auth pool's connections;
//   GATEWAY_PG_STATEMENT_TIMEOUT_MS is also one history read's whole deadline, queue wait included,
//   GATEWAY_FAUCET_MAX_AMOUNT none (assetId:amount,...; each amount a money decimal of at most 92233720368.54775807),
//   GATEWAY_FAUCET_RATE 1/s.
// oe-stream only:
//   STREAM_BALANCE_STREAM on|off (on), STREAM_EVENT_WINDOW 65536, STREAM_REPLAY_PAGE 1024, STREAM_GAP_BUFFER_EVENTS
//   65536, STREAM_QUERY_RATE 50/s + _BURST 50, STREAM_USER_LINGER_MS 30000, STREAM_HEAD_PROBE_MS 5000,
//   STREAM_READY_STALE_MS 15000, STREAM_HEARTBEAT_MS 5000, STREAM_WS_QUEUE_MESSAGES 256, STREAM_WS_QUEUE_BYTES
//   1048576, STREAM_WS_AUTH_TIMEOUT_MS 5000, STREAM_WS_PING_MS 15000, STREAM_WS_IDLE_MS 60000, STREAM_PG_AUTH_POOL 1,
//   STREAM_REQUEST_TIMEOUT_MS 5000 (the deadline of one core query: a snapshot page, a balance read, a replay page, a
//   head probe), STREAM_WS_MAX_MESSAGE_BYTES 65536 (a client frame above it closes the connection, 1009),
//   STREAM_PG_QUEUE 64, STREAM_PG_STATEMENT_TIMEOUT_MS 2000, STREAM_PG_RETRY_MS 1000 (demo token lookups: the auth pool's
//   queue, statement timeout and reconnect spacing; at most half the queue waits for unknown tokens),
//   STREAM_WS_SEND_BUFFER_BYTES unset (the kernel's send buffer of each WebSocket socket; unset leaves the OS default and
//   its autotuning, which can hold megabytes per connection before a slow reader reaches the outbound queue's bound),
//   STREAM_CORE_NODE_ENDPOINTS and STREAM_ME_NODE_ENDPOINTS unset (<memberId>@<host>:<port>,...: each core and matching
//   engine node's own probe endpoint, polled for the engine status feed; unset = that block is all null),
//   STREAM_ENGINE_POLL_MS 1000 (one round of GET /health and GET /metrics on every configured node),
//   STREAM_ENGINE_POLL_TIMEOUT_MS 500 (one request's deadline; a node that misses it is unreachable for that round),
//   STREAM_ENGINE_FRAME_MIN_MS 1000 (the rate cap of the engine frame: a changed frame waits until the last one is this
//   old), STREAM_ENGINE_MAX_BODY_BYTES 1048576 (a node's answer above it counts as no answer).
//   Pairs: STREAM_READY_STALE_MS above STREAM_HEAD_PROBE_MS, STREAM_WS_PING_MS below STREAM_WS_IDLE_MS.

namespace openexchange { namespace gateway {

namespace cl = cluster_tools::client;
namespace rt = cluster_tools::runtime;

namespace {

// Something exists at `path` (a file, a directory, a dangling symlink): the probe answers "is it a directory", this
// tells "missing" from "something else is there".
bool pathExists(const std::string &path)
{
    struct stat st{};
    return 0 == ::lstat(path.c_str(), &st);
}

bool isIpv4(const std::string &host)
{
    in_addr a{};
    return 1 == ::inet_pton(AF_INET, host.c_str(), &a);
}

bool isIpv6(const std::string &host)
{
    in6_addr a{};
    return 1 == ::inet_pton(AF_INET6, host.c_str(), &a);
}

// host:port (IPv6 as [addr]:port); port 0..65535 (0 = ephemeral, logged at bind).
std::optional<Bind> parseBind(const std::string &text, bool ipv4Only, std::string &why)
{
    const std::size_t colon = text.rfind(':');
    if (std::string::npos == colon || 0 == colon || colon + 1 >= text.size())
    {
        why = "host:port";
        return std::nullopt;
    }
    Bind b;
    b.text = text;
    b.host = text.substr(0, colon);
    if (b.host.size() > 2 && '[' == b.host.front() && ']' == b.host.back())
    {
        b.host = b.host.substr(1, b.host.size() - 2);
        if (!isIpv6(b.host))
        {
            why = "host:port with a valid IPv6 address in brackets";
            return std::nullopt;
        }
    }
    const std::string h = "localhost" == b.host ? "127.0.0.1" : b.host;
    if (ipv4Only && !isIpv4(h))
    {
        why = "host:port with an IPv4 address (or localhost)";
        return std::nullopt;
    }
    const std::string port = text.substr(colon + 1);
    int p = -1;
    const auto r = std::from_chars(port.data(), port.data() + port.size(), p);
    if (r.ec != std::errc() || r.ptr != port.data() + port.size() || p < 0 || p > 65535 || '+' == port.front())
    {
        why = "host:port with a port in 0..65535";
        return std::nullopt;
    }
    b.port = static_cast<std::uint16_t>(p);
    return b;
}

void credentialSetting(SettingsReader &r, const std::string &name, cl::SessionClass want, bool required,
    std::optional<cl::CoreCredentials> &out)
{
    const std::string form = std::string("keyId:") + cl::nameOf(want) + ":secretHex";
    const std::optional<std::string> spec = r.raw(name);
    if (!spec)
    {
        if (required)
        {
            r.problem(name + " is required (no default): " + form);
        }
        else
        {
            r.record(name, "(unset)", "unset");
        }
        return;
    }
    if (spec->empty())
    {
        r.problem(name + (required ? " is required (no default): " : " is set but empty (unset it, or give ") + form +
            (required ? "" : ")"));
        return;
    }
    const cl::CredentialsResult parsed = cl::parseCoreCredentials(*spec);
    if (!parsed.ok())
    {
        r.problem(name + ": " + parsed.error);
        return;
    }
    if (want != parsed.credentials.sessionClass)
    {
        r.problem(name + " is a " + cl::nameOf(parsed.credentials.sessionClass) + " credential; this process needs " +
            cl::nameOf(want));
        return;
    }
    r.record(name, std::to_string(parsed.credentials.keyId) + ":" + cl::nameOf(want) + ":(redacted)", "set");
    out = parsed.credentials;
}

// mtu: decimal, a multiple of 32 (Aeron's frame alignment), 32..65504 (the largest such below the UDP payload limit).
bool isChannelMtu(const std::string &v)
{
    if (v.empty() || '0' == v.front())
    {
        return false;
    }
    std::int64_t n = 0;
    const auto r = std::from_chars(v.data(), v.data() + v.size(), n);
    return r.ec == std::errc() && r.ptr == v.data() + v.size() && n >= 32 && n <= 65'504 && 0 == n % 32;
}

// term-length: a power of two in 64k..1g, decimal with an optional k, m or g suffix (either case).
bool isChannelTermLength(const std::string &v)
{
    if (v.empty())
    {
        return false;
    }
    std::int64_t unit = 1;
    std::size_t digits = v.size();
    switch (v.back())
    {
        case 'k': case 'K': unit = 1024LL; --digits; break;
        case 'm': case 'M': unit = 1024LL * 1024; --digits; break;
        case 'g': case 'G': unit = 1024LL * 1024 * 1024; --digits; break;
        default: break;
    }
    if (0 == digits || '0' == v.front() || digits > 10)
    {
        return false;
    }
    std::int64_t n = 0;
    const auto r = std::from_chars(v.data(), v.data() + digits, n);
    if (r.ec != std::errc() || r.ptr != v.data() + digits)
    {
        return false;
    }
    const std::int64_t bytes = n * unit;
    return bytes >= 64LL * 1024 && bytes <= 1024LL * 1024 * 1024 && 0 == (bytes & (bytes - 1));
}

void checkChannel(SettingsReader &r, const std::string &name, const std::string &uri, bool response)
{
    std::string error;
    const auto params = channelParams(uri, error);
    if (!params)
    {
        r.problem(name + ": " + error);
        return;
    }
    std::set<std::string> keys;
    for (const auto &kv : *params)
    {
        keys.insert(kv.first);
        if ("mtu" == kv.first && !isChannelMtu(kv.second))
        {
            r.problem(name + ": mtu must be a decimal multiple of 32 in 32..65504 (the core members' value, 8192 in "
                "the pinned driver properties), got '" + kv.second + "'");
        }
        if ("term-length" == kv.first && !isChannelTermLength(kv.second))
        {
            r.problem(name + ": term-length must be a power of two in 64k..1g (decimal, optional k, m or g suffix), "
                "got '" + kv.second + "'");
        }
    }
    std::string missing;
    for (const char *k : response ? std::vector<const char *>{ "endpoint", "term-length", "mtu" }
                                  : std::vector<const char *>{ "term-length", "mtu" })
    {
        if (0 == keys.count(k))
        {
            missing += missing.empty() ? k : std::string(", ") + k;
        }
    }
    if (!missing.empty())
    {
        r.problem(name + " must name " + (response ? "endpoint, term-length and mtu" : "term-length and mtu") +
            " explicitly (missing: " + missing + "), got '" + uri + "'");
        return;
    }
    if (response)
    {
        cl::ClientSettings s;
        s.ingressEndpoints = "0=127.0.0.1:1";
        s.ingressChannel = "aeron:udp";
        s.responseChannel = uri;
        s.credentials.assign(cl::CoreCredentials::ENCODED_LENGTH, 1);
        if (const std::string e = cl::validate(s); !e.empty())
        {
            r.problem(name + ": " + e);
        }
    }
    else if (const std::string e = cl::validateIngressBaseChannel(uri); !e.empty())
    {
        r.problem(name + ": " + e);
    }
}

void parseCommon(SettingsReader &r, Role role, const rt::DirProbe &probe, CommonSettings &c)
{
    const std::string P = Role::GATEWAY == role ? "GATEWAY" : "STREAM";

    // driver mode first, then its keys, strict in both directions
    const int mode = r.choice(P + "_DRIVER_MODE", { "embedded", "external" }, -1);
    c.driverMode = 0 == mode ? DriverMode::EMBEDDED : 1 == mode ? DriverMode::EXTERNAL : DriverMode::UNSET;
    const std::optional<std::string> aeronDir = r.raw("AERON_DIR");
    const std::optional<std::string> driverDir = r.raw(P + "_DRIVER_DIR");
    if (DriverMode::EXTERNAL == c.driverMode)
    {
        // A path that does not exist (yet) is not a configuration problem: after a reboot, or when aeronmd starts
        // later, it is "no driver", and the connect below waits the driver timeout and exits 70 (systemd restarts
        // it). Only the form of the path is refused here: empty, relative, or an existing non-directory.
        if (!aeronDir || aeronDir->empty())
        {
            r.problem("AERON_DIR is required with " + P + "_DRIVER_MODE=external (the host aeronmd's directory)");
        }
        else if ('/' != aeronDir->front())
        {
            r.problem("AERON_DIR must be an absolute path, got '" + *aeronDir + "'");
        }
        else if (!probe.isDirectory(*aeronDir) && pathExists(*aeronDir))
        {
            r.problem("AERON_DIR " + *aeronDir + " exists and is not a directory");
        }
        else
        {
            c.aeronDir = *aeronDir;
            r.record("AERON_DIR", *aeronDir, "set");
        }
        if (driverDir)
        {
            r.problem(P + "_DRIVER_DIR is set but " + P + "_DRIVER_MODE=external uses the host aeronmd at AERON_DIR: "
                "remove it");
        }
    }
    else if (DriverMode::EMBEDDED == c.driverMode)
    {
        const std::string key = P + "_DRIVER_DIR";
        if (!driverDir || driverDir->empty())
        {
            r.problem(key + " is required with " + P + "_DRIVER_MODE=embedded (the parent of the per-instance driver dir)");
        }
        else if ('/' != driverDir->front())
        {
            r.problem(key + " must be an absolute path, got '" + *driverDir + "'");
        }
        else if (!probe.isDirectory(*driverDir))
        {
            r.problem(key + " " + *driverDir + " is not an existing directory");
        }
        else if (!probe.isWritable(*driverDir))
        {
            r.problem(key + " " + *driverDir + " is not writable");
        }
        else
        {
            c.driverDir = *driverDir;
            r.record(key, *driverDir, "set");
        }
        if (aeronDir)
        {
            r.problem("AERON_DIR is set but " + P + "_DRIVER_MODE=embedded starts its own driver: remove it");
        }
    }
    {
        const std::string key = P + "_DRIVER_TIMEOUT_MS";
        const std::optional<std::string> raw = r.raw(key);
        if (!raw)
        {
            r.record(key, std::to_string(c.driverTimeoutMs), "default");
        }
        else if (raw->empty())
        {
            r.problem(key + " is set but empty (unset it, or give the media driver timeout in ms, " +
                std::to_string(rt::DRIVER_TIMEOUT_MIN_MS) + ".." + std::to_string(rt::DRIVER_TIMEOUT_MAX_MS) + ")");
        }
        else
        {
            std::string error;
            const auto timeout = rt::parseDriverTimeoutMs(key.c_str(), raw->c_str(), error);
            if (timeout)
            {
                c.driverTimeoutMs = *timeout;
                r.record(key, *raw, "set");
            }
            else
            {
                r.problem(error);
            }
        }
    }

    c.coreIngress = r.requiredString(P + "_CORE_INGRESS", "the core members, id=host:port,...");
    if (!c.coreIngress.empty())
    {
        const cl::ParsedEndpoints endpoints = cl::parseIngressEndpoints(c.coreIngress);
        if (!endpoints.ok())
        {
            r.problem(P + "_CORE_INGRESS: " + endpoints.error);
        }
    }
    c.ingressChannel = r.requiredString(P + "_CORE_INGRESS_CHANNEL",
        "a full aeron:udp URI naming term-length and mtu, e.g. aeron:udp?term-length=4m|mtu=8192");
    if (!c.ingressChannel.empty())
    {
        checkChannel(r, P + "_CORE_INGRESS_CHANNEL", c.ingressChannel, false);
    }
    c.responseChannel = r.requiredString(P + "_CORE_RESPONSE_CHANNEL",
        "a full aeron:udp URI naming endpoint, term-length and mtu, e.g. "
        "aeron:udp?endpoint=10.0.0.5:0|term-length=4m|mtu=8192");
    if (!c.responseChannel.empty())
    {
        checkChannel(r, P + "_CORE_RESPONSE_CHANNEL", c.responseChannel, true);
    }
    {
        std::optional<cl::CoreCredentials> creds;
        credentialSetting(r, "CORE_AUTH_CREDENTIAL", cl::SessionClass::GATEWAY, true, creds);
        if (creds)
        {
            c.credentials = *creds;
        }
    }
    c.instanceId = r.requiredPositive(P + "_INSTANCE_ID");

    const std::string publicKey = Role::GATEWAY == role ? "GATEWAY_HTTP_BIND" : "STREAM_WS_BIND";
    const std::string adminKey = P + "_ADMIN_BIND";
    const std::string publicText = r.requiredString(publicKey, "host:port of the public listener");
    const std::string adminText = r.requiredString(adminKey, "host:port of the admin listener (IPv4)");
    bool bindsOk = true;
    if (!publicText.empty())
    {
        std::string why;
        if (const auto b = parseBind(publicText, false, why))
        {
            c.publicBind = *b;
        }
        else
        {
            r.problem(publicKey + " must be " + why + ", got '" + publicText + "'");
            bindsOk = false;
        }
    }
    if (!adminText.empty())
    {
        std::string why;
        if (const auto b = parseBind(adminText, true, why))
        {
            c.adminBind = *b;
        }
        else
        {
            r.problem(adminKey + " must be " + why + ", got '" + adminText + "'");
            bindsOk = false;
        }
    }
    // The same port is the same listener when the hosts are equal (localhost read as 127.0.0.1) or either side is a
    // wildcard: a wildcard listener takes the port on every address.
    const auto normalHost = [](const std::string &h) { return "localhost" == h ? std::string("127.0.0.1") : h; };
    const auto wildcard = [](const std::string &h) { return "0.0.0.0" == h || "::" == h; };
    if (bindsOk && !publicText.empty() && !adminText.empty() && 0 != c.adminBind.port &&
        c.adminBind.port == c.publicBind.port &&
        (normalHost(c.adminBind.host) == normalHost(c.publicBind.host) || wildcard(c.adminBind.host) ||
            wildcard(c.publicBind.host)))
    {
        r.problem(adminKey + " and " + publicKey + " name the same listener (" + adminText + "): the admin endpoints "
            "never bind to the public listener");
    }

    c.ioThreads = r.positive(P + "_IO_THREADS", c.ioThreads);
    c.idle = 1 == r.choice(P + "_IDLE", { "backoff", "busy" }, 0) ? IdleMode::BUSY : IdleMode::BACKOFF;
    c.commandRing = r.positive(P + "_COMMAND_RING", c.commandRing);
    c.maxConnections = r.positive(P + "_MAX_CONNECTIONS", c.maxConnections);
    c.dutyWatchdogMs = r.positive(P + "_DUTY_WATCHDOG_MS", c.dutyWatchdogMs);
    c.stopTimeoutMs = r.positive(P + "_STOP_TIMEOUT_MS", c.stopTimeoutMs);
    c.statusIntervalMs = r.positive(P + "_STATUS_INTERVAL_MS", c.statusIntervalMs);
    c.reconnectDelayMs = r.positive(P + "_RECONNECT_DELAY_MS", c.reconnectDelayMs);
    c.registerResendMs = r.positive(P + "_REGISTER_RESEND_MS", c.registerResendMs);
    {
        const std::string key = P + "_EMBEDDED_RETRY_MS";
        if (DriverMode::EXTERNAL == c.driverMode && r.isSet(key))
        {
            r.problem(key + " is set but " + P + "_DRIVER_MODE=external starts no driver: remove it");
        }
        else
        {
            c.embeddedRetryMs = r.positive(key, c.embeddedRetryMs);
        }
    }

    // auth modes and the keys each one needs (read here because both binaries enforce them at boot)
    c.allowDevAuth = r.flag01("GATEWAY_ALLOW_DEV_AUTH", false);
    c.apiKeysFile = r.optionalString("GATEWAY_API_KEYS_FILE", "the path of the key:userId file");
    c.jwtSecretFile = r.optionalString("GATEWAY_JWT_SECRET_FILE", "the path of the HS256 secret file");
    c.pgUrl = r.optionalString("GATEWAY_PG_URL", "postgresql://...", true);
    if (c.pgUrl && 0 != c.pgUrl->rfind("postgresql://", 0) && 0 != c.pgUrl->rfind("postgres://", 0))
    {
        r.problem("GATEWAY_PG_URL must be a postgresql:// or postgres:// URL");
    }
    c.tokenCacheTtlMs = r.positive("GATEWAY_TOKEN_CACHE_TTL_MS", c.tokenCacheTtlMs);
    c.tokenStaleMs = r.positive("GATEWAY_TOKEN_STALE_MS", c.tokenStaleMs);
    // A second spelling of the stale bound must never be ignored (oe-stream would otherwise skip it silently, since
    // it does not refuse unknown GATEWAY_ names): one name, refused by name in both binaries.
    if (r.isSet("GATEWAY_TOKEN_CACHE_STALE_MS"))
    {
        r.problem("GATEWAY_TOKEN_CACHE_STALE_MS is not read: use GATEWAY_TOKEN_STALE_MS (the stale bound of the token "
            "cache)");
    }
    const std::string modesKey = P + "_AUTH_MODES";
    const std::string modes = r.requiredString(modesKey, "a list of demo, api-key, jwt, dev (comma-separated)");
    if (!modes.empty())
    {
        const auto items = splitList(modes);
        if (!items)
        {
            r.problem(modesKey + " must be a comma-separated list with no empty item, got '" + modes + "'");
        }
        else
        {
            for (const std::string &m : *items)
            {
                const unsigned bit = "demo" == m ? auth_mode::DEMO : "api-key" == m ? auth_mode::API_KEY
                    : "jwt" == m ? auth_mode::JWT : "dev" == m ? auth_mode::DEV : 0u;
                if (0 == bit)
                {
                    r.problem(modesKey + ": unknown mode '" + m + "' (demo, api-key, jwt, dev)");
                }
                else if (0 != (c.authModes & bit))
                {
                    r.problem(modesKey + ": mode '" + m + "' is listed twice");
                }
                c.authModes |= bit;
            }
            if (0 != (c.authModes & auth_mode::DEV) && !c.allowDevAuth)
            {
                r.problem(modesKey + " lists dev, which needs GATEWAY_ALLOW_DEV_AUTH=1 (never in production)");
            }
            if (0 != (c.authModes & auth_mode::API_KEY) && !c.apiKeysFile && !r.isSet("GATEWAY_API_KEYS_FILE"))
            {
                r.problem(modesKey + " lists api-key, which needs GATEWAY_API_KEYS_FILE");
            }
            if (0 != (c.authModes & auth_mode::JWT) && !c.jwtSecretFile && !r.isSet("GATEWAY_JWT_SECRET_FILE"))
            {
                r.problem(modesKey + " lists jwt, which needs GATEWAY_JWT_SECRET_FILE");
            }
            if (0 != (c.authModes & auth_mode::DEMO) && !c.pgUrl && !r.isSet("GATEWAY_PG_URL"))
            {
                r.problem(modesKey + " lists demo, which needs GATEWAY_PG_URL");
            }
        }
    }
}

void checkProxies(SettingsReader &r, const std::optional<std::string> &value, std::vector<std::string> &out)
{
    if (!value)
    {
        return;
    }
    const auto items = splitList(*value);
    if (!items)
    {
        r.problem("GATEWAY_TRUSTED_PROXIES must be a comma-separated list with no empty item");
        return;
    }
    for (const std::string &item : *items)
    {
        const std::size_t slash = item.find('/');
        const std::string addr = item.substr(0, slash);
        const bool v4 = isIpv4(addr);
        const bool v6 = !v4 && isIpv6(addr);
        bool ok = v4 || v6;
        if (ok && std::string::npos != slash)
        {
            const std::string bits = item.substr(slash + 1);
            int n = -1;
            const auto res = std::from_chars(bits.data(), bits.data() + bits.size(), n);
            ok = res.ec == std::errc() && res.ptr == bits.data() + bits.size() && !bits.empty() && '+' != bits.front() &&
                n >= 0 && n <= (v4 ? 32 : 128);
        }
        if (!ok)
        {
            r.problem("GATEWAY_TRUSTED_PROXIES: '" + item + "' is not an IP address or CIDR block");
        }
        else
        {
            out.push_back(item);
        }
    }
}

bool isHeaderToken(const std::string &s)
{
    if (s.empty())
    {
        return false;
    }
    for (const char ch : s)
    {
        const bool ok = (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || '-' == ch;
        if (!ok)
        {
            return false;
        }
    }
    return true;
}

bool isOrigin(const std::string &s)
{
    std::string rest;
    if (0 == s.rfind("https://", 0))
    {
        rest = s.substr(8);
    }
    else if (0 == s.rfind("http://", 0))
    {
        rest = s.substr(7);
    }
    else
    {
        return false;
    }
    if (rest.empty())
    {
        return false;
    }
    for (const char ch : rest)
    {
        if ('/' == ch || '*' == ch || ' ' == ch || '?' == ch || '#' == ch)
        {
            return false;
        }
    }
    return true;
}

bool isDecimalAmount(const std::string &s)
{
    const std::size_t dot = s.find('.');
    const std::string whole = s.substr(0, dot);
    if (whole.empty())
    {
        return false;
    }
    for (const char ch : whole)
    {
        if (ch < '0' || ch > '9')
        {
            return false;
        }
    }
    bool nonZero = std::string::npos != whole.find_first_not_of('0');
    if (std::string::npos != dot)
    {
        const std::string frac = s.substr(dot + 1);
        if (frac.empty() || frac.size() > 8)
        {
            return false;
        }
        for (const char ch : frac)
        {
            if (ch < '0' || ch > '9')
            {
                return false;
            }
        }
        nonZero = nonZero || std::string::npos != frac.find_first_not_of('0');
    }
    return nonZero;
}

}

const char *driverModeName(DriverMode mode) noexcept
{
    switch (mode)
    {
        case DriverMode::EMBEDDED: return "embedded";
        case DriverMode::EXTERNAL: return "external";
        case DriverMode::UNSET: break;
    }
    return "unset";
}

std::optional<std::vector<std::pair<std::string, std::string>>> channelParams(const std::string &uri, std::string &error)
{
    if (0 != uri.rfind("aeron:udp", 0))
    {
        error = "must be an aeron:udp URI, got '" + uri + "'";
        return std::nullopt;
    }
    std::vector<std::pair<std::string, std::string>> out;
    if (uri.size() == 9)
    {
        return out;
    }
    if ('?' != uri[9] || uri.size() == 10)
    {
        error = "must be aeron:udp?key=value|key=value, got '" + uri + "'";
        return std::nullopt;
    }
    const auto items = [&]() {
        std::vector<std::string> v;
        std::size_t start = 10;
        for (;;)
        {
            const std::size_t bar = uri.find('|', start);
            v.push_back(uri.substr(start, std::string::npos == bar ? std::string::npos : bar - start));
            if (std::string::npos == bar)
            {
                return v;
            }
            start = bar + 1;
        }
    }();
    for (const std::string &item : items)
    {
        const std::size_t eq = item.find('=');
        if (std::string::npos == eq || 0 == eq || eq + 1 == item.size())
        {
            error = "parameter '" + item + "' is not key=value in '" + uri + "'";
            return std::nullopt;
        }
        out.emplace_back(item.substr(0, eq), item.substr(eq + 1));
    }
    return out;
}

cl::ClientSettings sessionClientSettings(const CommonSettings &settings, const char *processName)
{
    cl::ClientSettings s;
    s.ingressEndpoints = settings.coreIngress;
    s.ingressChannel = settings.ingressChannel;
    s.responseChannel = settings.responseChannel;
    s.credentials = settings.credentials.encode();
    s.clientName = std::string(processName) + " identity=" + std::to_string(settings.instanceId);
    return s;
}

ParseResult<GatewaySettings> parseGatewaySettings(const Environment &env, const rt::DirProbe &probe)
{
    ParseResult<GatewaySettings> out;
    SettingsReader r(env);
    GatewaySettings &g = out.settings;
    parseCommon(r, Role::GATEWAY, probe, g.common);

    credentialSetting(r, "GATEWAY_TREASURY_CREDENTIAL", cl::SessionClass::TREASURY, false, g.treasuryCredentials);
    g.allowTreasuryLane = r.flag01("GATEWAY_ALLOW_TREASURY_LANE", false);
    const bool credentialGiven = r.isSet("GATEWAY_TREASURY_CREDENTIAL");
    if (credentialGiven && !g.allowTreasuryLane)
    {
        r.problem("GATEWAY_TREASURY_CREDENTIAL is set without GATEWAY_ALLOW_TREASURY_LANE=1: a TREASURY key can credit "
            "and debit any user; the lane is on only with both");
    }
    if (g.allowTreasuryLane && !credentialGiven)
    {
        r.problem("GATEWAY_ALLOW_TREASURY_LANE=1 needs GATEWAY_TREASURY_CREDENTIAL (keyId:TREASURY:secretHex)");
    }
    g.maxInflight = r.positive("GATEWAY_MAX_INFLIGHT", g.maxInflight);
    g.requestTimeoutMs = r.positive("GATEWAY_REQUEST_TIMEOUT_MS", g.requestTimeoutMs);
    g.maxBodyBytes = r.positive("GATEWAY_MAX_BODY_BYTES", g.maxBodyBytes);
    g.principalRate = r.rate("GATEWAY_PRINCIPAL_RATE", "50/s");
    g.principalBurst = r.positive("GATEWAY_PRINCIPAL_BURST", g.principalBurst);
    g.queryRate = r.rate("GATEWAY_QUERY_RATE", "100/s");
    g.queryBurst = r.positive("GATEWAY_QUERY_BURST", g.queryBurst);
    g.queryMinIntervalMs = r.positive("GATEWAY_QUERY_MIN_INTERVAL_MS", g.queryMinIntervalMs);
    g.bulkCancelMax = r.positive("GATEWAY_BULK_CANCEL_MAX", g.bulkCancelMax);
    g.readyStaleMs = r.positive("GATEWAY_READY_STALE_MS", g.readyStaleMs);
    g.readyTimeoutStreak = r.positive("GATEWAY_READY_TIMEOUT_STREAK", g.readyTimeoutStreak);
    g.hashThreads = r.positive("GATEWAY_HASH_THREADS", g.hashThreads);
    g.hashQueue = r.positive("GATEWAY_HASH_QUEUE", g.hashQueue);
    g.registerRate = r.rate("GATEWAY_REGISTER_RATE", "1/s");
    g.registerBurst = r.positive("GATEWAY_REGISTER_BURST", g.registerBurst);
    g.loginRate = r.rate("GATEWAY_LOGIN_RATE", "10/s");
    g.loginBurst = r.positive("GATEWAY_LOGIN_BURST", g.loginBurst);
    g.authSourceRate = r.rate("GATEWAY_AUTH_SOURCE_RATE", "10/min");
    g.authSourceBurst = r.positive("GATEWAY_AUTH_SOURCE_BURST", g.authSourceBurst);
    g.sourceV6PrefixBits = r.positive("GATEWAY_SOURCE_IPV6_PREFIX", g.sourceV6PrefixBits);
    if (g.sourceV6PrefixBits > 128)
    {
        r.problem("GATEWAY_SOURCE_IPV6_PREFIX is a prefix length of an IPv6 address (1 to 128), got " +
            std::to_string(g.sourceV6PrefixBits));
    }
    g.pbkdf2Iterations = r.positive("GATEWAY_PBKDF2_ITERATIONS", g.pbkdf2Iterations);
    if (g.pbkdf2Iterations > 0x7fffffffLL)
    {
        r.problem("GATEWAY_PBKDF2_ITERATIONS must fit PBKDF2's 31-bit round count (at most 2147483647), got " +
            std::to_string(g.pbkdf2Iterations));
    }
    g.maxHeaderBytes = r.positive("GATEWAY_MAX_HEADER_BYTES", g.maxHeaderBytes);
    if (g.maxHeaderBytes > 0xffffffffLL)
    {
        r.problem("GATEWAY_MAX_HEADER_BYTES must fit the parser's 32-bit header bound (at most 4294967295), got " +
            std::to_string(g.maxHeaderBytes));
    }
    g.httpIdleTimeoutMs = r.positive("GATEWAY_HTTP_IDLE_TIMEOUT_MS", g.httpIdleTimeoutMs);
    checkProxies(r, r.optionalString("GATEWAY_TRUSTED_PROXIES", "a list of IP addresses or CIDR blocks"),
        g.trustedProxies);
    {
        const std::optional<std::string> header = r.raw("GATEWAY_CLIENT_IP_HEADER");
        if (!header)
        {
            r.record("GATEWAY_CLIENT_IP_HEADER", g.clientIpHeader, "default");
        }
        else if (!isHeaderToken(*header))
        {
            r.problem("GATEWAY_CLIENT_IP_HEADER must be an HTTP header name (letters, digits, '-'), got '" + *header + "'");
        }
        else
        {
            g.clientIpHeader = *header;
            r.record("GATEWAY_CLIENT_IP_HEADER", *header, "set");
        }
    }
    if (const auto cors = r.optionalString("GATEWAY_CORS_ORIGINS", "a list of http(s)://host[:port] origins"))
    {
        const auto items = splitList(*cors);
        if (!items)
        {
            r.problem("GATEWAY_CORS_ORIGINS must be a comma-separated list with no empty item");
        }
        else
        {
            for (const std::string &o : *items)
            {
                if ("*" == o)
                {
                    r.problem("GATEWAY_CORS_ORIGINS: '*' is refused: list each origin explicitly");
                }
                else if (!isOrigin(o))
                {
                    r.problem("GATEWAY_CORS_ORIGINS: '" + o + "' is not an origin (http(s)://host[:port], no path)");
                }
                else
                {
                    g.corsOrigins.push_back(o);
                }
            }
        }
    }
    g.pgHistoryPool = r.positive("GATEWAY_PG_HISTORY_POOL", g.pgHistoryPool);
    g.pgAuthPool = r.positive("GATEWAY_PG_AUTH_POOL", g.pgAuthPool);
    g.pgStatementTimeoutMs = r.positive("GATEWAY_PG_STATEMENT_TIMEOUT_MS", g.pgStatementTimeoutMs);
    g.pgQueue = r.positive("GATEWAY_PG_QUEUE", g.pgQueue);
    g.pgRetryMs = r.positive("GATEWAY_PG_RETRY_MS", g.pgRetryMs);
    g.tokenLookupRate = r.rate("GATEWAY_TOKEN_LOOKUP_RATE", "60/min");
    g.tokenLookupBurst = r.positive("GATEWAY_TOKEN_LOOKUP_BURST", g.tokenLookupBurst);
    {
        const std::string key = "GATEWAY_TOKEN_LOOKUP_INFLIGHT";
        const std::int64_t derived = std::max<std::int64_t>(1, g.pgQueue / 2);
        if (r.isSet(key))
        {
            g.tokenLookupInflight = r.positive(key, derived);
        }
        else
        {
            g.tokenLookupInflight = derived;
            r.record(key, std::to_string(derived), "default");
        }
        if (0 != (g.common.authModes & auth_mode::DEMO) && g.tokenLookupInflight >= g.pgQueue)
        {
            r.problem(key + " (" + std::to_string(g.tokenLookupInflight) + ") must be below GATEWAY_PG_QUEUE (" +
                std::to_string(g.pgQueue) + "): the rest of the queue is kept for login, register and logout");
        }
    }
    g.historyLimitDefault = r.positive("GATEWAY_HISTORY_LIMIT_DEFAULT", g.historyLimitDefault);
    g.historyLimitMax = r.positive("GATEWAY_HISTORY_LIMIT_MAX", g.historyLimitMax);
    g.historyRetryAfterMs = r.positive("GATEWAY_HISTORY_RETRY_AFTER_MS", g.historyRetryAfterMs);
    g.historyBusyRetryAfterMs = r.positive("GATEWAY_HISTORY_BUSY_RETRY_AFTER_MS", g.historyBusyRetryAfterMs);
    g.historyLagRetryAfterMs = r.positive("GATEWAY_HISTORY_LAG_RETRY_AFTER_MS", g.historyLagRetryAfterMs);
    if (g.historyLimitDefault > g.historyLimitMax)
    {
        r.problem("GATEWAY_HISTORY_LIMIT_DEFAULT (" + std::to_string(g.historyLimitDefault) +
            ") must not exceed GATEWAY_HISTORY_LIMIT_MAX (" + std::to_string(g.historyLimitMax) + ")");
    }
    if (const auto caps = r.optionalString("GATEWAY_FAUCET_MAX_AMOUNT", "a list of assetId:amount"))
    {
        const auto items = splitList(*caps);
        std::set<std::int64_t> seen;
        if (!items)
        {
            r.problem("GATEWAY_FAUCET_MAX_AMOUNT must be a comma-separated list with no empty item");
        }
        else
        {
            for (const std::string &item : *items)
            {
                const std::size_t colon = item.find(':');
                FaucetCap cap;
                // asset 0 is a real asset id (the core accepts it), so the id is 0 or a positive decimal
                const std::string assetText = std::string::npos == colon ? std::string() : item.substr(0, colon);
                const bool assetOk = "0" == assetText || parsePositiveDecimal(assetText, cap.assetId);
                if (std::string::npos == colon || !assetOk || cap.assetId > 2147483647 ||
                    !isDecimalAmount(item.substr(colon + 1)))
                {
                    r.problem("GATEWAY_FAUCET_MAX_AMOUNT: '" + item + "' is not assetId:amount (an asset id from 0 to "
                        "2147483647 and a positive decimal amount, at most 8 fraction digits)");
                    continue;
                }
                cap.amount = item.substr(colon + 1);
                // the REST layer reads the cap with the money parser; an amount it refuses (above INT64_MAX in units)
                // is refused here, so every cap that passes this reader is one the faucet route enforces
                if (!rest::parseMoney(cap.amount))
                {
                    r.problem("GATEWAY_FAUCET_MAX_AMOUNT: asset " + std::to_string(cap.assetId) + " amount '" +
                        cap.amount + "' is out of range (at most 92233720368.54775807)");
                    continue;
                }
                if (!seen.insert(cap.assetId).second)
                {
                    r.problem("GATEWAY_FAUCET_MAX_AMOUNT: asset " + std::to_string(cap.assetId) + " is listed twice");
                    continue;
                }
                g.faucetMaxAmount.push_back(cap);
            }
        }
    }
    else if (g.allowTreasuryLane)
    {
        r.problem("GATEWAY_ALLOW_TREASURY_LANE=1 needs GATEWAY_FAUCET_MAX_AMOUNT (the lane's caps live in this process)");
    }
    g.faucetRate = r.rate("GATEWAY_FAUCET_RATE", "1/s");

    r.refuseUnknown({ "GATEWAY_" });
    out.errors = r.errors();
    out.records = r.records();
    return out;
}

std::optional<std::vector<NodeEndpoint>> parseNodeEndpoints(const std::string &text, std::string &error)
{
    const std::optional<std::vector<std::string>> items = splitList(text);
    if (!items)
    {
        error = "expected <memberId>@<host>:<port>[,...], got an empty item in '" + text + "'";
        return std::nullopt;
    }
    std::vector<NodeEndpoint> out;
    for (const std::string &item : *items)
    {
        const std::size_t at = item.find('@');
        const std::size_t colon = item.rfind(':');
        NodeEndpoint n;
        std::int64_t port = 0;
        const bool idZero = at == 1 && '0' == item[0];
        if (at == std::string::npos || colon == std::string::npos || colon < at + 2 ||
            !(idZero || parsePositiveDecimal(item.substr(0, at), n.memberId)) ||
            !parsePositiveDecimal(item.substr(colon + 1), port) || port > 65535)
        {
            error = "expected <memberId>@<host>:<port>[,...] (memberId >= 0, port 1..65535), got '" + item + "'";
            return std::nullopt;
        }
        n.host = item.substr(at + 1, colon - at - 1);
        // an IPv6 literal is written in brackets ([::1]:9090); the host is the address without them. A bare colon in
        // the host could be part of the address or the port separator, so it is refused, as is a stray bracket.
        if ('[' == n.host.front())
        {
            if (n.host.size() < 3 || ']' != n.host.back())
            {
                error = "expected [<IPv6 address>] in brackets, got '" + item + "'";
                return std::nullopt;
            }
            n.host = n.host.substr(1, n.host.size() - 2);
        }
        if (n.host.find_first_of("[]") != std::string::npos ||
            ('[' != item[at + 1] && n.host.find(':') != std::string::npos))
        {
            error = "an IPv6 host is written in brackets (<memberId>@[<address>]:<port>), got '" + item + "'";
            return std::nullopt;
        }
        n.port = static_cast<std::uint16_t>(port);
        for (const NodeEndpoint &o : out)
        {
            if (o.memberId == n.memberId)
            {
                error = "member id " + std::to_string(n.memberId) + " named twice (one row per node)";
                return std::nullopt;
            }
        }
        out.push_back(std::move(n));
    }
    return out;
}

ParseResult<StreamSettings> parseStreamSettings(const Environment &env, const rt::DirProbe &probe)
{
    ParseResult<StreamSettings> out;
    SettingsReader r(env);
    StreamSettings &s = out.settings;
    parseCommon(r, Role::STREAM, probe, s.common);
    s.balanceStream = 0 == r.choice("STREAM_BALANCE_STREAM", { "on", "off" }, 0);
    s.eventWindow = r.positive("STREAM_EVENT_WINDOW", s.eventWindow);
    s.replayPage = r.positive("STREAM_REPLAY_PAGE", s.replayPage);
    s.gapBufferEvents = r.positive("STREAM_GAP_BUFFER_EVENTS", s.gapBufferEvents);
    s.queryRate = r.rate("STREAM_QUERY_RATE", "50/s");
    s.queryBurst = r.positive("STREAM_QUERY_BURST", s.queryBurst);
    s.userLingerMs = r.positive("STREAM_USER_LINGER_MS", s.userLingerMs);
    s.headProbeMs = r.positive("STREAM_HEAD_PROBE_MS", s.headProbeMs);
    s.readyStaleMs = r.positive("STREAM_READY_STALE_MS", s.readyStaleMs);
    s.heartbeatMs = r.positive("STREAM_HEARTBEAT_MS", s.heartbeatMs);
    s.wsQueueMessages = r.positive("STREAM_WS_QUEUE_MESSAGES", s.wsQueueMessages);
    s.wsQueueBytes = r.positive("STREAM_WS_QUEUE_BYTES", s.wsQueueBytes);
    s.wsAuthTimeoutMs = r.positive("STREAM_WS_AUTH_TIMEOUT_MS", s.wsAuthTimeoutMs);
    s.wsPingMs = r.positive("STREAM_WS_PING_MS", s.wsPingMs);
    s.wsIdleMs = r.positive("STREAM_WS_IDLE_MS", s.wsIdleMs);
    s.pgAuthPool = r.positive("STREAM_PG_AUTH_POOL", s.pgAuthPool);
    s.requestTimeoutMs = r.positive("STREAM_REQUEST_TIMEOUT_MS", s.requestTimeoutMs);
    s.wsMaxMessageBytes = r.positive("STREAM_WS_MAX_MESSAGE_BYTES", s.wsMaxMessageBytes);
    s.pgQueue = r.positive("STREAM_PG_QUEUE", s.pgQueue);
    s.pgStatementTimeoutMs = r.positive("STREAM_PG_STATEMENT_TIMEOUT_MS", s.pgStatementTimeoutMs);
    s.pgRetryMs = r.positive("STREAM_PG_RETRY_MS", s.pgRetryMs);
    if (s.readyStaleMs <= s.headProbeMs)
    {
        // /ready counts the head probe stale after STREAM_READY_STALE_MS; at or below the probe interval a healthy
        // process would flap not ready between two probes
        r.problem("STREAM_READY_STALE_MS (" + std::to_string(s.readyStaleMs) + ") must be above "
            "STREAM_HEAD_PROBE_MS (" + std::to_string(s.headProbeMs) + ")");
    }
    if (s.wsPingMs >= s.wsIdleMs)
    {
        // a quiet client answers the ping with a pong; at or above the idle bound it is closed before it is asked
        r.problem("STREAM_WS_PING_MS (" + std::to_string(s.wsPingMs) + ") must be below STREAM_WS_IDLE_MS (" +
            std::to_string(s.wsIdleMs) + ")");
    }
    if (r.isSet("STREAM_WS_SEND_BUFFER_BYTES"))
    {
        s.wsSendBufferBytes = r.positive("STREAM_WS_SEND_BUFFER_BYTES", 0);
    }
    else
    {
        r.record("STREAM_WS_SEND_BUFFER_BYTES", "os", "default");
    }

    for (const auto &[key, target] : { std::pair{ "STREAM_CORE_NODE_ENDPOINTS", &s.coreNodeEndpoints },
             std::pair{ "STREAM_ME_NODE_ENDPOINTS", &s.meNodeEndpoints } })
    {
        const std::optional<std::string> text = r.optionalString(key, "<memberId>@<host>:<port>[,...]");
        if (!text)
        {
            continue; // unset: no node of that engine is polled (recorded "(unset)")
        }
        std::string error;
        const std::optional<std::vector<NodeEndpoint>> nodes = parseNodeEndpoints(*text, error);
        if (!nodes)
        {
            r.problem(std::string(key) + ": " + error);
            continue;
        }
        *target = *nodes;
    }
    s.enginePollMs = r.positive("STREAM_ENGINE_POLL_MS", s.enginePollMs);
    s.enginePollTimeoutMs = r.positive("STREAM_ENGINE_POLL_TIMEOUT_MS", s.enginePollTimeoutMs);
    s.engineFrameMinMs = r.positive("STREAM_ENGINE_FRAME_MIN_MS", s.engineFrameMinMs);
    s.engineMaxBodyBytes = r.positive("STREAM_ENGINE_MAX_BODY_BYTES", s.engineMaxBodyBytes);
    if (s.enginePollTimeoutMs >= s.enginePollMs)
    {
        // a round (name lookups included) ends by the timeout; at or above the interval rounds run back to back and
        // the stated request rate no longer holds
        r.problem("STREAM_ENGINE_POLL_TIMEOUT_MS (" + std::to_string(s.enginePollTimeoutMs) +
            ") must be below STREAM_ENGINE_POLL_MS (" + std::to_string(s.enginePollMs) + ")");
    }

    r.refuseUnknown({ "STREAM_" });
    out.errors = r.errors();
    out.records = r.records();
    return out;
}

}}
