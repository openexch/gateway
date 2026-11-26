#include "MarketDataSettings.h"

#include <algorithm>
#include <charconv>
#include <cctype>

#include "cluster_tools/archive/FramePeek.h"
#include "cluster_tools/client/IngressEndpoints.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace ar = cluster_tools::archive;
namespace cl = cluster_tools::client;
namespace rt = cluster_tools::runtime;

namespace {

const char *const MISSING = "missing setting: ";
const char *const INVALID = "invalid setting: ";

// The client-facing port offset of the cluster-kit port rule (ClusterPorts.CLIENT_FACING_OFFSET): member i of a
// deployment listens for cluster clients on portBase + i * stride + 2.
constexpr std::int64_t CLIENT_FACING_OFFSET = 2;

bool wildcardHost(const std::string &host)
{
    return "0.0.0.0" == host || "::" == host || "[::]" == host;
}

// A non-negative decimal (no sign, no unit).
bool parseNonNegative(const std::string &text, std::int64_t &out)
{
    if (text.empty() || !std::isdigit(static_cast<unsigned char>(text.front())))
    {
        return false;
    }
    std::int64_t v = 0;
    const auto r = std::from_chars(text.data(), text.data() + text.size(), v);
    if (r.ec != std::errc() || r.ptr != text.data() + text.size() || v < 0)
    {
        return false;
    }
    out = v;
    return true;
}

// Every problem the shared readers wrote begins with the key it is about: say whether it is missing or invalid, so
// every line of a refusal reads "missing setting: <KEY> ..." or "invalid setting: <KEY> ...".
std::string classify(const std::string &problem)
{
    if (0 == problem.rfind(MISSING, 0) || 0 == problem.rfind(INVALID, 0))
    {
        return problem;
    }
    std::size_t end = 0;
    while (end < problem.size() &&
        (std::isupper(static_cast<unsigned char>(problem[end])) || std::isdigit(static_cast<unsigned char>(problem[end])) ||
            '_' == problem[end]))
    {
        end++;
    }
    if (0 == end)
    {
        return std::string(INVALID) + problem;
    }
    const std::string key = problem.substr(0, end);
    std::string rest = problem.substr(end);
    if (!rest.empty() && ':' == rest.front())
    {
        rest.erase(0, 1);
    }
    while (!rest.empty() && ' ' == rest.front())
    {
        rest.erase(0, 1);
    }
    if (0 == rest.rfind("is required", 0) || 0 == rest.rfind("is empty", 0))
    {
        rest.erase(0, 0 == rest.rfind("is required", 0) ? 11 : 8);
        while (!rest.empty() && ' ' == rest.front())
        {
            rest.erase(0, 1);
        }
        return std::string(MISSING) + key + (rest.empty() ? "" : " " + rest);
    }
    return std::string(INVALID) + key + (rest.empty() ? "" : ": " + rest);
}

void requiredBind(SettingsReader &r, const std::string &key, bool ipv4Only, Bind &out, bool &ok)
{
    const std::optional<std::string> text = r.raw(key);
    if (!text || text->empty())
    {
        r.problem(MISSING + key + " (host:port" + (ipv4Only ? ", IPv4" : "") + "; there is no default port)");
        ok = false;
        return;
    }
    std::string why;
    const auto bind = parseBindText(*text, ipv4Only, why);
    if (!bind)
    {
        r.problem(INVALID + key + ": must be " + why + ", got '" + *text + "'");
        ok = false;
        return;
    }
    if (0 == bind->port)
    {
        r.problem(INVALID + key + ": port 0 is refused (a listener this service names must have a known port), got '" +
            *text + "'");
        ok = false;
        return;
    }
    out = *bind;
    r.record(key, *text, "set");
}

std::int64_t positive(SettingsReader &r, const std::string &key, std::int64_t defaultValue)
{
    return r.positive(key, defaultValue);
}

}

ParseResult<MarketDataSettings> parseMarketDataSettings(const Environment &env, const rt::DirProbe &probe)
{
    ParseResult<MarketDataSettings> out;
    SettingsReader r(env);
    MarketDataSettings &s = out.settings;
    CommonSettings &c = s.common;

    // the media driver (the gateway repository's rule for every <P>_DRIVER_TIMEOUT_MS: 3000..60000)
    parseDriverSettings(r, "MD", probe, c);
    c.instanceId = r.requiredPositive("MD_INSTANCE_ID");

    // the ME session
    {
        const std::string key = "MD_ME_CLUSTER_ADDRESSES";
        const std::optional<std::string> text = r.raw(key);
        if (!text || text->empty())
        {
            r.problem(MISSING + key + " (the ME members' hosts in member id order, h0,h1,...)");
        }
        else if (const auto items = splitList(*text); !items)
        {
            r.problem(INVALID + key + ": an entry is empty, got '" + *text + "'");
        }
        else
        {
            bool ok = true;
            for (const std::string &host : *items)
            {
                if (wildcardHost(host) || std::string::npos != host.find(':') || std::string::npos != host.find('='))
                {
                    r.problem(INVALID + key + ": '" + host + "' must be a host the ME is reached at (no port, no member "
                        "id, never a wildcard address)");
                    ok = false;
                }
            }
            if (ok)
            {
                s.meAddresses = *items;
                r.record(key, *text, "set");
            }
        }
    }
    {
        const std::string key = "MD_ME_PORT_BASE";
        const std::optional<std::string> text = r.raw(key);
        std::int64_t v = 0;
        if (!text || text->empty())
        {
            r.problem(MISSING + key + " (the ME deployment's port base)");
        }
        else if (!parsePositiveDecimal(*text, v) || v > 65535)
        {
            r.problem(INVALID + key + ": must be a port base in 1..65535, got '" + *text + "'");
        }
        else
        {
            s.mePortBase = v;
            r.record(key, *text, "set");
        }
    }
    {
        const std::string key = "CLUSTER_PORT_STRIDE";
        const std::optional<std::string> text = r.raw(key);
        std::int64_t v = 0;
        if (!text || text->empty())
        {
            r.problem(MISSING + key + " (the deployment's member port stride: 0 = one member per address, 100 = members "
                "sharing a host)");
        }
        else if (!parseNonNegative(*text, v) || v > 65535)
        {
            r.problem(INVALID + key + ": must be a non-negative integer, got '" + *text + "'");
        }
        else
        {
            s.portStride = v;
            r.record(key, *text, "set");
        }
    }
    if (!s.meAddresses.empty() && s.mePortBase > 0)
    {
        std::string ingress;
        for (std::size_t i = 0; i < s.meAddresses.size(); ++i)
        {
            const std::int64_t port = s.mePortBase + static_cast<std::int64_t>(i) * s.portStride + CLIENT_FACING_OFFSET;
            if (port > 65535)
            {
                r.problem(INVALID + std::string("MD_ME_PORT_BASE: member ") + std::to_string(i) + "'s ingress port " +
                    std::to_string(port) + " (MD_ME_PORT_BASE + " + std::to_string(i) + " x CLUSTER_PORT_STRIDE + " +
                    std::to_string(CLIENT_FACING_OFFSET) + ") is above 65535");
                ingress.clear();
                break;
            }
            ingress += (0 == i ? "" : ",") + std::to_string(i) + "=" + s.meAddresses[i] + ":" + std::to_string(port);
        }
        if (!ingress.empty())
        {
            const cl::ParsedEndpoints parsed = cl::parseIngressEndpoints(ingress);
            if (!parsed.ok())
            {
                r.problem(INVALID + std::string("MD_ME_CLUSTER_ADDRESSES: ") + parsed.error);
            }
            else
            {
                s.meIngress = ingress;
            }
        }
    }
    {
        const std::string key = "MD_ME_EGRESS_ENDPOINT";
        const std::optional<std::string> text = r.raw(key);
        if (!text || text->empty())
        {
            r.problem(MISSING + key + " (host:port of this process's egress; the ME must route to it)");
        }
        else
        {
            std::string why;
            const auto bind = parseBindText(*text, false, why);
            if (!bind)
            {
                r.problem(INVALID + key + ": must be " + why + ", got '" + *text + "'");
            }
            else if (wildcardHost(bind->host))
            {
                r.problem(INVALID + key + ": never a wildcard address (the ME sends its egress to this address), got '" +
                    *text + "'");
            }
            else
            {
                s.meEgressEndpoint = *text;
                r.record(key, *text, "set");
            }
        }
    }
    {
        const std::string key = "MD_ME_INGRESS_CHANNEL";
        const std::optional<std::string> text = r.raw(key);
        if (!text)
        {
            r.record(key, s.meIngressChannel, "default");
        }
        else
        {
            const std::size_t before = r.errors().size();
            checkSessionChannel(r, key, *text, false);
            if (r.errors().size() == before)
            {
                s.meIngressChannel = *text;
                r.record(key, *text, "set");
            }
        }
    }
    {
        const std::string key = "ME_AUTH_CREDENTIAL";
        const std::optional<std::string> spec = r.raw(key);
        if (!spec || spec->empty())
        {
            r.problem(MISSING + key + " (keyId:GATEWAY:secretHex)");
        }
        else
        {
            const cl::CredentialsResult parsed = cl::parseCoreCredentials(*spec);
            if (!parsed.ok())
            {
                r.problem(INVALID + key + ": " + parsed.error);
            }
            else if (cl::SessionClass::GATEWAY != parsed.credentials.sessionClass)
            {
                r.problem(INVALID + key + ": ME credential class must be GATEWAY, got " +
                    cl::nameOf(parsed.credentials.sessionClass));
            }
            else
            {
                s.meCredentials = parsed.credentials;
                r.record(key, std::to_string(parsed.credentials.keyId) + ":GATEWAY:(redacted)", "set");
            }
        }
    }
    s.meReconnectMs = positive(r, "MD_ME_RECONNECT_MS", s.meReconnectMs);

    // the ME journal (the trade path): the archive reader's own rules under the MD_ names
    {
        ar::ArchiveSettingsKeys keys;
        keys.prefix = "MD_";
        keys.archives = "ME_JOURNAL_ARCHIVES";
        keys.role = "oe-marketdata";
        keys.profile = ar::ME_JOURNAL;
        keys.refuseHaltOnGap = false;
        const ar::ArchiveSettingsResult archive = ar::parseArchiveSettings(
            [&r](const std::string &key) { return r.raw(key); }, ar::SettingsScope::TOOL, keys);
        for (const std::string &e : archive.errors)
        {
            r.problem(e);
        }
        if (archive.settings)
        {
            s.journal = *archive.settings;
            std::string list;
            for (const ar::ArchiveEndpoint &e : s.journal.archives)
            {
                list += (list.empty() ? "" : ",") + e.text();
            }
            r.record("MD_ME_JOURNAL_ARCHIVES", list, "set");
            r.record("MD_JOURNAL_CONTROL_STREAM", std::to_string(s.journal.controlStreamId), "set");
            r.record("MD_JOURNAL_STREAM", std::to_string(s.journal.journalStreamId), "set");
            r.record("MD_JOURNAL_CHANNEL", s.journal.journalChannel, "set");
            r.record("MD_REPLAY_STREAM", std::to_string(s.journal.replayStreamId), "set");
            r.record("MD_LOCAL_HOST", s.journal.localHost, "set");
            r.record("MD_JOURNAL_MTU", std::to_string(s.journal.journalMtu), "set");
        }
    }

    // the listeners
    {
        bool bindsOk = true;
#ifdef MD_NC_DEFAULT_PUBLIC_BIND
        // control build only (never published): the public bind falls back to a default port
        if (!r.isSet("MD_PUBLIC_BIND"))
        {
            std::string why;
            c.publicBind = *parseBindText("0.0.0.0:8081", false, why);
            r.record("MD_PUBLIC_BIND", "0.0.0.0:8081", "default");
        }
        else
#endif
        requiredBind(r, "MD_PUBLIC_BIND", false, c.publicBind, bindsOk);
        requiredBind(r, "MD_ADMIN_BIND", true, c.adminBind, bindsOk);
        const auto normalHost = [](const std::string &h) { return "localhost" == h ? std::string("127.0.0.1") : h; };
        if (bindsOk && 0 != c.adminBind.port && c.adminBind.port == c.publicBind.port &&
            (normalHost(c.adminBind.host) == normalHost(c.publicBind.host) || wildcardHost(c.adminBind.host) ||
                wildcardHost(c.publicBind.host)))
        {
            r.problem(INVALID + std::string("MD_ADMIN_BIND: names the same listener as MD_PUBLIC_BIND (") +
                c.adminBind.text + "): the admin endpoints never bind to the public listener");
        }
    }

    // the market history database (the trade path)
    s.pgUrl = r.requiredString("MD_PG_URL", "postgresql://... of the market history database", true);
    if (!s.pgUrl.empty() && 0 != s.pgUrl.rfind("postgresql://", 0) && 0 != s.pgUrl.rfind("postgres://", 0))
    {
        r.problem(INVALID + std::string("MD_PG_URL: must be a postgresql:// or postgres:// URL"));
    }
    s.pgUser = r.requiredString("MD_PG_USER", "the writer role of the market history database");
    s.pgPasswordFile = r.requiredString("MD_PG_PASSWORD_FILE", "the absolute path of the file holding that role's password");
    if (!s.pgPasswordFile.empty() && '/' != s.pgPasswordFile.front())
    {
        r.problem(INVALID + std::string("MD_PG_PASSWORD_FILE: must be an absolute path, got '") + s.pgPasswordFile + "'");
    }

    // CORS of the REST route
    {
        const std::string key = "MD_CORS_ORIGINS";
        const std::optional<std::string> text = r.raw(key);
        if (!text || text->empty())
        {
            r.problem(MISSING + key + " (a list of http(s) origins, or exactly none)");
        }
        else if ("none" == *text)
        {
            r.record(key, *text, "set");
        }
        else if (const auto items = splitList(*text); !items)
        {
            r.problem(INVALID + key + ": an entry is empty, got '" + *text + "'");
        }
        else
        {
            bool ok = true;
            for (const std::string &origin : *items)
            {
                const bool scheme = 0 == origin.rfind("http://", 0) || 0 == origin.rfind("https://", 0);
                const std::size_t hostAt = origin.find("://") + 3;
                if (!scheme || origin.size() <= hostAt || std::string::npos != origin.find('/', hostAt) ||
                    "none" == origin)
                {
                    r.problem(INVALID + key + ": '" + origin + "' must be an origin, scheme://host[:port] with http or "
                        "https and no path (or the whole value exactly none)");
                    ok = false;
                }
            }
            if (ok)
            {
                s.corsOrigins = *items;
                r.record(key, *text, "set");
            }
        }
    }

    // the markets served
    {
        const std::string key = "MD_EXPECTED_MARKETS";
        const std::optional<std::string> text = r.raw(key);
        if (!text || text->empty())
        {
            r.problem(MISSING + key + " (the market ids served, each " + std::to_string(MARKET_ID_MIN) + ".." +
                std::to_string(MARKET_ID_MAX) + ")");
        }
        else if (const auto items = splitList(*text); !items)
        {
            r.problem(INVALID + key + ": an entry is empty, got '" + *text + "'");
        }
        else
        {
            std::vector<int> ids;
            bool ok = true;
            for (const std::string &item : *items)
            {
                std::int64_t v = 0;
                if (!parsePositiveDecimal(item, v) || v < MARKET_ID_MIN || v > MARKET_ID_MAX)
                {
                    r.problem(INVALID + key + ": '" + item + "' is not a market id in " + std::to_string(MARKET_ID_MIN) +
                        ".." + std::to_string(MARKET_ID_MAX) + " (the book ladders of the library)");
                    ok = false;
                }
                else if (std::find(ids.begin(), ids.end(), static_cast<int>(v)) != ids.end())
                {
                    r.problem(INVALID + key + ": market " + item + " is listed twice");
                    ok = false;
                }
                else
                {
                    ids.push_back(static_cast<int>(v));
                }
            }
            if (ok)
            {
                std::sort(ids.begin(), ids.end());
                s.expectedMarkets = ids;
                r.record(key, *text, "set");
            }
        }
    }

    // bounds and sizes, every one a setting with a default
    s.egressStaleMs = positive(r, "MD_EGRESS_STALE_MS", s.egressStaleMs);
    s.bookStaleMs = positive(r, "MD_BOOK_STALE_MS", s.bookStaleMs);
    s.historyStaleMs = positive(r, "MD_HISTORY_STALE_MS", s.historyStaleMs);
    s.sendStallMs = positive(r, "MD_SEND_STALL_MS", s.sendStallMs);
    s.sourceProbeMs = positive(r, "MD_SOURCE_PROBE_MS", s.sourceProbeMs);
    s.tradeFlushMs = positive(r, "MD_TRADE_FLUSH_MS", s.tradeFlushMs);
    s.tickersIntervalMs = positive(r, "MD_TICKERS_INTERVAL_MS", s.tickersIntervalMs);
    s.pendingTrades = positive(r, "MD_PENDING_TRADES", s.pendingTrades);
    s.commitBatch = positive(r, "MD_COMMIT_BATCH", s.commitBatch);
    s.commitIntervalMs = positive(r, "MD_COMMIT_INTERVAL_MS", s.commitIntervalMs);
    s.recentTrades = positive(r, "MD_RECENT_TRADES", s.recentTrades);
    s.candleHistory = positive(r, "MD_CANDLE_HISTORY", s.candleHistory);
    s.candlesLimitMax = positive(r, "MD_CANDLES_LIMIT_MAX", s.candlesLimitMax);
    s.wsMaxConnections = positive(r, "MD_WS_MAX_CONNECTIONS", s.wsMaxConnections);
    s.wsQueueBytes = positive(r, "MD_WS_QUEUE_BYTES", s.wsQueueBytes);
    s.wsUnwritableCloseMs = positive(r, "MD_WS_UNWRITABLE_CLOSE_MS", s.wsUnwritableCloseMs);
    s.wsIdleMs = positive(r, "MD_WS_IDLE_MS", s.wsIdleMs);
    s.wsMaxFrame = positive(r, "MD_WS_MAX_FRAME", s.wsMaxFrame);
    s.archiveTimeoutMs = positive(r, "MD_ARCHIVE_TIMEOUT_MS", s.archiveTimeoutMs);
    s.sourceBackoffMs = positive(r, "MD_SOURCE_BACKOFF_MS", s.sourceBackoffMs);
    s.gapRescanMs = positive(r, "MD_GAP_RESCAN_MS", s.gapRescanMs);
    s.pgStatementTimeoutMs = positive(r, "MD_PG_STATEMENT_TIMEOUT_MS", s.pgStatementTimeoutMs);
    s.pgRetryMs = positive(r, "MD_PG_RETRY_MS", s.pgRetryMs);
    s.candlesQueue = positive(r, "MD_CANDLES_QUEUE", s.candlesQueue);
    s.connectRate = r.rate("MD_RATE_CONNECT", "10/s");
    s.connectBurst = positive(r, "MD_RATE_CONNECT_BURST", s.connectBurst);
    s.messageRate = r.rate("MD_RATE_MESSAGE", "50/s");
    s.messageBurst = positive(r, "MD_RATE_MESSAGE_BURST", s.messageBurst);
    parseTrustedProxies(r, "MD_TRUSTED_PROXIES", s.trustedProxies);
    parseClientIpHeader(r, "MD_CLIENT_IP_HEADER", s.clientIpHeader);
    s.sourceV6PrefixBits = positive(r, "MD_SOURCE_IPV6_PREFIX", s.sourceV6PrefixBits);
    if (s.sourceV6PrefixBits > 128)
    {
        r.problem(INVALID + std::string("MD_SOURCE_IPV6_PREFIX: a prefix length of an IPv6 address (1 to 128), got ") +
            std::to_string(s.sourceV6PrefixBits));
    }

    // the process shell
    c.ioThreads = positive(r, "MD_IO_THREADS", c.ioThreads);
    c.idle = readIdleMode(r, "MD_IDLE");
    c.commandRing = positive(r, "MD_COMMAND_RING", c.commandRing);
    c.dutyWatchdogMs = positive(r, "MD_DUTY_WATCHDOG_MS", c.dutyWatchdogMs);
    c.stopTimeoutMs = positive(r, "MD_STOP_TIMEOUT_MS", c.stopTimeoutMs);
    c.statusIntervalMs = positive(r, "MD_STATUS_INTERVAL_MS", c.statusIntervalMs);
    c.maxConnections = s.wsMaxConnections;
    {
        const std::string key = "MD_EMBEDDED_RETRY_MS";
        if (DriverMode::EXTERNAL == c.driverMode && r.isSet(key))
        {
            r.problem(INVALID + key + ": is set but MD_DRIVER_MODE=external starts no driver: remove it");
        }
        else
        {
            c.embeddedRetryMs = positive(r, key, c.embeddedRetryMs);
        }
    }

    if (c.dutyWatchdogMs <= s.archiveTimeoutMs)
    {
        r.problem(INVALID + std::string("MD_ARCHIVE_TIMEOUT_MS: ") + std::to_string(s.archiveTimeoutMs) +
            " is not below MD_DUTY_WATCHDOG_MS (" + std::to_string(c.dutyWatchdogMs) + "): an archive control call that "
            "waits out its timeout would trip the duty watchdog");
    }

    r.refuseUnknown({ "MD_" });
    for (const std::string &e : r.errors())
    {
        out.errors.push_back(classify(e));
    }
    out.records = r.records();
    return out;
}

cl::ClientSettings meClientSettings(const MarketDataSettings &s)
{
    cl::ClientSettings c;
    c.ingressEndpoints = s.meIngress;
    c.ingressChannel = s.meIngressChannel;
    c.responseChannel = "aeron:udp?endpoint=" + s.meEgressEndpoint;
    c.credentials = s.meCredentials.encode();
    c.clientName = "oe-marketdata instance=" + std::to_string(s.common.instanceId);
    return c;
}

}}}
