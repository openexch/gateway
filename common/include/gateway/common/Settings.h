#ifndef GATEWAY_COMMON_SETTINGS_H
#define GATEWAY_COMMON_SETTINGS_H

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cluster_tools/client/ClientSettings.h"
#include "cluster_tools/client/Credentials.h"
#include "cluster_tools/runtime/DirProbe.h"

#include "gateway/common/Environment.h"
#include "gateway/common/SettingsReader.h"

// The settings of oe-gateway and oe-stream: one parser per binary over one shared part, read from the environment
// with the rules of SettingsReader. Every problem is found in one pass; the binary prints them all and exits 78
// before anything starts (no bind, no driver, no Aeron object).
//
// Shared part, <P> = GATEWAY or STREAM:
//   <P>_DRIVER_MODE        required: embedded (dev and demo: the C media driver in this process) or external (prod: the
//                          host aeronmd). No default.
//   AERON_DIR              external only, required there and refused with embedded: an absolute path that is a
//                          directory or does not exist yet (no driver yet: the connect waits the driver timeout, 70).
//   <P>_DRIVER_DIR         embedded only, required there and refused with external: an absolute, existing, writable
//                          directory, the parent of <dir>/oe-gateway-<id> (or oe-stream-<id>) and its .lock file.
//   <P>_DRIVER_TIMEOUT_MS  10000: the connect wait, the CnC liveness check and the client-conductor heartbeat bound;
//                          the cluster-tools rule applies (3000..60000: the floor spans two driver heartbeats).
//   <P>_CORE_INGRESS       required: the core members, id=host:port,...
//   <P>_CORE_INGRESS_CHANNEL   required: a full aeron:udp URI naming term-length and mtu.
//   <P>_CORE_RESPONSE_CHANNEL  required: a full aeron:udp URI naming endpoint, term-length and mtu.
//                          In both, mtu is a decimal multiple of 32 in 32..65504 and term-length a power of two in
//                          64k..1g (optional k, m or g suffix, either case).
//   CORE_AUTH_CREDENTIAL   required: keyId:GATEWAY:secretHex (a GATEWAY key; another class is refused).
//   <P>_INSTANCE_ID        required: a positive int64; the registration identity and the embedded dir suffix.
//   GATEWAY_HTTP_BIND / STREAM_WS_BIND, <P>_ADMIN_BIND   required host:port; the admin bind is an IPv4 address and
//                          never the public bind.
//   <P>_IO_THREADS 2, <P>_IDLE backoff|busy, <P>_COMMAND_RING 4096, <P>_MAX_CONNECTIONS 2000,
//   <P>_DUTY_WATCHDOG_MS 5000 (duty heartbeat bound: /health 503 and exit 70), <P>_STOP_TIMEOUT_MS 2000 (every join
//   and close on the stop path), <P>_STATUS_INTERVAL_MS 100 (the duty thread's Status publish interval),
//   <P>_EMBEDDED_RETRY_MS 1000 (embedded only, refused with external: the retry spacing of a start starved of storage).
//   <P>_RECONNECT_DELAY_MS 1000 (a core session the cluster closed, or a connect cycle that failed, is replaced after
//   this), <P>_REGISTER_RESEND_MS 5000 (a RegisterSession whose AdminAck did not arrive is sent again after this).
//   <P>_AUTH_MODES         required: a list of demo, api-key, jwt, dev (no repeats). dev needs GATEWAY_ALLOW_DEV_AUTH=1;
//                          api-key needs GATEWAY_API_KEYS_FILE; jwt needs GATEWAY_JWT_SECRET_FILE; demo needs
//                          GATEWAY_PG_URL. Those four and GATEWAY_TOKEN_CACHE_TTL_MS / _STALE_MS are read by both
//                          binaries under their GATEWAY_ names.
// The per-binary settings are listed in Settings.cpp next to their parsers.

namespace openexchange { namespace gateway {

enum class Role : std::uint8_t
{
    GATEWAY,  // oe-gateway
    STREAM,   // oe-stream
    PROJECTOR, // oe-projector (projector/ProjectorSettings.h)
    MARKETDATA // oe-marketdata (marketdata/MarketDataSettings.h)
};

enum class DriverMode : std::uint8_t
{
    UNSET,
    EMBEDDED,
    EXTERNAL
};
const char *driverModeName(DriverMode mode) noexcept;

enum class IdleMode : std::uint8_t
{
    BACKOFF,
    BUSY
};

namespace auth_mode {
inline constexpr unsigned DEMO = 1u;
inline constexpr unsigned API_KEY = 2u;
inline constexpr unsigned JWT = 4u;
inline constexpr unsigned DEV = 8u;
}

struct Bind
{
    std::string host;
    std::uint16_t port = 0;
    std::string text; // as configured
};

struct CommonSettings
{
    DriverMode driverMode = DriverMode::UNSET;
    std::string aeronDir;   // external
    std::string driverDir;  // embedded (the parent dir)
    std::int64_t driverTimeoutMs = 10'000;
    std::string coreIngress;
    std::string ingressChannel;
    std::string responseChannel;
    cluster_tools::client::CoreCredentials credentials;
    std::int64_t instanceId = 0;
    Bind publicBind;
    Bind adminBind;
    std::int64_t ioThreads = 2;
    IdleMode idle = IdleMode::BACKOFF;
    std::int64_t commandRing = 4096;
    std::int64_t maxConnections = 2000;
    std::int64_t dutyWatchdogMs = 5000;
    std::int64_t stopTimeoutMs = 2000;
    std::int64_t statusIntervalMs = 100;  // the duty thread publishes its Status at most this often
    std::int64_t embeddedRetryMs = 1000;  // an embedded start starved of storage is retried this far apart
    std::int64_t reconnectDelayMs = 1000; // a closed or failed core session is replaced after this
    std::int64_t registerResendMs = 5000; // a RegisterSession without its AdminAck is re-sent after this
    unsigned authModes = 0;
    bool allowDevAuth = false;
    std::optional<std::string> apiKeysFile;
    std::optional<std::string> jwtSecretFile;
    std::optional<std::string> pgUrl;
    std::int64_t tokenCacheTtlMs = 5000;
    std::int64_t tokenStaleMs = 600'000;
};

struct FaucetCap
{
    std::int64_t assetId = 0;
    std::string amount; // decimal text, validated for form and range (rest::parseMoney reads it) by the reader
};

struct GatewaySettings
{
    CommonSettings common;
    std::optional<cluster_tools::client::CoreCredentials> treasuryCredentials;
    bool allowTreasuryLane = false;
    std::int64_t maxInflight = 4096;
    std::int64_t requestTimeoutMs = 5000;
    std::int64_t maxBodyBytes = 65'536;
    Rate principalRate;
    std::int64_t principalBurst = 100;
    Rate queryRate;
    std::int64_t queryBurst = 200;
    std::int64_t queryMinIntervalMs = 250;
    std::int64_t bulkCancelMax = 200;
    std::int64_t readyStaleMs = 3000;
    std::int64_t readyTimeoutStreak = 5;
    std::int64_t hashThreads = 1;
    std::int64_t hashQueue = 32;
    Rate registerRate;
    std::int64_t registerBurst = 5;
    Rate loginRate;
    std::int64_t loginBurst = 20;
    Rate authSourceRate;
    std::int64_t authSourceBurst = 10;
    std::int64_t sourceV6PrefixBits = 64; // IPv6 sources share a budget per this prefix
    std::int64_t pbkdf2Iterations = 600'000;
    std::int64_t maxHeaderBytes = 8192;
    std::int64_t httpIdleTimeoutMs = 60'000;
    std::vector<std::string> trustedProxies;
    std::string clientIpHeader = "X-Forwarded-For";
    std::vector<std::string> corsOrigins;
    std::int64_t pgHistoryPool = 4;
    std::int64_t pgAuthPool = 2;
    std::optional<std::string> moneyPgUrl; // the core money database (balance movements); unset: those reads are 503
    std::int64_t pgMoneyPool = 2;
    std::int64_t pgStatementTimeoutMs = 2000;
    std::int64_t pgQueue = 64;
    std::int64_t pgRetryMs = 1000;
    Rate tokenLookupRate;                 // per source: lookups of demo tokens the cache does not hold
    std::int64_t tokenLookupBurst = 20;
    std::int64_t tokenLookupInflight = 0; // in force after parsing: set, or half of pgQueue (at least 1)
    std::int64_t historyLimitDefault = 100;
    std::int64_t historyLimitMax = 1000;
    std::int64_t historyRetryAfterMs = 60'000;   // Retry-After of 503 HISTORY_UNAVAILABLE
    std::int64_t historyBusyRetryAfterMs = 1000; // Retry-After of 503 HISTORY_BUSY
    std::int64_t historyLagRetryAfterMs = 1000;  // Retry-After of 503 HISTORY_UNAVAILABLE: an order not projected yet
    std::int64_t historyStaleMs = 5000;          // asOf.stale above this age
    std::int64_t historyMaxAgeMs = 0;            // 0 = off; above it every history read is 503 HISTORY_UNAVAILABLE
    std::int64_t historyProbeMs = 5000;          // the /health history probe's cadence
    std::vector<FaucetCap> faucetMaxAmount;
    Rate faucetRate;
    std::vector<std::string> warnings;           // boot WARN lines (a setting accepted but not recommended)
};

// One cluster node's own probe endpoint (cluster-kit NodeEndpoint: /health, /ready, /metrics), as configured.
struct NodeEndpoint
{
    std::int64_t memberId = 0;
    std::string host;
    std::uint16_t port = 0;
};

// "<memberId>@<host>:<port>[,...]" -> the list, or nullopt with `error`. Member ids are distinct (one row per node). An
// IPv6 host is written in brackets ("0@[fd00::1]:9090"); `host` holds the address without them.
std::optional<std::vector<NodeEndpoint>> parseNodeEndpoints(const std::string &text, std::string &error);

struct StreamSettings
{
    CommonSettings common;
    bool balanceStream = true;
    std::int64_t eventWindow = 65'536;
    std::int64_t replayPage = 1024;
    std::int64_t gapBufferEvents = 65'536;
    Rate queryRate;
    std::int64_t queryBurst = 10; // below the rate, so a storm second stays within 1.2x the rate
    std::int64_t userLingerMs = 30'000;
    std::int64_t headProbeMs = 5000;
    std::int64_t readyStaleMs = 15'000;
    std::int64_t heartbeatMs = 5000;
    std::int64_t wsQueueMessages = 256;
    std::int64_t wsQueueBytes = 1'048'576;
    std::int64_t anonMaxConnections = 500;  // anonymous (engine status only) connections, below <P>_MAX_CONNECTIONS
    std::int64_t anonQueueMessages = 8;     // an anonymous connection's outbound bound
    std::int64_t anonQueueBytes = 65'536;
    std::int64_t wsAuthTimeoutMs = 5000;
    std::int64_t wsPingMs = 15'000;
    std::int64_t wsIdleMs = 60'000;
    std::int64_t pgAuthPool = 1;
    std::int64_t requestTimeoutMs = 5000;   // one core query's deadline
    std::int64_t wsMaxMessageBytes = 65'536; // a client frame above it closes the connection
    std::int64_t pgQueue = 64;
    std::int64_t pgStatementTimeoutMs = 2000;
    std::int64_t pgRetryMs = 1000;
    std::int64_t wsSendBufferBytes = 0;    // 0 = the OS default (autotuned)
    // the engine status feed: each node's own endpoint, polled over the network (an empty list = that block all null)
    std::vector<NodeEndpoint> coreNodeEndpoints;
    std::vector<NodeEndpoint> meNodeEndpoints;
    std::int64_t enginePollMs = 1000;
    std::int64_t enginePollTimeoutMs = 500;
    std::int64_t engineFrameMinMs = 1000;
    std::int64_t engineMaxBodyBytes = 1'048'576;
};

template<typename S>
struct ParseResult
{
    S settings;
    std::vector<std::string> errors;     // every problem, in reading order
    std::vector<SettingRecord> records;  // every setting in force (boot log and /health)

    bool ok() const noexcept { return errors.empty(); }
};

ParseResult<GatewaySettings> parseGatewaySettings(const Environment &env,
    const cluster_tools::runtime::DirProbe &probe = cluster_tools::runtime::systemDirProbe());
ParseResult<StreamSettings> parseStreamSettings(const Environment &env,
    const cluster_tools::runtime::DirProbe &probe = cluster_tools::runtime::systemDirProbe());

// The media driver keys of one binary, <prefix>_DRIVER_MODE, AERON_DIR, <prefix>_DRIVER_DIR and
// <prefix>_DRIVER_TIMEOUT_MS, with the rules listed at the top of this file (oe-projector reads them under
// HISTORY_PROJECTOR_).
void parseDriverSettings(SettingsReader &r, const std::string &prefix, const cluster_tools::runtime::DirProbe &probe,
    CommonSettings &c);
// A core session channel (<P>_CORE_INGRESS_CHANNEL, `response` false; <P>_CORE_RESPONSE_CHANNEL, true) with the rules
// the gateway binaries apply: a full aeron:udp URI naming term-length and mtu (and the endpoint of a response channel).
void checkSessionChannel(SettingsReader &r, const std::string &name, const std::string &uri, bool response);
// host:port (IPv6 as [addr]:port); `ipv4Only` for an admin bind. nullopt with `why` (the expected form).
std::optional<Bind> parseBindText(const std::string &text, bool ipv4Only, std::string &why);
// <key>: trusted proxies, IP addresses or CIDR blocks (unset: none), appended to `out`. The gateway's
// GATEWAY_TRUSTED_PROXIES rule under another key.
void parseTrustedProxies(SettingsReader &r, const std::string &key, std::vector<std::string> &out);
// <key>: the client-IP header read behind a trusted proxy (an HTTP header name); unset keeps `out` (the default).
void parseClientIpHeader(SettingsReader &r, const std::string &key, std::string &out);

// The cluster session settings these values make (the client's defaults otherwise).
cluster_tools::client::ClientSettings sessionClientSettings(const CommonSettings &settings, const char *processName);

// "aeron:udp?k=v|k=v": the parameters, or nullopt with `error` when the text is not that form.
std::optional<std::vector<std::pair<std::string, std::string>>> channelParams(const std::string &uri, std::string &error);

}}

#endif
