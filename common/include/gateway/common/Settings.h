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
//   <P>_AUTH_MODES         required: a list of demo, api-key, jwt, dev (no repeats). dev needs GATEWAY_ALLOW_DEV_AUTH=1;
//                          api-key needs GATEWAY_API_KEYS_FILE; jwt needs GATEWAY_JWT_SECRET_FILE; demo needs
//                          GATEWAY_PG_URL. Those four and GATEWAY_TOKEN_CACHE_TTL_MS / _STALE_MS are read by both
//                          binaries under their GATEWAY_ names.
// The per-binary settings are listed in Settings.cpp next to their parsers.

namespace openexchange { namespace gateway {

enum class Role : std::uint8_t
{
    GATEWAY, // oe-gateway
    STREAM   // oe-stream
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
    std::string amount; // decimal text, validated for form only here
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
    std::vector<std::string> trustedProxies;
    std::string clientIpHeader = "X-Forwarded-For";
    std::vector<std::string> corsOrigins;
    std::int64_t pgHistoryPool = 4;
    std::int64_t pgAuthPool = 2;
    std::int64_t pgStatementTimeoutMs = 2000;
    std::int64_t pgQueue = 64;
    std::int64_t historyLimitDefault = 100;
    std::int64_t historyLimitMax = 1000;
    std::vector<FaucetCap> faucetMaxAmount;
    Rate faucetRate;
};

struct StreamSettings
{
    CommonSettings common;
    bool balanceStream = true;
    std::int64_t eventWindow = 65'536;
    std::int64_t replayPage = 1024;
    std::int64_t gapBufferEvents = 65'536;
    Rate queryRate;
    std::int64_t queryBurst = 50;
    std::int64_t userLingerMs = 30'000;
    std::int64_t headProbeMs = 5000;
    std::int64_t readyStaleMs = 15'000;
    std::int64_t heartbeatMs = 5000;
    std::int64_t wsQueueMessages = 256;
    std::int64_t wsQueueBytes = 1'048'576;
    std::int64_t wsAuthTimeoutMs = 5000;
    std::int64_t wsPingMs = 15'000;
    std::int64_t wsIdleMs = 60'000;
    std::int64_t pgAuthPool = 1;
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

// The cluster session settings these values make (the client's defaults otherwise).
cluster_tools::client::ClientSettings sessionClientSettings(const CommonSettings &settings, const char *processName);

// "aeron:udp?k=v|k=v": the parameters, or nullopt with `error` when the text is not that form.
std::optional<std::vector<std::pair<std::string, std::string>>> channelParams(const std::string &uri, std::string &error);

}}

#endif
