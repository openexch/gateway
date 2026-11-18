#include "ProjectorSettings.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>

#include "cluster_tools/archive/FramePeek.h"
#include "cluster_tools/client/IngressEndpoints.h"

namespace openexchange { namespace gateway {

namespace ar = cluster_tools::archive;
namespace cl = cluster_tools::client;
namespace rt = cluster_tools::runtime;

namespace {

bool readKeyFile(const std::string &path, std::string &out, std::string &why)
{
    std::ifstream in(path, std::ios::binary);
    if (!in)
    {
        why = std::strerror(errno);
        return false;
    }
    std::ostringstream buffer;
    buffer << in.rdbuf();
    if (in.bad())
    {
        why = "read error";
        return false;
    }
    out = buffer.str();
    // one line: a single trailing newline is the file's end, not part of the key
    if (!out.empty() && '\n' == out.back())
    {
        out.pop_back();
    }
    return true;
}

// "<a> must be above <b>" with both values, when it does not hold.
void above(SettingsReader &r, const std::string &a, std::int64_t va, const std::string &b, std::int64_t vb,
    const std::string &why)
{
    if (va <= vb)
    {
        r.problem(a + " (" + std::to_string(va) + ") must be above " + b + " (" + std::to_string(vb) + "): " + why);
    }
}

}

ParseResult<ProjectorSettings> parseProjectorSettings(const Environment &env, const rt::DirProbe &probe)
{
    ParseResult<ProjectorSettings> out;
    SettingsReader r(env);
    ProjectorSettings &s = out.settings;
    CommonSettings &c = s.common;
    const std::string P = PROJECTOR_PREFIX;
    const std::string X = P + "_";

    parseDriverSettings(r, P, probe, c);
    c.instanceId = r.requiredPositive(X + "INSTANCE_ID");
    {
        const std::string key = X + "ADMIN_BIND";
        const std::string text = r.requiredString(key, "host:port of the admin listener (IPv4)");
        if (!text.empty())
        {
            std::string why;
            if (const auto b = parseBindText(text, true, why))
            {
                c.adminBind = *b;
            }
            else
            {
                r.problem(key + " must be " + why + ", got '" + text + "'");
            }
        }
    }
    c.idle = 1 == r.choice(X + "IDLE", { "backoff", "busy" }, 0) ? IdleMode::BUSY : IdleMode::BACKOFF;
    c.dutyWatchdogMs = r.positive(X + "DUTY_WATCHDOG_MS", c.dutyWatchdogMs);
    c.stopTimeoutMs = r.positive(X + "STOP_TIMEOUT_MS", c.stopTimeoutMs);
    c.statusIntervalMs = r.positive(X + "STATUS_INTERVAL_MS", c.statusIntervalMs);
    {
        const std::string key = X + "EMBEDDED_RETRY_MS";
        if (DriverMode::EXTERNAL == c.driverMode && r.isSet(key))
        {
            r.problem(key + " is set but " + X + "DRIVER_MODE=external starts no driver: remove it");
        }
        else
        {
            c.embeddedRetryMs = r.positive(key, c.embeddedRetryMs);
        }
    }

    // the journal sources: the archive reader's own rules under this prefix (the core order journal's frame bound)
    {
        const ar::ArchiveSettingsKeys keys = ar::ArchiveSettingsKeys::withPrefix(X, ar::CORE_ORDER_JOURNAL,
            "the history projector");
        const ar::ArchiveSettingsResult archive = ar::parseArchiveSettings(
            [&r](const std::string &key) { return r.raw(key); }, ar::SettingsScope::TOOL, keys);
        for (const std::string &e : archive.errors)
        {
            r.problem(e);
        }
        if (archive.settings)
        {
            s.archive = *archive.settings;
            std::string list;
            for (const ar::ArchiveEndpoint &e : s.archive.archives)
            {
                list += (list.empty() ? "" : ",") + e.text();
            }
            r.record(X + "JOURNAL_ARCHIVES", list, "set");
            r.record(X + "JOURNAL_CONTROL_STREAM", std::to_string(s.archive.controlStreamId), "set");
            r.record(X + "JOURNAL_STREAM", std::to_string(s.archive.journalStreamId), "set");
            r.record(X + "JOURNAL_CHANNEL", s.archive.journalChannel, "set");
            r.record(X + "REPLAY_STREAM", std::to_string(s.archive.replayStreamId), "set");
            r.record(X + "LOCAL_HOST", s.archive.localHost, "set");
            r.record(X + "JOURNAL_MTU", std::to_string(s.archive.journalMtu), "set");
        }
    }

    // the head-probe session
    s.clusterMembers = r.requiredString(X + "CLUSTER_MEMBERS", "the core members, id=host:port,...");
    if (!s.clusterMembers.empty())
    {
        const cl::ParsedEndpoints endpoints = cl::parseIngressEndpoints(s.clusterMembers);
        if (!endpoints.ok())
        {
            r.problem(X + "CLUSTER_MEMBERS: " + endpoints.error);
        }
    }
    s.clusterKeyFile = r.requiredString(X + "CLUSTER_KEY_FILE", "the path of a file holding keyId:GATEWAY:secretHex");
    if (!s.clusterKeyFile.empty())
    {
        std::string text;
        std::string why;
        if (!readKeyFile(s.clusterKeyFile, text, why))
        {
            r.problem(X + "CLUSTER_KEY_FILE: cannot read " + s.clusterKeyFile + ": " + why);
        }
        else
        {
            const cl::CredentialsResult parsed = cl::parseCoreCredentials(text);
            if (!parsed.ok())
            {
                r.problem(X + "CLUSTER_KEY_FILE " + s.clusterKeyFile + ": " + parsed.error);
            }
            else if (cl::SessionClass::GATEWAY != parsed.credentials.sessionClass)
            {
                r.problem(X + "CLUSTER_KEY_FILE " + s.clusterKeyFile + " holds a " +
                    cl::nameOf(parsed.credentials.sessionClass) + " key; the projector's head probe needs GATEWAY");
            }
            else
            {
                s.clusterCredentials = parsed.credentials;
            }
        }
    }

    // PostgreSQL
    s.pgUrl = r.requiredString(X + "PG_URL", "postgresql://... of the projection database", true);
    if (!s.pgUrl.empty() && 0 != s.pgUrl.rfind("postgresql://", 0) && 0 != s.pgUrl.rfind("postgres://", 0))
    {
        r.problem(X + "PG_URL must be a postgresql:// or postgres:// URL");
    }
    s.pgStatementTimeoutMs = r.positive(X + "PG_STATEMENT_TIMEOUT_MS", s.pgStatementTimeoutMs);
    s.pgRetryMs = r.positive(X + "PG_RETRY_MS", s.pgRetryMs);
    s.pgIdleInTxTimeoutMs = r.positive(X + "PG_IDLE_IN_TX_TIMEOUT_MS", s.pgIdleInTxTimeoutMs);
    s.pgKeepaliveIdleS = r.positive(X + "PG_KEEPALIVE_IDLE_S", s.pgKeepaliveIdleS);
    s.pgKeepaliveIntervalS = r.positive(X + "PG_KEEPALIVE_INTERVAL_S", s.pgKeepaliveIntervalS);
    s.pgKeepaliveCount = r.positive(X + "PG_KEEPALIVE_COUNT", s.pgKeepaliveCount);
    s.rebuildLockTimeoutMs = r.positive(X + "REBUILD_LOCK_TIMEOUT_MS", s.rebuildLockTimeoutMs);

    // the writer
    s.leaseMs = r.positive(X + "LEASE_MS", s.leaseMs);
    s.leasePollMs = r.positive(X + "LEASE_POLL_MS", s.leasePollMs);
    s.batchEvents = r.positive(X + "BATCH_EVENTS", s.batchEvents);
    s.batchMs = r.positive(X + "BATCH_MS", s.batchMs);
    s.queueEvents = r.positive(X + "QUEUE_EVENTS", s.queueEvents);
    s.headProbeMs = r.positive(X + "HEAD_PROBE_MS", s.headProbeMs);
    s.headFreshMs = r.positive(X + "HEAD_FRESH_MS", s.headFreshMs);
    s.archiveTimeoutMs = r.positive(X + "ARCHIVE_TIMEOUT_MS", s.archiveTimeoutMs);
    s.relistMs = r.positive(X + "RELIST_MS", s.relistMs);
    s.sourceBehindMs = r.positive(X + "SOURCE_BEHIND_MS", s.sourceBehindMs);
    s.sourceBackoffMs = r.positive(X + "SOURCE_BACKOFF_MS", s.sourceBackoffMs);
    s.sourceBackoffMaxMs = r.positive(X + "SOURCE_BACKOFF_MAX_MS", s.sourceBackoffMaxMs);
    s.gapRetryMs = r.positive(X + "GAP_RETRY_MS", s.gapRetryMs);
    s.readyLagMs = r.positive(X + "READY_LAG_MS", s.readyLagMs);
    above(r, X + "HEAD_FRESH_MS", s.headFreshMs, X + "HEAD_PROBE_MS", s.headProbeMs,
        "a healthy copy would read not ready between two probes");
    above(r, X + "READY_LAG_MS", s.readyLagMs, X + "HEAD_PROBE_MS", s.headProbeMs,
        "a beat follows an answered probe, so a healthy copy would read not ready between two beats");
    above(r, X + "LEASE_MS", s.leaseMs, X + "LEASE_POLL_MS", s.leasePollMs,
        "a standby polling less often than a lease lasts adds a whole lease to every takeover");
    if (s.pgIdleInTxTimeoutMs > s.leaseMs)
    {
        // not a refusal: the fence still stops the stopped writer; only the takeover's bound grows
        s.warnings.push_back(X + "PG_IDLE_IN_TX_TIMEOUT_MS (" + std::to_string(s.pgIdleInTxTimeoutMs) +
            ") is above " + X + "LEASE_MS (" + std::to_string(s.leaseMs) + "): a writer stopped inside a transaction "
            "holds the state row until the server ends that transaction, so a takeover can wait that long instead of "
            "at most one lease");
    }
    if (s.pgStatementTimeoutMs * 3 >= s.leaseMs)
    {
        r.problem(X + "PG_STATEMENT_TIMEOUT_MS (" + std::to_string(s.pgStatementTimeoutMs) + ") must be below a third of " +
            X + "LEASE_MS (" + std::to_string(s.leaseMs) + " / 3 = " + std::to_string(s.leaseMs / 3) +
            ", the renewal interval): one slow statement would outlive a renewal and step the writer down");
    }
    above(r, X + "PG_STATEMENT_TIMEOUT_MS", s.pgStatementTimeoutMs, X + "REBUILD_LOCK_TIMEOUT_MS",
        s.rebuildLockTimeoutMs, "the statement timeout would end the rebuild's lock wait first");
    if (s.sourceBackoffMaxMs < s.sourceBackoffMs)
    {
        r.problem(X + "SOURCE_BACKOFF_MAX_MS (" + std::to_string(s.sourceBackoffMaxMs) + ") must be at least " + X +
            "SOURCE_BACKOFF_MS (" + std::to_string(s.sourceBackoffMs) + "): it caps the doubling of that wait");
    }

    r.refuseUnknown({ X });
    out.errors = r.errors();
    out.records = r.records();
    return out;
}

}}
