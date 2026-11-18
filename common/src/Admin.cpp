#include "gateway/common/Admin.h"

#include <cstdio>

#include "gateway/common/Wire.h"

namespace openexchange { namespace gateway {

namespace rt = cluster_tools::runtime;

namespace {

const char *const JSON = "application/json";
const char *const PROMETHEUS = "text/plain; version=0.0.4; charset=utf-8";

std::string q(const std::string &text)
{
    return "\"" + jsonEscape(text) + "\"";
}

const char *b(bool v)
{
    return v ? "true" : "false";
}

std::vector<Condition> readyConditions(const AdminView &v)
{
    switch (v.role)
    {
        case Role::GATEWAY: return gatewayReadyConditions(v.status, v.nowNs, v.gatewayBounds);
        case Role::STREAM: return streamReadyConditions(v.status, v.nowNs, v.streamBounds);
        case Role::PROJECTOR: break;
    }
    return projectorReadyConditions(v.status, v.nowNs, v.projectorBounds);
}

// `extra`: members placed after "ready" (",\"role\":\"standby\"" for a projector).
rt::HttpResponse readyResponse(const std::vector<Condition> &conditions, const std::string &extra = "")
{
    rt::HttpResponse r;
    const std::vector<std::string> names = failing(conditions);
    if (names.empty())
    {
        r.status = 200;
        r.contentType = JSON;
        r.body = "{\"ready\":true" + extra + ",\"failing\":[]}\n";
        return r;
    }
    r.status = 503;
    r.contentType = JSON;
    r.body = "{\"ready\":false" + extra + ",\"failing\":[";
    for (std::size_t i = 0; i < names.size(); ++i)
    {
        r.body += (0 == i ? "" : ",") + q(names[i]);
    }
    r.body += "]}\n";
    return r;
}

std::string conditionsJson(const std::vector<Condition> &conditions)
{
    std::string out = "{";
    for (std::size_t i = 0; i < conditions.size(); ++i)
    {
        out += (0 == i ? "" : ",") + q(conditions[i].name) + ":" + b(conditions[i].ok);
    }
    return out + "}";
}

std::string ageMs(std::int64_t lastNs, std::int64_t nowNs)
{
    return lastNs < 0 ? "null" : std::to_string((nowNs - lastNs) / 1'000'000LL);
}

}

const char *metricPrefix(Role role) noexcept
{
    switch (role)
    {
        case Role::GATEWAY: return "gateway_";
        case Role::STREAM: return "stream_";
        case Role::PROJECTOR: break;
    }
    return "projector_";
}

std::string jsonEscape(const std::string &text)
{
    std::string out;
    out.reserve(text.size() + 2);
    for (const char ch : text)
    {
        const unsigned char u = static_cast<unsigned char>(ch);
        switch (ch)
        {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (u < 0x20)
                {
                    char hex[8];
                    std::snprintf(hex, sizeof(hex), "\\u%04x", u);
                    out += hex;
                }
                else
                {
                    out += ch;
                }
        }
    }
    return out;
}

std::string renderHealth(const AdminView &v, int &status)
{
    const bool connected = v.status.driverConnected;
    const bool up = connected && v.heartbeatAgeMs < v.watchdogMs;
    status = up ? 200 : 503;
    const std::vector<Condition> conditions = readyConditions(v);
    const Status &s = v.status;
    std::string out = "{";
    out += "\"status\":" + q(!connected ? "STARTING" : up ? "UP" : "STALLED");
    out += ",\"process\":" + q(v.processName);
    out += ",\"version\":" + q(v.version);
    out += ",\"build\":" + q(v.build);
    out += ",\"clusterTools\":" + q(v.clusterTools);
    out += ",\"moneySchema\":{\"version\":" + std::to_string(v.moneySchemaVersion) + ",\"sha256\":" +
        q(v.moneySchemaSha256) + "}";
    out += ",\"driver\":{\"mode\":" + q(v.driverMode) + ",\"dir\":" + q(v.driverDir) + ",\"timeoutMs\":" +
        std::to_string(v.driverTimeoutMs) + ",\"reclaimed\":" + b(v.driverReclaimed) + ",\"connected\":" + b(connected) + ",\"conductorHeartbeatAgeMs\":" +
        std::to_string(v.conductorHeartbeatAgeMs) + "}";
    out += ",\"duty\":{\"heartbeatAgeMs\":" + std::to_string(v.heartbeatAgeMs) + ",\"watchdogMs\":" +
        std::to_string(v.watchdogMs) + ",\"maxCycleUs\":" + std::to_string(s.dutyMaxCycleUs) + ",\"iterations\":" +
        std::to_string(s.dutyIterations) + "}";
    out += ",\"session\":{\"connected\":" + std::string(b(s.connected)) + ",\"registered\":" + b(s.registered) +
        ",\"epoch\":" + std::to_string(s.sessionEpoch) + ",\"leader\":" + std::to_string(s.leaderMemberId) +
        ",\"keepaliveAgeMs\":" + ageMs(s.lastKeepaliveOfferNs, v.nowNs) + "}";
    if (Role::GATEWAY == v.role)
    {
        out += ",\"config\":{\"loaded\":" + std::string(b(s.configLoaded)) + ",\"gateOpen\":" + b(s.gateOpen) +
            ",\"timeoutStreak\":" + std::to_string(s.timeoutStreak) + "}";
        out += ",\"storage\":{\"starved\":" + std::string(b(s.storageStarved)) + ",\"starvedSeconds\":" +
            std::to_string(s.storageStarvedSeconds) + "}";
        out += ",\"treasuryLane\":" + q(v.treasuryLane);
    }
    else if (Role::STREAM == v.role)
    {
        out += ",\"events\":{\"gapOpen\":" + std::string(b(s.gapOpen)) + ",\"headProbeAgeMs\":" +
            ageMs(s.lastHeadProbeNs, v.nowNs) + "}";
    }
    out += v.frontHealth;
    out += ",\"ready\":" + std::string(b(failing(conditions).empty())) + ",\"conditions\":" + conditionsJson(conditions);
    out += ",\"aeronErrors\":" + std::to_string(s.aeronErrors) + ",\"logDropped\":" + std::to_string(v.logDropped);
    out += ",\"settings\":{";
    if (nullptr != v.settings)
    {
        bool first = true;
        for (const SettingRecord &r : *v.settings)
        {
            out += (first ? "" : ",") + q(r.name) + ":{\"value\":" + q(r.value) + ",\"source\":" + q(r.source) + "}";
            first = false;
        }
    }
    out += "}}\n";
    return out;
}

std::string renderMetrics(const AdminView &v)
{
    const std::string p = metricPrefix(v.role);
    const Status &s = v.status;
    std::string out;
    auto gauge = [&](const std::string &name, const std::string &help, const std::string &value) {
        out += "# HELP " + p + name + " " + help + "\n# TYPE " + p + name + " gauge\n" + p + name + " " + value + "\n";
    };
    auto counter = [&](const std::string &name, const std::string &help, const std::string &value) {
        out += "# HELP " + p + name + " " + help + "\n# TYPE " + p + name + " counter\n" + p + name + " " + value + "\n";
    };
    out += "# HELP " + p + "build_info The build of this process.\n# TYPE " + p + "build_info gauge\n" + p +
        "build_info{version=" + q(v.version) + ",build=" + q(v.build) + ",cluster_tools=" + q(v.clusterTools) +
        ",money_schema_version=\"" + std::to_string(v.moneySchemaVersion) + "\",driver_mode=" + q(v.driverMode) +
        "} 1\n";
    gauge("up", "1 while the media driver is connected and the duty heartbeat is younger than the watchdog bound.",
        s.driverConnected && v.heartbeatAgeMs < v.watchdogMs ? "1" : "0");
    gauge("driver_connected", "1 once the Aeron client is connected and the process counter registered.",
        s.driverConnected ? "1" : "0");
    const std::vector<Condition> conditions = readyConditions(v);
    gauge("ready", "1 when every readiness condition holds.", failing(conditions).empty() ? "1" : "0");
    out += "# HELP " + p + "ready_condition Each readiness condition, 1 = holds.\n# TYPE " + p +
        "ready_condition gauge\n";
    for (const Condition &c : conditions)
    {
        out += p + "ready_condition{name=\"" + c.name + "\"} " + (c.ok ? "1" : "0") + "\n";
    }
    gauge("session_connected", "1 while a session to a leader is open.", s.connected ? "1" : "0");
    gauge("session_registered", "1 after the registration ack of the current session.", s.registered ? "1" : "0");
    counter("session_epoch", "Sessions opened since start.", std::to_string(s.sessionEpoch));
    // the core session's counters (both binaries: each holds one GATEWAY-class session)
    {
        counter("session_registrations_total", "Registration rounds that reached REGISTERED (the AdminAck arrived).",
            std::to_string(s.registrations));
        gauge("inflight", "Requests the core session holds, offered or waiting to be offered.", std::to_string(s.inflight));
    }
    if (Role::GATEWAY == v.role)
    {
        counter("reoffers_total", "Successful offers of a request after its first (re-offers after a new session or leader).",
            std::to_string(s.reoffers));
        counter("late_replies_total", "Replies for a correlation id no longer held (answered or expired), dropped.",
            std::to_string(s.lateReplies));
        counter("outcome_unknown_total", "Requests that reached their deadline offered and unanswered (504).",
            std::to_string(s.outcomeUnknown));
    }
    {
        counter("query_budget_wait_seconds", "Total time core queries waited for a query budget token.",
            std::to_string(static_cast<double>(s.queryBudgetWaitNs) / 1e9));
        counter("decode_refused_total", "Egress frames the money-frame decoder refused.", std::to_string(s.decodeRefused));
        out += "# HELP " + p + "wire_ignored_total Egress frames of a template the core sends on a subscribed channel " +
            "and the gateway does not read (counted, not refused, not logged).\n# TYPE " + p +
            "wire_ignored_total counter\n";
        for (std::size_t k = 0; k < wire::IGNORED_TEMPLATES.size(); ++k)
        {
            out += p + "wire_ignored_total{template=\"" + std::to_string(wire::IGNORED_TEMPLATES[k]) + "\"} " +
                std::to_string(s.wireIgnored[k]) + "\n";
        }
        counter("storage_starved_seconds_total",
            "Seconds the core session's connect attempts were refused for storage space (exit 70 once one episode lasts 30 s).",
            std::to_string(s.storageStarvedSeconds));
    }
    gauge("duty_max_cycle_us", "The longest duty iteration since start, idle excluded.", std::to_string(s.dutyMaxCycleUs));
    gauge("duty_heartbeat_age_ms", "Age of the duty heartbeat at scrape time.", std::to_string(v.heartbeatAgeMs));
    gauge("conductor_heartbeat_age_ms", "Age of the Aeron client conductor heartbeat at the last check (-1 = not found yet).",
        std::to_string(v.conductorHeartbeatAgeMs));
    counter("aeron_errors", "Errors reported by the Aeron client conductor.", std::to_string(s.aeronErrors));
    counter("log_dropped_total", "Log records dropped because the log ring was full.", std::to_string(v.logDropped));
    out += v.frontMetrics;
    return out;
}

rt::HttpResponse adminRoute(const rt::HttpRequest &request, const AdminView &view)
{
    rt::HttpResponse r;
    const std::string path = request.path.substr(0, request.path.find('?'));
    const bool known = "/health" == path || "/ready" == path || "/metrics" == path ||
        ("/ready/session" == path && Role::GATEWAY == view.role);
    if (!known)
    {
        r.status = 404;
        r.body = "not found\n";
        return r;
    }
    if ("GET" != request.method)
    {
        r.status = 405;
        r.body = "method not allowed\n";
        return r;
    }
    if ("/health" == path)
    {
        r.contentType = JSON;
        r.body = renderHealth(view, r.status);
        return r;
    }
    if ("/ready" == path)
    {
        return readyResponse(readyConditions(view), Role::PROJECTOR == view.role
                ? std::string(",\"role\":") + (view.status.leaseHeld ? "\"writer\"" : "\"standby\"")
                : std::string());
    }
    if ("/ready/session" == path)
    {
        return readyResponse(gatewaySessionConditions(view.status));
    }
    r.status = 200;
    r.contentType = PROMETHEUS;
    r.body = renderMetrics(view);
    return r;
}

}}
