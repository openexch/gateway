#include "gateway/common/Readiness.h"

namespace openexchange { namespace gateway {

namespace {

bool fresh(std::int64_t lastNs, std::int64_t nowNs, std::int64_t boundMs)
{
    return lastNs >= 0 && nowNs - lastNs <= boundMs * 1'000'000LL;
}

}

std::vector<Condition> gatewayReadyConditions(const Status &s, std::int64_t nowNs, const GatewayReadyBounds &bounds)
{
    return {
        { condition::DRIVER_CONNECTED, s.driverConnected },
        { condition::SESSION_CONNECTED, s.connected },
        { condition::SESSION_REGISTERED, s.registered },
        { condition::CONFIG_LOADED, s.configLoaded },
        { condition::GATE_OPEN, s.gateOpen },
        { condition::KEEPALIVE_FRESH, fresh(s.lastKeepaliveOfferNs, nowNs, bounds.readyStaleMs) },
        { condition::TIMEOUT_STREAK, static_cast<std::int64_t>(s.timeoutStreak) < bounds.timeoutStreak },
        { condition::PUBLIC_LISTENER, s.publicListenerAccepting },
    };
}

std::vector<Condition> gatewaySessionConditions(const Status &s)
{
    return {
        { condition::DRIVER_CONNECTED, s.driverConnected },
        { condition::SESSION_CONNECTED, s.connected },
        { condition::SESSION_REGISTERED, s.registered },
    };
}

std::vector<Condition> streamReadyConditions(const Status &s, std::int64_t nowNs, const StreamReadyBounds &bounds)
{
    return {
        { condition::DRIVER_CONNECTED, s.driverConnected },
        { condition::SESSION_REGISTERED, s.registered },
        { condition::EVENT_GAP, !s.gapOpen },
        { condition::HEAD_PROBE_FRESH, fresh(s.lastHeadProbeNs, nowNs, bounds.readyStaleMs) },
        { condition::WS_LISTENER, s.wsListenerAccepting },
    };
}

std::vector<std::string> failing(const std::vector<Condition> &conditions)
{
    std::vector<std::string> out;
    for (const Condition &c : conditions)
    {
        if (!c.ok)
        {
            out.emplace_back(c.name);
        }
    }
    return out;
}

}}
