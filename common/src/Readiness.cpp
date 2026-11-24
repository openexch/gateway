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
        { condition::STORAGE_SPACE, !s.storageStarved },
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

std::vector<Condition> projectorReadyConditions(const Status &s, std::int64_t nowNs, const ProjectorReadyBounds &bounds)
{
    return {
        { condition::DRIVER_CONNECTED, s.driverConnected },
        { condition::LEASE_HELD, s.leaseHeld },
        { condition::SESSION_REGISTERED, s.registered },
        { condition::HEAD_PROBE_FRESH, fresh(s.lastHeadProbeNs, nowNs, bounds.headFreshMs) },
        { condition::NOT_HALTED, !s.halted },
        { condition::BEAT_FRESH, s.lastBeatNs >= 0 && nowNs - s.lastBeatNs < bounds.readyLagMs * 1'000'000LL },
    };
}

const char *nameOf(MdBookState state) noexcept
{
    switch (state)
    {
        case MdBookState::NOT_EXPECTED: return "not_expected";
        case MdBookState::FRESH: return "fresh";
        case MdBookState::NO_SNAPSHOT: return "no_snapshot";
        case MdBookState::BROKEN: return "broken";
        case MdBookState::STALE: break;
    }
    return "stale";
}

bool mdEgressFresh(std::int64_t lastMeFrameNs, std::int64_t nowNs, const MarketDataReadyBounds &bounds) noexcept
{
    return lastMeFrameNs >= 0 && nowNs - lastMeFrameNs < bounds.egressStaleMs * 1'000'000LL;
}

MdBookState mdBookState(const MdMarketStatus &m, std::int64_t lastMeFrameNs, std::int64_t nowNs,
    const MarketDataReadyBounds &bounds) noexcept
{
    if (!m.expected)
    {
        return MdBookState::NOT_EXPECTED;
    }
    if (!m.snapshot)
    {
        return MdBookState::NO_SNAPSHOT;
    }
    if (!m.ladderOk)
    {
        return MdBookState::BROKEN;
    }
    const bool recentFrame = m.lastBookFrameNs >= 0 && nowNs - m.lastBookFrameNs < bounds.bookStaleMs * 1'000'000LL;
    if (recentFrame || (m.empty && mdEgressFresh(lastMeFrameNs, nowNs, bounds)))
    {
        return MdBookState::FRESH;
    }
    return MdBookState::STALE;
}

namespace {

bool mdSession(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds)
{
    return s.mdSessionUp && mdEgressFresh(s.mdLastMeFrameNs, nowNs, bounds);
}

bool mdBooksFresh(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds)
{
    bool any = false;
    for (const MdMarketStatus &m : s.mdMarkets)
    {
#ifdef MD_NC_SEEN_MARKETS
        // control build only (never published): judged on the markets seen so far
        any = true;
        if (!m.snapshot)
        {
            continue;
        }
#endif
        if (!m.expected)
        {
            continue;
        }
        any = true;
        if (MdBookState::FRESH != mdBookState(m, s.mdLastMeFrameNs, nowNs, bounds))
        {
            return false;
        }
    }
    return any; // an empty expected set is refused by the settings; never ready on it
}

}

std::vector<Condition> marketDataReadyConditions(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds)
{
#ifdef MD_NC_READY_LIVENESS
    (void)s, (void)nowNs, (void)bounds;
    return { { "process", true } }; // control build only (never published): ready is process liveness
#endif
    return {
        { condition::DRIVER_CONNECTED, s.driverConnected },
        { condition::ME_SESSION, mdSession(s, nowNs, bounds) },
        { condition::BOOK_FRESH, mdBooksFresh(s, nowNs, bounds) },
        { condition::JOURNAL_ATTACHED, s.mdJournalAttached },
        { condition::JOURNAL_FRESH, s.mdJournalFresh },
        { condition::HISTORY_WRITABLE, s.mdHistoryWritable },
        { condition::NO_HALT, !s.mdHalted },
        { condition::CLIENTS_SERVED, s.mdClientsServed },
    };
}

std::vector<Condition> marketDataBookConditions(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds)
{
#ifdef MD_NC_READY_LIVENESS
    (void)s, (void)nowNs, (void)bounds;
    return { { "process", true } }; // control build only (never published): ready is process liveness
#endif
    return {
        { condition::ME_SESSION, mdSession(s, nowNs, bounds) },
        { condition::BOOK_FRESH, mdBooksFresh(s, nowNs, bounds) },
        { condition::CLIENTS_SERVED, s.mdClientsServed },
    };
}

std::vector<Condition> marketDataSessionConditions(const Status &s, std::int64_t nowNs, const MarketDataReadyBounds &bounds)
{
    return { { condition::ME_SESSION, mdSession(s, nowNs, bounds) } };
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
