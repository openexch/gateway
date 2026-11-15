#include "gateway/common/SessionParts.h"

#include <algorithm>
#include <iterator>
#include <type_traits>

namespace openexchange { namespace gateway { namespace session {

std::string validate(const SessionSettings &s)
{
    std::string out;
    auto problem = [&](const std::string &text) { out += (out.empty() ? "" : "\n") + text; };
    if (s.maxInflight < 1)
    {
        problem("session: maxInflight must be >= 1");
    }
    // the client's own bound (MAX_TIMEOUT_NS, 1 h) keeps now + timeout far from int64 overflow
    if (s.requestTimeoutMs < 1 || s.requestTimeoutMs > cl::MAX_TIMEOUT_NS / 1'000'000)
    {
        problem("session: requestTimeoutMs must be in 1..3600000");
    }
    if (s.queryRateCount < 1 || s.queryRatePeriodMs < 1)
    {
        problem("session: the query rate must be at least one token per period");
    }
    if (s.queryBurst < 1)
    {
        problem("session: queryBurst must be >= 1");
    }
    if (s.reconnectDelayMs < 0 || s.reconnectDelayMs > cl::MAX_TIMEOUT_NS / 1'000'000)
    {
        problem("session: reconnectDelayMs must be in 0..3600000");
    }
    if (s.registerResendMs < 1 || s.registerResendMs > cl::MAX_TIMEOUT_NS / 1'000'000)
    {
        problem("session: registerResendMs must be in 1..3600000");
    }
    if (s.pollFragments < 1)
    {
        problem("session: pollFragments must be >= 1");
    }
    if (s.identity <= 0)
    {
        problem("session: identity must be a positive int64");
    }
    return out;
}

std::uint16_t templateOf(const Command &command) noexcept
{
    return std::visit(
        [](const auto &c) -> std::uint16_t {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, wire::CreateOrder>) return wire::tid::CREATE_ORDER;
            else if constexpr (std::is_same_v<T, wire::CancelOrder>) return wire::tid::CANCEL_ORDER;
            else if constexpr (std::is_same_v<T, wire::AmendOrder>) return wire::tid::AMEND_ORDER;
            else if constexpr (std::is_same_v<T, Deposit>) return wire::tid::DEPOSIT;
            else if constexpr (std::is_same_v<T, Withdraw>) return wire::tid::WITHDRAW;
            else if constexpr (std::is_same_v<T, wire::RequestOrderSnapshot>) return wire::tid::REQUEST_ORDER_SNAPSHOT;
            else if constexpr (std::is_same_v<T, wire::RequestOrderEvents>) return wire::tid::REQUEST_ORDER_EVENTS;
            else if constexpr (std::is_same_v<T, wire::RequestUserBalances>) return wire::tid::REQUEST_USER_BALANCES;
            else return wire::tid::REQUEST_CORE_CONFIG;
        },
        command);
}

bool isQuery(std::uint16_t templateId) noexcept
{
    return wire::tid::REQUEST_ORDER_SNAPSHOT == templateId || wire::tid::REQUEST_ORDER_EVENTS == templateId ||
        wire::tid::REQUEST_USER_BALANCES == templateId || wire::tid::REQUEST_CORE_CONFIG == templateId;
}

void requestIdOf(const Command &command, std::int64_t &high, std::int64_t &low) noexcept
{
    high = 0;
    low = 0;
    std::visit(
        [&](const auto &c) {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, wire::CreateOrder> || std::is_same_v<T, wire::CancelOrder> ||
                std::is_same_v<T, wire::AmendOrder>)
            {
                high = c.requestIdHigh;
                low = c.requestIdLow;
            }
            else if constexpr (std::is_same_v<T, Deposit> || std::is_same_v<T, Withdraw>)
            {
                high = c.command.requestIdHigh;
                low = c.command.requestIdLow;
            }
        },
        command);
}

std::size_t encodeCommand(Command &command, std::int64_t correlationId, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    return std::visit(
        [&](auto &c) -> std::size_t {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, Deposit>)
            {
                c.command.correlationId = correlationId;
                return wire::encodeDeposit(c.command, buffer, capacity);
            }
            else if constexpr (std::is_same_v<T, Withdraw>)
            {
                c.command.correlationId = correlationId;
                return wire::encodeWithdraw(c.command, buffer, capacity);
            }
            else
            {
                c.correlationId = correlationId;
                return wire::encode(c, buffer, capacity);
            }
        },
        command);
}

const char *nameOf(OutcomeKind kind) noexcept
{
    switch (kind)
    {
        case OutcomeKind::REPLY: return "REPLY";
        case OutcomeKind::OUTCOME_UNKNOWN: return "OUTCOME_UNKNOWN";
        case OutcomeKind::CORE_UNAVAILABLE: return "CORE_UNAVAILABLE";
        case OutcomeKind::OVERLOADED: return "OVERLOADED";
        case OutcomeKind::INTERNAL: return "INTERNAL";
    }
    return "?";
}

// ---- TokenBucket

TokenBucket::TokenBucket(std::int64_t count, std::int64_t periodMs, std::int64_t burst) noexcept :
    m_perNs(static_cast<double>(count) / (static_cast<double>(periodMs) * 1e6)),
    m_burst(static_cast<double>(burst)),
    m_tokens(static_cast<double>(burst))
{
}

void TokenBucket::refill(std::int64_t nowNs) noexcept
{
    if (!m_started)
    {
        m_started = true;
        m_lastNs = nowNs;
        return;
    }
    if (nowNs > m_lastNs)
    {
        m_tokens = std::min(m_burst, m_tokens + static_cast<double>(nowNs - m_lastNs) * m_perNs);
        m_lastNs = nowNs;
    }
}

bool TokenBucket::tryTake(std::int64_t nowNs) noexcept
{
    refill(nowNs);
    if (m_tokens >= 1.0)
    {
        m_tokens -= 1.0;
        return true;
    }
    return false;
}

void TokenBucket::refund() noexcept
{
    m_tokens = std::min(m_burst, m_tokens + 1.0);
}

double TokenBucket::tokens(std::int64_t nowNs) noexcept
{
    refill(nowNs);
    return m_tokens;
}

// ---- CoreConfig and ConfigCache

const wire::MarketConfig *CoreConfig::market(std::int32_t marketId) const noexcept
{
    for (const wire::MarketConfig &m : markets)
    {
        if (m.marketId == marketId)
        {
            return &m;
        }
    }
    return nullptr;
}

const GateView *CoreConfig::marketGate(std::int32_t marketId) const noexcept
{
    for (std::size_t i = 0; i < markets.size() && i < marketGates.size(); ++i)
    {
        if (markets[i].marketId == marketId)
        {
            return &marketGates[i];
        }
    }
    return nullptr;
}

void ConfigCache::beginReport(const wire::CoreConfigHead &head)
{
    m_building = CoreConfig{};
    m_building.head = head;
    m_building.reported = true;
    m_building.global.state = head.gateState;
    m_building.global.reasons = head.gateReasons;
    m_building.global.logPosition = head.asOfLogPosition;
}

void ConfigCache::addMarket(const wire::MarketConfig &market)
{
    m_building.markets.push_back(market);
    GateView gate;
    gate.state = market.gateState;
    gate.reasons = market.gateReasons;
    gate.circuitBreaker = market.circuitBreaker;
    gate.logPosition = m_building.head.asOfLogPosition;
    m_building.marketGates.push_back(gate);
}

void ConfigCache::addAsset(const wire::AssetConfig &asset)
{
    m_building.assets.push_back(asset);
}

void ConfigCache::endReport()
{
    // a gate moved by an AdmissionState newer than this report keeps that state
    if (m_current.global.logPosition > m_building.global.logPosition)
    {
        m_building.global = m_current.global;
    }
    for (std::size_t i = 0; i < m_building.markets.size(); ++i)
    {
        const std::int32_t marketId = m_building.markets[i].marketId;
        const GateView *older = m_current.marketGate(marketId);
        if (nullptr != older && older->logPosition > m_building.marketGates[i].logPosition)
        {
            m_building.marketGates[i] = *older;
        }
        const auto pending = m_pending.find(marketId);
        if (m_pending.end() != pending && pending->second.logPosition > m_building.marketGates[i].logPosition)
        {
            m_building.marketGates[i] = pending->second;
        }
    }
    // what this report holds, or is newer than, is settled; a market it does not list keeps its newer admission
    for (auto it = m_pending.begin(); it != m_pending.end();)
    {
        const bool held = nullptr != m_building.marketGate(it->first);
        it = (held || it->second.logPosition <= m_building.head.asOfLogPosition) ? m_pending.erase(it) : std::next(it);
    }
    m_current = std::move(m_building);
    m_building = CoreConfig{};
    publish();
}

bool ConfigCache::onAdmission(const wire::AdmissionState &a)
{
    GateView *gate = nullptr;
    if (0 == a.marketId)
    {
        gate = &m_current.global;
    }
    else
    {
        for (std::size_t i = 0; i < m_current.markets.size() && i < m_current.marketGates.size(); ++i)
        {
            if (m_current.markets[i].marketId == a.marketId)
            {
                gate = &m_current.marketGates[i];
            }
        }
    }
    if (nullptr == gate)
    {
        // a market this view does not hold yet: kept (the newest per market) for the report that lists it
        GateView &p = m_pending[a.marketId];
        if (a.logPosition >= p.logPosition)
        {
            p.state = a.gateState;
            p.reasons = a.gateReasons;
            p.circuitBreaker = a.circuitBreaker;
            p.logPosition = a.logPosition;
        }
        return false;
    }
    if (a.logPosition < gate->logPosition)
    {
        return false; // older than what we hold
    }
    gate->state = a.gateState;
    gate->reasons = a.gateReasons;
    gate->circuitBreaker = a.circuitBreaker;
    gate->logPosition = a.logPosition;
    publish();
    return true;
}

bool ConfigCache::staleAfter(const wire::ConfigApplied &applied) const noexcept
{
    return !m_current.reported || applied.logPosition > m_current.head.asOfLogPosition;
}

void ConfigCache::publish()
{
    m_published.store(std::make_shared<const CoreConfig>(m_current), std::memory_order_release);
}

}}}
