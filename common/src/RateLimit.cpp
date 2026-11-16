#include "gateway/common/RateLimit.h"

#include <algorithm>
#include <cmath>

namespace openexchange { namespace gateway {

namespace ip = boost::asio::ip;

TokenBucketMs::TokenBucketMs(std::int64_t count, std::int64_t periodMs, std::int64_t burst) noexcept :
    m_perMs(periodMs > 0 ? static_cast<double>(count) / static_cast<double>(periodMs) : 0.0),
    m_burst(static_cast<double>(burst)),
    m_tokens(static_cast<double>(burst))
{
}

void TokenBucketMs::refill(std::int64_t nowMs) noexcept
{
    if (!m_started)
    {
        m_started = true;
        m_lastMs = nowMs;
        return;
    }
    if (nowMs > m_lastMs)
    {
        m_tokens = std::min(m_burst, m_tokens + static_cast<double>(nowMs - m_lastMs) * m_perMs);
        m_lastMs = nowMs;
    }
}

TakeResult TokenBucketMs::take(std::int64_t nowMs) noexcept
{
    refill(nowMs);
    TakeResult r;
    if (m_tokens >= 1.0)
    {
        m_tokens -= 1.0;
        r.ok = true;
        return r;
    }
    r.retryAfterMs = m_perMs > 0.0 ? static_cast<std::int64_t>(std::ceil((1.0 - m_tokens) / m_perMs)) : 0;
    return r;
}

bool TokenBucketMs::full(std::int64_t nowMs) noexcept
{
    refill(nowMs);
    return m_tokens >= m_burst;
}

void TokenBucketMs::refund() noexcept
{
    m_tokens = std::min(m_burst, m_tokens + 1.0);
}

TakeResult RateBucket::take(std::int64_t nowMs)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_bucket.take(nowMs);
}

KeyedRateLimiter::KeyedRateLimiter(const Rate &rate, std::int64_t burst, std::size_t sweepEvery) :
    m_rate(rate), m_burst(burst), m_sweepEvery(std::max<std::size_t>(1, sweepEvery))
{
}

TakeResult KeyedRateLimiter::take(const std::string &key, std::int64_t nowMs)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (++m_sinceSweep >= m_sweepEvery)
    {
        m_sinceSweep = 0;
        for (auto it = m_buckets.begin(); it != m_buckets.end();)
        {
            it = it->second.full(nowMs) ? m_buckets.erase(it) : std::next(it);
        }
    }
    auto it = m_buckets.find(key);
    if (it == m_buckets.end())
    {
        it = m_buckets.emplace(key, TokenBucketMs(m_rate.count, m_rate.periodMs, m_burst)).first;
    }
    return it->second.take(nowMs);
}

void KeyedRateLimiter::refund(const std::string &key, std::int64_t nowMs)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto it = m_buckets.find(key);
    if (it == m_buckets.end())
    {
        return;
    }
    it->second.refund();
    if (it->second.full(nowMs))
    {
        m_buckets.erase(it);
    }
}

std::size_t KeyedRateLimiter::keys()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_buckets.size();
}

ip::address normalized(const ip::address &a)
{
    if (a.is_v6() && a.to_v6().is_v4_mapped())
    {
        return ip::make_address_v4(ip::v4_mapped, a.to_v6());
    }
    return a;
}

std::string budgetKey(const std::string &source, int v6PrefixBits)
{
    const auto a = parseAddress(source);
    if (!a || !a->is_v6() || v6PrefixBits >= 128)
    {
        return source;
    }
    auto bytes = a->to_v6().to_bytes();
    int left = std::max(0, v6PrefixBits);
    for (auto &b : bytes)
    {
        b = left >= 8 ? b : static_cast<unsigned char>(left <= 0 ? 0 : b & (0xFF << (8 - left)));
        left -= 8;
    }
    return ip::address_v6(bytes).to_string() + "/" + std::to_string(std::max(0, v6PrefixBits));
}

std::optional<ip::address> parseAddress(std::string_view text)
{
    boost::system::error_code ec;
    const ip::address a = ip::make_address(std::string(text), ec);
    if (ec)
    {
        return std::nullopt;
    }
    return normalized(a);
}

SourceAddress::SourceAddress(const std::vector<std::string> &proxies)
{
    for (const std::string &p : proxies)
    {
        const std::size_t slash = p.find('/');
        const auto addr = parseAddress(p.substr(0, slash));
        if (!addr)
        {
            continue; // the settings parser refused it already
        }
        if (addr->is_v4())
        {
            const int bits = std::string::npos == slash ? 32 : std::stoi(p.substr(slash + 1));
            const std::uint32_t mask = 0 == bits ? 0u : ~0u << (32 - bits);
            m_v4.push_back(Net4{ addr->to_v4().to_uint() & mask, mask });
        }
        else
        {
            const int bits = std::string::npos == slash ? 128 : std::stoi(p.substr(slash + 1));
            m_v6.push_back(Net6{ addr->to_v6().to_bytes(), bits });
        }
    }
}

bool SourceAddress::trusted(const ip::address &raw) const
{
    const ip::address a = normalized(raw);
    if (a.is_v4())
    {
        const std::uint32_t v = a.to_v4().to_uint();
        for (const Net4 &n : m_v4)
        {
            if ((v & n.mask) == n.net)
            {
                return true;
            }
        }
        return false;
    }
    const auto bytes = a.to_v6().to_bytes();
    for (const Net6 &n : m_v6)
    {
        int left = n.bits;
        bool match = true;
        for (std::size_t i = 0; i < 16 && left > 0 && match; ++i, left -= 8)
        {
            const unsigned char mask = left >= 8 ? 0xFF : static_cast<unsigned char>(0xFF << (8 - left));
            match = (bytes[i] & mask) == (n.net[i] & mask);
        }
        if (match)
        {
            return true;
        }
    }
    return false;
}

std::string SourceAddress::resolve(const ip::address &peerRaw, const std::vector<std::string_view> &headerValues) const
{
    const ip::address peer = normalized(peerRaw);
    if (!enabled() || !trusted(peer))
    {
        return peer.to_string();
    }
    std::vector<std::string_view> hops;
    for (std::string_view value : headerValues)
    {
        std::size_t start = 0;
        for (;;)
        {
            const std::size_t comma = value.find(',', start);
            std::string_view item = value.substr(start, std::string_view::npos == comma ? std::string_view::npos : comma - start);
            while (!item.empty() && (' ' == item.front() || '\t' == item.front()))
            {
                item.remove_prefix(1);
            }
            while (!item.empty() && (' ' == item.back() || '\t' == item.back()))
            {
                item.remove_suffix(1);
            }
            hops.push_back(item);
            if (std::string_view::npos == comma)
            {
                break;
            }
            start = comma + 1;
        }
    }
    std::string lastTrusted = peer.to_string();
    for (auto it = hops.rbegin(); it != hops.rend(); ++it)
    {
        const auto a = parseAddress(*it);
        if (!a)
        {
            return lastTrusted;
        }
        if (!trusted(*a))
        {
            return a->to_string();
        }
        lastTrusted = a->to_string();
    }
    return lastTrusted;
}

}}
