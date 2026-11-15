#ifndef GATEWAY_COMMON_RATE_LIMIT_H
#define GATEWAY_COMMON_RATE_LIMIT_H

#include <array>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/asio/ip/address.hpp>

#include "gateway/common/SettingsReader.h"

// RateLimit - the request budgets of the I/O threads (never the duty thread): token buckets with the settings' rate
// syntax (<n>/s, <n>/min) and a burst, one shared bucket (RateBucket) or one per key (KeyedRateLimiter, e.g. per
// source address). A refused take says how long until the next token (Retry-After). Time is the caller's monotonic ms.
// SourceAddress decides the key of the per-source budget: the socket peer, unless the peer is a trusted proxy.

namespace openexchange { namespace gateway {

struct TakeResult
{
    bool ok = false;
    std::int64_t retryAfterMs = 0; // refused: time until one token is back
};

// Not thread-safe; RateBucket and KeyedRateLimiter lock around it.
class TokenBucketMs
{
public:
    TokenBucketMs(std::int64_t count, std::int64_t periodMs, std::int64_t burst) noexcept;
    TakeResult take(std::int64_t nowMs) noexcept;
    bool full(std::int64_t nowMs) noexcept; // refilled to the burst (equal to a fresh bucket)
    void refund() noexcept;                 // gives back one taken token (never above the burst)

private:
    void refill(std::int64_t nowMs) noexcept;
    double m_perMs;
    double m_burst;
    double m_tokens;
    std::int64_t m_lastMs = 0;
    bool m_started = false;
};

class RateBucket
{
public:
    RateBucket(const Rate &rate, std::int64_t burst) : m_bucket(rate.count, rate.periodMs, burst) {}
    TakeResult take(std::int64_t nowMs);

private:
    std::mutex m_mutex;
    TokenBucketMs m_bucket;
};

// One bucket per key, created full on first use. A bucket that has refilled to its burst is the same as a fresh one,
// so it is dropped at the next sweep (every `sweepEvery` takes): the map holds only keys active inside one refill
// period, never a fixed count.
class KeyedRateLimiter
{
public:
    KeyedRateLimiter(const Rate &rate, std::int64_t burst, std::size_t sweepEvery = 1024);
    TakeResult take(const std::string &key, std::int64_t nowMs);
    // Gives back the token a take of `key` spent (the request was refused by another budget); a bucket that is full
    // again is dropped at once, so a refused request leaves no key behind.
    void refund(const std::string &key, std::int64_t nowMs);
    std::size_t keys();

private:
    std::mutex m_mutex;
    Rate m_rate;
    std::int64_t m_burst;
    std::size_t m_sweepEvery;
    std::size_t m_sinceSweep = 0;
    std::unordered_map<std::string, TokenBucketMs> m_buckets;
};

// The source address of a request: the socket peer; when the peer is inside one of the trusted proxy networks,
// the right-most address of the client-IP header (every occurrence, comma lists joined in order) that is not itself a
// trusted proxy; when every listed address is trusted, the left-most one. A header entry that is not an IP address
// ends the walk at the last trusted hop (the peer, or the hop to its right). With no trusted proxy the header is never
// read. IPv4-mapped IPv6 addresses are compared and reported as IPv4.
class SourceAddress
{
public:
    // `proxies`: IP addresses or CIDR blocks, as validated by the settings parser.
    explicit SourceAddress(const std::vector<std::string> &proxies);
    bool trusted(const boost::asio::ip::address &a) const;
    std::string resolve(const boost::asio::ip::address &peer, const std::vector<std::string_view> &headerValues) const;
    bool enabled() const noexcept { return !m_v4.empty() || !m_v6.empty(); }

private:
    struct Net4
    {
        std::uint32_t net;
        std::uint32_t mask;
    };
    struct Net6
    {
        std::array<unsigned char, 16> net;
        int bits;
    };
    std::vector<Net4> m_v4;
    std::vector<Net6> m_v6;
};

// The budget key of a source address: an IPv4 address as is; an IPv6 address cut to its first `v6PrefixBits` bits
// ("2001:db8:1:2::/64"), since one subscriber usually holds a whole /64 and would otherwise get a bucket per address.
std::string budgetKey(const std::string &source, int v6PrefixBits);

// The address with IPv4-mapped IPv6 turned into IPv4.
boost::asio::ip::address normalized(const boost::asio::ip::address &a);
std::optional<boost::asio::ip::address> parseAddress(std::string_view text);

}}

#endif
