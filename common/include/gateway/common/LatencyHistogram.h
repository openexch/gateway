#ifndef GATEWAY_COMMON_LATENCY_HISTOGRAM_H
#define GATEWAY_COMMON_LATENCY_HISTOGRAM_H

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

// LatencyHistogram - a fixed-bucket latency histogram for the Prometheus text format, plus a recent maximum.
//
// Buckets. 1, 1.5, 2, 3, 5 and 7 per decade from 100 us to 7 s, then 10 s and +Inf: 31 finite bounds, 32 counters. A
// value goes into the first bucket whose bound is at or above it (Prometheus `le`): exactly 1 ms is in le="0.001".
// Values are nanoseconds of one monotonic clock; a negative value is recorded as 0 (callers count it as an anomaly
// before they get here).
//
// Threads. record() is safe from any number of threads at once: relaxed fetch_add on one bucket counter and on the sum,
// and a compare-and-swap loop on one window slot. No lock, no allocation. Rendering reads the counters without a lock:
// a scrape that runs during a record can see the bucket and not yet the sum (or the reverse); `_count` is derived
// from the same bucket snapshot as the cumulative buckets, so `_count` always equals the +Inf bucket.
//
// Window maximum. WindowMax keeps the largest value of each steady-clock second in a ring of 16 slots (slot = second mod
// 16). Each slot is one 64-bit atomic word: the second it belongs to (32 bits) and the value in microseconds, rounded
// up (32 bits, clamped at about 71 minutes). A writer raises its slot with compare-and-swap; a slot that holds an older
// second is overwritten, and a slot that holds a newer second than the writer's own (a writer delayed by 16 s or more)
// is left alone: that sample is older than the window anyway. A read takes the maximum of the slots whose tag is one of
// the last WINDOW_SECONDS seconds (the current one included) and changes nothing, so any number of scrapers see the
// same value and none resets it for another.
// Guarantee: a value recorded at time t (steady second S = floor(t)) is in every read made at a time r with
// floor(r) in [S, S + 9], an interval of more than 9 s that starts at t. So with scrapes at most 9 s apart, every
// recorded value is seen by at least one scrape. With longer scrape intervals (Prometheus defaults to 15 s) a short
// spike can fall between two scrapes; the histogram still counts it. A slot is reused only 16 s after its second, so
// it is never overwritten while it is still inside the window.

namespace openexchange { namespace gateway {

class WindowMax
{
public:
    static constexpr std::int64_t SLOTS = 16;
    static constexpr std::int64_t WINDOW_SECONDS = 10;
    static_assert(WINDOW_SECONDS < SLOTS, "a slot must not be reused while it is inside the window");

    // valueNs >= 0; nowNs is the steady clock of the caller (>= 0).
    void record(std::int64_t valueNs, std::int64_t nowNs) noexcept
    {
        if (valueNs < 0 || nowNs < 0)
        {
            return;
        }
        const std::int64_t second = nowNs / 1'000'000'000;
        const std::uint32_t tag = static_cast<std::uint32_t>(second);
        const std::uint64_t us64 = (static_cast<std::uint64_t>(valueNs) + 999) / 1000;
        const std::uint32_t us = static_cast<std::uint32_t>(std::min<std::uint64_t>(us64, UINT32_MAX));
        const std::uint64_t mine = pack(tag, us);
        std::atomic<std::uint64_t> &slot = m_slots[static_cast<std::size_t>(second % SLOTS)];
        std::uint64_t cur = slot.load(std::memory_order_relaxed);
        for (;;)
        {
            if (0 != cur)
            {
                const std::uint32_t curTag = tagOf(cur);
                if (curTag == tag)
                {
                    if (valueOf(cur) >= us)
                    {
                        return;
                    }
                }
                else if (static_cast<std::int32_t>(curTag - tag) > 0)
                {
                    return; // the slot already holds a newer second: this sample is older than the window
                }
            }
            if (slot.compare_exchange_weak(cur, mine, std::memory_order_relaxed, std::memory_order_relaxed))
            {
                return;
            }
        }
    }

    // The largest value (ns, a multiple of 1000) recorded in the last WINDOW_SECONDS seconds as of nowNs; 0 when none.
    std::int64_t maxNs(std::int64_t nowNs) const noexcept
    {
        if (nowNs < 0)
        {
            return 0;
        }
        const std::int64_t now = nowNs / 1'000'000'000;
        std::uint32_t best = 0;
        for (std::int64_t k = 0; k < WINDOW_SECONDS && now - k >= 0; ++k)
        {
            const std::int64_t second = now - k;
            const std::uint64_t w = m_slots[static_cast<std::size_t>(second % SLOTS)].load(std::memory_order_relaxed);
            if (0 != w && tagOf(w) == static_cast<std::uint32_t>(second))
            {
                best = std::max(best, valueOf(w));
            }
        }
        return static_cast<std::int64_t>(best) * 1000;
    }

private:
    static constexpr std::uint64_t pack(std::uint32_t tag, std::uint32_t us) noexcept
    {
        return (static_cast<std::uint64_t>(tag) << 32) | us;
    }
    static constexpr std::uint32_t tagOf(std::uint64_t w) noexcept { return static_cast<std::uint32_t>(w >> 32); }
    static constexpr std::uint32_t valueOf(std::uint64_t w) noexcept { return static_cast<std::uint32_t>(w); }

    std::array<std::atomic<std::uint64_t>, SLOTS> m_slots{}; // 0 = never written
};

class LatencyHistogram
{
public:
    static constexpr std::size_t FINITE = 31;
    static constexpr std::size_t BUCKETS = FINITE + 1; // the last one is +Inf

    static constexpr std::array<std::int64_t, FINITE> BOUNDS_NS = {
        100'000, 150'000, 200'000, 300'000, 500'000, 700'000,
        1'000'000, 1'500'000, 2'000'000, 3'000'000, 5'000'000, 7'000'000,
        10'000'000, 15'000'000, 20'000'000, 30'000'000, 50'000'000, 70'000'000,
        100'000'000, 150'000'000, 200'000'000, 300'000'000, 500'000'000, 700'000'000,
        1'000'000'000, 1'500'000'000, 2'000'000'000, 3'000'000'000, 5'000'000'000, 7'000'000'000,
        10'000'000'000,
    };
    // the `le` label of each bucket, in seconds, written out so no float formatting is involved
    static constexpr std::array<std::string_view, BUCKETS> LE = {
        "0.0001", "0.00015", "0.0002", "0.0003", "0.0005", "0.0007",
        "0.001", "0.0015", "0.002", "0.003", "0.005", "0.007",
        "0.01", "0.015", "0.02", "0.03", "0.05", "0.07",
        "0.1", "0.15", "0.2", "0.3", "0.5", "0.7",
        "1", "1.5", "2", "3", "5", "7",
        "10", "+Inf",
    };

    static std::size_t bucketOf(std::int64_t valueNs) noexcept
    {
        return static_cast<std::size_t>(
            std::lower_bound(BOUNDS_NS.begin(), BOUNDS_NS.end(), valueNs) - BOUNDS_NS.begin());
    }

    // valueNs: the latency; nowNs: the caller's steady clock (the window maximum's second).
    void record(std::int64_t valueNs, std::int64_t nowNs) noexcept
    {
        const std::int64_t v = valueNs < 0 ? 0 : valueNs;
        m_counts[bucketOf(v)].fetch_add(1, std::memory_order_relaxed);
        m_sumNs.fetch_add(static_cast<std::uint64_t>(v), std::memory_order_relaxed);
        m_window.record(v, nowNs);
    }

    struct Snapshot
    {
        std::array<std::uint64_t, BUCKETS> counts{}; // per bucket, not cumulative
        std::uint64_t sumNs = 0;
        std::uint64_t count = 0; // the sum of counts
    };
    Snapshot snapshot() const noexcept
    {
        Snapshot s;
        for (std::size_t i = 0; i < BUCKETS; ++i)
        {
            s.counts[i] = m_counts[i].load(std::memory_order_relaxed);
            s.count += s.counts[i];
        }
        s.sumNs = m_sumNs.load(std::memory_order_relaxed);
        return s;
    }

    std::int64_t windowMaxNs(std::int64_t nowNs) const noexcept { return m_window.maxNs(nowNs); }

    // `name`_bucket{`labels`,le="..."} (cumulative), `name`_sum (seconds) and `name`_count. `labels` is the label list
    // without braces, never empty.
    void render(std::string &out, std::string_view name, std::string_view labels) const
    {
        const Snapshot s = snapshot();
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < BUCKETS; ++i)
        {
            cumulative += s.counts[i];
            out.append(name).append("_bucket{").append(labels).append(",le=\"").append(LE[i]).append("\"} ")
                .append(std::to_string(cumulative)).append("\n");
        }
        out.append(name).append("_sum{").append(labels).append("} ").append(seconds(s.sumNs, 1'000'000'000, 9))
            .append("\n");
        out.append(name).append("_count{").append(labels).append("} ").append(std::to_string(s.count)).append("\n");
    }

    // `name`{`labels`} <window maximum in seconds>
    void renderWindowMax(std::string &out, std::string_view name, std::string_view labels, std::int64_t nowNs) const
    {
        out.append(name).append("{").append(labels).append("} ")
            .append(seconds(static_cast<std::uint64_t>(windowMaxNs(nowNs)), 1'000'000'000, 9)).append("\n");
    }

    // an integer count of `perSecond` units as decimal seconds with `digits` fraction digits, exactly
    static std::string seconds(std::uint64_t units, std::uint64_t perSecond, int digits)
    {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "%llu.%0*llu", static_cast<unsigned long long>(units / perSecond), digits,
            static_cast<unsigned long long>(units % perSecond));
        return buf;
    }

private:
    std::array<std::atomic<std::uint64_t>, BUCKETS> m_counts{};
    std::atomic<std::uint64_t> m_sumNs{ 0 };
    WindowMax m_window;
};

}}

#endif
