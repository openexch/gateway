// LatencyHistogram and WindowMax (common/include/gateway/common/LatencyHistogram.h): bucket edges, the cumulative
// Prometheus rendering, sum and count, the window maximum under two writer threads and a moving clock (the clock is
// the caller's argument), stale slots, and the scrape guarantee (every value is seen by one scrape when scrapes are at
// most 9 s apart).
#include <algorithm>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "Check.h"
#include "gateway/common/LatencyHistogram.h"

using namespace openexchange::gateway;

namespace {

constexpr std::int64_t MS = 1'000'000;
constexpr std::int64_t SEC = 1'000'000'000;

}

TEST(latency_bucket_edges)
{
    using H = LatencyHistogram;
    CHECK(0 == H::bucketOf(0));
    CHECK(0 == H::bucketOf(99'999));      // below the first bound
    CHECK(0 == H::bucketOf(100'000));     // exactly on the first bound: le="0.0001"
    CHECK(1 == H::bucketOf(100'001));
    CHECK(1 == H::bucketOf(150'000));
    CHECK(6 == H::bucketOf(1 * MS));      // exactly 1 ms: le="0.001"
    CHECK(7 == H::bucketOf(1 * MS + 1));
    CHECK(16 == H::bucketOf(50 * MS));
    CHECK(30 == H::bucketOf(10 * SEC));   // exactly on the last finite bound
    CHECK(31 == H::bucketOf(10 * SEC + 1)); // above the last: +Inf
    CHECK(31 == H::bucketOf(INT64_MAX));
    CHECK(H::LE[0] == "0.0001" && H::LE[6] == "0.001" && H::LE[30] == "10" && H::LE[31] == "+Inf");
    // every bound is the 1-1.5-2-3-5-7 step of its decade, and LE spells the same value
    for (std::size_t i = 0; i < H::FINITE; ++i)
    {
        const double fromLabel = std::stod(std::string(H::LE[i]));
        CHECK_MSG(static_cast<std::int64_t>(fromLabel * 1e9 + 0.5) == H::BOUNDS_NS[i], std::string(H::LE[i]));
        if (i > 0)
        {
            CHECK(H::BOUNDS_NS[i] > H::BOUNDS_NS[i - 1]);
        }
    }
}

TEST(latency_render_cumulative_sum_count)
{
    LatencyHistogram h;
    h.record(50'000, 0);        // le 0.0001
    h.record(100'000, 0);       // le 0.0001 (on the bound)
    h.record(1 * MS, 0);        // le 0.001
    h.record(31 * MS, 0);       // le 0.05
    h.record(11 * SEC, 0);      // +Inf
    h.record(-5, 0);            // a negative value is 0: le 0.0001
    const auto s = h.snapshot();
    CHECK(6 == s.count);
    CHECK(static_cast<std::uint64_t>(50'000 + 100'000 + MS + 31 * MS + 11 * SEC) == s.sumNs);
    std::string out;
    h.render(out, "x_seconds", "route=\"create\"");
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"0.0001\"} 3\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"0.00015\"} 3\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"0.001\"} 4\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"0.03\"} 4\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"0.05\"} 5\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"10\"} 5\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_bucket{route=\"create\",le=\"+Inf\"} 6\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_sum{route=\"create\"} 11.032150000\n") != std::string::npos, out);
    CHECK_MSG(out.find("x_seconds_count{route=\"create\"} 6\n") != std::string::npos, out);
    // every cumulative line is the running sum of the per-bucket counts, and never decreases
    std::uint64_t running = 0;
    for (std::size_t i = 0; i < LatencyHistogram::BUCKETS; ++i)
    {
        running += s.counts[i];
        const std::string line = "x_seconds_bucket{route=\"create\",le=\"" + std::string(LatencyHistogram::LE[i]) +
            "\"} " + std::to_string(running) + "\n";
        CHECK_MSG(out.find(line) != std::string::npos, line);
    }
    CHECK(running == s.count);
    CHECK(LatencyHistogram::BUCKETS + 2 == static_cast<std::size_t>(std::count(out.begin(), out.end(), '\n')));
    CHECK(LatencyHistogram::seconds(0, 1'000'000'000, 9) == "0.000000000");
    CHECK(LatencyHistogram::seconds(1'234'567'891, 1'000'000'000, 9) == "1.234567891");
}

TEST(latency_window_max_basic_and_stale_slots)
{
    WindowMax w;
    const std::int64_t t = 100 * SEC + 300 * MS;
    CHECK(0 == w.maxNs(t));
    w.record(5 * MS + 1, t);                     // rounded up to the microsecond
    CHECK(5 * MS + 1000 == w.maxNs(t));
    w.record(2 * MS, t + 10 * MS);               // smaller: no change
    CHECK(5 * MS + 1000 == w.maxNs(t));
    CHECK(5 * MS + 1000 == w.maxNs(109 * SEC + 999 * MS)); // second 109 still covers second 100
    CHECK(0 == w.maxNs(110 * SEC));               // second 110 covers 101..110
    // a slot holding second 100 is read only as second 100: at second 116 (the same slot) it is stale
    CHECK(0 == w.maxNs(116 * SEC + 500 * MS));
    // a write at second 116 replaces the stale second 100 in that slot (a smaller value wins over the stale one)
    w.record(1 * MS, 116 * SEC);
    CHECK(1 * MS == w.maxNs(116 * SEC));
    // a writer whose clock is 16 s behind the slot's second (descheduled) leaves the newer second alone
    w.record(9 * MS, 100 * SEC);
    CHECK(1 * MS == w.maxNs(116 * SEC));
    // the maximum of several seconds inside the window
    w.record(3 * MS, 118 * SEC);
    w.record(7 * MS, 120 * SEC);
    CHECK(7 * MS == w.maxNs(125 * SEC));
    CHECK(7 * MS == w.maxNs(129 * SEC));
    CHECK(0 == w.maxNs(130 * SEC));
    // reading changes nothing: a second scraper sees the same value
    CHECK(7 * MS == w.maxNs(125 * SEC) && 7 * MS == w.maxNs(125 * SEC));
    // a value above 32 bits of microseconds is clamped, not wrapped
    WindowMax big;
    big.record(INT64_MAX, 5 * SEC);
    CHECK(static_cast<std::int64_t>(UINT32_MAX) * 1000 == big.maxNs(5 * SEC));
}

// Two writers record into the same slots at once while the clock moves 700 ms per step (seconds are skipped and
// repeated); after each step the window maximum equals the largest value either writer recorded in the last 10
// seconds, through more than two turns of the 16-slot ring.
TEST(latency_window_max_two_writers_moving_clock)
{
    WindowMax w;
    LatencyHistogram h;
    std::atomic<std::int64_t> clock{ 1000 * SEC };
    constexpr int STEPS = 60;
    constexpr int PER_STEP = 3000;
    std::barrier sync(3);
    std::map<std::int64_t, std::int64_t> maxBySecond[2];
    auto writer = [&](int id) {
        std::uint64_t x = 12345 + static_cast<std::uint64_t>(id);
        for (int step = 0; step < STEPS; ++step)
        {
            sync.arrive_and_wait(); // the clock is set
            for (int i = 0; i < PER_STEP; ++i)
            {
                x = x * 6364136223846793005ULL + 1442695040888963407ULL;
                const std::int64_t now = clock.load(std::memory_order_relaxed);
                // whole microseconds; each second has its own scale, so a window one second too wide or too
                // narrow reads another maximum
                const std::int64_t second = now / SEC;
                const std::int64_t value = ((second * 7919) % 97 + 1) * 100'000 +
                    static_cast<std::int64_t>((x >> 33) % 100) * 1000;
                w.record(value, now);
                h.record(value, now);
                std::int64_t &m = maxBySecond[id][now / SEC];
                m = std::max(m, value);
            }
            sync.arrive_and_wait(); // done with this step
        }
    };
    std::thread a(writer, 0);
    std::thread b(writer, 1);
    bool allMatch = true;
    for (int step = 0; step < STEPS; ++step)
    {
        clock.store(1000 * SEC + static_cast<std::int64_t>(step) * 700 * MS, std::memory_order_relaxed);
        sync.arrive_and_wait();
        sync.arrive_and_wait();
        const std::int64_t now = clock.load();
        std::int64_t expected = 0;
        for (std::int64_t s = now / SEC - 9; s <= now / SEC; ++s)
        {
            for (const auto &byThread : maxBySecond)
            {
                const auto it = byThread.find(s);
                if (it != byThread.end())
                {
                    expected = std::max(expected, it->second);
                }
            }
        }
        if (expected != w.maxNs(now) || expected != h.windowMaxNs(now))
        {
            allMatch = false;
            CHECK_MSG(false, "step " + std::to_string(step) + ": window max " + std::to_string(w.maxNs(now)) +
                " expected " + std::to_string(expected));
        }
    }
    a.join();
    b.join();
    CHECK(allMatch);
    CHECK(static_cast<std::uint64_t>(2 * STEPS * PER_STEP) == h.snapshot().count);
}

// The guarantee: a value recorded at t is in every read at r with floor(r) in [floor(t), floor(t) + 9], so scrapes at
// most 9 s apart (any phase) always see it; 10 s apart can miss it.
TEST(latency_window_max_scrape_guarantee)
{
    auto seenByOneScrape = [](std::int64_t spikeAt, std::int64_t phase, std::int64_t interval) {
        WindowMax w;
        w.record(5 * MS, spikeAt);
        // the first scrape at or after the spike, then every `interval`, for longer than the window
        std::int64_t r = phase;
        while (r < spikeAt)
        {
            r += interval;
        }
        for (; r < spikeAt + 30 * SEC; r += interval)
        {
            if (w.maxNs(r) >= 5 * MS)
            {
                return true;
            }
        }
        return false;
    };
    bool nineAlways = true;
    bool tenMissed = false;
    for (std::int64_t spike = 100 * SEC; spike < 101 * SEC; spike += 50 * MS)
    {
        for (std::int64_t phase = 0; phase < 10 * SEC; phase += 100 * MS)
        {
            nineAlways = nineAlways && seenByOneScrape(spike, phase, 9 * SEC);
            tenMissed = tenMissed || !seenByOneScrape(spike, phase, 10 * SEC);
        }
    }
    CHECK_MSG(nineAlways, "a spike was missed by scrapes 9 s apart");
    CHECK_MSG(tenMissed, "the stated bound is not tight: no spike was missed at 10 s");
    // the edge itself: recorded at 100.999 s, still seen at 109.999 s, gone at 110.0 s
    WindowMax w;
    w.record(5 * MS, 100 * SEC + 999 * MS);
    CHECK(5 * MS == w.maxNs(109 * SEC + 999 * MS));
    CHECK(0 == w.maxNs(110 * SEC));
}
