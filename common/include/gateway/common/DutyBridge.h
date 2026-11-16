#ifndef GATEWAY_COMMON_DUTY_BRIDGE_H
#define GATEWAY_COMMON_DUTY_BRIDGE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "gateway/common/SessionParts.h"

// DutyBridge - the only path between the threads around the duty thread and the core session:
//
//   I/O side -> duty: submit() puts one command into a bounded lock-free ring (GATEWAY_COMMAND_RING); a full ring is
//     refused at once (the caller answers 503 OVERLOADED with Retry-After). The duty thread drains the ring every
//     cycle into CoreSession::submit; it never waits for the ring and never takes a lock here.
//   duty -> I/O side: the duty thread pushes each outcome into a second lock-free ring and wakes the completion thread
//     (<prefix>-done) with an atomic notify (no file descriptor, no socket, no lock on the duty thread). The
//     completion thread runs the callback the command was submitted with; a callback hands its answer to an I/O
//     strand and returns.
// The completion ring holds GATEWAY_COMMAND_RING + GATEWAY_MAX_INFLIGHT outcomes: every command is either in the
// command ring or held by the session (at most maxInflight), so it cannot overflow; a push that still found it full
// would be a defect, counted as dropped and logged by the completion side. With the treasury lane on, two sessions hold
// requests: the owner sizes maxInflight for both.
//
// The duty thread also publishes, for the I/O side to read without a lock: the core config its session holds
// (publishConfig, the same immutable snapshot the session's ConfigCache publishes) and whether the session is
// registered (publishRegistered).

namespace openexchange { namespace gateway {

// Vyukov's bounded multi-producer multi-consumer queue, any capacity >= 1.
template<typename T>
class MpmcRing
{
public:
    explicit MpmcRing(std::size_t capacity) : m_capacity(capacity < 1 ? 1 : capacity), m_cells(m_capacity)
    {
        for (std::size_t i = 0; i < m_capacity; ++i)
        {
            m_cells[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    bool tryPush(T &&value)
    {
        std::size_t pos = m_tail.load(std::memory_order_relaxed);
        for (;;)
        {
            Cell &cell = m_cells[pos % m_capacity];
            const std::size_t seq = cell.sequence.load(std::memory_order_acquire);
            const std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);
            if (0 == diff)
            {
                if (m_tail.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                {
                    cell.value = std::move(value);
                    cell.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            }
            else if (diff < 0)
            {
                return false;
            }
            else
            {
                pos = m_tail.load(std::memory_order_relaxed);
            }
        }
    }

    bool tryPop(T &out)
    {
        std::size_t pos = m_head.load(std::memory_order_relaxed);
        for (;;)
        {
            Cell &cell = m_cells[pos % m_capacity];
            const std::size_t seq = cell.sequence.load(std::memory_order_acquire);
            const std::intptr_t diff = static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);
            if (0 == diff)
            {
                if (m_head.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                {
                    out = std::move(cell.value);
                    cell.value = T{};
                    cell.sequence.store(pos + m_capacity, std::memory_order_release);
                    return true;
                }
            }
            else if (diff < 0)
            {
                return false;
            }
            else
            {
                pos = m_head.load(std::memory_order_relaxed);
            }
        }
    }

    std::size_t sizeApprox() const noexcept
    {
        const std::size_t t = m_tail.load(std::memory_order_relaxed);
        const std::size_t h = m_head.load(std::memory_order_relaxed);
        return t > h ? t - h : 0;
    }
    std::size_t capacity() const noexcept { return m_capacity; }

private:
    struct Cell
    {
        std::atomic<std::size_t> sequence{ 0 };
        T value{};
    };
    std::size_t m_capacity;
    std::vector<Cell> m_cells;
    alignas(64) std::atomic<std::size_t> m_tail{ 0 };
    alignas(64) std::atomic<std::size_t> m_head{ 0 };
};

class DutyBridge
{
public:
    using Callback = std::function<void(const session::Outcome &)>;

    struct Request
    {
        std::uint64_t id = 0;
        session::Command command;
    };
    struct Completion
    {
        std::uint64_t id = 0;
        session::Outcome outcome;
    };

    DutyBridge(std::size_t commandRing, std::size_t maxInflight, std::string threadPrefix = "gw");
    ~DutyBridge();

    // ---- I/O side (any thread but the duty thread)
    // False when the command ring is full: nothing was queued and `done` will never run.
    bool submit(session::Command command, Callback done);
    void startCompletions();
    bool stopCompletions(std::int64_t boundMs);

    // ---- duty side (the duty thread only; never blocks)
    // Takes at most `max` commands; `sink(Request&&)` hands each to the session.
    template<typename Sink>
    int drain(Sink &&sink, int max)
    {
        int n = 0;
        Request r;
        while (n < max && m_commands.tryPop(r))
        {
            sink(std::move(r));
            ++n;
        }
        return n;
    }
    void complete(std::uint64_t id, session::Outcome outcome);

    // the duty thread publishes; any thread reads
    void publishConfig(std::shared_ptr<const session::CoreConfig> config)
    {
        if (config != m_config.load(std::memory_order_relaxed))
        {
            m_config.store(std::move(config), std::memory_order_release);
        }
    }
    std::shared_ptr<const session::CoreConfig> config() const { return m_config.load(std::memory_order_acquire); }
    void publishRegistered(bool registered) noexcept { m_registered.store(registered, std::memory_order_release); }
    bool registered() const noexcept { return m_registered.load(std::memory_order_acquire); }

    std::size_t commandsQueued() const noexcept { return m_commands.sizeApprox(); }
    std::uint64_t refused() const noexcept { return m_refused.load(std::memory_order_relaxed); }
    std::uint64_t dropped() const noexcept { return m_dropped.load(std::memory_order_relaxed); }

private:
    void runCompletions();

    MpmcRing<Request> m_commands;
    MpmcRing<Completion> m_completions;
    std::string m_threadPrefix;
    std::atomic<std::uint64_t> m_nextId{ 1 };
    std::mutex m_callbacksMutex;
    std::unordered_map<std::uint64_t, Callback> m_callbacks;
    std::atomic<std::uint32_t> m_signal{ 0 };
    std::atomic<bool> m_stop{ false };
    std::atomic<bool> m_finished{ false };
    std::thread m_thread;
    std::atomic<std::uint64_t> m_refused{ 0 };
    std::atomic<std::uint64_t> m_dropped{ 0 };
    std::atomic<std::shared_ptr<const session::CoreConfig>> m_config{ std::make_shared<const session::CoreConfig>() };
    std::atomic<bool> m_registered{ false };
};

}}

#endif
