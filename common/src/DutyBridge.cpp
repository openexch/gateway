#include "gateway/common/DutyBridge.h"

#include <pthread.h>

#include <chrono>
#include <cstdio>

#include "gateway/common/FrontLog.h"

namespace openexchange { namespace gateway {

DutyBridge::DutyBridge(std::size_t commandRing, std::size_t maxInflight, std::string threadPrefix) :
    m_commands(commandRing), m_completions(commandRing + maxInflight), m_threadPrefix(std::move(threadPrefix))
{
}

DutyBridge::~DutyBridge()
{
    stopCompletions(2000);
}

bool DutyBridge::submit(session::Command command, Callback done)
{
    const std::uint64_t id = m_nextId.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(m_callbacksMutex);
        m_callbacks.emplace(id, std::move(done));
    }
    Request r;
    r.id = id;
    r.command = std::move(command);
    if (!m_commands.tryPush(std::move(r)))
    {
        std::lock_guard<std::mutex> lock(m_callbacksMutex);
        m_callbacks.erase(id);
        m_refused.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void DutyBridge::complete(std::uint64_t id, session::Outcome outcome)
{
    Completion c;
    c.id = id;
    c.outcome = std::move(outcome);
    if (!m_completions.tryPush(std::move(c)))
    {
        m_dropped.fetch_add(1, std::memory_order_relaxed);
    }
    m_signal.fetch_add(1, std::memory_order_release);
    m_signal.notify_one();
}

void DutyBridge::startCompletions()
{
    if (m_thread.joinable())
    {
        return;
    }
    m_thread = std::thread([this] { runCompletions(); });
}

void DutyBridge::runCompletions()
{
    char name[16];
    std::snprintf(name, sizeof(name), "%s-done", m_threadPrefix.c_str());
    pthread_setname_np(pthread_self(), name);
    std::uint64_t droppedSeen = 0;
    while (!m_stop.load(std::memory_order_acquire))
    {
        const std::uint32_t seen = m_signal.load(std::memory_order_acquire);
        Completion c;
        int handled = 0;
        while (m_completions.tryPop(c))
        {
            Callback cb;
            {
                std::lock_guard<std::mutex> lock(m_callbacksMutex);
                const auto it = m_callbacks.find(c.id);
                if (it != m_callbacks.end())
                {
                    cb = std::move(it->second);
                    m_callbacks.erase(it);
                }
            }
            if (cb)
            {
                cb(c.outcome);
            }
            ++handled;
        }
        const std::uint64_t dropped = m_dropped.load(std::memory_order_relaxed);
        if (dropped != droppedSeen)
        {
            frontLog(FrontLevel::CRITICAL, "duty bridge: %llu outcome(s) found the completion ring full (a defect: it "
                "holds the command ring plus the in-flight bound)", static_cast<unsigned long long>(dropped - droppedSeen));
            droppedSeen = dropped;
        }
        if (0 == handled)
        {
            m_signal.wait(seen, std::memory_order_acquire);
        }
    }
    m_finished.store(true, std::memory_order_release);
}

bool DutyBridge::stopCompletions(std::int64_t boundMs)
{
    if (!m_thread.joinable())
    {
        return true;
    }
    m_stop.store(true, std::memory_order_release);
    m_signal.fetch_add(1, std::memory_order_release);
    m_signal.notify_one();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (!m_finished.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (!m_finished.load(std::memory_order_acquire))
    {
        m_thread.detach();
        return false;
    }
    m_thread.join();
    return true;
}

}}
