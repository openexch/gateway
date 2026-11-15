#include "gateway/common/WorkPool.h"

#include <pthread.h>

#include <chrono>
#include <cstdio>

namespace openexchange { namespace gateway {

WorkPool::WorkPool(std::string namePrefix, int threads, std::size_t queueCapacity) :
    m_prefix(std::move(namePrefix)), m_threadCount(threads), m_capacity(queueCapacity)
{
}

WorkPool::~WorkPool()
{
    stop(2000);
}

void WorkPool::start()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_threads.empty())
    {
        return;
    }
    for (int i = 0; i < m_threadCount; ++i)
    {
        m_threads.emplace_back([this, i] { run(i); });
        ++m_running;
    }
}

bool WorkPool::trySubmit(Job job)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_stopping || m_jobs.size() >= m_capacity)
        {
            ++m_refused;
            return false;
        }
        m_jobs.push_back(std::move(job));
    }
    m_cv.notify_one();
    return true;
}

void WorkPool::run(int worker)
{
    char name[16];
    std::snprintf(name, sizeof(name), "%s-%d", m_prefix.c_str(), worker);
    pthread_setname_np(pthread_self(), name);
    for (;;)
    {
        Job job;
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait(lock, [this] { return m_stopping || !m_jobs.empty(); });
            if (m_stopping)
            {
                break;
            }
            job = std::move(m_jobs.front());
            m_jobs.pop_front();
        }
        job(worker);
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_completed;
    }
    std::lock_guard<std::mutex> lock(m_mutex);
    --m_running;
    m_done.notify_all();
}

bool WorkPool::stop(std::int64_t boundMs)
{
    std::vector<std::thread> threads;
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_stopping = true;
        m_jobs.clear();
        threads.swap(m_threads);
        m_cv.notify_all();
        if (!m_done.wait_for(lock, std::chrono::milliseconds(boundMs), [this] { return 0 == m_running; }))
        {
            for (std::thread &t : threads)
            {
                t.detach();
            }
            return false;
        }
    }
    for (std::thread &t : threads)
    {
        t.join();
    }
    return true;
}

std::size_t WorkPool::queued() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_jobs.size();
}

std::uint64_t WorkPool::refused() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_refused;
}

std::uint64_t WorkPool::completed() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_completed;
}

}}
