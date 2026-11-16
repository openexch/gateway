#ifndef GATEWAY_COMMON_WORK_POOL_H
#define GATEWAY_COMMON_WORK_POOL_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// WorkPool - a fixed set of named threads over one bounded queue, for the slow work the I/O threads hand off: PBKDF2
// (gw-hash, GATEWAY_HASH_THREADS over GATEWAY_HASH_QUEUE) and the blocking PostgreSQL calls (gw-pg-auth,
// GATEWAY_PG_AUTH_POOL over GATEWAY_PG_QUEUE). trySubmit never blocks: a full queue is refused at once (the caller
// answers 503 AUTH_BUSY). A job runs with the index of its worker (a per-worker connection, for example). The duty
// thread never submits here and never waits on this pool.

namespace openexchange { namespace gateway {

class WorkPool
{
public:
    using Job = std::function<void(int worker)>;

    // `namePrefix` names the threads <prefix>-<n> (pthread names are cut to 15 characters).
    WorkPool(std::string namePrefix, int threads, std::size_t queueCapacity);
    ~WorkPool();
    WorkPool(const WorkPool &) = delete;
    WorkPool &operator=(const WorkPool &) = delete;

    void start();
    // False when the queue holds queueCapacity jobs (or the pool is stopping): the job was not taken.
    bool trySubmit(Job job);
    // Stops taking jobs, runs no queued job, joins every thread inside `boundMs`; false when one did not stop (it is
    // detached and the caller exits the process).
    bool stop(std::int64_t boundMs);

    std::size_t queued() const;
    std::size_t capacity() const noexcept { return m_capacity; }
    int threads() const noexcept { return m_threadCount; }
    std::uint64_t refused() const;
    std::uint64_t completed() const;

private:
    void run(int worker);

    std::string m_prefix;
    int m_threadCount;
    std::size_t m_capacity;
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    std::condition_variable m_done;
    std::deque<Job> m_jobs;
    std::vector<std::thread> m_threads;
    int m_running = 0;
    bool m_stopping = false;
    std::uint64_t m_refused = 0;
    std::uint64_t m_completed = 0;
};

}}

#endif
