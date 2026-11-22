#include "HistoryWriter.h"

#include <pthread.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "gateway/common/ExitCodes.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace {

std::string stateText(const MdState &s)
{
    return "epoch " + (s.epoch ? std::to_string(*s.epoch) : std::string("none")) + ", last_journal_seq " +
        std::to_string(s.lastSeq) + ", first_journal_seq " +
        (s.firstSeq ? std::to_string(*s.firstSeq) : std::string("none"));
}

}

int HistoryWriter::startCheck()
{
    std::string error;
    if (!connectHistory(m_conn, m_settings.pg, error))
    {
        std::fprintf(stderr, "oe-marketdata: market history unreachable at start: %s: the trade path waits for it (retried "
            "every %lld ms); the book path serves\n", error.c_str(), static_cast<long long>(m_settings.retryMs));
        m_ledger.setError(error);
        return 0;
    }
    if (!open())
    {
        return m_ledger.fatalExit.load(std::memory_order_acquire);
    }
    return 0;
}

bool HistoryWriter::open()
{
    std::string error;
    if (!m_conn.ok() && !connectHistory(m_conn, m_settings.pg, error))
    {
        failed("connect: " + error);
        return false;
    }
    MdState state;
    std::string detail;
    const StartVerdict v = startHistory(m_conn, m_settings.pg.statementTimeoutMs, state, detail);
    switch (v)
    {
        case StartVerdict::LEGACY:
            fatal(exit_code::CONFIG, detail);
            return false;
        case StartVerdict::LOCK_HELD:
            fatal(exit_code::TEMPFAIL, detail);
            return false;
        case StartVerdict::FOREIGN:
            fatal(exit_code::CONFIG, "the market history database holds something else: " + detail);
            return false;
        case StartVerdict::FAILED:
            m_conn.close();
            failed(detail);
            return false;
        case StartVerdict::READY:
            break;
    }
    m_ledger.connected.store(true, std::memory_order_release);
    if (!m_statePublished)
    {
        std::fprintf(stderr, "oe-marketdata: market history: %s; writer lock held; md_state: %s\n", detail.c_str(),
            stateText(state).c_str());
        m_ledger.publishState(state);
        m_statePublished = true;
    }
    m_lastOpen = state;
    return true;
}

void HistoryWriter::fatal(int code, const std::string &text)
{
    std::fprintf(stderr, "oe-marketdata: CRITICAL %s (exit %d)\n", text.c_str(), code);
    std::fflush(stderr);
    m_ledger.setError(text);
    m_ledger.writable.store(false, std::memory_order_release);
    m_ledger.fatalExit.store(code, std::memory_order_release);
}

void HistoryWriter::failed(const std::string &what)
{
    m_ledger.failures.fetch_add(1, std::memory_order_relaxed);
    m_ledger.setError(what);
    m_ledger.connected.store(m_conn.ok(), std::memory_order_release);
    m_wasDown = true;
    m_ledger.writable.store(false, std::memory_order_release);
    std::uint64_t held = 0;
    if (m_warn.take(5'000'000'000LL, held))
    {
        std::fprintf(stderr, "oe-marketdata: WARN market history: %s (%llu more held back); pending trades are kept and "
            "committed when it returns\n", what.c_str(), static_cast<unsigned long long>(held));
    }
}

std::function<void()> HistoryWriter::pauseHook(const CommitBatch &batch)
{
#ifdef MD_TC_COMMIT_PAUSE
    // test build only: a window between the statements and the COMMIT the tests kill the process in
    const char *text = std::getenv("MDTC_COMMIT_PAUSE_MS");
    const char *skipText = std::getenv("MDTC_COMMIT_PAUSE_SKIP"); // commits of this process that go through first
    const long long ms = nullptr == text ? 0 : std::atoll(text);
    const unsigned long long skip = nullptr == skipText ? 0 : std::strtoull(skipText, nullptr, 10);
    if (ms > 0 && m_ledger.commits.load(std::memory_order_relaxed) >= skip)
    {
        const std::int64_t from = batch.fromSeq + 1;
        const std::int64_t to = batch.toSeq;
        return [ms, from, to] {
            std::fprintf(stderr, "oe-marketdata: test build: commit of journalSeq %lld..%lld paused %lld ms before COMMIT\n",
                static_cast<long long>(from), static_cast<long long>(to), ms);
            std::fflush(stderr);
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        };
    }
#else
    (void)batch;
#endif
    return {};
}

bool HistoryWriter::resolveUncertain(const CommitBatch &batch)
{
    // the answer of a COMMIT was lost: the cursor says what happened
    while (!m_stop.load(std::memory_order_acquire))
    {
        if (open())
        {
            if (m_lastOpen.lastSeq == batch.toSeq)
            {
                m_ledger.committed(batch);
                return true;
            }
            if (m_lastOpen.lastSeq == batch.fromSeq)
            {
                return true; // not committed: the batch is taken again
            }
            fatal(exit_code::TEMPFAIL, std::string(ANOTHER_WRITER_TEXT) + " (after an uncertain COMMIT the cursor is " +
                std::to_string(m_lastOpen.lastSeq) + ", neither " + std::to_string(batch.fromSeq) + " nor " +
                std::to_string(batch.toSeq) + ")");
            return false;
        }
        if (0 != m_ledger.fatalExit.load(std::memory_order_acquire))
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(m_settings.retryMs));
    }
    return false;
}

void HistoryWriter::run()
{
    pthread_setname_np(pthread_self(), "md-pg");
    const std::size_t batchMax = static_cast<std::size_t>(m_settings.commitBatch);
    while (!m_stop.load(std::memory_order_acquire) && 0 == m_ledger.fatalExit.load(std::memory_order_acquire))
    {
        if (!m_conn.ok())
        {
            if (!open())
            {
                if (0 != m_ledger.fatalExit.load(std::memory_order_acquire))
                {
                    break;
                }
                m_ledger.waitForWork(SIZE_MAX, m_settings.retryMs);
                continue;
            }
            if (m_lastOpen.lastSeq != m_ledger.committedSeq())
            {
                fatal(exit_code::TEMPFAIL, std::string(ANOTHER_WRITER_TEXT) + " (md_state.last_journal_seq is " +
                    std::to_string(m_lastOpen.lastSeq) + " after a reconnect; this writer committed " +
                    std::to_string(m_ledger.committedSeq()) + ")");
                break;
            }
        }
        if (!m_wasDown)
        {
            m_ledger.writable.store(true, std::memory_order_release);
        }
        CommitBatch batch;
        if (!m_ledger.nextBatch(batchMax, batch))
        {
            if (m_wasDown)
            {
                m_wasDown = false;
                m_ledger.recoveries.fetch_add(1, std::memory_order_relaxed);
                m_ledger.writable.store(true, std::memory_order_release);
            }
            m_ledger.waitForWork(batchMax, m_settings.commitIntervalMs);
            continue;
        }
        if (batch.entries < batchMax)
        {
            // a short batch waits out the commit interval (more may come); a full one goes at once
            m_ledger.waitForWork(batchMax, m_settings.commitIntervalMs);
            if (m_stop.load(std::memory_order_acquire))
            {
                break;
            }
            m_ledger.nextBatch(batchMax, batch);
        }
        std::string error;
        const std::int64_t arrival = arrivalClockMs ? arrivalClockMs() : 0;
        const Commit c = commitBatch(m_conn, m_settings.pg.statementTimeoutMs, batch, arrival, error, pauseHook(batch));
        switch (c)
        {
            case Commit::COMMITTED:
                m_ledger.committed(batch);
                if (m_wasDown)
                {
                    m_wasDown = false;
                    m_ledger.recoveries.fetch_add(1, std::memory_order_relaxed);
                    std::fprintf(stderr, "oe-marketdata: market history writable again at journalSeq %lld\n",
                        static_cast<long long>(batch.toSeq));
                }
                m_ledger.writable.store(true, std::memory_order_release);
                break;
            case Commit::ANOTHER_WRITER:
                fatal(exit_code::TEMPFAIL, error.rfind(ANOTHER_WRITER_TEXT, 0) == 0 ? error
                                                                                    : std::string(ANOTHER_WRITER_TEXT) +
                        " (" + error + ")");
                break;
            case Commit::FAILED:
                failed("commit of journalSeq " + std::to_string(batch.fromSeq + 1) + ".." + std::to_string(batch.toSeq) +
                    ": " + error);
                m_conn.close(); // the next round reconnects and checks the cursor
                m_ledger.waitForWork(SIZE_MAX, m_settings.retryMs);
                break;
            case Commit::UNCERTAIN:
                failed("commit of journalSeq " + std::to_string(batch.fromSeq + 1) + ".." + std::to_string(batch.toSeq) +
                    ": " + error + " (outcome unknown: reading the cursor)");
                resolveUncertain(batch);
                break;
        }
    }
    m_conn.close(); // the writer lock goes with the connection
    m_ledger.connected.store(false, std::memory_order_release);
    m_finished.store(true, std::memory_order_release);
}

void HistoryWriter::start()
{
    m_thread = std::thread([this] { run(); });
}

bool HistoryWriter::stop(std::int64_t boundMs)
{
    m_stop.store(true, std::memory_order_release);
    m_ledger.wake();
    if (!m_thread.joinable())
    {
        return true;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (!m_finished.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (!m_finished.load(std::memory_order_acquire))
    {
        m_thread.detach(); // the process exits 70 next
        return false;
    }
    m_thread.join();
    return true;
}

}}}
