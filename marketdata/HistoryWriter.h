#ifndef GATEWAY_MARKETDATA_HISTORY_WRITER_H
#define GATEWAY_MARKETDATA_HISTORY_WRITER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

#include "gateway/common/PgConn.h"

#include "MarketHistory.h"
#include "TradePath.h"

// HistoryWriter - md-pg, the market history writer thread of oe-marketdata: the only code that writes the md_*
// relations. It commits the pending list (TradeLedger) in journal order, at most MD_COMMIT_BATCH entries a transaction,
// at least every MD_COMMIT_INTERVAL_MS while anything is pending (MarketHistory.h: one transaction per batch, the cursor
// moved from the expected old value).
//
// The start (startCheck, on the main thread before any listener binds): one connect attempt. Reached: the legacy guard
// (exit 78), the writer lock (held elsewhere: exit 75 with `market history writer lock is held`, before the public
// port opens), the schema (foreign: exit 78) and md_state are settled on that connection, which the thread keeps.
// Not reached: the market history is down at the start; the cursor is unknown, so the trade path does not start, and
// the thread retries every MD_PG_RETRY_MS with the same checks (a lock held by then stops the process with 75).
// While running: a failed transaction (rolled back) or a lost connection clears history_writable and keeps the batch
// pending; the thread reconnects (the lock again, then md_state: the cursor must be the one it holds) and commits it
// again. A COMMIT whose answer was lost is resolved by reading the cursor: the new value = committed, the old one = not,
// any other = another writer. A cursor that is not the expected one, or a record stored already, stops the process with
// exit 75 and `market history has another writer`. Nothing is ever dropped from the pending list but by a commit.
// Test build only: MD_TC_COMMIT_PAUSE pauses each commit for MDTC_COMMIT_PAUSE_MS (environment) after its statements
// and before COMMIT (MD_NC_COMMIT_SPLIT: between the cursor's own transaction and the rows), printing a line first;
// the first MDTC_COMMIT_PAUSE_SKIP commits of the process go through unpaused.

namespace openexchange { namespace gateway { namespace marketdata {

struct HistoryWriterSettings
{
    HistoryOptions pg;
    std::int64_t retryMs = 1000;
    std::int64_t commitBatch = 512;
    std::int64_t commitIntervalMs = 50;
};

class HistoryWriter
{
public:
    HistoryWriter(HistoryWriterSettings settings, TradeLedger &ledger) : m_settings(std::move(settings)), m_ledger(ledger) {}
    ~HistoryWriter() { stop(2000); }

    // One connect attempt and the start checks: 0, or the exit code the process must stop with (75, 78).
    int startCheck();
    void start();
    bool stop(std::int64_t boundMs);

    // The arrival clock handed to the candle aggregation (only the MD_NC_WALLCLOCK_BUCKET test build reads it).
    std::function<std::int64_t()> arrivalClockMs;

private:
    void run();
    // Connects and runs the start checks; true when READY (the state compared or published).
    bool open();
    void fatal(int code, const std::string &text);
    void failed(const std::string &what);
    bool resolveUncertain(const CommitBatch &batch);
    std::function<void()> pauseHook(const CommitBatch &batch);

    HistoryWriterSettings m_settings;
    TradeLedger &m_ledger;
    PgConn m_conn;
    bool m_statePublished = false;
    MdState m_lastOpen;      // md_state as the last open() read it
    bool m_wasDown = false;  // a failure since the last commit (history_writable is false)
    std::thread m_thread;
    std::atomic<bool> m_stop{ false };
    std::atomic<bool> m_finished{ false };
    pg::WarnLimiter m_warn;
};

}}}

#endif
