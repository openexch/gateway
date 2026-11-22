#ifndef GATEWAY_MARKETDATA_TRADE_PATH_H
#define GATEWAY_MARKETDATA_TRADE_PATH_H

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cluster_tools/archive/Chain.h"

#include "Candles.h"

// The trade path of oe-marketdata: what the journal follower (md-duty, JournalFollow) and the market history writer
// (md-pg, HistoryWriter) decide and hand each other. The rules here are pure (no Aeron, no PostgreSQL, no clock of their
// own), so the tests drive them with synthetic records.
//
// Two cursors. The commit cursor is md_state.last_journal_seq, the only cursor: every journal record up to it is stored
// and nothing above it, and it is what md_journal_seq reports (the console's retention watermark). The live-emit cursor
// is the highest journalSeq this process handed to the live path; it lives in memory, only grows, and a record at or
// below it is never handed on again, whatever made the follower read it again. The read position (the next record the
// follower takes) runs ahead of the commit cursor by the pending list at most.
//
// The forward rule, per admitted record, in this order:
//   1. the epoch: once the history has an epoch (md_state.me_epoch, or the first record taken into an empty history),
//      a record of another meEpoch is the halt EPOCH_MISMATCH (`market history belongs to another genesis`); nothing of
//      it is read further and nothing is written. Only the genesis reset clears it (the history is truncated).
//   2. an empty history (no cursor yet): the first record a read from its chain start delivers is where the history
//      starts (first_journal_seq); above 1 the history is partial (md_history_partial 1). A read that resumed from a
//      memo is not believed: read again from the chain start.
//   3. seq <= read position - 1: SKIP (already read; committed or pending).
//   4. seq == the next one: TAKE. Record 1 is decoded without its parties (journal_codec, the four order ids and the two
//      user ids are never read); records 2 to 5 only advance the sequence, their fields are never read. A trade of a
//      market outside MD_EXPECTED_MARKETS is the halt UNKNOWN_MARKET: not stored, not skipped, the read position stays
//      on it until a restart with the market in the set. A takerIsBuy byte that is neither 0 nor 1 is SOURCE_SCHEMA.
//   5. seq > the next one: this source does not hold the next record. Believed only on a read from the chain start:
//      the first record of such a read is BELOW_RANGE (the source's prefix is gone), a later one a HOLE. On a read from
//      a memo it is RE_READ (the memo is dropped, the source read again from its chain start).
// Halts (one kind names each; none is reported as another), when no source can deliver the next record:
//   GAP                    a source holds a hole over the next record and none holds it: re-scanned from every chain
//                          start every MD_GAP_RESCAN_MS, cleared when a source delivers it
//   JOURNAL_PREFIX_PURGED  every source begins above the next record: the records were purged before they were stored.
//                          A latch (the console's marketdata-reset is the exit; the history then starts partial)
//   EPOCH_MISMATCH         above; a latch
//   SOURCE_SCHEMA          a frame of another journal schema (the archive gate refuses it: schema 4 version 1 only) or
//                          another order schema on the ME egress (12 only); never decoded by guess; a latch
//   UNKNOWN_MARKET         above; a latch
// A halt stops the whole trade path: no trade is handed to the live path while one is set. The book path serves under
// every halt. Records taken before the halt still commit.

namespace openexchange { namespace gateway { namespace marketdata {

namespace ar = cluster_tools::archive;

enum class MdHalt : std::uint8_t
{
    NONE,
    GAP,
    EPOCH_MISMATCH,
    JOURNAL_PREFIX_PURGED,
    SOURCE_SCHEMA,
    UNKNOWN_MARKET,
    COUNT
};
const char *nameOf(MdHalt halt) noexcept;
inline constexpr const char *ANOTHER_GENESIS_TEXT = "market history belongs to another genesis";

// ---- the forward rule

enum class Forward : std::uint8_t
{
    SKIP,
    TAKE,
    START,          // TAKE, and the history starts here (an empty history's first record)
    BELOW_RANGE,
    HOLE,
    RE_READ,
    EPOCH_MISMATCH
};
const char *nameOf(Forward f) noexcept;

struct ForwardInput
{
    std::int64_t seq = 0;
    std::int64_t epoch = 0;
    std::int64_t next = 0;                     // the next record to take; 0 = an empty history, nothing taken yet
    std::optional<std::int64_t> boundEpoch;    // the history's epoch, once known
    bool memoFreeRead = true;                  // the read started at its chain start
    bool firstOfRead = true;                   // no record of this read was delivered before
};
Forward forwardRule(const ForwardInput &in) noexcept;

// What a taken record is (template 1 decoded without parties; 2..5 advance only).
enum class Taken : std::uint8_t
{
    TRADE,
    ADVANCE,
    UNKNOWN_MARKET,
    BAD_RECORD // a record 1 that does not decode, or a takerIsBuy byte that is not 0 or 1
};
// `expectedMarkets`: MD_EXPECTED_MARKETS. `out` is set for TRADE and UNKNOWN_MARKET.
Taken takeRecord(const std::uint8_t *frame, std::size_t length, std::uint16_t templateId,
    const std::vector<int> &expectedMarkets, TradeRow &out) noexcept;

// ---- the halts of the trade path (md-duty)

class TradeHalts
{
public:
    // Sets `halt` (a latch, except GAP). The first detail of a kind is kept. Returns true when it was not set before.
    bool set(MdHalt halt, const std::string &detail);
    void clearGap() noexcept;
    bool any() const noexcept;
    bool has(MdHalt halt) const noexcept { return m_set[static_cast<std::size_t>(halt)]; }
    // The one /health names first: EPOCH_MISMATCH, SOURCE_SCHEMA, UNKNOWN_MARKET, JOURNAL_PREFIX_PURGED, GAP.
    MdHalt primary() const noexcept;
    const std::string &detail(MdHalt halt) const { return m_detail[static_cast<std::size_t>(halt)]; }

private:
    std::array<bool, static_cast<std::size_t>(MdHalt::COUNT)> m_set{};
    std::array<std::string, static_cast<std::size_t>(MdHalt::COUNT)> m_detail{};
};

// ---- md_state and the pending list (shared by md-duty and md-pg)

struct MdState
{
    std::optional<std::int64_t> epoch;      // me_epoch; empty: an empty history
    std::int64_t lastSeq = 0;               // last_journal_seq (the commit cursor)
    std::optional<std::int64_t> firstSeq;   // first_journal_seq
    bool operator==(const MdState &) const = default;
};

// One record read and not yet committed: a trade, or a run of records 2..5 (only their last sequence is kept).
struct PendingEntry
{
    std::int64_t seq = 0;
    bool hasTrade = false;
    TradeRow trade;
    std::size_t source = 0;      // where it was read (a memo proof once committed)
    ar::RecordingKey key;
    std::int64_t endPosition = 0;
    std::int64_t readAtNs = 0;   // monotonic
};

// One commit of the writer: the entries from the front of the pending list, the cursor move, and the epoch and first
// record an empty history binds with it.
struct CommitBatch
{
    std::int64_t fromSeq = 0;               // the cursor the transaction expects
    std::int64_t toSeq = 0;                 // the cursor it moves to
    std::int64_t epoch = 0;
    std::int64_t firstSeq = 0;              // written only while first_journal_seq is NULL
    std::vector<TradeRow> trades;
    std::size_t entries = 0;
    PendingEntry last;                       // the last entry (its source position proves the cursor)
};

// What the writer reports to the duty (collected once per duty cycle).
struct WriterReport
{
    std::optional<MdState> state;           // set once, when md_state was read at start (or after a reset)
    std::int64_t committedSeq = -1;         // -1: no commit since the last collect
    std::optional<PendingEntry> proof;      // the last committed entry
};

class TradeLedger
{
public:
    explicit TradeLedger(std::size_t capacity) : m_capacity(capacity) {}

    // ---- md-duty
    // False when the list holds `capacity` entries (the follower stops reading; nothing is dropped). A record 2..5 that
    // follows another one is folded into it and always fits.
    bool push(const PendingEntry &e);
    // The epoch and first record an empty history binds with its first commit.
    void bind(std::int64_t epoch, std::int64_t firstSeq);
    WriterReport collect();

    // ---- md-pg
    void publishState(const MdState &state);
    // The next batch, at most `max` entries; empty `trades` and `entries` 0 when nothing is pending. Needs the state.
    bool nextBatch(std::size_t max, CommitBatch &out) const;
    void committed(const CommitBatch &batch);
    // Waits until `max` entries are pending, the stop flag is set or `timeoutMs` passed.
    void waitForWork(std::size_t max, std::int64_t timeoutMs);
    void wake();

    // ---- both (and the admin thread)
    std::size_t pending() const;
    std::int64_t oldestReadAtNs() const;   // -1 when nothing is pending
    std::int64_t committedSeq() const noexcept { return m_committed.load(std::memory_order_acquire); }
    bool stateKnown() const noexcept { return m_stateKnown.load(std::memory_order_acquire); }
    std::size_t capacity() const noexcept { return m_capacity; }

    // the writer's condition, read by md-duty and the admin thread
    std::atomic<bool> writable{ false };
    std::atomic<bool> connected{ false };
    std::atomic<int> fatalExit{ 0 };       // 75 or 78: the process stops with it
    std::atomic<std::uint64_t> commits{ 0 };
    std::atomic<std::uint64_t> tradesCommitted{ 0 };
    std::atomic<std::uint64_t> failures{ 0 };
    std::atomic<std::uint64_t> recoveries{ 0 };    // writable again after a failure
    std::atomic<bool> partial{ false };            // first_journal_seq above 1
    std::atomic<std::int64_t> epoch{ 0 };          // the history's epoch (0 = none yet)
    std::atomic<std::int64_t> firstSeq{ 0 };       // 0 = none yet
    void setError(const std::string &text);
    std::string lastError() const;

private:
    mutable std::mutex m_lock;
    std::condition_variable m_cv;
    std::size_t m_capacity;
    std::deque<PendingEntry> m_entries;
    mutable std::size_t m_inFlight = 0;      // front entries handed to the writer's current batch
    std::optional<MdState> m_state;          // as the writer knows it (the cursor moves with each commit)
    std::optional<MdState> m_stateReport;    // not collected yet
    std::int64_t m_bindEpoch = 0;
    std::int64_t m_bindFirst = 0;
    std::int64_t m_reportCommitted = -1;
    std::optional<PendingEntry> m_reportProof;
    std::atomic<std::int64_t> m_committed{ 0 };
    std::atomic<bool> m_stateKnown{ false };
    bool m_woken = false;
    std::string m_lastError;
};

// The live path's view of a trade (the tape frames it): handed on once, in journal order, never while a halt is set.
class LiveTrades
{
public:
    virtual ~LiveTrades() = default;
    virtual void onLiveTrade(const TradeRow &trade) = 0;
};

// The live-emit cursor: hands a trade to the live path when its sequence is above every one handed before.
class LiveEmitCursor
{
public:
    // True when `seq` was handed on now (the cursor moved to it).
    bool offer(std::int64_t seq) noexcept;
    std::int64_t value() const noexcept { return m_cursor; }

private:
    std::int64_t m_cursor = 0;
};

}}}

#endif
