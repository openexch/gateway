#include "TradePath.h"

#include <algorithm>
#include <chrono>

#include "cluster_tools/adapters/sbe/JournalTradeCodec.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace jc = cluster_tools::sbe_adapter::journal_codec;

const char *nameOf(MdHalt halt) noexcept
{
    switch (halt)
    {
        case MdHalt::NONE: return "NONE";
        case MdHalt::GAP: return "GAP";
        case MdHalt::EPOCH_MISMATCH: return "EPOCH_MISMATCH";
        case MdHalt::JOURNAL_PREFIX_PURGED: return "JOURNAL_PREFIX_PURGED";
        case MdHalt::SOURCE_SCHEMA: return "SOURCE_SCHEMA";
        case MdHalt::UNKNOWN_MARKET: return "UNKNOWN_MARKET";
        case MdHalt::COUNT: break;
    }
    return "?";
}

const char *nameOf(Forward f) noexcept
{
    switch (f)
    {
        case Forward::SKIP: return "SKIP";
        case Forward::TAKE: return "TAKE";
        case Forward::START: return "START";
        case Forward::BELOW_RANGE: return "BELOW_RANGE";
        case Forward::HOLE: return "HOLE";
        case Forward::RE_READ: return "RE_READ";
        case Forward::EPOCH_MISMATCH: return "EPOCH_MISMATCH";
    }
    return "?";
}

Forward forwardRule(const ForwardInput &in) noexcept
{
#ifndef MD_NC_NO_EPOCH_CHECK
    if (in.boundEpoch && in.epoch != *in.boundEpoch)
    {
        return Forward::EPOCH_MISMATCH;
    }
#endif
    if (0 == in.next)
    {
        // an empty history starts at the first record a read from the chain start delivers
        return in.memoFreeRead ? Forward::START : Forward::RE_READ;
    }
    if (in.seq < in.next)
    {
        return Forward::SKIP;
    }
    if (in.seq == in.next)
    {
        return Forward::TAKE;
    }
    if (!in.memoFreeRead)
    {
        return Forward::RE_READ;
    }
#ifdef MD_NC_NO_PREFIX_CHECK
    // test build only: a jump over the next record is taken as if the records in between did not exist
    return Forward::TAKE;
#else
    return in.firstOfRead ? Forward::BELOW_RANGE : Forward::HOLE;
#endif
}

Taken takeRecord(const std::uint8_t *frame, std::size_t length, std::uint16_t templateId,
    const std::vector<int> &expectedMarkets, TradeRow &out) noexcept
{
    if (jc::TEMPLATE_TRADE != templateId)
    {
        return Taken::ADVANCE; // records 2..5: only the sequence advances, no field of them is read
    }
    jc::TradeRecord t;
    if (!jc::decodeTrade(frame, length, t)) // no parties: the order and user ids are never read
    {
        return Taken::BAD_RECORD;
    }
    if (t.takerIsBuyRaw > 1)
    {
        return Taken::BAD_RECORD;
    }
    out.marketId = t.marketId;
    out.tradeId = t.tradeId;
    out.price = t.price;
    out.quantity = t.quantity;
    out.takerIsBuy = 1 == t.takerIsBuyRaw;
    out.executedAtMs = t.timestamp;
    out.journalSeq = t.journalSeq;
#ifndef MD_NC_NO_MARKET_CHECK
    if (expectedMarkets.end() == std::find(expectedMarkets.begin(), expectedMarkets.end(), t.marketId))
    {
        return Taken::UNKNOWN_MARKET;
    }
#else
    (void)expectedMarkets;
#endif
    return Taken::TRADE;
}

// ---- TradeHalts

bool TradeHalts::set(MdHalt halt, const std::string &detail)
{
    const std::size_t i = static_cast<std::size_t>(halt);
    if (MdHalt::NONE == halt || m_set[i])
    {
        return false;
    }
    m_set[i] = true;
    m_detail[i] = detail;
    return true;
}

void TradeHalts::clearGap() noexcept
{
    m_set[static_cast<std::size_t>(MdHalt::GAP)] = false;
    m_detail[static_cast<std::size_t>(MdHalt::GAP)].clear();
}

bool TradeHalts::any() const noexcept
{
    return std::any_of(m_set.begin(), m_set.end(), [](bool b) { return b; });
}

MdHalt TradeHalts::primary() const noexcept
{
    for (const MdHalt h : { MdHalt::EPOCH_MISMATCH, MdHalt::SOURCE_SCHEMA, MdHalt::UNKNOWN_MARKET,
             MdHalt::JOURNAL_PREFIX_PURGED, MdHalt::GAP })
    {
        if (has(h))
        {
            return h;
        }
    }
    return MdHalt::NONE;
}

// ---- TradeLedger

bool TradeLedger::push(const PendingEntry &e)
{
    std::lock_guard<std::mutex> lock(m_lock);
    if (!e.hasTrade && m_entries.size() > m_inFlight && !m_entries.back().hasTrade)
    {
        PendingEntry &b = m_entries.back(); // a run of records 2..5: one entry, its last sequence and position
        b.seq = e.seq;
        b.source = e.source;
        b.key = e.key;
        b.endPosition = e.endPosition;
        return true;
    }
    if (m_entries.size() >= m_capacity)
    {
        return false;
    }
    m_entries.push_back(e);
    m_cv.notify_all();
    return true;
}

void TradeLedger::bind(std::int64_t epoch, std::int64_t firstSeq)
{
    std::lock_guard<std::mutex> lock(m_lock);
    m_bindEpoch = epoch;
    m_bindFirst = firstSeq;
}

WriterReport TradeLedger::collect()
{
    std::lock_guard<std::mutex> lock(m_lock);
    WriterReport r;
    r.state = std::move(m_stateReport);
    m_stateReport.reset();
    r.seed = std::move(m_seedReport);
    m_seedReport.reset();
    r.committedSeq = m_reportCommitted;
    r.proof = std::move(m_reportProof);
    m_reportProof.reset();
    m_reportCommitted = -1;
    return r;
}

void TradeLedger::publishState(const MdState &state, MarketSeed seed)
{
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_state = state;
        m_stateReport = state;
        m_seedReport = std::move(seed);
        m_committed.store(state.lastSeq, std::memory_order_release);
        epoch.store(state.epoch.value_or(0), std::memory_order_release);
        firstSeq.store(state.firstSeq.value_or(0), std::memory_order_release);
        partial.store(state.firstSeq && *state.firstSeq > 1, std::memory_order_release);
    }
    m_stateKnown.store(true, std::memory_order_release);
}

bool TradeLedger::nextBatch(std::size_t max, CommitBatch &out) const
{
    std::lock_guard<std::mutex> lock(m_lock);
    out = CommitBatch{};
    if (!m_state || m_entries.empty())
    {
        return false;
    }
    out.fromSeq = m_state->lastSeq;
    out.epoch = m_state->epoch ? *m_state->epoch : m_bindEpoch;
    out.firstSeq = m_state->firstSeq ? *m_state->firstSeq : m_bindFirst;
    const std::size_t n = std::min(max, m_entries.size());
    for (std::size_t i = 0; i < n; ++i)
    {
        const PendingEntry &e = m_entries[i];
        if (e.hasTrade)
        {
            out.trades.push_back(e.trade);
        }
        out.toSeq = e.seq;
    }
    out.entries = n;
    out.last = m_entries[n - 1];
    m_inFlight = n; // frozen until committed: a later record 2..5 is not folded into them
    return out.epoch > 0 && out.firstSeq > 0;
}

void TradeLedger::committed(const CommitBatch &batch)
{
    std::lock_guard<std::mutex> lock(m_lock);
    for (std::size_t i = 0; i < batch.entries && !m_entries.empty(); ++i)
    {
        m_entries.pop_front();
    }
    m_inFlight = 0;
    if (m_state)
    {
        m_state->lastSeq = batch.toSeq;
        if (!m_state->epoch)
        {
            m_state->epoch = batch.epoch;
            epoch.store(batch.epoch, std::memory_order_release);
        }
        if (!m_state->firstSeq)
        {
            m_state->firstSeq = batch.firstSeq;
            firstSeq.store(batch.firstSeq, std::memory_order_release);
            partial.store(batch.firstSeq > 1, std::memory_order_release);
        }
    }
    m_reportCommitted = batch.toSeq;
    m_reportProof = batch.last;
    m_committed.store(batch.toSeq, std::memory_order_release);
    commits.fetch_add(1, std::memory_order_relaxed);
    tradesCommitted.fetch_add(batch.trades.size(), std::memory_order_relaxed);
}

void TradeLedger::waitForWork(std::size_t max, std::int64_t timeoutMs)
{
    std::unique_lock<std::mutex> lock(m_lock);
    m_cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [&] { return m_woken || m_entries.size() >= max; });
    m_woken = false;
}

void TradeLedger::wake()
{
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_woken = true;
    }
    m_cv.notify_all();
}

std::size_t TradeLedger::pending() const
{
    std::lock_guard<std::mutex> lock(m_lock);
    return m_entries.size();
}

std::int64_t TradeLedger::oldestReadAtNs() const
{
    std::lock_guard<std::mutex> lock(m_lock);
    return m_entries.empty() ? -1 : m_entries.front().readAtNs;
}

void TradeLedger::setError(const std::string &text)
{
    std::lock_guard<std::mutex> lock(m_lock);
    m_lastError = text;
}

std::string TradeLedger::lastError() const
{
    std::lock_guard<std::mutex> lock(m_lock);
    return m_lastError;
}

// ---- LiveEmitCursor

bool LiveEmitCursor::offer(std::int64_t seq) noexcept
{
#ifndef MD_NC_NO_LIVE_EMIT_CURSOR
    if (seq <= m_cursor)
    {
        return false;
    }
#endif
    m_cursor = std::max(m_cursor, seq);
    return true;
}

}}}
