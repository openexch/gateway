#include "BookCore.h"

#include <algorithm>
#include <cstring>

#include "cluster_tools/adapters/sbe/ForwarderCodec.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace fwd = cluster_tools::domain::forwarder;
namespace codec = cluster_tools::sbe_adapter::forwarder_codec;

namespace {

constexpr std::size_t STAMPS_END = codec::MESSAGE_HEADER_LENGTH + 28; // through askVersion

std::int64_t readI64(const std::uint8_t *p)
{
    std::int64_t v = 0;
    std::memcpy(&v, p, sizeof(v)); // little-endian hosts only, like the library's codecs
    return v;
}

Frame make(std::string text, FrameType type)
{
    return Frame{ std::make_shared<const std::string>(std::move(text)), type };
}

const std::vector<std::string> HISTORY_PARTS = { "tape", "candles", "ticker" };

}

const char *nameOf(FrameType type) noexcept
{
    switch (type)
    {
        case FrameType::BOOK_SNAPSHOT: return "BOOK_SNAPSHOT";
        case FrameType::BOOK_DELTA: return "BOOK_DELTA";
        case FrameType::SUBSCRIPTION_CONFIRMED: return "SUBSCRIPTION_CONFIRMED";
        case FrameType::PONG: return "PONG";
        case FrameType::ERROR: break;
        case FrameType::COUNT: break;
    }
    return "ERROR";
}

BookStamps readBookStamps(const std::uint8_t *payload, std::size_t length) noexcept
{
    BookStamps s;
    if (nullptr == payload || length < STAMPS_END)
    {
        return s;
    }
    const std::uint8_t *block = payload + codec::MESSAGE_HEADER_LENGTH;
    s.timestampMs = readI64(block + 4);
    s.bidVersion = readI64(block + 12);
    s.askVersion = readI64(block + 20);
    return s;
}

BookCore::BookCore(BookCoreSettings settings, MdStats &stats, std::function<std::int64_t()> wallClockMs) :
    m_settings(std::move(settings)), m_stats(stats), m_wallClockMs(std::move(wallClockMs))
{
    for (const int id : m_settings.expectedMarkets)
    {
        if (fwd::BookStitcher::marketInRange(id))
        {
            market(id).status.expected = true;
        }
    }
}

bool BookCore::expected(std::int64_t marketId) const noexcept
{
    if (marketId < 1 || marketId > fwd::TICK_MARKETS)
    {
        return false;
    }
#ifdef MD_NC_NO_EXPECTED_SET_CHECK
    return true; // control build only (never published): every market of the library is served
#else
    return market(static_cast<int>(marketId)).status.expected;
#endif
}

MdBookState BookCore::bookState(int marketId, std::int64_t nowNs) const noexcept
{
    MdMarketStatus s = market(marketId).status;
#ifdef MD_NC_NO_EXPECTED_SET_CHECK
    s.expected = true;
#endif
    return mdBookState(s, m_lastHeartbeatNs, nowNs, m_settings.bounds);
}

void BookCore::refreshLadderFacts(int marketId)
{
    const fwd::BookLadder &l = m_stitcher.ladder(marketId);
    MdMarketStatus &s = market(marketId).status;
    s.ladderOk = fwd::LadderState::OK == l.state();
    s.empty = 0 == l.bidCount() && 0 == l.askCount();
}

void BookCore::onSessionUp(std::int64_t)
{
    m_sessionUp = true;
}

void BookCore::onSessionEnd(std::int64_t nowNs)
{
    m_stats.sessionsEnded.fetch_add(1, std::memory_order_relaxed);
    m_sessionUp = false;
    m_lastHeartbeatNs = -1;
    m_stitcher.onMeSessionEnd(nowNs);
    for (int id = 1; id <= fwd::TICK_MARKETS; ++id)
    {
        Market &m = market(id);
        m.status.snapshot = false;
        m.status.lastBookFrameNs = -1;
        refreshLadderFacts(id);
        if (expected(id) && Sent::STALE_EMPTY != m.sent)
        {
            emitStale(id, nowNs);
        }
    }
}

void BookCore::onEgress(const std::uint8_t *payload, std::size_t length, std::int64_t frameTerm, std::int64_t sessionTerm,
    std::int64_t nowNs)
{
    m_stats.lastEgressNs.store(nowNs, std::memory_order_relaxed);
    codec::Header h;
    if (!codec::readHeader(payload, length, h) || codec::MATCH_SCHEMA_ID != h.schemaId)
    {
        m_stats.dropped[MdStats::DROPPED_TEMPLATES.size()].fetch_add(1, std::memory_order_relaxed);
        return;
    }
    const bool book = codec::T_BOOK_SNAPSHOT == h.templateId || codec::T_BOOK_DELTA == h.templateId;
    if (!book && codec::T_CLUSTER_HEARTBEAT != h.templateId)
    {
        // counted and dropped before any field is read: 24 and 26 carry user and order ids
        std::size_t slot = MdStats::DROPPED_TEMPLATES.size();
        for (std::size_t i = 0; i < MdStats::DROPPED_TEMPLATES.size(); ++i)
        {
            slot = MdStats::DROPPED_TEMPLATES[i] == h.templateId ? i : slot;
        }
        m_stats.dropped[slot].fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (codec::MATCH_VERSION != h.version)
    {
        m_stats.schemaRefused.fetch_add(1, std::memory_order_relaxed); // never decoded by guess
        return;
    }
    if (codec::T_CLUSTER_HEARTBEAT == h.templateId)
    {
        std::int64_t epoch = 0;
        if (codec::decodeHeartbeatEpoch(payload, length, h, epoch))
        {
            m_stats.heartbeats.fetch_add(1, std::memory_order_relaxed);
            m_lastHeartbeatNs = nowNs;
            m_meEpoch = epoch;
        }
        return;
    }
    if (frameTerm < sessionTerm)
    {
        m_stats.staleTerm.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (codec::T_BOOK_SNAPSHOT == h.templateId)
    {
        codec::decodeBookSnapshot(payload, length, h, m_frame);
    }
    else
    {
        codec::decodeBookDelta(payload, length, h, m_frame);
    }
    onBookFrame(m_frame, payload, length, nowNs);
}

void BookCore::onBookFrame(const fwd::BookFrame &f, const std::uint8_t *payload, std::size_t length, std::int64_t nowNs)
{
    if (f.marketReadable && fwd::BookStitcher::marketInRange(f.marketId) && !expected(f.marketId))
    {
        m_stats.foreignMarket.fetch_add(1, std::memory_order_relaxed); // inside 1..5, outside MD_EXPECTED_MARKETS
        return;
    }
    fwd::LadderOutcome outcome = fwd::LadderOutcome::IGNORED;
    const int id = m_stitcher.onBookFrame(f, nowNs, &outcome);
    if (0 == id)
    {
        if (f.marketReadable)
        {
            m_stats.foreignMarket.fetch_add(1, std::memory_order_relaxed); // outside 1..5 (the library counted it)
        }
        else
        {
            m_stats.malformedNoMarket.fetch_add(1, std::memory_order_relaxed);
        }
        return;
    }
    (f.snapshot ? m_stats.snapshots : m_stats.deltas).fetch_add(1, std::memory_order_relaxed);
    Market &m = market(id);
    refreshLadderFacts(id);
#ifdef MD_NC_NO_CHAIN_CHECK
    // control build only (never published): every decoded delta is sent, whatever the ladder made of it
    if (!f.snapshot && fwd::LadderOutcome::DELTA_APPLIED != outcome)
    {
        if (fwd::LadderOutcome::BROKE == outcome)
        {
            m_stats.breaks.fetch_add(1, std::memory_order_relaxed);
        }
        emit(id, make(renderBookDelta(id, f, readBookStamps(payload, length)), FrameType::BOOK_DELTA));
        return;
    }
#endif
    switch (outcome)
    {
        case fwd::LadderOutcome::SNAPSHOT_APPLIED:
            m.status.snapshot = true;
            m.status.lastBookFrameNs = nowNs;
            m.stamps = readBookStamps(payload, length);
            emit(id, make(renderBookSnapshot(id, &m_stitcher.ladder(id), m_stitcher.ladder(id).version(),
                m.stamps.timestampMs, false), FrameType::BOOK_SNAPSHOT));
            m.sent = Sent::FRESH;
            return;
        case fwd::LadderOutcome::DELTA_APPLIED:
            m.status.lastBookFrameNs = nowNs;
            m.stamps = readBookStamps(payload, length);
            emit(id, make(renderBookDelta(id, f, m.stamps), FrameType::BOOK_DELTA));
            m.sent = Sent::FRESH;
            return;
        case fwd::LadderOutcome::BROKE:
            m_stats.breaks.fetch_add(1, std::memory_order_relaxed);
            emitStale(id, nowNs);
            return;
        case fwd::LadderOutcome::UNVERSIONED:
            m.status.snapshot = false; // no versioned state: as if no snapshot arrived
            m.status.lastBookFrameNs = -1;
            if (Sent::STALE_EMPTY != m.sent)
            {
                emitStale(id, nowNs);
            }
            return;
        case fwd::LadderOutcome::IGNORED:
            return; // a delta without a chain is never sent
    }
}

Frame BookCore::snapshotFrame(int id, std::int64_t nowNs, Sent *kind) const
{
    const Market &m = market(id);
    const fwd::BookLadder &l = m_stitcher.ladder(id);
    const MdBookState state = bookState(id, nowNs);
    const bool fresh = MdBookState::FRESH == state;
    const bool levels = fwd::LadderState::OK == l.state();
    if (nullptr != kind)
    {
        *kind = fresh ? Sent::FRESH : levels ? Sent::STALE_LEVELS : Sent::STALE_EMPTY;
    }
    return make(renderBookSnapshot(id, levels ? &l : nullptr, l.version(), m.stamps.timestampMs, !fresh),
        FrameType::BOOK_SNAPSHOT);
}

void BookCore::emitStale(int id, std::int64_t nowNs)
{
    Market &m = market(id);
    const fwd::BookLadder &l = m_stitcher.ladder(id);
    const bool levels = fwd::LadderState::OK == l.state();
    emit(id, make(renderBookSnapshot(id, levels ? &l : nullptr, l.version(), m.stamps.timestampMs, true),
        FrameType::BOOK_SNAPSHOT));
    m.sent = levels ? Sent::STALE_LEVELS : Sent::STALE_EMPTY;
    (void)nowNs;
}

void BookCore::emit(int id, const Frame &frame)
{
    for (const std::uint64_t connId : market(id).subscribers)
    {
        const auto it = m_conns.find(connId);
        if (m_conns.end() != it)
        {
            send(it->second, frame);
        }
    }
}

void BookCore::send(Conn &conn, const Frame &frame)
{
    if (conn.outlet && conn.outlet->offer(frame))
    {
        m_stats.queued[static_cast<std::size_t>(frame.type)].fetch_add(1, std::memory_order_relaxed);
    }
}

int BookCore::doWork(std::int64_t nowNs)
{
    int work = 0;
    std::int64_t stale = 0;
    for (int id = 1; id <= fwd::TICK_MARKETS; ++id)
    {
        if (!expected(id))
        {
            continue;
        }
        Market &m = market(id);
        refreshLadderFacts(id);
        const bool fresh = MdBookState::FRESH == bookState(id, nowNs);
        stale += fresh ? 0 : 1;
        if (!fresh && Sent::FRESH == m.sent)
        {
            emitStale(id, nowNs);
            work++;
        }
    }
    m_stats.booksStale.store(stale, std::memory_order_relaxed);
    return work;
}

void BookCore::open(std::uint64_t connId, std::shared_ptr<Outlet> outlet)
{
    m_conns[connId] = Conn{ std::move(outlet), 0 };
}

void BookCore::initialState(std::uint64_t, Conn &conn, std::int64_t nowNs)
{
    send(conn, snapshotFrame(conn.market, nowNs, nullptr));
    // No trade path in this build: the tape, the candles and the ticker do not exist, and an empty series is never sent
    // as if it were whole.
    send(conn, make(renderError("HISTORY_UNAVAILABLE", "the market history is not available: no tape, candles or ticker",
        HISTORY_PARTS), FrameType::ERROR));
}

void BookCore::subscribe(std::uint64_t connId, std::int64_t marketId, std::int64_t nowNs)
{
    const auto it = m_conns.find(connId);
    if (m_conns.end() == it)
    {
        return;
    }
    Conn &conn = it->second;
    if (!expected(marketId))
    {
        send(conn, make(renderError("UNKNOWN_MARKET", "market " + std::to_string(marketId) + " is not served here"),
            FrameType::ERROR));
        return;
    }
    const int id = static_cast<int>(marketId);
    if (0 != conn.market)
    {
        std::vector<std::uint64_t> &old = market(conn.market).subscribers;
        old.erase(std::remove(old.begin(), old.end(), connId), old.end());
    }
    conn.market = id;
    market(id).subscribers.push_back(connId);
    send(conn, make(renderSubscribed(id), FrameType::SUBSCRIPTION_CONFIRMED));
    initialState(connId, conn, nowNs);
}

void BookCore::refresh(std::uint64_t connId, std::int64_t marketId, std::int64_t nowNs)
{
    const auto it = m_conns.find(connId);
    if (m_conns.end() == it)
    {
        return;
    }
    Conn &conn = it->second;
    if (0 == conn.market || conn.market != marketId)
    {
        send(conn, make(renderError("NOT_SUBSCRIBED", "refresh names market " + std::to_string(marketId) +
            "; this connection is subscribed to " + (0 == conn.market ? std::string("none") : std::to_string(conn.market))),
            FrameType::ERROR));
        return;
    }
    initialState(connId, conn, nowNs);
}

void BookCore::ping(std::uint64_t connId)
{
    const auto it = m_conns.find(connId);
    if (m_conns.end() != it)
    {
        send(it->second, make(renderPong(m_wallClockMs()), FrameType::PONG));
    }
}

void BookCore::badRequest(std::uint64_t connId, const std::string &message)
{
    const auto it = m_conns.find(connId);
    if (m_conns.end() != it)
    {
        send(it->second, make(renderError("BAD_REQUEST", message), FrameType::ERROR));
    }
}

void BookCore::resync(std::uint64_t connId, std::int64_t nowNs)
{
    const auto it = m_conns.find(connId);
    if (m_conns.end() == it)
    {
        return;
    }
    m_stats.resyncs.fetch_add(1, std::memory_order_relaxed);
    it->second.outlet->resyncBegins();
    if (0 != it->second.market)
    {
        initialState(connId, it->second, nowNs);
    }
}

void BookCore::gone(std::uint64_t connId)
{
    const auto it = m_conns.find(connId);
    if (m_conns.end() == it)
    {
        return;
    }
    if (0 != it->second.market)
    {
        std::vector<std::uint64_t> &subs = market(it->second.market).subscribers;
        subs.erase(std::remove(subs.begin(), subs.end(), connId), subs.end());
    }
    m_conns.erase(it);
}

void BookCore::fillStatus(Status &s) const
{
    s.mdSessionUp = m_sessionUp;
    s.mdLastHeartbeatNs = m_lastHeartbeatNs;
    s.mdMeEpoch = m_meEpoch;
    for (std::size_t i = 0; i < m_markets.size(); ++i)
    {
        s.mdMarkets[i] = m_markets[i].status;
    }
}

}}}
