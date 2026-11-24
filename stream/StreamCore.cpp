#include "StreamCore.h"

#include <algorithm>
#include <cinttypes>
#include <limits>

#include "gateway/common/Api.h"
#include "gateway/common/Rest.h"

namespace openexchange { namespace gateway { namespace stream {

namespace rt = cluster_tools::runtime;

namespace {

constexpr std::int64_t MS = 1'000'000LL;
// closed sockets and expired lingers are looked for this often (a sweep step, not a bound of anything)
constexpr std::int64_t SWEEP_NS = 20 * MS;

std::string channelsJson(unsigned channels)
{
    std::string out = "[";
    bool first = true;
    for (const auto &[bit, name] : { std::pair{ channel::ORDERS, "orders" }, std::pair{ channel::FILLS, "fills" },
             std::pair{ channel::BALANCES, "balances" }, std::pair{ channel::ENGINE, "engine" } })
    {
        if (0 != (channels & bit))
        {
            out += (first ? "\"" : ",\"") + std::string(name) + "\"";
            first = false;
        }
    }
    return out + "]";
}

Text text(std::string s)
{
    return std::make_shared<const std::string>(std::move(s));
}

std::string rowJson(const std::variant<wire::OrderSnapshotEntry, wire::OrderEvent> &v)
{
    return std::visit([](const auto &row) { return rest::orderJson(row); }, v);
}

// A fill frame is a trade of the user: a FILL with tradeId 0 (the core closing an order whose fills inside a replace
// window reached its quantity) names none and goes out as an order frame only, so a user's fill frames and the user's
// fills rows are the same set.
bool isFill(const wire::OrderEvent &e)
{
#ifdef GW_NC_FILL_ZERO_TRADE
    return wire::money::EventKind::FILL == e.eventKind; // test build only: every FILL is a fill frame
#else
    return wire::money::EventKind::FILL == e.eventKind && e.tradeId > 0;
#endif
}

}

const char *nameOf(ResyncReason reason) noexcept
{
    switch (reason)
    {
        case ResyncReason::EVENT_GAP: return "EVENT_GAP";
        case ResyncReason::LEADER_CHANGE: return "LEADER_CHANGE";
        case ResyncReason::SESSION_LOST: return "SESSION_LOST";
        case ResyncReason::RESUME_TOO_OLD: return "RESUME_TOO_OLD";
    }
    return "?";
}

StreamCore::StreamCore(StreamCoreSettings settings, QueryPort &queries, ConfigSource config, rt::LogRing *log,
    StreamStats *stats) :
    m_settings(settings), m_queries(queries), m_config(std::move(config)), m_log(log),
    m_stats(nullptr == stats ? m_ownStats : *stats)
{
}

StreamCore::~StreamCore() = default;

std::int64_t StreamCore::windowFirstSeq() const noexcept
{
    return m_window.empty() ? m_watermark + 1 : m_window.front().eventSeq;
}

bool StreamCore::userCached(std::int64_t userId) const
{
    const auto it = m_users.find(userId);
    return it != m_users.end() && (Cache::READY == it->second.orders || Bal::READY == it->second.bal);
}

// ================================================================ the core session

void StreamCore::onRegistered(std::uint64_t sessionEpoch, std::int64_t leadershipTermId, std::int64_t nowNs)
{
    if (!m_registeredOnce)
    {
        m_registeredOnce = true;
        m_epoch = sessionEpoch;
        m_term = leadershipTermId;
        m_lastProbeSentNs = -1; // the first head probe now: it sets the watermark
        return;
    }
    const ResyncReason reason = sessionEpoch != m_epoch ? ResyncReason::SESSION_LOST : ResyncReason::LEADER_CHANGE;
    m_epoch = sessionEpoch;
    m_term = leadershipTermId;
    logLine(rt::LogLevel::INFO, nowNs, "stream: registered again (%s, session epoch %" PRIu64 ", term %" PRId64
        "): replaying from eventSeq %" PRId64 ", every cache dropped, balances read again", nameOf(reason), sessionEpoch,
        leadershipTermId, m_watermark);
    // every cache is derived from the session that ended; a stitch in flight stays (its pages are re-offered by the
    // session and it waits for the replay below)
    std::vector<std::int64_t> idle;
    for (auto &[id, u] : m_users)
    {
        if (u.conns.empty())
        {
            idle.push_back(id);
            continue;
        }
        if (Cache::READY == u.orders)
        {
            u.orders = Cache::NONE;
            u.open.clear();
        }
        bool wantsBalances = false;
        for (const std::uint64_t cid : u.conns)
        {
            const Conn *c = conn(cid);
            wantsBalances = wantsBalances || (nullptr != c && 0 != (c->subscribed & channel::BALANCES));
        }
        u.bal = Bal::NONE;
        u.balances.clear();
        if (wantsBalances)
        {
            startBalances(u); // the lines of the outage are gone: the user's balances are read again and sent as a snapshot
        }
    }
    for (const std::int64_t id : idle)
    {
        dropUser(id);
    }
    if (m_watermark >= 0)
    {
        openGap(reason);
    }
}

void StreamCore::onLiveOrderEvent(const wire::OrderEvent &event, std::int64_t)
{
    if (m_watermark < 0)
    {
        if (static_cast<std::int64_t>(m_initBuffer.size()) < m_settings.gapBufferEvents)
        {
            m_initBuffer.push_back(event); // before the first head probe: kept, sorted out when the watermark is known
        }
        return;
    }
    if (m_gap.open)
    {
        if (event.eventSeq > m_watermark)
        {
            m_gap.buffer.emplace(event.eventSeq, event);
            if (static_cast<std::int64_t>(m_gap.buffer.size()) > m_settings.gapBufferEvents)
            {
                overrun(m_gap.buffer.rbegin()->first, m_gap.reason);
            }
        }
        return;
    }
    if (event.eventSeq <= m_watermark)
    {
        return; // already held (a replay page delivered it first)
    }
    if (event.eventSeq == m_watermark + 1)
    {
        acceptInOrder(event);
        return;
    }
#ifdef GW_NC_NO_REPLAY
    // test-only control: a hole is skipped, never replayed
    m_watermark = event.eventSeq - 1;
    acceptInOrder(event);
    return;
#else
    openGap(ResyncReason::EVENT_GAP);
    m_gap.buffer.emplace(event.eventSeq, event);
#endif
}

void StreamCore::onLiveBalance(const wire::BalanceUpdate &line, std::int64_t)
{
    const auto it = m_users.find(line.userId);
    if (it == m_users.end())
    {
        return;
    }
    User &u = it->second;
    if (Bal::LOADING == u.bal)
    {
        // older than the UserBalances reply still to come (one egress queue per session, apply order): discarded
        m_stats.staleBalanceLines.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (Bal::READY != u.bal)
    {
        return;
    }
    u.balances[line.assetId] = { line.available, line.locked };
    Text frame;
    for (const std::uint64_t cid : u.conns)
    {
        Conn *c = conn(cid);
        if (nullptr == c || !c->balancesActive)
        {
            continue;
        }
        if (!frame)
        {
            const std::string code = assetCode(line.assetId);
            frame = text("{\"type\":\"balance\",\"assetId\":" + api::quote(std::to_string(line.assetId)) + ",\"asset\":" +
                (code.empty() ? std::string("null") : api::quote(code)) + ",\"available\":" +
                api::quote(rest::formatMoney(line.available)) + ",\"locked\":" + api::quote(rest::formatMoney(line.locked)) +
                "}");
        }
        enqueue(*c, frame);
    }
}

void StreamCore::onAdmission(const wire::AdmissionState &a)
{
    if (m_conns.empty())
    {
        return;
    }
    const Text frame = text("{\"type\":\"admission\",\"marketId\":" + api::quote(std::to_string(a.marketId)) + ",\"gate\":" +
        api::quote(wire::money::GateState::c_str(a.gateState)) + ",\"reasons\":" + rest::gateReasonsJson(a.gateReasons) +
        ",\"circuitBreaker\":" + (a.circuitBreaker ? "true" : "false") + "}");
    for (auto &[id, c] : m_conns)
    {
        if (!c.anonymous) // the admission gate goes to signed-in connections only
        {
            enqueue(c, frame);
        }
    }
}

// an anonymous connection gets engine and election frames while it holds the engine channel; a signed-in one always
static bool wantsEngine(bool anonymous, unsigned subscribed) noexcept
{
    return !anonymous || 0 != (subscribed & channel::ENGINE);
}

void StreamCore::onEngineStatus(Text frame)
{
    m_engineStatus = std::move(frame);
    for (auto &[id, c] : m_conns)
    {
        if (wantsEngine(c.anonymous, c.subscribed))
        {
            enqueue(c, m_engineStatus, ENGINE_FRAME_KEY);
        }
    }
}

void StreamCore::onEngineEvent(const Text &frame)
{
    for (auto &[id, c] : m_conns)
    {
        if (wantsEngine(c.anonymous, c.subscribed))
        {
            enqueue(c, frame);
        }
    }
}

bool StreamCore::onOutcome(const session::Outcome &o, std::int64_t nowNs)
{
    const auto it = m_outstanding.find(o.ticket);
    if (it == m_outstanding.end())
    {
        return false;
    }
    const Work w = it->second;
    m_outstanding.erase(it);
    if (WorkKind::SNAPSHOT_PAGE == w.kind || WorkKind::BALANCES == w.kind)
    {
        m_outstandingUserWork--;
    }
    const auto *events = std::get_if<session::EventsReply>(&o.reply);
    const auto *snapshot = std::get_if<session::SnapshotReply>(&o.reply);
    const auto *balances = std::get_if<session::BalancesReply>(&o.reply);
    const bool answered = session::OutcomeKind::REPLY == o.kind &&
        (((WorkKind::REPLAY == w.kind || WorkKind::BACKFILL == w.kind || WorkKind::PROBE == w.kind) && nullptr != events) ||
            (WorkKind::SNAPSHOT_PAGE == w.kind && nullptr != snapshot) || (WorkKind::BALANCES == w.kind && nullptr != balances));
    if (!answered)
    {
        m_stats.queryFailures.fetch_add(1, std::memory_order_relaxed);
        logLine(rt::LogLevel::WARN, nowNs, "stream: a %s query ended %s; asked again",
            WorkKind::SNAPSHOT_PAGE == w.kind ? "snapshot" : WorkKind::BALANCES == w.kind ? "balance"
                : WorkKind::PROBE == w.kind ? "head probe" : "replay",
            session::OutcomeKind::REPLY == o.kind ? "with a reject" : session::nameOf(o.kind));
        retryLater(w);
        return true;
    }
    switch (w.kind)
    {
        case WorkKind::PROBE:
            onProbeReply(*events, nowNs);
            break;
        case WorkKind::REPLAY:
            onEventsReply(w, *events, nowNs);
            break;
        case WorkKind::BACKFILL:
            onBackfillReply(*events);
            break;
        case WorkKind::SNAPSHOT_PAGE:
        {
            const auto u = m_users.find(w.userId);
            if (u != m_users.end() && Cache::STITCHING == u->second.orders && u->second.stitchGen == w.gen)
            {
                onSnapshotPage(u->second, *snapshot);
            }
            break;
        }
        case WorkKind::BALANCES:
        {
            const auto u = m_users.find(w.userId);
            if (u != m_users.end() && Bal::LOADING == u->second.bal && u->second.balGen == w.gen)
            {
                onBalancesReply(u->second, *balances);
            }
            break;
        }
    }
    return true;
}

void StreamCore::retryLater(const Work &w)
{
    switch (w.kind)
    {
        case WorkKind::PROBE:
            m_probeInFlight = false;
            if (m_watermark < 0)
            {
                m_lastProbeSentNs = -1; // the first probe sets the watermark and every snapshot waits on it: at once
            }
            break;
        case WorkKind::REPLAY:
            m_gap.inFlight = false; // doWork asks again while the gap is open
            break;
        case WorkKind::BACKFILL:
            m_backfill.inFlight = false;
            break;
        case WorkKind::SNAPSHOT_PAGE:
        case WorkKind::BALANCES:
            queueWork(w); // the same page (or read) again; a stale one is skipped by the scheduler
            break;
    }
}

// ================================================================ events, the window and gaps

void StreamCore::append(const wire::OrderEvent &event)
{
    m_window.push_back(event);
    m_watermark = event.eventSeq;
    while (static_cast<std::int64_t>(m_window.size()) > m_settings.eventWindow)
    {
        m_evictedMaxPosition = std::max(m_evictedMaxPosition, m_window.front().logPosition);
        m_window.pop_front();
    }
}

void StreamCore::acceptInOrder(const wire::OrderEvent &event)
{
    append(event);
    route(event);
}

void StreamCore::route(const wire::OrderEvent &e)
{
    const auto it = m_users.find(e.userId);
    if (it == m_users.end())
    {
        return;
    }
    User &u = it->second;
    if (Cache::READY == u.orders)
    {
        applyEvent(u.open, e);
    }
    Text order;
    Text fill;
    for (const std::uint64_t cid : u.conns)
    {
        Conn *c = conn(cid);
        if (nullptr == c)
        {
            continue;
        }
        if (c->ordersActive)
        {
            if (!order)
            {
                order = orderFrame(e);
            }
            enqueue(*c, order, e.orderId);
        }
        if (c->fillsActive && isFill(e))
        {
            if (!fill)
            {
                fill = fillFrame(e);
            }
#ifdef GW_NC_COALESCE_FILLS
            enqueue(*c, fill, -e.orderId); // test-only control: fills of one order coalesce
#else
            enqueue(*c, fill);
#endif
        }
    }
}

void StreamCore::applyEvent(std::map<std::int64_t, OrderRow> &rows, const wire::OrderEvent &e)
{
    if (rest::terminal(e.status))
    {
        rows.erase(e.orderId);
    }
    else
    {
        rows[e.orderId] = OrderRow{ e };
    }
}

void StreamCore::openGap(ResyncReason reason)
{
    if (m_gap.open)
    {
        if (ResyncReason::EVENT_GAP == m_gap.reason)
        {
            m_gap.reason = reason; // a seam names the gap it widened
        }
        return;
    }
    m_gap.open = true;
    m_gap.reason = reason;
    m_gap.inFlight = false;
    m_gap.targetSeq = std::numeric_limits<std::int64_t>::max(); // closed only after a reply names the core's head
    m_stats.gaps.fetch_add(1, std::memory_order_relaxed);
}

void StreamCore::drainBuffer()
{
    while (!m_gap.buffer.empty())
    {
        const auto it = m_gap.buffer.begin();
        if (it->first <= m_watermark)
        {
            m_gap.buffer.erase(it);
        }
        else if (it->first == m_watermark + 1)
        {
            acceptInOrder(it->second);
            m_gap.buffer.erase(it);
        }
        else
        {
            break;
        }
    }
}

void StreamCore::closeGapIfDone()
{
    if (m_gap.open && m_watermark >= m_gap.targetSeq && m_gap.buffer.empty())
    {
        m_gap.open = false;
        afterGapClosed();
    }
}

void StreamCore::afterGapClosed()
{
    std::vector<std::int64_t> waiting;
    for (auto &[id, u] : m_users)
    {
        if (Cache::STITCHING == u.orders && u.pagesDone)
        {
            waiting.push_back(id);
        }
    }
    for (const std::int64_t id : waiting)
    {
        const auto it = m_users.find(id);
        if (it != m_users.end())
        {
            finishStitch(it->second);
        }
    }
}

void StreamCore::onProbeReply(const session::EventsReply &r, std::int64_t nowNs)
{
    m_probeInFlight = false;
    m_lastProbeAnswerNs = nowNs;
    m_coreLastSeq = r.end.lastSeq;
    m_coreFirstAvailable = r.end.firstAvailableSeq;
    if (m_watermark < 0)
    {
        m_watermark = r.end.lastSeq;
        logLine(rt::LogLevel::INFO, nowNs, "stream: watermark %" PRId64 " from the first head probe (the core's ring holds "
            "%" PRId64 "..%" PRId64 ")", m_watermark, r.end.firstAvailableSeq, r.end.lastSeq);
        for (const wire::OrderEvent &e : m_initBuffer)
        {
            if (e.eventSeq > m_watermark)
            {
                m_gap.buffer.emplace(e.eventSeq, e);
            }
            else
            {
                // at or below the watermark: never in the window, so a stitch whose span reaches it must read again
                m_evictedMaxPosition = std::max(m_evictedMaxPosition, e.logPosition);
            }
        }
        m_initBuffer.clear();
        drainBuffer();
        if (!m_gap.buffer.empty())
        {
            openGap(ResyncReason::EVENT_GAP);
        }
        else
        {
            afterGapClosed(); // a stitch that waited for the watermark finishes now
        }
        return;
    }
    if (!m_gap.open && r.end.lastSeq > m_watermark)
    {
        logLine(rt::LogLevel::WARN, nowNs, "stream: the head probe reports eventSeq %" PRId64 " above the watermark %" PRId64
            ": replaying", r.end.lastSeq, m_watermark);
#ifndef GW_NC_NO_REPLAY
        openGap(ResyncReason::EVENT_GAP);
#endif
    }
}

void StreamCore::onEventsReply(const Work &, const session::EventsReply &r, std::int64_t nowNs)
{
    m_gap.inFlight = false;
    m_stats.replayPages.fetch_add(1, std::memory_order_relaxed);
    m_coreLastSeq = r.end.lastSeq;
    m_coreFirstAvailable = r.end.firstAvailableSeq;
    if (!m_gap.open)
    {
        return;
    }
    if ((0 == r.end.count && r.end.firstAvailableSeq > m_watermark + 1) || r.end.nextFromSeq > r.end.lastSeq)
    {
        logLine(rt::LogLevel::WARN, nowNs, "stream: overrun: the core's ring holds %" PRId64 "..%" PRId64
            " and the watermark is %" PRId64 " (%s): every connection resyncs", r.end.firstAvailableSeq, r.end.lastSeq,
            m_watermark, nameOf(m_gap.reason));
        overrun(r.end.lastSeq, m_gap.reason);
        return;
    }
    for (const wire::OrderEvent &e : r.events)
    {
        if (e.eventSeq <= m_watermark)
        {
            continue;
        }
        if (e.eventSeq != m_watermark + 1)
        {
            break; // never inside one reply; asked again from the watermark
        }
        acceptInOrder(e);
        m_stats.replayedEvents.fetch_add(1, std::memory_order_relaxed);
    }
    m_gap.targetSeq = r.end.lastSeq;
    drainBuffer();
    closeGapIfDone();
}

void StreamCore::overrun(std::int64_t newWatermark, ResyncReason reason)
{
    if (!m_window.empty())
    {
        m_evictedMaxPosition = std::max(m_evictedMaxPosition, m_window.back().logPosition);
    }
    for (const auto &[seq, e] : m_gap.buffer)
    {
        if (seq <= newWatermark)
        {
            m_evictedMaxPosition = std::max(m_evictedMaxPosition, e.logPosition);
        }
    }
    m_window.clear();
    m_watermark = std::max(m_watermark, newWatermark);
    m_backfill = Backfill{};
    // every order cache and stitch is void; balances are not sequenced and stay
    for (auto &[id, u] : m_users)
    {
        u.orders = Cache::NONE;
        u.open.clear();
        u.pages.clear();
        u.pagesDone = false;
        u.stitchGen = ++m_generation;
    }
    std::vector<Conn *> order;
    order.reserve(m_conns.size());
    for (auto &[id, c] : m_conns)
    {
        order.push_back(&c);
    }
    std::sort(order.begin(), order.end(), [](const Conn *a, const Conn *b) { return a->openOrder < b->openOrder; });
    for (Conn *c : order)
    {
        if (c->closing || 0 == (c->subscribed & (channel::ORDERS | channel::FILLS)))
        {
            continue;
        }
        sendResync(*c, reason);
        if (c->closing)
        {
            continue;
        }
        c->resumePending = false;
        if (0 != (c->subscribed & channel::ORDERS))
        {
            c->ordersActive = false;
            c->ordersPending = true;
            if (0 != (c->subscribed & channel::FILLS))
            {
                c->fillsActive = false;
                c->fillsWithOrders = true;
            }
        }
        else
        {
            c->fillsActive = true;
        }
    }
    // re-snapshots in the order of the oldest connection (the scheduler paces them through the budget)
    for (Conn *c : order)
    {
        if (!c->closing && !c->anonymous)
        {
            ensureSnapshots(*c, m_users.at(c->userId));
        }
    }
    drainBuffer();
    m_gap.targetSeq = m_watermark;
    closeGapIfDone();
}

void StreamCore::onBackfillReply(const session::EventsReply &r)
{
    Backfill &b = m_backfill;
    b.inFlight = false;
    m_coreLastSeq = r.end.lastSeq;
    m_coreFirstAvailable = r.end.firstAvailableSeq;
    if (!b.running)
    {
        return;
    }
    if ((0 == r.end.count && r.end.firstAvailableSeq > b.cursor + 1) || r.end.nextFromSeq > r.end.lastSeq ||
        0 == r.end.count)
    {
        b = Backfill{}; // the core's ring no longer covers it: the waiting resumes are judged again (too old)
        return;
    }
    for (const wire::OrderEvent &e : r.events)
    {
        if (e.eventSeq <= b.cursor)
        {
            continue;
        }
        if (e.eventSeq != b.cursor + 1 || e.eventSeq > b.to)
        {
            break;
        }
        b.events.push_back(e);
        b.cursor = e.eventSeq;
        m_stats.replayedEvents.fetch_add(1, std::memory_order_relaxed);
    }
    if (b.cursor < b.to)
    {
        return; // the next page (doWork)
    }
    const bool fits = static_cast<std::int64_t>(m_window.size() + b.events.size()) <= m_settings.eventWindow;
    if (windowFirstSeq() == b.frontSeq && fits)
    {
        for (auto it = b.events.rbegin(); it != b.events.rend(); ++it)
        {
            m_window.push_front(*it);
        }
        m_stats.backfills.fetch_add(1, std::memory_order_relaxed);
    }
    b = Backfill{};
}

// ================================================================ users, caches and the stitch

StreamCore::User &StreamCore::userOf(std::int64_t userId)
{
    User &u = m_users[userId];
    u.id = userId;
    return u;
}

void StreamCore::dropUser(std::int64_t userId)
{
    m_users.erase(userId); // queued work for it is skipped by the scheduler
}

void StreamCore::startStitch(User &u)
{
    u.orders = Cache::STITCHING;
    u.stitchGen = ++m_generation;
    u.pages.clear();
    u.open.clear();
    u.firstAsOf = -1;
    u.lastAsOf = -1;
    u.nextOrderId = 0;
    u.pagesDone = false;
    queueWork(Work{ WorkKind::SNAPSHOT_PAGE, u.id, u.stitchGen });
}

void StreamCore::onSnapshotPage(User &u, const session::SnapshotReply &r)
{
    for (const wire::OrderSnapshotEntry &e : r.entries)
    {
        u.pages[e.orderId] = OrderRow{ e };
    }
#ifdef GW_NC_SINGLE_PAGE_ASOF
    u.firstAsOf = r.end.asOfLogPosition; // test-only control: the span starts at the last page, not the first
#else
    if (u.firstAsOf < 0)
    {
        u.firstAsOf = r.end.asOfLogPosition;
    }
#endif
    u.lastAsOf = r.end.asOfLogPosition;
    if (0 != r.end.nextOrderId)
    {
        u.nextOrderId = r.end.nextOrderId;
        queueWork(Work{ WorkKind::SNAPSHOT_PAGE, u.id, u.stitchGen }, true); // the user's next page before other users
        return;
    }
    u.pagesDone = true;
    finishStitch(u);
}

void StreamCore::finishStitch(User &u)
{
    if (gapOpen())
    {
        return; // the window must hold every event up to the last page's position: afterGapClosed() comes back
    }
    if (m_evictedMaxPosition > u.firstAsOf)
    {
        startStitch(u); // the window lost an event of the span: read the pages again
        return;
    }
    std::map<std::int64_t, OrderRow> rows = std::move(u.pages);
    u.pages.clear();
    std::int64_t asOfSeq = m_window.empty() ? m_watermark : m_window.front().eventSeq - 1;
    std::vector<const wire::OrderEvent *> foldedFills;
    std::vector<const wire::OrderEvent *> following;
    const wire::OrderEvent *lastFolded = nullptr;
    for (const wire::OrderEvent &e : m_window)
    {
        if (e.logPosition <= u.lastAsOf)
        {
            asOfSeq = e.eventSeq;
            if (e.userId == u.id && e.logPosition > u.firstAsOf)
            {
                applyEvent(rows, e);
                lastFolded = &e;
                if (isFill(e))
                {
                    foldedFills.push_back(&e);
                }
            }
        }
        else if (e.userId == u.id)
        {
            following.push_back(&e);
        }
    }
#ifdef GW_NC_STITCH_INCLUSIVE
    // test-only control: the live frames start at asOf.eventSeq instead of above it
    if (nullptr != lastFolded && lastFolded->eventSeq == asOfSeq)
    {
        following.insert(following.begin(), lastFolded);
    }
#else
    (void)lastFolded;
#endif
    std::string snapshot = "{\"type\":\"snapshot\",\"channel\":\"orders\",\"asOf\":{\"eventSeq\":" + std::to_string(asOfSeq) +
        "},\"orders\":[";
    bool first = true;
    for (const auto &[orderId, row] : rows)
    {
        snapshot += (first ? "" : ",") + rowJson(row.v);
        first = false;
    }
    snapshot += "]}";
    const Text snapshotText = text(std::move(snapshot));
    u.open = std::move(rows);
    for (const wire::OrderEvent *e : following)
    {
        applyEvent(u.open, *e);
    }
    u.orders = Cache::READY;
    u.pagesDone = false;
    for (const std::uint64_t cid : u.conns)
    {
        Conn *c = conn(cid);
        if (nullptr == c || !c->ordersPending)
        {
            continue;
        }
        const bool fillsNow = c->fillsWithOrders;
        enqueue(*c, snapshotText);
        if (fillsNow)
        {
            for (const wire::OrderEvent *e : foldedFills)
            {
                enqueue(*c, fillFrame(*e)); // a fill is never dropped by folding
            }
        }
        for (const wire::OrderEvent *e : following)
        {
            enqueue(*c, orderFrame(*e), e->orderId);
            if (fillsNow && isFill(*e))
            {
                enqueue(*c, fillFrame(*e));
            }
        }
        if (c->closing)
        {
            continue;
        }
        c->ordersPending = false;
        c->ordersActive = true;
        if (fillsNow)
        {
            c->fillsWithOrders = false;
            c->fillsActive = true;
        }
    }
}

void StreamCore::serveOrdersFromCache(User &u, Conn &c)
{
    std::string snapshot = "{\"type\":\"snapshot\",\"channel\":\"orders\",\"asOf\":{\"eventSeq\":" +
        std::to_string(m_watermark) + "},\"orders\":[";
    bool first = true;
    for (const auto &[orderId, row] : u.open)
    {
        snapshot += (first ? "" : ",") + rowJson(row.v);
        first = false;
    }
    snapshot += "]}";
    enqueue(c, text(std::move(snapshot)));
    m_stats.cacheServes.fetch_add(1, std::memory_order_relaxed);
    if (c.closing)
    {
        return;
    }
    c.ordersPending = false;
    c.ordersActive = true;
    if (c.fillsWithOrders)
    {
        c.fillsWithOrders = false;
        c.fillsActive = true;
    }
}

void StreamCore::startBalances(User &u)
{
    u.bal = Bal::LOADING;
    u.balGen = ++m_generation;
    u.heldReply.reset();
    queueWork(Work{ WorkKind::BALANCES, u.id, u.balGen });
}

void StreamCore::onBalancesReply(User &u, const session::BalancesReply &r)
{
#ifdef GW_NC_BALANCE_REPLY_LAST
    // test-only control: the reply is applied a cycle later, after the newer lines that followed it
    if (!u.heldReply)
    {
        u.heldReply = r;
        u.bal = Bal::READY;
        return;
    }
    u.heldReply.reset();
#endif
    u.balances.clear();
    for (const wire::BalanceLine &l : r.lines)
    {
        u.balances[l.assetId] = { l.available, l.locked };
    }
    u.bal = Bal::READY;
    for (const std::uint64_t cid : u.conns)
    {
        Conn *c = conn(cid);
        if (nullptr != c && 0 != (c->subscribed & channel::BALANCES) && (c->balancesPending || c->balancesActive))
        {
            sendBalanceSnapshot(u, *c);
        }
    }
}

void StreamCore::sendBalanceSnapshot(User &u, Conn &c)
{
    std::map<std::int32_t, std::pair<std::int64_t, std::int64_t>> rows;
    const std::shared_ptr<const session::CoreConfig> config = m_config ? m_config() : nullptr;
    if (config)
    {
        for (const wire::AssetConfig &a : config->assets)
        {
            rows[a.assetId] = { 0, 0 };
        }
    }
    for (const auto &[assetId, v] : u.balances)
    {
        rows[assetId] = v;
    }
    std::string out = "{\"type\":\"snapshot\",\"channel\":\"balances\",\"assets\":[";
    bool first = true;
    for (const auto &[assetId, v] : rows)
    {
        const std::string code = assetCode(assetId);
        out += (first ? "" : ",") + std::string("{\"assetId\":") + api::quote(std::to_string(assetId)) + ",\"asset\":" +
            (code.empty() ? std::string("null") : api::quote(code)) + ",\"available\":" +
            api::quote(rest::formatMoney(v.first)) + ",\"locked\":" + api::quote(rest::formatMoney(v.second)) + "}";
        first = false;
    }
    out += "]}";
    enqueue(c, text(std::move(out)));
    if (!c.closing)
    {
        c.balancesPending = false;
        c.balancesActive = true;
    }
}

std::string StreamCore::assetCode(std::int32_t assetId) const
{
    const std::shared_ptr<const session::CoreConfig> config = m_config ? m_config() : nullptr;
    if (config)
    {
        for (const wire::AssetConfig &a : config->assets)
        {
            if (a.assetId == assetId)
            {
                return rest::fixedText(a.code.data(), a.code.size());
            }
        }
    }
    return std::string();
}

// ================================================================ connections

StreamCore::Conn *StreamCore::conn(std::uint64_t connId)
{
    const auto it = m_conns.find(connId);
    return it == m_conns.end() ? nullptr : &it->second;
}

void StreamCore::open(std::uint64_t connId, std::int64_t userId, std::shared_ptr<Outlet> outlet, std::int64_t)
{
    Conn &c = m_conns[connId];
    c.id = connId;
    c.userId = userId;
    c.openOrder = ++m_nextOpenOrder;
    c.out = std::move(outlet);
    User &u = userOf(userId);
    u.conns.push_back(connId);
    u.lingerUntilNs = -1;
    m_stats.connections.store(static_cast<std::int64_t>(m_conns.size()), std::memory_order_relaxed);
    if (m_engineStatus)
    {
        enqueue(c, m_engineStatus, ENGINE_FRAME_KEY);
    }
}

void StreamCore::openAnonymous(std::uint64_t connId, std::shared_ptr<Outlet> outlet, std::int64_t)
{
    Conn &c = m_conns[connId];
    c.id = connId;
    c.userId = 0;
    c.anonymous = true;
    c.openOrder = ++m_nextOpenOrder;
    c.out = std::move(outlet);
    c.subscribed = channel::ENGINE;
    m_stats.connections.store(static_cast<std::int64_t>(m_conns.size()), std::memory_order_relaxed);
    enqueue(c, text("{\"type\":\"subscribed\",\"channels\":" + channelsJson(c.subscribed) + "}"));
    if (m_engineStatus)
    {
        enqueue(c, m_engineStatus, ENGINE_FRAME_KEY);
    }
}

void StreamCore::subscribe(std::uint64_t connId, unsigned channels, std::optional<std::int64_t> resumeFrom,
    std::int64_t nowNs, std::int64_t frameUserId)
{
    Conn *c = conn(connId);
    if (nullptr == c || c->closing)
    {
        return;
    }
    if (c->anonymous)
    {
        // the I/O side refuses every other channel and a resume; whatever reaches here, only `engine` is taken
        const unsigned added = channels & channel::ENGINE & ~c->subscribed;
        c->subscribed |= channels & channel::ENGINE;
        enqueue(*c, text("{\"type\":\"subscribed\",\"channels\":" + channelsJson(c->subscribed) + "}"));
        if (0 != added && m_engineStatus)
        {
            enqueue(*c, m_engineStatus, ENGINE_FRAME_KEY);
        }
        return;
    }
#ifdef GW_NC_ROUTE_BY_FRAME_USER
    // test-only control: the router takes the user a client frame names
    if (frameUserId > 0 && frameUserId != c->userId)
    {
        User &old = m_users.at(c->userId);
        old.conns.erase(std::remove(old.conns.begin(), old.conns.end(), connId), old.conns.end());
        c->userId = frameUserId;
        userOf(frameUserId).conns.push_back(connId);
    }
#else
    (void)frameUserId;
#endif
    if (!m_settings.balanceStream && 0 != (channels & channel::BALANCES))
    {
        enqueue(*c, text("{\"type\":\"error\",\"code\":\"FEATURE_DISABLED\",\"error\":\"the balances channel is off on "
            "this stream (STREAM_BALANCE_STREAM)\"}"));
        channels &= ~channel::BALANCES;
    }
    const unsigned added = channels & ~c->subscribed;
    c->subscribed |= channels;
    enqueue(*c, text("{\"type\":\"subscribed\",\"channels\":" + channelsJson(c->subscribed) + "}"));
    if (c->closing)
    {
        return;
    }
    User &u = m_users.at(c->userId);
    const unsigned seq = added & (channel::ORDERS | channel::FILLS);
    if (resumeFrom.has_value() && 0 != seq)
    {
        c->resumePending = true;
        c->resumeFrom = *resumeFrom;
        c->resumeChannels |= seq;
    }
    else
    {
        if (0 != (added & channel::ORDERS))
        {
            c->ordersPending = true;
            if (0 != (added & channel::FILLS))
            {
                c->fillsWithOrders = true;
            }
        }
        if (0 != (added & channel::FILLS) && 0 == (added & channel::ORDERS))
        {
            if (c->ordersPending)
            {
                c->fillsWithOrders = true;
            }
            else
            {
                c->fillsActive = true;
            }
        }
    }
    if (0 != (added & channel::BALANCES))
    {
        c->balancesPending = true;
    }
    ensureSnapshots(*c, u);
    if (c->resumePending && !gapOpen())
    {
        processResume(*c, nowNs);
    }
}

void StreamCore::unsubscribe(std::uint64_t connId, unsigned channels)
{
    Conn *c = conn(connId);
    if (nullptr == c || c->closing)
    {
        return;
    }
    c->subscribed &= ~channels;
    if (0 != (channels & channel::ORDERS))
    {
        c->ordersActive = false;
        c->ordersPending = false;
        if (c->fillsWithOrders)
        {
            c->fillsWithOrders = false;
            c->fillsActive = 0 != (c->subscribed & channel::FILLS); // fills go on without the order snapshot
        }
    }
    if (0 != (channels & channel::FILLS))
    {
        c->fillsActive = false;
        c->fillsWithOrders = false;
    }
    if (0 != (channels & channel::BALANCES))
    {
        c->balancesActive = false;
        c->balancesPending = false;
    }
    c->resumeChannels &= c->subscribed;
    if (0 == c->resumeChannels)
    {
        c->resumePending = false;
    }
    enqueue(*c, text("{\"type\":\"subscribed\",\"channels\":" + channelsJson(c->subscribed) + "}"));
}

void StreamCore::ping(std::uint64_t connId)
{
    Conn *c = conn(connId);
    if (nullptr == c || c->closing)
    {
        return;
    }
#ifdef GW_NC_NO_HEARTBEAT
    const bool live = false; // test-only control: no watermark in pong
#else
    const bool live = (c->ordersActive || c->fillsActive) && !c->resumePending && m_watermark >= 0;
#endif
    enqueue(*c, text(std::string("{\"type\":\"pong\",\"eventSeq\":") + (live ? std::to_string(m_watermark) : "null") + "}"));
}

void StreamCore::gone(std::uint64_t connId, std::int64_t nowNs)
{
    removeConn(connId, nowNs);
}

void StreamCore::ensureSnapshots(Conn &c, User &u)
{
    if (c.closing)
    {
        return;
    }
    if (c.ordersPending)
    {
        if (Cache::READY == u.orders)
        {
            serveOrdersFromCache(u, c);
        }
        else if (Cache::NONE == u.orders)
        {
            startStitch(u);
        }
    }
    if (c.balancesPending && !c.closing)
    {
        if (Bal::READY == u.bal)
        {
            sendBalanceSnapshot(u, c);
            m_stats.cacheServes.fetch_add(1, std::memory_order_relaxed);
        }
        else if (Bal::NONE == u.bal)
        {
            startBalances(u);
        }
    }
}

void StreamCore::processResume(Conn &c, std::int64_t)
{
    if (!c.resumePending || c.closing)
    {
        return;
    }
    const std::int64_t n = c.resumeFrom;
    bool tooOld = false;
    if (n > m_watermark)
    {
        if (m_coreLastSeq >= 0 && n > m_coreLastSeq)
        {
            tooOld = true; // ahead of the core itself (a cursor from another genesis): it can never be served
        }
        else
        {
            return; // this stream is behind the client: served once the watermark passes it
        }
    }
    const std::int64_t first = windowFirstSeq();
    if (!tooOld && first <= n + 1)
    {
        replayFromWindow(c, n, 0 != (c.resumeChannels & channel::ORDERS), 0 != (c.resumeChannels & channel::FILLS));
        c.resumePending = false;
        c.resumeChannels = 0;
        m_stats.windowResumes.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (!tooOld)
    {
        const std::int64_t need = first - (n + 1);
        const bool ringCovers = m_coreFirstAvailable < 0 || n + 1 >= m_coreFirstAvailable;
        const bool fits = static_cast<std::int64_t>(m_window.size()) + need <= m_settings.eventWindow;
        if (ringCovers && fits && n >= 0)
        {
            if (!m_backfill.running)
            {
                m_backfill = Backfill{};
                m_backfill.running = true;
                m_backfill.from = n;
                m_backfill.cursor = n;
                m_backfill.frontSeq = first;
                m_backfill.to = first - 1;
            }
            else if (n < m_backfill.from)
            {
                // older than the round in progress: the next round, once this one ends
            }
            return;
        }
        tooOld = true;
    }
    const unsigned ch = c.resumeChannels;
    c.resumePending = false;
    c.resumeChannels = 0;
    sendResync(c, ResyncReason::RESUME_TOO_OLD);
    if (c.closing)
    {
        return;
    }
    if (0 != (ch & channel::ORDERS))
    {
        c.ordersPending = true;
        if (0 != (ch & channel::FILLS))
        {
            c.fillsWithOrders = true;
        }
    }
    else if (0 != (ch & channel::FILLS))
    {
        c.fillsActive = true;
    }
    ensureSnapshots(c, m_users.at(c.userId));
}

void StreamCore::replayFromWindow(Conn &c, std::int64_t fromSeq, bool orders, bool fills)
{
    for (const wire::OrderEvent &e : m_window)
    {
        if (e.eventSeq <= fromSeq || e.userId != c.userId)
        {
            continue;
        }
        if (orders)
        {
            enqueue(c, orderFrame(e), e.orderId);
        }
        if (fills && isFill(e))
        {
            enqueue(c, fillFrame(e));
        }
    }
    if (!c.closing)
    {
        c.ordersActive = c.ordersActive || orders;
        c.fillsActive = c.fillsActive || fills;
    }
}

void StreamCore::sendResync(Conn &c, ResyncReason reason)
{
    m_stats.resyncs[static_cast<std::size_t>(reason)].fetch_add(1, std::memory_order_relaxed);
    enqueue(c, text(std::string("{\"type\":\"resync\",\"reason\":\"") + nameOf(reason) + "\"}"));
}

void StreamCore::enqueue(Conn &c, Text frame, std::int64_t key)
{
    if (c.closing)
    {
        return;
    }
    const std::int64_t before = c.queuedMessages + c.out->unwrittenMessages();
#ifdef GW_NC_ANON_USER_BOUND
    // test-only control: an anonymous connection is held to the signed-in bound
    const std::int64_t boundMessages = m_settings.wsQueueMessages;
    const std::int64_t boundBytes = m_settings.wsQueueBytes;
#else
    const std::int64_t boundMessages = c.anonymous ? m_settings.anonQueueMessages : m_settings.wsQueueMessages;
    const std::int64_t boundBytes = c.anonymous ? m_settings.anonQueueBytes : m_settings.wsQueueBytes;
#endif
    if (0 != key)
    {
        const auto f = c.orderFrameAt.find(key);
        if (f != c.orderFrameAt.end() && f->second >= c.queueBase)
        {
            Pending &p = c.queue[static_cast<std::size_t>(f->second - c.queueBase)];
            if (!p.dead && p.key == key)
            {
                p.dead = true; // the latest state of this order replaces the queued one, at the tail
                c.queuedMessages--;
                c.queuedBytes -= static_cast<std::int64_t>(p.text->size());
            }
        }
    }
    const std::int64_t bytes = static_cast<std::int64_t>(frame->size());
    c.largestFrameBytes = before > 0 ? std::max(c.largestFrameBytes, bytes) : bytes;
    const bool direct = c.queue.empty() && c.out->offer(frame);
    if (!direct)
    {
        c.queue.push_back(Pending{ std::move(frame), key, false });
        if (0 != key)
        {
            c.orderFrameAt[key] = c.queueBase + c.queue.size() - 1;
        }
        c.queuedMessages++;
        c.queuedBytes += bytes;
    }
    // the bound counts what waits here and what the writer holds unwritten, whichever path the frame took (the
    // writer's handover can hold more than the bound)
    const std::int64_t messages = c.queuedMessages + c.out->unwrittenMessages();
    if (messages > m_stats.queueHighWater.load(std::memory_order_relaxed))
    {
        m_stats.queueHighWater.store(messages, std::memory_order_relaxed);
    }
#ifndef GW_NC_UNBOUNDED_QUEUE
    // The byte test leaves out the largest frame handed over since the connection was last empty: a frame larger than
    // the byte bound on its own (a large snapshot) still goes out, with the frames that follow it inside the bound (it
    // could never be sent otherwise); the bound is on what waits behind it.
    const bool overBytes = c.queuedBytes + c.out->unwrittenBytes() - c.largestFrameBytes > boundBytes;
    if (messages > boundMessages || overBytes)
    {
        m_stats.closedAtBound.fetch_add(1, std::memory_order_relaxed);
        closeConn(c, close_code::RESYNC, "RESYNC");
        return;
    }
#else
    (void)before;
    (void)boundMessages;
    (void)boundBytes;
#endif
    if (direct)
    {
        return; // nothing waits here: the queue only holds what the writer cannot take yet
    }
    if (!c.dirty)
    {
        c.dirty = true;
        m_dirty.push_back(c.id);
    }
}

void StreamCore::closeConn(Conn &c, std::uint16_t code, const char *reason)
{
    if (c.closing)
    {
        return;
    }
    c.closing = true;
    c.out->close(code, reason);
    c.queue.clear();
    c.orderFrameAt.clear();
    c.queuedMessages = 0;
    c.queuedBytes = 0;
    m_toRemove.push_back(c.id);
}

void StreamCore::removeConn(std::uint64_t connId, std::int64_t nowNs)
{
    const auto it = m_conns.find(connId);
    if (it == m_conns.end())
    {
        return;
    }
    const std::int64_t userId = it->second.userId;
    m_conns.erase(it);
    const auto u = m_users.find(userId);
    if (u != m_users.end())
    {
        auto &v = u->second.conns;
        v.erase(std::remove(v.begin(), v.end(), connId), v.end());
        if (v.empty())
        {
            u->second.lingerUntilNs = nowNs + m_settings.userLingerMs * MS;
        }
    }
    m_stats.connections.store(static_cast<std::int64_t>(m_conns.size()), std::memory_order_relaxed);
}

int StreamCore::pump()
{
    int work = 0;
    std::vector<std::uint64_t> still;
    for (const std::uint64_t id : m_dirty)
    {
        Conn *c = conn(id);
        if (nullptr == c)
        {
            continue;
        }
        c->dirty = false;
        if (c->closing)
        {
            continue;
        }
        while (!c->queue.empty())
        {
            Pending &p = c->queue.front();
            if (!p.dead)
            {
                if (!c->out->offer(p.text))
                {
                    break;
                }
                c->queuedMessages--;
                c->queuedBytes -= static_cast<std::int64_t>(p.text->size());
                if (0 != p.key)
                {
                    const auto f = c->orderFrameAt.find(p.key);
                    if (f != c->orderFrameAt.end() && f->second == c->queueBase)
                    {
                        c->orderFrameAt.erase(f);
                    }
                }
                work++;
            }
            c->queue.pop_front();
            c->queueBase++;
        }
        if (!c->queue.empty())
        {
            c->dirty = true;
            still.push_back(id);
        }
    }
    m_dirty.swap(still);
    return work;
}

// ================================================================ queries

void StreamCore::queueWork(Work w, bool front)
{
    if (front)
    {
        m_work.push_front(w);
    }
    else
    {
        m_work.push_back(w);
    }
}

void StreamCore::submitDirect(Work w, session::Command command)
{
    const session::SubmitResult r = m_queries.submit(std::move(command));
    m_stats.eventQueries.fetch_add(1, std::memory_order_relaxed);
    if (r.accepted)
    {
        m_outstanding.emplace(r.ticket, w);
        return;
    }
    retryLater(w);
}

int StreamCore::schedule(std::int64_t)
{
    int work = 0;
    std::vector<Work> held; // snapshot pages before the watermark is known (their span could not be checked)
    while (!m_work.empty())
    {
#ifndef GW_NC_NO_RESNAPSHOT_PACING
        if (m_outstandingUserWork >= m_settings.queryBurst)
        {
            break; // the rest wait here, in order, not in the session (whose deadline would expire them)
        }
#endif
        const Work w = m_work.front();
        m_work.pop_front();
        if (WorkKind::SNAPSHOT_PAGE == w.kind && m_watermark < 0)
        {
            held.push_back(w);
            continue;
        }
        const auto it = m_users.find(w.userId);
        if (it == m_users.end())
        {
            continue;
        }
        User &u = it->second;
        session::Command command;
        if (WorkKind::SNAPSHOT_PAGE == w.kind)
        {
            if (Cache::STITCHING != u.orders || u.stitchGen != w.gen || u.pagesDone)
            {
                continue;
            }
            wire::RequestOrderSnapshot q;
            q.userId = u.id;
            q.fromOrderId = u.nextOrderId;
            command = q;
        }
        else
        {
            if (Bal::LOADING != u.bal || u.balGen != w.gen)
            {
                continue;
            }
            wire::RequestUserBalances q;
            q.userId = u.id;
            command = q;
        }
        const session::SubmitResult r = m_queries.submit(std::move(command));
        if (!r.accepted)
        {
            m_work.push_front(w); // the session's table is full: next cycle
            break;
        }
        (WorkKind::SNAPSHOT_PAGE == w.kind ? m_stats.snapshotQueries : m_stats.balanceQueries)
            .fetch_add(1, std::memory_order_relaxed);
        m_outstanding.emplace(r.ticket, w);
        m_outstandingUserWork++;
        work++;
    }
    for (auto it = held.rbegin(); it != held.rend(); ++it)
    {
        m_work.push_front(*it); // back at the front, in their order
    }
    return work;
}

// ================================================================ the duty cycle

int StreamCore::doWork(std::int64_t nowNs)
{
    int work = 0;
    // the replay of an open gap, one page at a time
    if (m_gap.open && !m_gap.inFlight && m_watermark >= 0)
    {
        m_gap.inFlight = true;
        wire::RequestOrderEvents q;
        q.fromEventSeq = m_watermark;
        q.maxEvents = static_cast<std::int32_t>(std::min<std::int64_t>(m_settings.replayPage, 1 << 30));
        submitDirect(Work{ WorkKind::REPLAY, 0, 0 }, session::Command{ q });
        work++;
    }
    // the backfill of a resume, one page at a time
    if (m_backfill.running && !m_backfill.inFlight)
    {
        m_backfill.inFlight = true;
        wire::RequestOrderEvents q;
        q.fromEventSeq = m_backfill.cursor;
        q.maxEvents = static_cast<std::int32_t>(std::min<std::int64_t>(
            std::min<std::int64_t>(m_settings.replayPage, m_backfill.to - m_backfill.cursor), 1 << 30));
        submitDirect(Work{ WorkKind::BACKFILL, 0, 0 }, session::Command{ q });
        work++;
    }
    // the head probe: a real round trip through the log, also the readiness signal
    if (m_registeredOnce && !m_probeInFlight && (m_lastProbeSentNs < 0 || nowNs - m_lastProbeSentNs >= m_settings.headProbeMs * MS))
    {
        m_probeInFlight = true;
        m_lastProbeSentNs = nowNs;
        wire::RequestOrderEvents q;
        q.fromEventSeq = std::max<std::int64_t>(m_watermark, 0);
        q.maxEvents = 0;
        submitDirect(Work{ WorkKind::PROBE, 0, 0 }, session::Command{ q });
        work++;
    }
    // the watermark to every connection that follows the event stream
#ifndef GW_NC_NO_HEARTBEAT
    if (m_watermark >= 0 && (m_lastHeartbeatNs < 0 || nowNs - m_lastHeartbeatNs >= m_settings.heartbeatMs * MS))
    {
        m_lastHeartbeatNs = nowNs;
        const Text beat = text("{\"type\":\"heartbeat\",\"eventSeq\":" + std::to_string(m_watermark) + "}");
        for (auto &[id, c] : m_conns)
        {
            if ((c.ordersActive || c.fillsActive) && !c.resumePending)
            {
                enqueue(c, beat);
            }
        }
    }
#endif
    // resumes waiting for the gap to close, the watermark to pass them, or a backfill round
    if (!gapOpen())
    {
        for (auto &[id, c] : m_conns)
        {
            if (c.resumePending)
            {
                processResume(c, nowNs);
            }
        }
    }
#ifdef GW_NC_BALANCE_REPLY_LAST
    for (auto &[id, u] : m_users)
    {
        if (u.heldReply)
        {
            const session::BalancesReply held = *u.heldReply;
            onBalancesReply(u, held);
        }
    }
#endif
    work += schedule(nowNs);
    // closed sockets, expired lingers
    if (m_lastSweepNs < 0 || nowNs - m_lastSweepNs >= SWEEP_NS)
    {
        m_lastSweepNs = nowNs;
        for (auto &[id, c] : m_conns)
        {
            if (!c.closing && c.out->closed())
            {
                c.closing = true;
                m_toRemove.push_back(id);
            }
        }
        std::vector<std::int64_t> expired;
        std::int64_t cached = 0;
        for (const auto &[id, u] : m_users)
        {
            if (u.conns.empty() && u.lingerUntilNs >= 0 && nowNs - u.lingerUntilNs >= 0)
            {
                expired.push_back(id);
            }
            cached += (Cache::READY == u.orders || Bal::READY == u.bal) ? 1 : 0;
        }
        for (const std::int64_t id : expired)
        {
            dropUser(id);
        }
        m_stats.usersCached.store(cached, std::memory_order_relaxed);
        m_stats.users.store(static_cast<std::int64_t>(m_users.size()), std::memory_order_relaxed);
    }
    for (const std::uint64_t id : m_toRemove)
    {
        removeConn(id, nowNs);
    }
    m_toRemove.clear();
    work += pump();
    m_stats.watermark.store(m_watermark, std::memory_order_relaxed);
    m_stats.windowFirstSeq.store(windowFirstSeq(), std::memory_order_relaxed);
    return work;
}

// ================================================================ frames

Text StreamCore::orderFrame(const wire::OrderEvent &e) const
{
    return text("{\"type\":\"order\",\"eventSeq\":" + std::to_string(e.eventSeq) + ",\"event\":" +
        api::quote(rest::nameOf(e.eventKind)) + ",\"order\":" + rest::orderJson(e) + "}");
}

Text StreamCore::fillFrame(const wire::OrderEvent &e) const
{
    return text("{\"type\":\"fill\",\"eventSeq\":" + std::to_string(e.eventSeq) + ",\"fill\":" + rest::fillJson(e) + "}");
}

}}}
