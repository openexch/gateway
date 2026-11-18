#include "FollowRules.h"

#include <algorithm>
#include <limits>

namespace openexchange { namespace gateway { namespace projector {

const char *nameOf(Forward f) noexcept
{
    switch (f)
    {
        case Forward::COMPARE: return "COMPARE";
        case Forward::APPLY: return "APPLY";
        case Forward::BELOW_RANGE: return "BELOW_RANGE";
        case Forward::HOLE: return "HOLE";
        case Forward::RE_READ: return "RE_READ";
    }
    return "?";
}

ForwardVerdict forwardRule(std::int64_t seq, std::int64_t next, bool memoFreeRead, bool firstOfRead)
{
    ForwardVerdict v;
    if (seq < next)
    {
        v.kind = Forward::COMPARE;
        return v;
    }
    if (seq == next)
    {
        v.kind = Forward::APPLY;
        return v;
    }
#ifndef PROJ_NC_HOLE_FROM_MEMO
    if (!memoFreeRead)
    {
        v.kind = Forward::RE_READ; // a jump seen on a read that began at a memo proves nothing about the source
        return v;
    }
#else
    (void)memoFreeRead; // test build only: a jump on a memo read is believed
#endif
    v.kind = firstOfRead ? Forward::BELOW_RANGE : Forward::HOLE;
    v.from = next;
    v.to = seq - 1;
    return v;
}

const char *nameOf(NodeRole r) noexcept
{
    switch (r)
    {
        case NodeRole::UNKNOWN: return "unknown";
        case NodeRole::FOLLOWER: return "follower";
        case NodeRole::LEADER: return "leader";
        case NodeRole::NOT_A_MEMBER: return "not-a-member";
    }
    return "?";
}

const char *nameOf(Mark m) noexcept
{
    switch (m)
    {
        case Mark::NONE: return "NONE";
        case Mark::BELOW_RANGE: return "BELOW_RANGE";
        case Mark::HOLE: return "HOLE";
        case Mark::PAYLOAD_CONFLICT: return "PAYLOAD_CONFLICT";
        case Mark::SCHEMA: return "SCHEMA";
        case Mark::MTU: return "MTU";
    }
    return "?";
}

const char *nameOf(Halt h) noexcept
{
    switch (h)
    {
        case Halt::NONE: return "NONE";
        case Halt::GAP: return "GAP";
        case Halt::PAYLOAD_CONFLICT: return "PAYLOAD_CONFLICT";
        case Halt::EPOCH_MISMATCH: return "EPOCH_MISMATCH";
        case Halt::SCHEMA: return "SCHEMA";
        case Halt::MTU: return "MTU";
    }
    return "?";
}

bool budgetFull(const SourceView &v) noexcept
{
    return v.replaysActive && *v.replaysActive >= 0 && v.readerBudget && *v.replaysActive >= *v.readerBudget;
}

int tierOf(NodeRole role) noexcept
{
    switch (role)
    {
        case NodeRole::FOLLOWER:
        case NodeRole::NOT_A_MEMBER: return 0;
        case NodeRole::UNKNOWN: return 1;
        case NodeRole::LEADER: return 2;
    }
    return 1;
}

namespace {

bool eligible(const SourceView &v, std::int64_t nowNs, bool isCurrent)
{
    if (Mark::NONE != v.mark)
    {
        return false;
    }
    if (!isCurrent && nowNs < v.blockedUntilNs)
    {
        return false;
    }
    return isCurrent || !budgetFull(v);
}

// True when a source other than the leader may still be read: not marked, not left as unreachable, its budget not used
// up (a source only backing off is waited for)
bool holdsTheLeaderBack(const SourceView &v, bool isCurrent)
{
    return Mark::NONE == v.mark && !v.unreachable && (isCurrent || !budgetFull(v));
}

int effectiveTier(const SourceView &v)
{
#ifdef PROJ_NC_READ_THE_LEADER
    (void)v; // test build only: the leader is read like any other member
    return 0;
#else
    return tierOf(v.role);
#endif
}

}

std::optional<std::size_t> chooseSource(const std::vector<SourceView> &sources, std::int64_t nowNs,
    std::optional<std::size_t> current)
{
    std::optional<std::size_t> best;
    int bestTier = std::numeric_limits<int>::max();
    bool leaderHeldBack = false;
    for (std::size_t i = 0; i < sources.size(); ++i)
    {
        leaderHeldBack = leaderHeldBack || (effectiveTier(sources[i]) < tierOf(NodeRole::LEADER) &&
            holdsTheLeaderBack(sources[i], current && *current == i));
    }
    for (std::size_t i = 0; i < sources.size(); ++i)
    {
        if (!eligible(sources[i], nowNs, current && *current == i))
        {
            continue;
        }
        const int tier = effectiveTier(sources[i]);
        if (tier >= tierOf(NodeRole::LEADER) && leaderHeldBack)
        {
            continue; // another source can still be read once its back-off ends: it is waited for
        }
        if (tier < bestTier)
        {
            best = i;
            bestTier = tier;
        }
    }
    return best;
}

bool betterSourceAvailable(const std::vector<SourceView> &sources, std::int64_t nowNs, std::size_t current)
{
    if (current >= sources.size())
    {
        return false;
    }
    const int mine = effectiveTier(sources[current]);
    for (std::size_t i = 0; i < sources.size(); ++i)
    {
        if (i != current && eligible(sources[i], nowNs, false) && effectiveTier(sources[i]) < mine)
        {
            return true;
        }
    }
    return false;
}

std::int64_t Backoff::leave(std::int64_t nowNs) noexcept
{
#ifdef PROJ_NC_NO_BACKOFF
    (void)nowNs; // test build only: a left source may be asked again at once
    return 0;
#else
    const std::int64_t until = nowNs + m_nextNs;
    m_nextNs = std::min(m_maxNs, m_nextNs * 2);
    return until;
#endif
}

void BehindWindow::restart(std::int64_t nowNs) noexcept
{
    m_startNs = nowNs;
    m_started = true;
    m_lagAtStart = -1;
    m_probed = false;
    m_allAbove = true;
    m_delivered = false;
}

void BehindWindow::onProbe(std::int64_t head, std::int64_t applied, std::int64_t read) noexcept
{
    if (!m_started)
    {
        return;
    }
    m_probed = true;
    if (head <= applied)
    {
        m_allAbove = false;
    }
    if (m_lagAtStart < 0)
    {
        m_lagAtStart = head - read;
    }
}

bool BehindWindow::behind(std::int64_t nowNs, std::int64_t head, std::int64_t read) noexcept
{
    if (!m_started || nowNs - m_startNs < m_windowNs)
    {
        return false;
    }
    const bool lagging = m_probed && m_allAbove;
    const std::int64_t lagAtEnd = head - read;
#ifdef PROJ_NC_BEHIND_SILENT_ONLY
    // test build only: BEHIND means "delivered nothing"
    const bool verdict = lagging && !m_delivered;
#else
    const bool verdict = lagging && (!m_delivered || (m_lagAtStart >= 0 && lagAtEnd >= m_lagAtStart));
#endif
    restart(nowNs);
    return verdict;
}

HaltState haltOf(const std::vector<SourceView> &sources, std::int64_t applied)
{
    HaltState h;
    if (sources.empty())
    {
        return h;
    }
    bool conflict = false;
    bool schema = false;
    bool mtu = false;
    std::int64_t gapTo = std::numeric_limits<std::int64_t>::max();
    for (const SourceView &v : sources)
    {
        switch (v.mark)
        {
            case Mark::NONE: return h; // a source is left to read
            case Mark::PAYLOAD_CONFLICT: conflict = true; break;
            case Mark::SCHEMA: schema = true; break;
            case Mark::MTU: mtu = true; break;
            case Mark::BELOW_RANGE:
            case Mark::HOLE: gapTo = std::min(gapTo, v.missingTo); break;
        }
    }
    if (conflict)
    {
        h.halt = Halt::PAYLOAD_CONFLICT;
        h.detail = "no source agrees with the stored bytes after journalSeq " + std::to_string(applied);
    }
    else if (schema)
    {
        h.halt = Halt::SCHEMA;
        h.detail = "every source holds a frame or a record this projector refuses";
    }
    else if (mtu)
    {
        h.halt = Halt::MTU;
        h.detail = "replay MTU differs from the recording on every source";
    }
    else
    {
        h.halt = Halt::GAP;
        h.detail = "no source holds journalSeq " + std::to_string(applied + 1) + ".." + std::to_string(gapTo);
    }
    return h;
}

}}}
