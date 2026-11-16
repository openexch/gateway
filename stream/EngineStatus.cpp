#include "EngineStatus.h"

#include <algorithm>
#include <charconv>

#include "gateway/common/Api.h"

namespace openexchange { namespace gateway { namespace stream { namespace engine {

namespace {

bool parseInt(std::string_view text, std::int64_t &out)
{
    if (text.empty())
    {
        return false;
    }
    const char *first = text.data();
    const char *last = text.data() + text.size();
    const auto r = std::from_chars(first, last, out);
    return r.ec == std::errc() && r.ptr == last;
}

std::string num(const std::optional<std::int64_t> &v)
{
    return v ? std::to_string(*v) : std::string("null");
}

std::string str(const std::optional<std::string> &v)
{
    return v ? api::quote(*v) : std::string("null");
}

std::string boolean(const std::optional<bool> &v)
{
    return v ? (*v ? "true" : "false") : "null";
}

std::string ids(const std::vector<std::int64_t> &v)
{
    std::string out = "[";
    for (std::size_t i = 0; i < v.size(); ++i)
    {
        out += (i ? "," : "") + std::to_string(v[i]);
    }
    return out + "]";
}

std::optional<std::string> roleName(const std::string &role)
{
    if ("LEADER" == role)
    {
        return std::string("leader");
    }
    if ("FOLLOWER" == role)
    {
        return std::string("follower");
    }
    if ("CANDIDATE" == role)
    {
        return std::string("candidate");
    }
    return std::nullopt;
}

}

namespace {

// A snapshot state hash is the engine's unsigned decimal; anything else (a sentinel such as -1, a word, an empty text) is
// not a hash.
bool isDigits(const std::string &text)
{
    if (text.empty())
    {
        return false;
    }
    for (const char c : text)
    {
        if (c < '0' || c > '9')
        {
            return false;
        }
    }
    return true;
}

}

std::optional<HealthBody> parseHealth(std::string_view body)
{
    HealthBody h;
    std::size_t i = 0;
    while (i < body.size())
    {
        while (i < body.size() && (' ' == body[i] || '\n' == body[i] || '\r' == body[i] || '\t' == body[i]))
        {
            i++;
        }
        std::size_t j = i;
        while (j < body.size() && ' ' != body[j] && '\n' != body[j] && '\r' != body[j] && '\t' != body[j])
        {
            j++;
        }
        const std::string_view token = body.substr(i, j - i);
        i = j;
        const std::size_t eq = token.find('=');
        if (eq == std::string_view::npos || 0 == eq)
        {
            continue;
        }
        const std::string_view key = token.substr(0, eq);
        const std::string_view value = token.substr(eq + 1);
        std::int64_t v = 0;
        if ("role" == key)
        {
            h.role = std::string(value);
        }
        else if ("recovery" == key)
        {
            h.recovery = std::string(value);
        }
        else if ("version" == key)
        {
            h.version = value.empty() ? std::nullopt : std::optional<std::string>(std::string(value));
        }
        else if ("build" == key)
        {
            h.build = value.empty() ? std::nullopt : std::optional<std::string>(std::string(value));
        }
        else if (parseInt(value, v))
        {
            // -1 is the probe's "unknown"
            const std::optional<std::int64_t> known = v < 0 ? std::nullopt : std::optional<std::int64_t>(v);
            if ("term" == key)
            {
                h.term = known;
            }
            else if ("elections" == key)
            {
                h.elections = known;
            }
            else if ("applied" == key)
            {
                h.applied = known;
            }
            else if ("commit" == key)
            {
                h.commit = known;
            }
        }
    }
    if (!h.role)
    {
        return std::nullopt;
    }
    return h;
}

std::map<std::string, std::string> parseMetrics(std::string_view body)
{
    std::map<std::string, std::string> out;
    std::size_t start = 0;
    while (start < body.size())
    {
        std::size_t end = body.find('\n', start);
        if (end == std::string_view::npos)
        {
            end = body.size();
        }
        std::string_view line = body.substr(start, end - start);
        start = end + 1;
        if (!line.empty() && '\r' == line.back())
        {
            line.remove_suffix(1);
        }
        if (line.empty() || '#' == line.front())
        {
            continue;
        }
        const std::size_t space = line.find(' ');
        if (space == std::string_view::npos || 0 == space)
        {
            continue;
        }
        const std::string_view name = line.substr(0, space);
        if (name.find('{') != std::string_view::npos)
        {
            continue; // labelled
        }
        std::string_view value = line.substr(space + 1);
        const std::size_t more = value.find(' ');
        if (more != std::string_view::npos)
        {
            value = value.substr(0, more); // a timestamp after the value
        }
        if (!value.empty())
        {
            out[std::string(name)] = std::string(value);
        }
    }
    return out;
}

std::optional<std::int64_t> metricInt(const std::map<std::string, std::string> &metrics, const std::string &name)
{
    const auto it = metrics.find(name);
    std::int64_t v = 0;
    if (it == metrics.end() || !parseInt(it->second, v))
    {
        return std::nullopt;
    }
    return v;
}

ClusterModel::ClusterModel(Flavor flavor, std::vector<NodeEndpointSpec> nodes) :
    m_flavor(flavor), m_prefix(Flavor::CORE == flavor ? "core_" : "match_"), m_specs(std::move(nodes))
{
    for (const NodeEndpointSpec &s : m_specs)
    {
        NodeView v;
        v.id = s.memberId;
        m_nodes.push_back(std::move(v)); // never polled yet: every value null
    }
}

const char *ClusterModel::key() const noexcept
{
    return Flavor::CORE == m_flavor ? "core" : "matchingEngine";
}

const NodeView *ClusterModel::leader() const noexcept
{
    if (!m_leaderId)
    {
        return nullptr;
    }
    for (const NodeView &n : m_nodes)
    {
        if (n.id == *m_leaderId)
        {
            return &n;
        }
    }
    return nullptr;
}

NodeView ClusterModel::readNode(const NodeEndpointSpec &spec, const NodeSample &s, std::int64_t nowMs,
    const NodeView *before)
{
    NodeView v;
    v.id = spec.memberId;
    if (!s.healthAnswered && !s.metricsAnswered)
    {
        v.role = std::string("unreachable");
        v.error = s.error.empty() ? std::string("no answer") : s.error;
        return v;
    }
    if (!s.healthAnswered || !s.metricsAnswered || 200 != s.metricsStatus)
    {
        v.error = !s.healthAnswered ? "/health did not answer" + (s.error.empty() ? std::string() : ": " + s.error)
            : !s.metricsAnswered    ? "/metrics did not answer" + (s.error.empty() ? std::string() : ": " + s.error)
                                    : "/metrics answered " + std::to_string(s.metricsStatus);
        return v;
    }
    const std::optional<HealthBody> h = parseHealth(s.healthBody);
    const std::map<std::string, std::string> m = parseMetrics(s.metricsBody);
    const std::optional<std::int64_t> member = metricInt(m, m_prefix + "member_id");
    if (!h)
    {
        v.error = std::string("/health body is not a node probe");
        return v;
    }
    if (!member || *member != spec.memberId)
    {
        v.error = member ? "the endpoint reports member id " + std::to_string(*member)
                         : "the endpoint reports no " + m_prefix + "member_id";
        return v;
    }
    v.role = roleName(h->role.value_or(""));
    if (!v.role)
    {
        v.error = "unknown role " + h->role.value_or("");
    }
    v.live = 200 == s.healthStatus;
    v.recovery = h->recovery;
    v.term = h->term;
    v.commitPosition = h->commit;
    v.appliedPosition = h->applied;
    v.version = h->version;
    v.buildCommit = h->build;
    {
        const std::optional<std::int64_t> pos = metricInt(m, m_prefix + "snapshot_last_log_position");
        v.snapshot.position = pos && *pos >= 0 ? pos : std::nullopt;
    }
    if (Flavor::CORE == m_flavor)
    {
        v.feedCursor = metricInt(m, "core_feed_cursor");
    }
    else
    {
        const std::optional<std::int64_t> next = metricInt(m, "match_engine_next_journal_seq");
        v.nextJournalSeq = next && *next > 0 ? next : std::nullopt;
    }
    const std::optional<std::int64_t> age = metricInt(m, m_prefix + "snapshot_age_seconds");
    if (age && *age >= 0)
    {
        // the time is fixed when the snapshot is first seen (the age moves every second; the snapshot does not)
        const bool same = nullptr != before && before->snapshot.atMs &&
            (v.snapshot.position ? before->snapshot.position == v.snapshot.position
                                 : !before->snapshot.position && *before->snapshot.atMs <= nowMs - *age * 1000 + 1999 &&
                                       *before->snapshot.atMs >= nowMs - *age * 1000 - 1999);
        v.snapshot.atMs = same ? before->snapshot.atMs : std::optional<std::int64_t>(nowMs - *age * 1000);
    }
    const auto hash = m.find(m_prefix + "snapshot_state_hash");
    if (hash != m.end() && isDigits(hash->second))
    {
        v.snapshot.stateHash = hash->second;
    }
    return v;
}

std::optional<std::string> snapshotAgreement(const std::vector<NodeView> &nodes, std::int64_t position,
    std::vector<std::int64_t> &diverged)
{
    diverged.clear();
    std::map<std::string, std::vector<std::int64_t>> byHash;
    std::size_t reported = 0;
    for (const NodeView &n : nodes)
    {
        if (n.snapshot.position && *n.snapshot.position == position && n.snapshot.stateHash)
        {
            byHash[*n.snapshot.stateHash].push_back(n.id);
            reported++;
        }
    }
    if (reported < 2)
    {
        return std::nullopt;
    }
    if (1 == byHash.size())
    {
        return std::string("matched");
    }
    // the reference: the most common hash; a tie goes to the hash of the lowest id among the tied
    const std::string *reference = nullptr;
    std::size_t best = 0;
    std::int64_t bestLowest = 0;
    for (const auto &[hash, members] : byHash)
    {
        const std::int64_t lowest = *std::min_element(members.begin(), members.end());
        if (members.size() > best || (members.size() == best && lowest < bestLowest))
        {
            reference = &hash;
            best = members.size();
            bestLowest = lowest;
        }
    }
    for (const auto &[hash, members] : byHash)
    {
        if (&hash != reference)
        {
            diverged.insert(diverged.end(), members.begin(), members.end());
        }
    }
    std::sort(diverged.begin(), diverged.end());
    return std::string("diverged");
}

void ClusterModel::deriveSnapshot()
{
    m_snapshot = SnapshotView{};
    m_diverged.clear();
    for (const NodeView &n : m_nodes)
    {
        if (n.snapshot.position && (!m_snapshot.position || *n.snapshot.position > *m_snapshot.position))
        {
            m_snapshot.position = n.snapshot.position;
            m_snapshot.atMs = n.snapshot.atMs;
        }
    }
    if (!m_snapshot.position)
    {
        // an engine without snapshot positions (the matching engine): the newest snapshot time any node reports
        for (const NodeView &n : m_nodes)
        {
            if (n.snapshot.atMs && (!m_snapshot.atMs || *n.snapshot.atMs > *m_snapshot.atMs))
            {
                m_snapshot.atMs = n.snapshot.atMs;
            }
        }
        return;
    }
    m_snapshot.agreement = snapshotAgreement(m_nodes, *m_snapshot.position, m_diverged);
    for (NodeView &n : m_nodes)
    {
        if (!m_snapshot.agreement || !n.snapshot.position || *n.snapshot.position != *m_snapshot.position ||
            !n.snapshot.stateHash)
        {
            continue;
        }
        const bool off = std::find(m_diverged.begin(), m_diverged.end(), n.id) != m_diverged.end();
        n.snapshot.agreement = std::string(off ? "diverged" : "matched");
    }
}

std::vector<Election> ClusterModel::ingest(const std::vector<NodeSample> &round, std::int64_t nowMs)
{
    std::vector<NodeView> next;
    next.reserve(m_specs.size());
    for (std::size_t i = 0; i < m_specs.size(); ++i)
    {
        const NodeView *before = i < m_nodes.size() ? &m_nodes[i] : nullptr;
        next.push_back(readNode(m_specs[i], i < round.size() ? round[i] : NodeSample{}, nowMs, before));
    }
    m_nodes = std::move(next);
#ifdef GW_NC_ENGINE_INFER_FROM_LEADER
    // test-only control: a node that did not answer is shown as the leader's follower (health inferred from the leader)
    bool someLeader = false;
    for (const NodeView &n : m_nodes)
    {
        someLeader = someLeader || (n.role && "leader" == *n.role);
    }
    for (NodeView &n : m_nodes)
    {
        if (someLeader && n.role && "unreachable" == *n.role)
        {
            n.role = std::string("follower");
            n.error.reset();
        }
    }
#endif

    // the leader: the live node that says it leads (two: the higher term; the same term twice is a conflict -> null).
    // A node whose /health is not live keeps reporting the role of its last evidence, so a stuck ex-leader's claim is
    // shown on its own row and never becomes the cluster's leader.
    m_leaderId.reset();
    m_term.reset();
    bool conflict = false;
    for (const NodeView &n : m_nodes)
    {
        if (!n.role || "leader" != *n.role || !n.live.value_or(false))
        {
            continue;
        }
        const std::int64_t t = n.term.value_or(-1);
        if (!m_leaderId || t > m_term.value_or(-1))
        {
            m_leaderId = n.id;
            m_term = n.term;
            conflict = false;
        }
        else if (t == m_term.value_or(-1))
        {
            conflict = true;
        }
    }
    if (conflict)
    {
        m_leaderId.reset();
        m_term.reset();
    }
    const NodeView *lead = leader();
    for (NodeView &n : m_nodes)
    {
        if (nullptr != lead && lead->commitPosition && n.appliedPosition)
        {
            // raw: two answers sampled at different instants, so a node polled after the leader can read negative
            n.behindBytes = *lead->commitPosition - *n.appliedPosition;
        }
    }
    deriveSnapshot();

    // elections
    std::vector<Election> events;
    if (m_leaderId)
    {
        const bool other = m_knownLeader &&
            (*m_leaderId != *m_knownLeader || (m_term && m_knownTerm && *m_term > *m_knownTerm));
        if (m_knownLeader && (other || m_electionOpen))
        {
            Election e;
            e.phase = "end";
            e.observedAtMs = nowMs;
            e.previousTerm = m_knownTerm;
            e.previousLeaderId = m_knownLeader;
            e.term = m_term;
            e.leaderId = m_leaderId;
            e.observedGapMs = nowMs - m_knownLeaderSeenMs;
            events.push_back(e);
            m_lastElection = e;
            m_electionOpen = false;
        }
        m_knownLeader = m_leaderId;
        m_knownTerm = m_term;
        m_knownLeaderCommit = nullptr != lead ? lead->commitPosition : std::nullopt;
        m_knownLeaderSeenMs = nowMs;
    }
    else if (m_knownLeader && !m_electionOpen && !conflict)
    {
        Election e;
        bool electing = false;
        for (const NodeView &n : m_nodes)
        {
            const bool candidate = n.role && "candidate" == *n.role;
            electing = electing || candidate || (n.recovery && "election" == *n.recovery);
            if (candidate)
            {
                e.candidates.push_back(n.id);
            }
        }
        if (electing)
        {
            e.phase = "start";
            e.observedAtMs = nowMs;
            e.previousTerm = m_knownTerm;
            e.previousLeaderId = m_knownLeader;
            e.lastCommitPosition = m_knownLeaderCommit;
            events.push_back(e);
            m_electionOpen = true;
        }
    }
    return events;
}

std::string ClusterModel::json() const
{
    std::string out = "{\"configured\":" + std::string(m_specs.empty() ? "false" : "true") + ",\"term\":" + num(m_term) +
        ",\"leaderId\":" + num(m_leaderId) + ",\"nodes\":[";
    for (std::size_t i = 0; i < m_nodes.size(); ++i)
    {
        const NodeView &n = m_nodes[i];
        out += (i ? ",{" : "{");
        out += "\"id\":" + std::to_string(n.id) + ",\"role\":" + str(n.role) + ",\"live\":" + boolean(n.live) +
            ",\"recovery\":" + str(n.recovery) + ",\"term\":" + num(n.term) + ",\"commitPosition\":" +
            num(n.commitPosition) + ",\"appliedPosition\":" + num(n.appliedPosition) + ",\"behindBytes\":" +
            num(n.behindBytes) + ",\"entriesBehind\":null,\"snapshot\":{\"position\":" + num(n.snapshot.position) +
            ",\"atMs\":" + num(n.snapshot.atMs) + ",\"stateHash\":" + str(n.snapshot.stateHash) + ",\"agreement\":" +
            str(n.snapshot.agreement) + "},\"version\":" + str(n.version) + ",\"buildCommit\":" + str(n.buildCommit) +
            ",\"error\":" + str(n.error) + "}";
    }
    out += "],\"lastElection\":";
    if (m_lastElection)
    {
        const Election &e = *m_lastElection;
        out += "{\"atMs\":" + std::to_string(e.observedAtMs) + ",\"previousLeaderId\":" + num(e.previousLeaderId) +
            ",\"leaderId\":" + num(e.leaderId) + ",\"previousTerm\":" + num(e.previousTerm) + ",\"term\":" + num(e.term) +
            ",\"durationMs\":null,\"observedGapMs\":" + num(e.observedGapMs) + "}";
    }
    else
    {
        out += "null";
    }
    out += ",\"snapshot\":{\"position\":" + num(m_snapshot.position) + ",\"atMs\":" + num(m_snapshot.atMs) +
        ",\"agreement\":" + str(m_snapshot.agreement) + ",\"divergedNodes\":" + ids(m_diverged) + "}}";
    return out;
}

std::string engineFrame(const ClusterModel &core, const ClusterModel &me)
{
    const NodeView *coreLeader = core.leader();
    const NodeView *meLeader = me.leader();
    const std::optional<std::int64_t> consumed = nullptr != coreLeader ? coreLeader->feedCursor : std::nullopt;
    const std::optional<std::int64_t> head = nullptr != meLeader && meLeader->nextJournalSeq
        ? std::optional<std::int64_t>(*meLeader->nextJournalSeq - 1)
        : std::nullopt;
    const std::optional<std::int64_t> behind = consumed && head ? std::optional<std::int64_t>(*head - *consumed)
                                                                : std::nullopt;
    return "{\"type\":\"engine\",\"core\":" + core.json() + ",\"matchingEngine\":" + me.json() +
        ",\"bridge\":{\"consumedJournalSeq\":" + num(consumed) + ",\"meJournalSeq\":" + num(head) +
        ",\"entriesBehind\":" + num(behind) + "}}";
}

std::string electionFrame(const char *clusterKey, const Election &e)
{
    std::string out = "{\"type\":\"election\",\"cluster\":" + api::quote(clusterKey) + ",\"phase\":" + api::quote(e.phase) +
        ",\"atMs\":" + std::to_string(e.observedAtMs) + ",\"previousTerm\":" + num(e.previousTerm) +
        ",\"previousLeaderId\":" + num(e.previousLeaderId);
    if ("start" == e.phase)
    {
        out += ",\"lastCommitPosition\":" + num(e.lastCommitPosition) + ",\"candidates\":" + ids(e.candidates);
    }
    else
    {
        out += ",\"term\":" + num(e.term) + ",\"leaderId\":" + num(e.leaderId) +
            ",\"firstEntryPosition\":null,\"durationMs\":null,\"observedGapMs\":" + num(e.observedGapMs) +
            ",\"orderEntryReopened\":null";
    }
    return out + "}";
}

std::optional<std::string> FrameGate::offer(std::string frame, std::int64_t nowMs)
{
    if (m_held && *m_held == frame)
    {
        return poll(nowMs);
    }
    if (!m_held && m_last && *m_last == frame)
    {
        m_unchanged++;
        return std::nullopt;
    }
    if (m_held && m_last && *m_last == frame)
    {
        m_held.reset(); // changed and changed back before it went out: nothing to send
        m_unchanged++;
        return std::nullopt;
    }
    m_held = std::move(frame);
    return poll(nowMs);
}

std::optional<std::int64_t> FrameGate::heldDueMs() const noexcept
{
    if (!m_held)
    {
        return std::nullopt;
    }
    return m_sentOnce ? m_lastSentMs + m_minIntervalMs : 0;
}

std::optional<std::string> FrameGate::poll(std::int64_t nowMs)
{
    if (!m_held)
    {
        return std::nullopt;
    }
#ifndef GW_NC_ENGINE_NO_RATE_CAP
    if (m_sentOnce && nowMs - m_lastSentMs < m_minIntervalMs)
    {
        return std::nullopt;
    }
#endif
    m_last = m_held;
    m_held.reset();
    m_lastSentMs = nowMs;
    m_sentOnce = true;
    return m_last;
}

}}}}
