#ifndef GATEWAY_PROJECTOR_FOLLOW_EXCHANGE_H
#define GATEWAY_PROJECTOR_FOLLOW_EXCHANGE_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cluster_tools/archive/Chain.h"

#include "FollowRules.h"
#include "JournalRecord.h"
#include "ProjectionStore.h"

// FollowExchange - everything pj-duty (the journal sources, the cluster session) and pj-pg (the fenced transactions)
// hand each other, under one mutex held only to copy. pj-duty never waits for PostgreSQL: a full queue pauses the replay
// (the frame is not accepted and is read again), nothing is dropped.
//
// pj-duty -> pj-pg: the queue of items, in the order pj-duty decided them (bounded by HISTORY_PROJECTOR_QUEUE_EVENTS).
// pj-pg -> pj-duty: what each committed transaction made true (the cursor, the memo proofs, the compares verified and
// any conflict among them, the replays recorded or forgotten, the epoch), and a reset (a new generation) after every
// takeover and every uncertain commit that did not resolve to the transaction pj-pg held. Items of an older generation
// are dropped by pj-pg; pj-duty starts again from the reset's cursor.

namespace openexchange { namespace gateway { namespace projector {

namespace ar = cluster_tools::archive;

// One stored resume memo row (oe_projection_source_memo).
struct MemoRow
{
    std::string source;
    ar::RecordingKey key;
    std::int64_t endPosition = 0;
    std::int64_t seq = 0;
    std::int64_t epoch = 0;
    bool operator==(const MemoRow &) const = default;
};

// One recorded replay (oe_projection_replay).
struct ReplayRow
{
    std::string source;
    std::int64_t archiveId = 0;
    std::int64_t replaySessionId = 0;
    bool operator==(const ReplayRow &) const = default;
};

struct Item
{
    enum class Kind : std::uint8_t
    {
        APPLY,         // the next record: written with its rows
        COMPARE,       // a record at or below the cursor: compared with the stored bytes of its sequence
        REPLAY_STARTED,// record the replay before any record of it is applied
        REPLAY_ENDED,  // the replay was stopped (or found gone): forget it
        EPOCH,         // the cluster's genesis for an empty database
        HALT           // the halt state changed (written to the state row)
    };
    Kind kind = Kind::APPLY;
    std::uint64_t generation = 0;
    std::size_t source = 0;
    std::string sourceName;
    ar::RecordingKey key;       // APPLY, COMPARE: the recording the record was read from
    std::int64_t endPosition = 0;
    JournalRecord record;       // APPLY, COMPARE
    ReplayRow replay;           // REPLAY_*
    std::int64_t epoch = 0;     // EPOCH
    HaltState halt;             // HALT
};

struct Reset
{
    std::uint64_t generation = 0;
    ProjectionState state;
    std::vector<MemoRow> memo;
    std::vector<ReplayRow> replays; // replays a previous writer (or this one before a restart) left recorded
};

struct Conflict
{
    std::size_t source = 0;
    std::string sourceName;
    std::int64_t seq = 0;
    std::string storedSha256;
    std::string readSha256;
};

struct CommittedProof
{
    std::size_t source = 0;
    ar::RecordingKey key;
    std::int64_t seq = 0;
    std::int64_t epoch = 0;
    std::int64_t endPosition = 0;
};

// What pj-duty collects each cycle.
struct Results
{
    std::optional<Reset> reset;
    std::int64_t appliedSeq = -1;           // -1: no commit since the last collect
    std::vector<CommittedProof> proofs;
    std::uint64_t comparesVerified = 0;     // compare items verified (equal or not) since the last collect
    std::uint64_t comparesEqual = 0;
    std::vector<Conflict> conflicts;
    std::vector<std::int64_t> replaysRecorded;
    std::optional<std::int64_t> epoch;      // the epoch the state row holds after a commit
    std::vector<std::size_t> appliedFrom;   // sources whose APPLY items were committed (their back-off resets)
    std::vector<std::size_t> comparedFrom;  // sources whose COMPARE items were verified equal (they deliver)
    std::vector<std::pair<std::size_t, std::int64_t>> comparedUpTo; // per such source: the highest sequence verified
};

class FollowExchange
{
public:
    explicit FollowExchange(std::size_t capacity) : m_capacity(capacity) {}

    // ---- pj-duty
    bool push(Item item)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        if (m_items.size() >= m_capacity && Item::Kind::APPLY == item.kind)
        {
            return false;
        }
        if (m_items.size() >= m_capacity && Item::Kind::COMPARE == item.kind)
        {
            return false;
        }
        m_items.push_back(std::move(item)); // bookkeeping items are never refused: they are few and must not be lost
        return true;
    }
    std::size_t free() const
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_items.size() >= m_capacity ? 0 : m_capacity - m_items.size();
    }
    Results collect()
    {
        std::lock_guard<std::mutex> lock(m_lock);
        Results out = std::move(m_results);
        m_results = Results{};
        return out;
    }
    std::uint64_t generation() const
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_generation;
    }

    // ---- pj-pg
    // Up to `max` items of the current generation (older ones are dropped).
    std::vector<Item> take(std::size_t max)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        std::vector<Item> out;
        while (!m_items.empty() && out.size() < max)
        {
            if (m_items.front().generation == m_generation)
            {
                out.push_back(std::move(m_items.front()));
            }
            m_items.pop_front();
        }
        return out;
    }
    bool pending() const
    {
        std::lock_guard<std::mutex> lock(m_lock);
        return !m_items.empty();
    }
    // A new generation: every queued item is dropped, pj-duty starts again from `reset`.
    void reset(Reset r)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_generation++;
        r.generation = m_generation;
        m_items.clear();
        m_results = Results{};
        m_results.reset = std::move(r);
    }
    // The copy is a standby: nothing queued survives, the next takeover resets.
    void standby()
    {
        std::lock_guard<std::mutex> lock(m_lock);
        m_generation++;
        m_items.clear();
        m_results = Results{};
    }
    template<typename F>
    void publish(F f)
    {
        std::lock_guard<std::mutex> lock(m_lock);
        f(m_results);
    }
    std::uint64_t currentGeneration() const { return generation(); }

private:
    mutable std::mutex m_lock;
    std::size_t m_capacity;
    std::deque<Item> m_items;
    Results m_results;
    std::uint64_t m_generation = 0;
};

}}}

#endif
