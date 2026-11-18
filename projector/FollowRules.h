#ifndef GATEWAY_PROJECTOR_FOLLOW_RULES_H
#define GATEWAY_PROJECTOR_FOLLOW_RULES_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

// FollowRules - the projector's decisions about the journal sources, as pure functions of what it observed (no Aeron,
// no PostgreSQL, no clock of their own: every call takes the time it runs at). pj-duty (FollowDuty) applies them.
//
// The forward rule, per record of the expected epoch, against `next` (the cursor + 1, counting the records already
// handed to the writer thread):
//   seq <  next   COMPARE: the stored bytes of that sequence are compared with these (equal: a counted skip; different:
//                 a payload conflict). Every overlap is compared; there is no window that skips it.
//   seq == next   APPLY
//   seq >  next   this source does not hold the next record. Believed only on a memo-free read: BELOW_RANGE(first)
//                 when it is the first record this read delivered (a restored or purged source), HOLE(next, seq - 1)
//                 when the jump is inside the source. Either marks the source and the set moves on. On a read that
//                 started from a resume memo it is not believed: the memo of that source is dropped and the source is
//                 read again memo-free (RE_READ). A projector never starts at "the first sequence a source has".
//
// The source choice (readers avoid the leader): among the configured sources that are not marked and not backing
// off, a core follower (or a source that is not a core member, such as a journal replica) first, then a member whose
// role is not known, the leader last. The leader's archive is read only when every other source is marked, was left
// because it could not be reached, or reports its reader replay budget used up; a source that is only backing off
// for another reason (BEHIND, a conflict it showed) is waited for, never replaced by the leader. A member whose node
// reports its reader replay budget used up (core_archive_replays_active at or above readerReplayBudget) is not chosen
// while that holds, unless it is the source already attached (its count holds this copy's own replay).
//
// Leaving a source starts its back-off (HISTORY_PROJECTOR_SOURCE_BACKOFF_MS), which doubles with every further leave
// of that source up to HISTORY_PROJECTOR_SOURCE_BACKOFF_MAX_MS and is reset when the source delivers a record that is
// applied: a projector that is itself the slow part does not flap at full speed.
//
// BEHIND: over a window of HISTORY_PROJECTOR_SOURCE_BEHIND_MS in which every answered head probe showed the core's
// head above the cursor (and at least one was answered), the attached source delivered nothing, or the lag in events
// at the end of the window is not smaller than at its start. Delivery is a record of the attached source that was
// applied or verified against the stored bytes (a source re-reading history the projection already holds delivers),
// and the lag is measured from the read position (the highest sequence of this read the writer verified or applied),
// not from the cursor.

namespace openexchange { namespace gateway { namespace projector {

// ---- the forward rule

enum class Forward : std::uint8_t
{
    COMPARE,
    APPLY,
    BELOW_RANGE,
    HOLE,
    RE_READ
};
const char *nameOf(Forward f) noexcept;

struct ForwardVerdict
{
    Forward kind = Forward::APPLY;
    std::int64_t from = 0; // BELOW_RANGE, HOLE: the first sequence this source does not hold
    std::int64_t to = 0;   // the last one (BELOW_RANGE: first - 1)
};

// `firstOfRead`: no record of the expected epoch was delivered by this read before this one.
ForwardVerdict forwardRule(std::int64_t seq, std::int64_t next, bool memoFreeRead, bool firstOfRead);

// ---- source marks and the choice

enum class NodeRole : std::uint8_t
{
    UNKNOWN,      // the member's node did not answer, or answered without a role
    FOLLOWER,
    LEADER,
    NOT_A_MEMBER  // a source with no node endpoint (a journal replica): never the leader
};
const char *nameOf(NodeRole r) noexcept;

enum class Mark : std::uint8_t
{
    NONE,
    BELOW_RANGE,      // a memo-free read began above the cursor + 1
    HOLE,             // a memo-free read jumped over the cursor + 1
    PAYLOAD_CONFLICT, // a record differs from the stored bytes of its sequence
    SCHEMA,           // a frame or a record the projector refuses (a record of another epoch included)
    MTU               // the recording's MTU differs from HISTORY_PROJECTOR_JOURNAL_MTU
};
const char *nameOf(Mark m) noexcept;

struct SourceView
{
    NodeRole role = NodeRole::UNKNOWN;
    std::optional<std::int64_t> replaysActive; // the member's core_archive_replays_active (absent or -1: unknown)
    std::optional<std::int64_t> readerBudget;  // the member's readerReplayBudget (absent: unknown)
    std::int64_t blockedUntilNs = 0;           // back-off: not chosen before this
    bool unreachable = false;                  // last left because its archive could not be reached or read (connect,
                                               // replay open, image or archive lost, stall); cleared by a delivery
    Mark mark = Mark::NONE;
    std::int64_t missingFrom = 0;              // BELOW_RANGE, HOLE: the range this source does not hold
    std::int64_t missingTo = 0;
};

bool budgetFull(const SourceView &v) noexcept;
// 0 (preferred) .. 2 (the leader)
int tierOf(NodeRole role) noexcept;

// The source to read next, or nullopt when none can be read now. `current`: the source attached now (its own replay is
// in its member's count), if any.
std::optional<std::size_t> chooseSource(const std::vector<SourceView> &sources, std::int64_t nowNs,
    std::optional<std::size_t> current);

// True when a source of a better tier than `current`'s can be read now (the attached one is then left for it).
bool betterSourceAvailable(const std::vector<SourceView> &sources, std::int64_t nowNs, std::size_t current);

// ---- the back-off of one source

class Backoff
{
public:
    Backoff(std::int64_t baseMs, std::int64_t maxMs) : m_baseNs(baseMs * 1'000'000LL), m_maxNs(maxMs * 1'000'000LL),
        m_nextNs(m_baseNs)
    {
    }
    // The source was left at `nowNs`: the time it may be chosen again (the wait doubles for the next leave).
    std::int64_t leave(std::int64_t nowNs) noexcept;
    void progress() noexcept { m_nextNs = m_baseNs; }
    std::int64_t currentNs() const noexcept { return m_nextNs; }

private:
    std::int64_t m_baseNs;
    std::int64_t m_maxNs;
    std::int64_t m_nextNs;
};

// ---- BEHIND

class BehindWindow
{
public:
    explicit BehindWindow(std::int64_t windowMs) : m_windowNs(windowMs * 1'000'000LL) {}
    // A new attach (or a cursor reset): the window starts again.
    void restart(std::int64_t nowNs) noexcept;
    // An answered head probe: the core's head, the cursor and the read position (the last sequence the attached source
    // handed on) at that time.
    void onProbe(std::int64_t head, std::int64_t applied, std::int64_t read) noexcept;
    // A record of this source was applied, or verified against the stored bytes.
    void onDelivered() noexcept { m_delivered = true; }
    // Evaluated every duty cycle: true when the window ended and the source is BEHIND (the window then starts again).
    bool behind(std::int64_t nowNs, std::int64_t head, std::int64_t read) noexcept;

private:
    std::int64_t m_windowNs;
    std::int64_t m_startNs = 0;
    bool m_started = false;
    std::int64_t m_lagAtStart = -1;
    bool m_probed = false;
    bool m_allAbove = true;
    bool m_delivered = false;
};

// ---- halts (none is a latch: each is re-evaluated continuously)

enum class Halt : std::uint8_t
{
    NONE,
    GAP,              // every source shows BELOW_RANGE or a HOLE covering the cursor + 1
    PAYLOAD_CONFLICT, // no source agrees with the stored bytes
    EPOCH_MISMATCH,   // the cluster's genesis is not the database's
    SCHEMA,           // every source shows a record or frame the projector refuses
    MTU               // every source's recording has another MTU than HISTORY_PROJECTOR_JOURNAL_MTU
};
const char *nameOf(Halt h) noexcept;

struct HaltState
{
    Halt halt = Halt::NONE;
    std::string detail;
    bool operator==(const HaltState &) const = default;
};

// The halt when no source can be read: every source is marked (back-off alone is never a halt). NONE otherwise. A
// GAP names the range no source holds: from the cursor + 1 to the smallest missing end.
HaltState haltOf(const std::vector<SourceView> &sources, std::int64_t applied);

// ---- the beat candidates (ProjectionStore.h): answered probes whose head the cursor has not reached yet, oldest first.
// A newer answer with the same head and genesis replaces the last one (it says more); otherwise each answered probe
// adds one. At `cap` (HISTORY_PROJECTOR_BEAT_CANDIDATES) the oldest is dropped: that only delays the beat to a newer
// probe, it never makes one. Returns how many were dropped (0 or 1).
struct BeatCandidate;
std::size_t addBeatCandidate(std::deque<BeatCandidate> &beats, const BeatCandidate &c, std::size_t cap);

}}}

#endif
