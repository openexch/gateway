#ifndef GATEWAY_PROJECTOR_APP_H
#define GATEWAY_PROJECTOR_APP_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "gateway/common/PgConn.h"
#include "gateway/common/ProcessShell.h"

#include "ProjectionStore.h"

// oe-projector - the history projector: one process per copy, N copies on N hosts, one writer among them by a lease in
// the projection database. It follows the core's order journal over the network (Aeron Archive control and replay
// from a list of member archives) and writes the history relations oe-gateway reads. It decides nothing about money:
// a projector that is down, slow or wrong makes history stale, never money wrong.
//
// Threads: main (the supervisor), pj-duty (Aeron: the journal sources and the head-probe session; never blocks), pj-pg
// (the only thread that talks to PostgreSQL: the schema, the lease, the fenced write transactions), pj-admin (/health,
// /ready, /metrics on HISTORY_PROJECTOR_ADMIN_BIND). A standby holds no replay and no cluster session: pj-duty attaches
// only while pj-pg holds the lease (ProjectorShared::writer).
//
// Commands:
//   oe-projector            the service
//   oe-projector --rebuild  waits for the writer lease, then in one transaction resets the state row and truncates every
//                           base table (ProjectionStore.h), releases the lease and exits 0; the service copies then
//                           project again from the first record. 75 when stopped or when the commit failed (run it
//                           again: it is idempotent), 78 on a configuration or schema problem.
// Exit codes: ExitCodes.h; 64 for an unknown argument.

namespace openexchange { namespace gateway {

// What pj-pg and pj-duty share. Nothing else crosses between them.
struct ProjectorShared
{
    std::atomic<bool> writer{ false };       // pj-pg holds the lease: pj-duty may hold replays and the session (writerNow)
    std::atomic<int> fatalExit{ 0 };         // nonzero: pj-pg asks the process to stop with this code
    std::atomic<std::int64_t> replaysOpen{ 0 }; // replays this copy holds (pj-duty)
    std::atomic<bool> sessionOpen{ false };  // the head-probe session is open (pj-duty)
    std::atomic<std::int64_t> writerUntilNs{ 0 }; // monotonic ns: the step-down point of pj-pg's last confirmation
};

// True while pj-pg holds the lease and `nowNs` (monotonic) is before the step-down point of its last confirmation.
// pj-duty holds replays and the session only while this is true, whether or not pj-pg is blocked in a statement.
bool writerNow(const ProjectorShared &shared, std::int64_t nowNs);
// The step-down point after a confirmation: the lease less one renewal interval (a third of it). A confirmation is
// the time its statement was sent, no later than the database's clock at the fence, so the copy stops acting as the
// writer one renewal interval before the database can hand the lease to another copy.
std::int64_t writerHoldNs(std::int64_t leaseMs);

// The writer's batches: the seam the follow-and-apply logic plugs into. Called on pj-pg only.
class FencedWork
{
public:
    virtual ~FencedWork() = default;
    // The lease was taken, or reasserted after a lost connection: `state` is the row as the database holds it now,
    // the only cursor there is.
    virtual void onWriter(const projector::ProjectionState &state) = 0;
    virtual void onStandby() = 0;
    // A batch is ready to commit now.
    virtual bool due(std::int64_t nowNs) = 0;
    // The batch's rows, inside the fenced transaction; `advance` is what the state row moves to with them.
    virtual bool write(PgConn &conn, const std::string &token, projector::StateAdvance &advance, std::string &error) = 0;
    virtual void committed(const projector::StateAdvance &advance) = 0;
};

// Test builds only (never set by the production binary).
struct ProjectorHooks
{
    ProcessHooks process;
    std::function<std::unique_ptr<FencedWork>()> makeWork; // empty: no batches (this version applies no record)
    std::function<void()> beforeFence;                     // runs inside each batch, between its writes and the fence
    std::function<void()> beforeCommit;                    // runs inside each batch, between the fence and the COMMIT
};

inline constexpr int EXIT_USAGE = 64;

int projectorMain(int argc, char **argv, const ProjectorHooks &hooks);

}}

#endif
