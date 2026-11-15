#ifndef GATEWAY_COMMON_EXIT_CODES_H
#define GATEWAY_COMMON_EXIT_CODES_H

// The process exit codes of oe-gateway and oe-stream, the same table as the cluster-tools processes (sysexits):
//   0  CLEAN      stopped by SIGTERM / SIGINT, every thread joined inside its bound
//   70 SOFTWARE   process health: the media driver died or did not answer (CnC timeout), the client conductor
//                 heartbeat went stale, the duty thread stalled (watchdog), a join bound was hit, an exception escaped,
//                 or the embedded driver was starved of storage space for the starvation bound
//   75 TEMPFAIL   a temporary condition; a restart may succeed
//   78 CONFIG     configuration: every problem is listed in one pass before anything starts; a bind that fails; an
//                 embedded driver that cannot start (the instance lock, a live driver in the dir, host limits)
// The service unit sets RestartPreventExitStatus=78: an operator fixes the setting, a restart cannot.

namespace openexchange { namespace gateway {

namespace exit_code {
inline constexpr int CLEAN = 0;
inline constexpr int SOFTWARE = 70;
inline constexpr int TEMPFAIL = 75;
inline constexpr int CONFIG = 78;
}

}}

#endif
