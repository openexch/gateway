#ifndef GATEWAY_COMMON_FRONT_LOG_H
#define GATEWAY_COMMON_FRONT_LOG_H

#include <cstdint>

// FrontLog - the log of the threads around the duty thread (I/O, hash, store workers, the completion dispatcher).
// The duty thread's LogRing has one producer, the duty thread, so these threads never push into it: they write one
// line to stderr each, serialised by a mutex. Never called on a duty thread. Lines carry the level and the
// process name, as the duty's lines do once main drains them.

namespace openexchange { namespace gateway {

enum class FrontLevel : std::uint8_t
{
    INFO,
    WARN,
    ERROR,
    CRITICAL
};

void setFrontLogProcess(const char *processName);
void frontLog(FrontLevel level, const char *format, ...) __attribute__((format(printf, 2, 3)));

}}

#endif
