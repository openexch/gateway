#include "gateway/common/FrontLog.h"

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <mutex>

namespace openexchange { namespace gateway {

namespace {

std::mutex g_mutex;
std::atomic<const char *> g_process{ "gateway" };

const char *levelName(FrontLevel level)
{
    switch (level)
    {
        case FrontLevel::INFO: return "INFO";
        case FrontLevel::WARN: return "WARN";
        case FrontLevel::ERROR: return "ERROR";
        case FrontLevel::CRITICAL: return "CRITICAL";
    }
    return "?";
}

}

void setFrontLogProcess(const char *processName)
{
    g_process.store(processName, std::memory_order_release);
}

void frontLog(FrontLevel level, const char *format, ...)
{
    char text[512];
    va_list args;
    va_start(args, format);
    std::vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    std::lock_guard<std::mutex> lock(g_mutex);
    std::fprintf(stderr, "%s %s %s\n", g_process.load(std::memory_order_acquire), levelName(level), text);
    std::fflush(stderr);
}

}}
