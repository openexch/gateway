#ifndef GATEWAY_COMMON_VERSION_H
#define GATEWAY_COMMON_VERSION_H

#include <cstdio>
#include <cstring>

#include "gateway/build_info.h"

namespace openexchange { namespace gateway {

// `<binary> --version`: one line on stdout, "<process> <version> source <commit> cluster-tools <commit>", then exit 0,
// before any setting is read, any thread starts or anything binds. The source commit is the one the binary was built
// from (GW_BUILD_SOURCE_SHA, or git HEAD at configure), the value /health reports as "build". The publish job and the
// box-side self-check compare it with the commit the release cut names; that check runs the binary on its own
// architecture, so it needs an answer that no configuration can stop.
inline bool versionRequested(int argc, char **argv)
{
    return 2 == argc && 0 == std::strcmp("--version", argv[1]);
}

inline int printVersion(const char *processName)
{
    std::printf("%s %s source %s cluster-tools %s\n", processName, GW_VERSION, GW_SOURCE_SHA, GW_CLUSTER_TOOLS_SHA);
    return 0 == std::fflush(stdout) ? 0 : 74; // 74: the line could not be written (EX_IOERR)
}

}}

#endif
