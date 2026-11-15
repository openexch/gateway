// Test helper, never published: a media driver for the external-mode tests (the role the host aeronmd plays in
// production). Starts the C media driver of the pinned Aeron at <dir>, prints "driver ready <dir>", and runs until
// SIGTERM or SIGINT; SIGKILL is the driver-death test.
//
// usage: gw_test_media_driver <dir>
#include <signal.h>

#include <cstdio>
#include <string>

#include "cluster_tools/runtime/EmbeddedDriver.h"

namespace rt = cluster_tools::runtime;

int main(int argc, char **argv)
{
    if (2 != argc)
    {
        std::fprintf(stderr, "usage: gw_test_media_driver <dir>\n");
        return 64;
    }
    sigset_t stop;
    sigemptyset(&stop);
    sigaddset(&stop, SIGTERM);
    sigaddset(&stop, SIGINT);
    pthread_sigmask(SIG_BLOCK, &stop, nullptr);

    rt::EmbeddedDriverOptions options;
    options.processName = "gw-test-media-driver";
    options.dirPrefix = "gw-test-media-driver-";
    options.instanceKey = "argv[1]";
    rt::EmbeddedDriver driver(options, [](const std::string &why) {
        std::fprintf(stderr, "gw_test_media_driver: terminated: %s\n", why.c_str());
    });
    std::string error;
    if (!driver.start(argv[1], false, error))
    {
        std::fprintf(stderr, "gw_test_media_driver: %s\n", error.c_str());
        return 1;
    }
    std::printf("driver ready %s\n", argv[1]);
    std::fflush(stdout);
    int sig = 0;
    sigwait(&stop, &sig);
    return driver.close(5'000) ? 0 : 1;
}
