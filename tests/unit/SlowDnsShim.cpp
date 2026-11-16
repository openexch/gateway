// Test-only LD_PRELOAD shim (never linked into a binary): getaddrinfo of a name ending in ".slow.invalid" sleeps
// GW_TEST_SLOW_DNS_MS (default 2000) and then answers EAI_NONAME, so a test can see whether a poll round waits for a
// name lookup that is already running. Every other name goes to the C library's getaddrinfo.

#include <dlfcn.h>
#include <netdb.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

extern "C" {

int gw_test_slow_dns_active = 1;

int getaddrinfo(const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res)
{
    using Real = int (*)(const char *, const char *, const struct addrinfo *, struct addrinfo **);
    static Real real = reinterpret_cast<Real>(dlsym(RTLD_NEXT, "getaddrinfo"));
    static const char suffix[] = ".slow.invalid";
    const std::size_t n = nullptr != node ? std::strlen(node) : 0;
    const std::size_t s = sizeof(suffix) - 1;
    if (n > s && 0 == std::strcmp(node + n - s, suffix))
    {
        const char *ms = std::getenv("GW_TEST_SLOW_DNS_MS");
        std::this_thread::sleep_for(std::chrono::milliseconds(nullptr != ms ? std::atol(ms) : 2000));
        return EAI_NONAME;
    }
    return real(node, service, hints, res);
}
}
