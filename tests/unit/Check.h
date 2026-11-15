#ifndef GATEWAY_TESTS_CHECK_H
#define GATEWAY_TESTS_CHECK_H

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

// A minimal test runner: TEST(name) registers a case, CHECK(cond) records a failure with its line and keeps going,
// main() runs every case (or the ones named on the command line) and exits 1 when any check failed.

namespace gwtest {

struct Case
{
    const char *name;
    std::function<void()> body;
};

inline std::vector<Case> &cases()
{
    static std::vector<Case> all;
    return all;
}

inline int &failures()
{
    static int n = 0;
    return n;
}

struct Register
{
    Register(const char *name, std::function<void()> body) { cases().push_back(Case{ name, std::move(body) }); }
};

inline void fail(const char *file, int line, const std::string &what)
{
    std::fprintf(stderr, "  FAILED %s:%d: %s\n", file, line, what.c_str());
    failures()++;
}

}

#define GW_CAT2(a, b) a##b
#define GW_CAT(a, b) GW_CAT2(a, b)
#define TEST(name)                                                                                                      \
    static void name();                                                                                                 \
    static ::gwtest::Register GW_CAT(register_, name)(#name, &name);                                                   \
    static void name()
#define CHECK(cond)                                                                                                     \
    do                                                                                                                  \
    {                                                                                                                   \
        if (!(cond))                                                                                                    \
        {                                                                                                               \
            ::gwtest::fail(__FILE__, __LINE__, #cond);                                                                  \
        }                                                                                                               \
    } while (0)
#define CHECK_MSG(cond, msg)                                                                                            \
    do                                                                                                                  \
    {                                                                                                                   \
        if (!(cond))                                                                                                    \
        {                                                                                                               \
            ::gwtest::fail(__FILE__, __LINE__, std::string(#cond) + ": " + (msg));                                       \
        }                                                                                                               \
    } while (0)

#endif
