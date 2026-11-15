#include <cstdio>
#include <cstring>

#include "Check.h"

int main(int argc, char **argv)
{
    int ran = 0;
    for (const gwtest::Case &c : gwtest::cases())
    {
        bool selected = argc < 2;
        for (int i = 1; i < argc; ++i)
        {
            selected = selected || 0 == std::strcmp(argv[i], c.name);
        }
        if (!selected)
        {
            continue;
        }
        const int before = gwtest::failures();
        c.body();
        std::printf("%s %s\n", before == gwtest::failures() ? "PASS" : "FAIL", c.name);
        ran++;
    }
    if (0 == ran)
    {
        std::fprintf(stderr, "no test case matched\n");
        return 1;
    }
    std::printf("%d case(s), %d failed check(s)\n", ran, gwtest::failures());
    return 0 == gwtest::failures() ? 0 : 1;
}
