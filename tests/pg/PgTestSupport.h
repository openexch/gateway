#ifndef GATEWAY_TESTS_PG_TEST_SUPPORT_H
#define GATEWAY_TESTS_PG_TEST_SUPPORT_H

// Helpers of the PostgreSQL tests (tests/pg/pg_it.py runs them against a throwaway container):
// env: GW_TEST_PG_URL (the container's database), GW_TEST_PG_ADMIN_URL (the same server's postgres database, to create
// fresh databases), GW_TEST_PG_PAUSE / GW_TEST_PG_UNPAUSE (shell commands that freeze and thaw the server).
#include <cstdlib>
#include <string>

#include <libpq-fe.h>

#include "Check.h"

namespace gwtest {

inline std::string env(const char *name)
{
    const char *v = std::getenv(name);
    return nullptr == v ? std::string() : std::string(v);
}

// One statement (or a script, the simple protocol) on a fresh connection; the first value of the first row ("" when
// none). Fails the test on an error.
inline std::string sql(const std::string &url, const std::string &statement)
{
    PGconn *c = PQconnectdb(url.c_str());
    std::string out;
    if (CONNECTION_OK != PQstatus(c))
    {
        gwtest::fail(__FILE__, __LINE__, std::string("connect: ") + PQerrorMessage(c));
        PQfinish(c);
        return out;
    }
    PGresult *r = PQexec(c, statement.c_str());
    const ExecStatusType st = PQresultStatus(r);
    if (PGRES_COMMAND_OK != st && PGRES_TUPLES_OK != st)
    {
        gwtest::fail(__FILE__, __LINE__, statement.substr(0, 200) + ": " + PQresultErrorMessage(r));
    }
    else if (PGRES_TUPLES_OK == st && PQntuples(r) > 0)
    {
        out = PQgetvalue(r, 0, 0);
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

// A fresh database on the test server; its URL.
inline std::string freshDatabase(const std::string &name)
{
    const std::string admin = env("GW_TEST_PG_ADMIN_URL");
    sql(admin, "DROP DATABASE IF EXISTS " + name);
    sql(admin, "CREATE DATABASE " + name);
    const std::string base = env("GW_TEST_PG_URL");
    return base.substr(0, base.rfind('/') + 1) + name;
}

inline void run(const char *what)
{
    const std::string cmd = env(what);
    if (cmd.empty() || 0 != std::system(cmd.c_str()))
    {
        gwtest::fail(__FILE__, __LINE__, std::string(what) + " failed: '" + cmd + "'");
    }
}

}

#endif
