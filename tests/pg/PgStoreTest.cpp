// The users store against a real PostgreSQL (tests/pg/pg_it.py starts a throwaway container and removes it):
// migration applied once under concurrency and recorded with its sha256, verify-only refusal, schema mismatch,
// the users round trip with hashed tokens, a bounded wait on an unreachable server, and the demo-token stale window
// over the real store (PG down with a cached token is served, then 503 after the stale bound).
//
// env: GW_TEST_PG_URL (the container's database), GW_TEST_PG_ADMIN_URL (the same server's postgres database, to create
// fresh databases), GW_TEST_PG_PAUSE / GW_TEST_PG_UNPAUSE (shell commands that freeze and thaw the server).
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <libpq-fe.h>

#include "Check.h"
#include "PgTestSupport.h"
#include "gateway/common/Authenticator.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/PgUserStore.h"
#include "gateway/common/WorkPool.h"

using namespace openexchange::gateway;
using gwtest::env;
using gwtest::freshDatabase;
using gwtest::run;
using gwtest::sql;

namespace {

PgSettings settingsFor(const std::string &url, bool migrate = true, int workers = 2, std::int64_t timeoutMs = 2000)
{
    PgSettings s;
    s.url = url;
    s.workers = workers;
    s.statementTimeoutMs = timeoutMs;
    s.migrate = migrate;
    return s;
}

std::string hashOf(const std::string &token)
{
    return std::string(crypto::view(crypto::sha256(token)));
}

}

TEST(pg_migration_applies_once_under_concurrency)
{
    const std::string url = freshDatabase("gw_migrate");
    PgUserStore a(settingsFor(url));
    PgUserStore b(settingsFor(url));
    std::int64_t idA = 0;
    std::int64_t idB = 0;
    StoreStatus sa = StoreStatus::UNAVAILABLE;
    StoreStatus sb = StoreStatus::UNAVAILABLE;
    std::thread ta([&] { sa = a.reserveUserId(0, "alice", idA); });
    std::thread tb([&] { sb = b.reserveUserId(0, "bob", idB); });
    ta.join();
    tb.join();
    CHECK_MSG(StoreStatus::OK == sa && StoreStatus::OK == sb, a.state() + " / " + b.state());
    CHECK(idA != idB && idA >= 100000 && idB >= 100000);
    CHECK(sql(url, "SELECT count(*) FROM gateway_schema_migrations") == "1");
    CHECK(sql(url, "SELECT sha256 FROM gateway_schema_migrations WHERE version = 1") == embeddedMigrations().at(0).sha256);
    CHECK(sql(url, "SELECT name FROM gateway_schema_migrations WHERE version = 1") == "users");
    CHECK(a.state() == "ready");
    // the token is never stored: one 32-byte hash column, no token text column
    CHECK(sql(url, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'token'") == "0");
    CHECK(sql(url, "SELECT data_type FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'token_hash'") == "bytea");
}

TEST(pg_users_round_trip)
{
    const std::string url = freshDatabase("gw_users");
    PgUserStore s(settingsFor(url));
    std::int64_t id = 0;
    CHECK(StoreStatus::OK == s.reserveUserId(0, "alice", id));
    CHECK(100000 == id);
    const std::string t1 = hashOf("token-one");
    CHECK(StoreStatus::OK == s.insertUser(0, id, "alice", "pbkdf2$1000$AAAA$BBBB", t1));
    CHECK(sql(url, "SELECT octet_length(token_hash) FROM users WHERE user_id = 100000") == "32");
    std::int64_t other = 0;
    CHECK(StoreStatus::CONFLICT == s.reserveUserId(1, "alice", other)); // taken: no id consumed
    CHECK(StoreStatus::OK == s.reserveUserId(1, "bob", other) && 100001 == other);
    CHECK(StoreStatus::CONFLICT == s.insertUser(1, 100002, "alice", "pbkdf2$1000$AAAA$BBBB", hashOf("x")));
    StoredUser u;
    CHECK(StoreStatus::OK == s.findByUsername(0, "alice", u));
    CHECK(100000 == u.userId && u.username == "alice" && u.passwordHash == "pbkdf2$1000$AAAA$BBBB" && u.tokenHash == t1);
    CHECK(StoreStatus::NOT_FOUND == s.findByUsername(0, "nobody", u));
    CHECK(StoreStatus::OK == s.findByToken(1, t1, u) && 100000 == u.userId);
    const std::string t2 = hashOf("token-two");
    CHECK(StoreStatus::OK == s.setToken(0, 100000, t2));
    CHECK(StoreStatus::NOT_FOUND == s.findByToken(0, t1, u));
    CHECK(StoreStatus::OK == s.findByToken(0, t2, u));
    CHECK(StoreStatus::NOT_FOUND == s.clearToken(0, 100000, t1)); // only the current token clears
    CHECK(StoreStatus::OK == s.clearToken(0, 100000, t2));
    CHECK(StoreStatus::NOT_FOUND == s.findByToken(0, t2, u));
    CHECK(StoreStatus::OK == s.findByUsername(0, "alice", u) && !u.tokenHash);
    CHECK(StoreStatus::NOT_FOUND == s.setToken(0, 999999, t2));
    // the sequence decides the next account; an operator moving it is followed exactly
    sql(url, "SELECT setval('demo_user_seq', 123456)");
    CHECK(StoreStatus::OK == s.reserveUserId(0, "carol", id) && 123457 == id);
    // the database refuses what the gateway would refuse
    PGconn *c = PQconnectdb(url.c_str());
    PGresult *r = PQexec(c, "INSERT INTO users (user_id, username, password_hash) VALUES (5, 'Bad Name', 'pbkdf2$x')");
    CHECK(PGRES_FATAL_ERROR == PQresultStatus(r));
    PQclear(r);
    PQfinish(c);
}

TEST(pg_verify_only_and_mismatch)
{
    const std::string url = freshDatabase("gw_verify");
    PgUserStore reader(settingsFor(url, false));
    StoredUser u;
    CHECK(StoreStatus::UNAVAILABLE == reader.findByToken(0, hashOf("t"), u));
    CHECK_MSG(reader.state().find("schema not applied") == 0, reader.state());
    CHECK(sql(url, "SELECT to_regclass('users') IS NULL") == "t"); // verify-only never applies anything
    PgUserStore writer(settingsFor(url));
    CHECK(StoreStatus::NOT_FOUND == writer.findByToken(0, hashOf("t"), u));
    CHECK(StoreStatus::NOT_FOUND == reader.findByToken(0, hashOf("t"), u));

    // an applied migration edited in the database (or a build carrying another file): refused, nothing applied
    sql(url, "UPDATE gateway_schema_migrations SET sha256 = 'not-the-file' WHERE version = 1");
    PgUserStore mismatched(settingsFor(url));
    CHECK(StoreStatus::UNAVAILABLE == mismatched.findByToken(0, hashOf("t"), u));
    CHECK_MSG(mismatched.state().find("schema mismatch: version 1 (users) is recorded with sha256 not-the-file") == 0,
        mismatched.state());
    CHECK(StoreStatus::UNAVAILABLE == mismatched.findByToken(1, hashOf("t"), u)); // every worker, for good
    sql(url, "UPDATE gateway_schema_migrations SET sha256 = '" + std::string(embeddedMigrations().at(0).sha256) +
        "' WHERE version = 1");
    sql(url, "INSERT INTO gateway_schema_migrations (version, name, sha256) VALUES (99, 'future', 'x')");
    PgUserStore older(settingsFor(url));
    CHECK(StoreStatus::UNAVAILABLE == older.findByToken(0, hashOf("t"), u));
    CHECK_MSG(older.state().find("schema mismatch: version 99 is recorded but unknown to this build") == 0, older.state());
}

TEST(pg_unreachable_is_bounded)
{
    // nothing listens: refused at once
    PgUserStore closed(settingsFor("postgresql://gw@127.0.0.1:1/gw", true, 1, 500));
    StoredUser u;
    auto t0 = std::chrono::steady_clock::now();
    CHECK(StoreStatus::UNAVAILABLE == closed.findByToken(0, hashOf("t"), u));
    CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(1500));
    CHECK_MSG(closed.state().find("connect failed") == 0, closed.state());

    // a frozen server: the statement deadline bounds the wait, then the connection is replaced
    const std::string url = freshDatabase("gw_frozen");
    PgUserStore s(settingsFor(url, true, 1, 500));
    CHECK(StoreStatus::NOT_FOUND == s.findByToken(0, hashOf("t"), u));
    run("GW_TEST_PG_PAUSE");
    t0 = std::chrono::steady_clock::now();
    const StoreStatus st = s.findByToken(0, hashOf("t"), u);
    const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
    run("GW_TEST_PG_UNPAUSE");
    CHECK(StoreStatus::UNAVAILABLE == st);
    CHECK_MSG(waited < std::chrono::milliseconds(1500), std::to_string(waited.count()) + " ms for a 500 ms deadline");
    CHECK(StoreStatus::NOT_FOUND == s.findByToken(0, hashOf("t"), u)); // reconnected
    CHECK(s.state() == "ready");
}

TEST(pg_demo_token_stale_window)
{
    const std::string url = freshDatabase("gw_stale");
    PgSettings ps = settingsFor(url, true, 1, 500);
    ps.retryMs = 300;
    PgUserStore store(ps);
    WorkPool pool("t-pg-auth", 1, 8);
    pool.start();
    std::atomic<std::int64_t> clock{ 1'000'000 };
    AuthenticatorSettings as;
    as.modes = auth_mode::DEMO;
    as.tokenCacheTtlMs = 5000;
    as.tokenStaleMs = 60'000;
    Authenticator auth(as, AuthMaterial{}, &store, &pool, [&] { return clock.load(); });
    const std::string token = newDemoToken();
    std::int64_t id = 0;
    CHECK(StoreStatus::OK == store.reserveUserId(0, "alice", id));
    CHECK(StoreStatus::OK == store.insertUser(0, id, "alice", "pbkdf2$1000$AAAA$BBBB", hashOf(token)));
    auto resolve = [&]() {
        std::mutex m;
        std::condition_variable cv;
        std::optional<AuthResult> out;
        CredentialHeaders h;
        const std::string header = "Bearer " + token;
        h.authorization = header;
        auth.resolve(h, [&](AuthResult r) {
            std::lock_guard<std::mutex> l(m);
            out = r;
            cv.notify_all();
        });
        std::unique_lock<std::mutex> l(m);
        cv.wait_for(l, std::chrono::seconds(10), [&] { return out.has_value(); });
        return out.value_or(AuthResult{});
    };
    CHECK(AuthOutcome::OK == resolve().outcome);
    run("GW_TEST_PG_PAUSE");
    clock += 6000; // past the TTL: the store is asked, cannot answer, the cached entry is served
    const AuthResult stale = resolve();
    clock += 60'000; // past the stale bound: 503, never 401
    const AuthResult gone = resolve();
    run("GW_TEST_PG_UNPAUSE");
    CHECK_MSG(AuthOutcome::OK == stale.outcome && 1 == auth.staleServed(), stale.detail);
    CHECK_MSG(AuthOutcome::UNAVAILABLE == gone.outcome, gone.detail);
    // the failed connect of `gone` holds reconnects back for GATEWAY_PG_RETRY_MS (300 ms here): a call inside that
    // window fails at once, the first one after it reconnects
    const auto back = std::chrono::steady_clock::now();
    AuthResult again = resolve();
    while (AuthOutcome::OK != again.outcome && std::chrono::steady_clock::now() - back < std::chrono::seconds(3))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        again = resolve();
    }
    CHECK_MSG(AuthOutcome::OK == again.outcome, again.detail);
    pool.stop(5000);
}
