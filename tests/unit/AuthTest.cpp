// The auth layer's parts (T0): crypto primitives against published vectors, the stored password form against a value
// the previous order service's Java hasher wrote, HS256 JWT verdicts, the credential files, the rate buckets and the
// source address rule, the principal resolver in every mode (each alone and combined, the refusals, the demo-token
// cache and its stale window, no token from a URL), the duty bridge and the worker pool.
#include <pthread.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <thread>

#include "Check.h"
#include "MemoryStore.h"
#include "gateway/common/Api.h"
#include "gateway/common/AuthMaterial.h"
#include "gateway/common/Authenticator.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/DutyBridge.h"
#include "gateway/common/Jwt.h"
#include "gateway/common/Password.h"
#include "gateway/common/PgUserStore.h"
#include "gateway/common/RateLimit.h"
#include "gateway/common/Settings.h"
#include "gateway/common/WorkPool.h"

using namespace openexchange::gateway;
using gwtest::MemoryStore;

namespace {

// Test fixture secrets: generated per run or obviously fake.
const std::string JWT_SECRET = "test-only-hs256-secret-0123456789abcdef";
const std::string API_KEY = "test-key-aaaaaaaaaaaaaaaa";

std::string hexOf(const crypto::Digest &d)
{
    return crypto::hex(crypto::view(d));
}

// Waits for one AuthResult (the demo path answers from a pool thread).
struct Waiter
{
    std::mutex m;
    std::condition_variable cv;
    std::optional<AuthResult> result;
    Authenticator::Done done()
    {
        return [this](AuthResult r) {
            std::lock_guard<std::mutex> l(m);
            result = std::move(r);
            cv.notify_all();
        };
    }
    AuthResult get()
    {
        std::unique_lock<std::mutex> l(m);
        cv.wait_for(l, std::chrono::seconds(5), [this] { return result.has_value(); });
        AuthResult r = result.value_or(AuthResult{});
        result.reset();
        return r;
    }
};

AuthResult resolveNow(Authenticator &a, std::optional<std::string> authorization, std::optional<std::string> apiKey,
    std::string target = "/api/v2/auth/me")
{
    Waiter w;
    CredentialHeaders h;
    if (authorization)
    {
        h.authorization = *authorization;
    }
    if (apiKey)
    {
        h.apiKey = *apiKey;
    }
    h.target = target;
    a.resolve(h, w.done());
    return w.get();
}

std::string jwtFor(const std::string &sub, std::int64_t exp, const std::string &secret = JWT_SECRET,
    const std::string &header = R"({"alg":"HS256","typ":"JWT"})")
{
    return jwt::signHs256(header, R"({"sub":")" + sub + R"(","exp":)" + std::to_string(exp) + "}", secret);
}

AuthMaterial material()
{
    AuthMaterial m;
    m.apiKeys.add(API_KEY, 4242);
    m.jwtSecret = JWT_SECRET;
    return m;
}

}

// ---- crypto vectors

TEST(crypto_vectors)
{
    CHECK(hexOf(crypto::sha256("abc")) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    // RFC 4231 test case 2
    CHECK(hexOf(crypto::hmacSha256("Jefe", "what do ya want for nothing?")) ==
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    // PBKDF2-HMAC-SHA256, P="password", S="salt", c=1, dkLen=32 (RFC 7914 section 11 family, widely published)
    CHECK(crypto::hex(crypto::pbkdf2Sha256("password", "salt", 1, 32)) ==
        "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b");
    CHECK(crypto::constantTimeEqual("abc", "abc"));
    CHECK(!crypto::constantTimeEqual("abc", "abd"));
    CHECK(!crypto::constantTimeEqual("abc", "abcd"));
    CHECK(crypto::randomBytes(32).size() == 32);
    CHECK(crypto::randomBytes(32) != crypto::randomBytes(32));
}

TEST(crypto_base64url_strict)
{
    for (std::size_t n = 0; n < 40; ++n)
    {
        const std::string bytes = crypto::randomBytes(n);
        const std::string enc = crypto::base64UrlEncode(bytes);
        CHECK(enc.find('=') == std::string::npos);
        CHECK(crypto::base64UrlDecode(enc) == bytes);
    }
    CHECK(crypto::base64UrlEncode("\xfb\xff") == "-_8");
    CHECK(!crypto::base64UrlDecode("YQ=="));  // padding refused
    CHECK(!crypto::base64UrlDecode("Y+8"));   // the standard alphabet refused
    CHECK(!crypto::base64UrlDecode("YWJjZ")); // a length no encoder writes
    CHECK(!crypto::base64UrlDecode("YR"));    // non-zero trailing bits: a second spelling of "a"
    CHECK(crypto::base64UrlDecode("YQ") == std::string("a"));
}

// ---- the stored password form

TEST(password_format_kept)
{
    // written by the previous order service's hasher (PBKDF2WithHmacSHA256, 1000 rounds, salt bytes 0..15, UTF-8)
    const std::string javaWritten = "pbkdf2$1000$AAECAwQFBgcICQoLDA0ODw$JexSuCXQT5WUpxhd7jsw7PPRRe90sIVDR4JCEICP8a4";
    CHECK(password::verify("fixture-p\xc3\xa4ssword", javaWritten));
    CHECK(!password::verify("fixture-passw0rd", javaWritten));
    const std::string ours = password::hash("another-fixture", 1200);
    std::uint32_t iterations = 0;
    CHECK(password::wellFormed(ours, &iterations) && 1200 == iterations);
    CHECK(0 == ours.rfind("pbkdf2$1200$", 0));
    CHECK(password::verify("another-fixture", ours));
    CHECK(!password::verify("another-fixturE", ours));
    CHECK(password::hash("x", 1000) != password::hash("x", 1000)); // salted
    for (const char *bad : { "", "pbkdf2$1000$AA$", "sha1$1000$AAECAwQFBgcICQoLDA0ODw$JexSuCXQT5WUpxhd7jsw7PPRRe90sIVDR4JCEICP8a4",
             "pbkdf2$0$AAECAwQFBgcICQoLDA0ODw$JexSuCXQT5WUpxhd7jsw7PPRRe90sIVDR4JCEICP8a4",
             "pbkdf2$01000$AAECAwQFBgcICQoLDA0ODw$JexSuCXQT5WUpxhd7jsw7PPRRe90sIVDR4JCEICP8a4",
             "pbkdf2$1000$AAECAwQFBgcICQoLDA0ODw$JexSuCXQT5WUpxhd7jsw7PPRRe90sIVDR4JCEICP8a",
             "pbkdf2$1000$AAECAwQFBgcICQoLDA0ODw$JexSuCXQT5WUpxhd7jsw7PPRRe90sIVDR4JCEICP8a4$x" })
    {
        bool malformed = false;
        CHECK_MSG(!password::verify("fixture", bad, &malformed) && malformed, bad);
    }
}

// ---- JWT

TEST(jwt_verdicts)
{
    const std::int64_t now = 1'800'000'000;
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60), JWT_SECRET, now).verdict == jwt::Verdict::OK);
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60), JWT_SECRET, now).userId == 1001);
    CHECK(jwt::verifyHs256(jwtFor("1001", now), JWT_SECRET, now).verdict == jwt::Verdict::EXPIRED);
    CHECK(jwt::verifyHs256(jwtFor("1001", now - 1), JWT_SECRET, now).verdict == jwt::Verdict::EXPIRED);
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60, "another-test-secret-0123456789abcdef"), JWT_SECRET, now).verdict ==
        jwt::Verdict::BAD_SIGNATURE);
    const std::string noExp = jwt::signHs256(R"({"alg":"HS256"})", R"({"sub":"1001"})", JWT_SECRET);
    CHECK(jwt::verifyHs256(noExp, JWT_SECRET, now).verdict == jwt::Verdict::MISSING_EXP);
    const std::string stringExp = jwt::signHs256(R"({"alg":"HS256"})", R"({"sub":"1001","exp":"9999999999"})", JWT_SECRET);
    CHECK(jwt::verifyHs256(stringExp, JWT_SECRET, now).verdict == jwt::Verdict::MISSING_EXP);
    const std::string future = jwt::signHs256(R"({"alg":"HS256"})",
        R"({"sub":"1001","exp":)" + std::to_string(now + 600) + R"(,"nbf":)" + std::to_string(now + 60) + "}", JWT_SECRET);
    CHECK(jwt::verifyHs256(future, JWT_SECRET, now).verdict == jwt::Verdict::NOT_YET_VALID);
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60, JWT_SECRET, R"({"alg":"none"})"), JWT_SECRET, now).verdict ==
        jwt::Verdict::BAD_ALGORITHM);
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60, JWT_SECRET, R"({"alg":"HS512"})"), JWT_SECRET, now).verdict ==
        jwt::Verdict::BAD_ALGORITHM);
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60, JWT_SECRET, R"({"alg":"HS256","crit":["x"]})"), JWT_SECRET, now)
              .verdict == jwt::Verdict::BAD_ALGORITHM);
    for (const char *sub : { "0", "-5", "01", "abc", "9223372036854775808", "" })
    {
        CHECK_MSG(jwt::verifyHs256(jwtFor(sub, now + 60), JWT_SECRET, now).verdict == jwt::Verdict::BAD_SUBJECT, sub);
    }
    const std::string numericSub = jwt::signHs256(R"({"alg":"HS256"})",
        R"({"sub":1001,"exp":)" + std::to_string(now + 60) + "}", JWT_SECRET);
    CHECK(jwt::verifyHs256(numericSub, JWT_SECRET, now).verdict == jwt::Verdict::BAD_SUBJECT);
    CHECK(jwt::verifyHs256("a.b", JWT_SECRET, now).verdict == jwt::Verdict::MALFORMED);
    CHECK(jwt::verifyHs256("a.b.c.d", JWT_SECRET, now).verdict == jwt::Verdict::MALFORMED);
    CHECK(jwt::verifyHs256(jwtFor("1001", now + 60) + "=", JWT_SECRET, now).verdict == jwt::Verdict::MALFORMED);
}

// ---- credential files

TEST(auth_material_api_keys)
{
    ApiKeyTable t;
    std::vector<std::string> errors;
    parseApiKeys("# bots\n\n" + API_KEY + ":1001\r\nsecond-test-key-bbbbbbbb:1002\n", "f", t, errors);
    CHECK(errors.empty());
    CHECK(2 == t.size());
    CHECK(t.find(API_KEY) == std::optional<std::int64_t>(1001));
    CHECK(t.find("second-test-key-bbbbbbbb") == std::optional<std::int64_t>(1002));
    CHECK(!t.find("test-key-aaaaaaaaaaaaaaab"));
    CHECK(!t.find(""));

    ApiKeyTable bad;
    errors.clear();
    parseApiKeys("short:1\nno-colon-here-at-all\n" + API_KEY + ":0\n" + API_KEY + ":x\nkey with space aaaa:5\n" +
        "valid-test-key-cccccccc:7\nvalid-test-key-cccccccc:8\n", "f", bad, errors);
    CHECK_MSG(6 == errors.size(), std::to_string(errors.size()));
    std::string all;
    for (const auto &e : errors)
    {
        all += e + "\n";
    }
    CHECK(all.find("f line 1: the key must be 16") != std::string::npos);
    CHECK(all.find("f line 2: expected key:userId") != std::string::npos);
    CHECK(all.find("f line 3: the userId must be a positive integer") != std::string::npos);
    CHECK(all.find("f line 7: the key is listed twice") != std::string::npos);
    CHECK(all.find("valid-test-key") == std::string::npos); // a problem never quotes a key

    ApiKeyTable empty;
    errors.clear();
    parseApiKeys("# nothing\n", "f", empty, errors);
    CHECK(1 == errors.size() && errors[0].find("holds no key:userId entry") != std::string::npos);
}

TEST(auth_material_jwt_secret_and_files)
{
    std::string secret;
    std::vector<std::string> errors;
    CHECK(parseJwtSecret(JWT_SECRET + "\n", "s", secret, errors) && secret == JWT_SECRET);
    CHECK(parseJwtSecret(JWT_SECRET + "\r\n", "s", secret, errors) && secret == JWT_SECRET); // a CRLF file too
    CHECK(parseJwtSecret(JWT_SECRET, "s", secret, errors) && secret == JWT_SECRET);
    CHECK(!parseJwtSecret("too-short-secret\n", "s", secret, errors));
    CHECK(1 == errors.size() && errors[0].find("needs at least 32") != std::string::npos);

    CommonSettings c;
    c.authModes = auth_mode::API_KEY | auth_mode::JWT;
    c.apiKeysFile = "/nonexistent/test-api-keys";
    c.jwtSecretFile = "/nonexistent/test-jwt-secret";
    errors.clear();
    loadAuthMaterial(c, errors);
    CHECK(2 == errors.size());
    CHECK(errors[0].find("GATEWAY_API_KEYS_FILE: cannot read /nonexistent/test-api-keys") == 0);
    CHECK(errors[1].find("GATEWAY_JWT_SECRET_FILE: cannot read /nonexistent/test-jwt-secret") == 0);
    // a mode that is off reads no file
    c.authModes = auth_mode::DEMO;
    errors.clear();
    loadAuthMaterial(c, errors);
    CHECK(errors.empty());
}

// ---- settings of this slice

TEST(settings_auth_rates_and_bounds)
{
    std::map<std::string, std::string> env = {
        { "GATEWAY_DRIVER_MODE", "external" },
        { "AERON_DIR", "/dev/shm/aeron-host" },
        { "GATEWAY_CORE_INGRESS", "0=10.0.0.1:9010" },
        { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=4m|mtu=8192" },
        { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp?endpoint=10.0.0.9:0|term-length=4m|mtu=8192" },
        { "CORE_AUTH_CREDENTIAL", "7:GATEWAY:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff" },
        { "GATEWAY_INSTANCE_ID", "1" },
        { "GATEWAY_HTTP_BIND", "0.0.0.0:8080" },
        { "GATEWAY_ADMIN_BIND", "127.0.0.1:9180" },
        { "GATEWAY_AUTH_MODES", "demo" },
        { "GATEWAY_PG_URL", "postgresql://gw@127.0.0.1/gw" },
    };
    cluster_tools::runtime::DirProbe probe;
    probe.isDirectory = [](const std::string &) { return true; };
    probe.isWritable = [](const std::string &) { return true; };
    auto p = parseGatewaySettings(MapEnvironment(env), probe);
    CHECK(p.ok());
    CHECK(p.settings.registerRate.count == 1 && p.settings.registerRate.periodMs == 1000 && 5 == p.settings.registerBurst);
    CHECK(p.settings.loginRate.count == 10 && 20 == p.settings.loginBurst);
    CHECK(p.settings.authSourceRate.count == 10 && p.settings.authSourceRate.periodMs == 60'000 &&
        10 == p.settings.authSourceBurst);
    CHECK(600'000 == p.settings.pbkdf2Iterations && 8192 == p.settings.maxHeaderBytes &&
        60'000 == p.settings.httpIdleTimeoutMs && 1 == p.settings.hashThreads && 32 == p.settings.hashQueue);
    std::set<std::string> recorded;
    for (const auto &r : p.records)
    {
        recorded.insert(r.name + "=" + r.value + "/" + r.source);
    }
    for (const char *want : { "GATEWAY_REGISTER_BURST=5/default", "GATEWAY_LOGIN_BURST=20/default",
             "GATEWAY_AUTH_SOURCE_BURST=10/default", "GATEWAY_PBKDF2_ITERATIONS=600000/default",
             "GATEWAY_MAX_HEADER_BYTES=8192/default", "GATEWAY_HTTP_IDLE_TIMEOUT_MS=60000/default",
             "GATEWAY_REGISTER_RATE=1/s/default", "GATEWAY_AUTH_SOURCE_RATE=10/min/default" })
    {
        CHECK_MSG(recorded.count(want), want);
    }
    env["GATEWAY_REGISTER_BURST"] = "0";
    env["GATEWAY_LOGIN_RATE"] = "5/h";
    env["GATEWAY_AUTH_SOURCE_RATE"] = "10 /min";
    env["GATEWAY_PBKDF2_ITERATIONS"] = "3000000000";
    env["GATEWAY_LOGIN_BURST"] = "";
    p = parseGatewaySettings(MapEnvironment(env), probe);
    CHECK_MSG(5 == p.errors.size(), std::to_string(p.errors.size()));
    std::string all;
    for (const auto &e : p.errors)
    {
        all += e + "\n";
    }
    CHECK(all.find("GATEWAY_REGISTER_BURST") != std::string::npos);
    CHECK(all.find("GATEWAY_LOGIN_RATE") != std::string::npos);
    CHECK(all.find("GATEWAY_AUTH_SOURCE_RATE") != std::string::npos);
    CHECK(all.find("GATEWAY_PBKDF2_ITERATIONS must fit") != std::string::npos);
    CHECK(all.find("GATEWAY_LOGIN_BURST") != std::string::npos);
}

TEST(settings_token_lookup_budget_and_pg_retry)
{
    std::map<std::string, std::string> env = {
        { "GATEWAY_DRIVER_MODE", "external" },
        { "AERON_DIR", "/dev/shm/aeron-host" },
        { "GATEWAY_CORE_INGRESS", "0=10.0.0.1:9010" },
        { "GATEWAY_CORE_INGRESS_CHANNEL", "aeron:udp?term-length=4m|mtu=8192" },
        { "GATEWAY_CORE_RESPONSE_CHANNEL", "aeron:udp?endpoint=10.0.0.9:0|term-length=4m|mtu=8192" },
        { "CORE_AUTH_CREDENTIAL", "7:GATEWAY:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff" },
        { "GATEWAY_INSTANCE_ID", "1" },
        { "GATEWAY_HTTP_BIND", "0.0.0.0:8080" },
        { "GATEWAY_ADMIN_BIND", "127.0.0.1:9180" },
        { "GATEWAY_AUTH_MODES", "demo" },
        { "GATEWAY_PG_URL", "postgresql://gw@127.0.0.1/gw" },
    };
    cluster_tools::runtime::DirProbe probe;
    probe.isDirectory = [](const std::string &) { return true; };
    probe.isWritable = [](const std::string &) { return true; };
    auto p = parseGatewaySettings(MapEnvironment(env), probe);
    CHECK_MSG(p.ok(), p.errors.empty() ? "" : p.errors[0]);
    CHECK(60 == p.settings.tokenLookupRate.count && 60'000 == p.settings.tokenLookupRate.periodMs &&
        20 == p.settings.tokenLookupBurst);
    CHECK(32 == p.settings.tokenLookupInflight); // half of GATEWAY_PG_QUEUE 64
    CHECK(1000 == p.settings.pgRetryMs && 64 == p.settings.sourceV6PrefixBits);
    std::set<std::string> recorded;
    for (const auto &r : p.records)
    {
        recorded.insert(r.name + "=" + r.value + "/" + r.source);
    }
    for (const char *want : { "GATEWAY_TOKEN_LOOKUP_RATE=60/min/default", "GATEWAY_TOKEN_LOOKUP_BURST=20/default",
             "GATEWAY_TOKEN_LOOKUP_INFLIGHT=32/default", "GATEWAY_PG_RETRY_MS=1000/default",
             "GATEWAY_SOURCE_IPV6_PREFIX=64/default" })
    {
        CHECK_MSG(recorded.count(want), want);
    }
    env["GATEWAY_PG_QUEUE"] = "10";
    p = parseGatewaySettings(MapEnvironment(env), probe);
    CHECK(p.ok() && 5 == p.settings.tokenLookupInflight);
    // the lookup share must leave part of the queue to login, register and logout
    env["GATEWAY_TOKEN_LOOKUP_INFLIGHT"] = "10";
    env["GATEWAY_SOURCE_IPV6_PREFIX"] = "129";
    p = parseGatewaySettings(MapEnvironment(env), probe);
    CHECK_MSG(2 == p.errors.size(), std::to_string(p.errors.size()));
    CHECK(!p.errors.empty() && p.errors[0].find("GATEWAY_SOURCE_IPV6_PREFIX") == 0);
    CHECK(p.errors.size() > 1 && p.errors[1].find("GATEWAY_TOKEN_LOOKUP_INFLIGHT (10) must be below GATEWAY_PG_QUEUE") == 0);
    env["GATEWAY_TOKEN_LOOKUP_INFLIGHT"] = "9";
    env.erase("GATEWAY_SOURCE_IPV6_PREFIX");
    p = parseGatewaySettings(MapEnvironment(env), probe);
    CHECK(p.ok() && 9 == p.settings.tokenLookupInflight);
}

// ---- rates and the source address

TEST(rate_buckets)
{
    TokenBucketMs b(2, 1000, 3); // 2 per second, burst 3
    CHECK(b.take(0).ok && b.take(0).ok && b.take(0).ok);
    const TakeResult refused = b.take(0);
    CHECK(!refused.ok && 500 == refused.retryAfterMs);
    CHECK(!b.take(499).ok);
    CHECK(b.take(500).ok);
    CHECK(b.full(10'000));

    Rate perMin{ 10, 60'000, "10/min" };
    KeyedRateLimiter k(perMin, 2, 4);
    CHECK(k.take("a", 0).ok && k.take("a", 0).ok && !k.take("a", 0).ok);
    CHECK(k.take("b", 0).ok); // another source has its own bucket
    CHECK(!k.take("a", 1000).ok);
    CHECK(k.take("a", 6100).ok); // one token per 6 s
    // a bucket refilled to its burst equals a fresh one and is dropped at the next sweep
    for (int i = 0; i < 4; ++i)
    {
        k.take("c", 1'000'000);
    }
    CHECK(k.keys() <= 2);

    // a refund gives the token back, and a bucket full again is dropped at once (no key left by a refused request)
    KeyedRateLimiter r(perMin, 1, 1'000'000);
    CHECK(r.take("x", 0).ok && !r.take("x", 0).ok);
    r.refund("x", 0);
    CHECK(0 == r.keys());
    CHECK(r.take("x", 0).ok);
    r.refund("absent", 0);

    // the budget key of a source: IPv6 by its prefix, IPv4 as is
    CHECK(budgetKey("2001:db8:1:2:aaaa::1", 64) == "2001:db8:1:2::/64");
    CHECK(budgetKey("2001:db8:1:2:bbbb:cc::9", 64) == "2001:db8:1:2::/64");
    CHECK(budgetKey("2001:db8:1:3::1", 64) == "2001:db8:1:3::/64");
    CHECK(budgetKey("2001:db8:1:2:aaaa::1", 56) == "2001:db8:1::/56");
    CHECK(budgetKey("2001:db8::1", 128) == "2001:db8::1");
    CHECK(budgetKey("198.51.100.7", 64) == "198.51.100.7");
}

TEST(source_address_rule)
{
    namespace ip = boost::asio::ip;
    const auto peer = ip::make_address("203.0.113.9");
    SourceAddress none({});
    CHECK(none.resolve(peer, { "198.51.100.1" }) == "203.0.113.9"); // no trusted proxy: the header is ignored
    SourceAddress trusted({ "10.0.0.0/8", "127.0.0.1", "fd00::/8" });
    // an untrusted peer forging the header is ignored
    CHECK(trusted.resolve(peer, { "198.51.100.1" }) == "203.0.113.9");
    const auto proxy = ip::make_address("10.1.2.3");
    CHECK(trusted.resolve(proxy, { "198.51.100.1, 10.9.9.9" }) == "198.51.100.1");
    CHECK(trusted.resolve(proxy, { "1.1.1.1, 198.51.100.7" }) == "198.51.100.7"); // right-most untrusted
    CHECK(trusted.resolve(proxy, { "1.1.1.1", "198.51.100.7, 10.0.0.5" }) == "198.51.100.7"); // header lines joined
    CHECK(trusted.resolve(proxy, { "10.0.0.7, 10.0.0.8" }) == "10.0.0.7"); // all trusted: the left-most
    CHECK(trusted.resolve(proxy, { "garbage, 10.0.0.8" }) == "10.0.0.8"); // a bad entry ends the walk
    CHECK(trusted.resolve(proxy, {}) == "10.1.2.3");
    CHECK(trusted.resolve(ip::make_address("::ffff:10.1.2.3"), { "198.51.100.2" }) == "198.51.100.2"); // v4-mapped
    CHECK(trusted.resolve(ip::make_address("fd00::1"), { "2001:db8::5" }) == "2001:db8::5");
    CHECK(trusted.resolve(ip::make_address("fe80::1"), { "2001:db8::5" }) == "fe80::1");
}

// ---- the principal resolver

TEST(auth_modes_alone_and_combined)
{
    const std::int64_t now = 1'800'000'000;
    for (unsigned modes : { auth_mode::API_KEY, auth_mode::JWT, auth_mode::DEV, auth_mode::API_KEY | auth_mode::JWT,
             auth_mode::API_KEY | auth_mode::JWT | auth_mode::DEV })
    {
        AuthenticatorSettings s;
        s.modes = modes;
        Authenticator a(s, material(), nullptr, nullptr, [] { return std::int64_t(0); }, [now] { return now; });
        const AuthResult key = resolveNow(a, std::nullopt, API_KEY);
        const AuthResult token = resolveNow(a, "Bearer " + jwtFor("1001", now + 60), std::nullopt);
        const AuthResult dev = resolveNow(a, "Bearer dev:77", std::nullopt);
        const std::string m = std::to_string(modes);
        CHECK_MSG((0 != (modes & auth_mode::API_KEY)) == (AuthOutcome::OK == key.outcome), m);
        CHECK_MSG((0 != (modes & auth_mode::JWT)) == (AuthOutcome::OK == token.outcome), m);
        CHECK_MSG((0 != (modes & auth_mode::DEV)) == (AuthOutcome::OK == dev.outcome), m);
        if (AuthOutcome::OK == key.outcome)
        {
            CHECK(4242 == key.principal.userId && PrincipalMode::API_KEY == key.principal.mode);
        }
        if (AuthOutcome::OK == token.outcome)
        {
            CHECK(1001 == token.principal.userId && PrincipalMode::JWT == token.principal.mode);
        }
        if (AuthOutcome::OK == dev.outcome)
        {
            CHECK(77 == dev.principal.userId);
        }
        // the refusals are 401, whatever the combination
        for (const AuthResult &r : { resolveNow(a, std::nullopt, std::string("test-key-unknown-zzzzzzzz")),
                 resolveNow(a, "Bearer " + jwtFor("1001", now - 1), std::nullopt),
                 resolveNow(a, "Bearer " + jwt::signHs256(R"({"alg":"HS256"})", R"({"sub":"1001"})", JWT_SECRET),
                     std::nullopt),
                 resolveNow(a, "Bearer " + jwtFor("1001", now + 60), API_KEY), // two credentials
                 resolveNow(a, "Basic dXNlcjpwYXNz", std::nullopt), resolveNow(a, "Bearer ", std::nullopt),
                 resolveNow(a, std::nullopt, std::nullopt), resolveNow(a, "Bearer dev:0", std::nullopt) })
        {
            CHECK_MSG(AuthOutcome::UNAUTHORIZED == r.outcome, m + " " + r.detail);
        }
    }
}

TEST(auth_token_never_read_from_url)
{
    AuthenticatorSettings s;
    s.modes = auth_mode::API_KEY | auth_mode::JWT | auth_mode::DEV;
    Authenticator a(s, material(), nullptr, nullptr);
    const std::string token = jwtFor("1001", 4'000'000'000LL);
    for (const std::string &target : std::vector<std::string>{ "/api/v2/auth/me?token=" + token, "/api/v2/auth/me?token=dev:5",
             "/api/v2/auth/me?access_token=" + token })
    {
        const AuthResult r = resolveNow(a, std::nullopt, std::nullopt, target);
        CHECK_MSG(AuthOutcome::UNAUTHORIZED == r.outcome, "token accepted from URL: " + target);
    }
}

TEST(auth_demo_cache_stale_and_store_down)
{
    MemoryStore store;
    WorkPool pool("t-pg", 1, 4);
    pool.start();
    std::atomic<std::int64_t> clock{ 1'000'000 };
    AuthenticatorSettings s;
    s.modes = auth_mode::DEMO;
    s.tokenCacheTtlMs = 5000;
    s.tokenStaleMs = 600'000;
    Authenticator a(s, AuthMaterial{}, &store, &pool, [&] { return clock.load(); });
    const std::string token = newDemoToken();
    CHECK(43 == token.size() && token.find('.') == std::string::npos);
    const std::string hash(crypto::view(crypto::sha256(token)));
    store.users[100001] = StoredUser{ 100001, "alice", "pbkdf2$x", hash };

    AuthResult r = resolveNow(a, "Bearer " + token, std::nullopt);
    CHECK(AuthOutcome::OK == r.outcome && 100001 == r.principal.userId && r.principal.username == std::string("alice"));
    const int afterFirst = store.calls.load();
    clock += 4999; // inside the TTL: no store call
    CHECK(AuthOutcome::OK == resolveNow(a, "Bearer " + token, std::nullopt).outcome);
    CHECK(store.calls.load() == afterFirst);

    store.down = true;
    clock += 2; // past the TTL, store down: served stale (never 401, never 503 while inside the stale bound)
    r = resolveNow(a, "Bearer " + token, std::nullopt);
    CHECK_MSG(AuthOutcome::OK == r.outcome, r.detail);
    CHECK(1 == a.staleServed());
    clock += 600'000; // past the stale bound: 503 AUTH_UNAVAILABLE, not 401
    r = resolveNow(a, "Bearer " + token, std::nullopt);
    CHECK_MSG(AuthOutcome::UNAVAILABLE == r.outcome, r.detail);
    // an unknown token while the store is down: 503 too (the store cannot say it is unknown)
    CHECK(AuthOutcome::UNAVAILABLE == resolveNow(a, "Bearer " + newDemoToken(), std::nullopt).outcome);

    store.down = false;
    CHECK(AuthOutcome::OK == resolveNow(a, "Bearer " + token, std::nullopt).outcome);
    // the store forgets the token (logout through another gateway): 401 once the cache entry expires
    store.users[100001].tokenHash.reset();
    clock += 5001;
    CHECK(AuthOutcome::UNAUTHORIZED == resolveNow(a, "Bearer " + token, std::nullopt).outcome);
    // the users table is the only demo source: a demo token in a JWT- or dev-only gateway is 401
    AuthenticatorSettings jwtOnly;
    jwtOnly.modes = auth_mode::JWT;
    Authenticator b(jwtOnly, material(), &store, &pool);
    CHECK(AuthOutcome::UNAUTHORIZED == resolveNow(b, "Bearer " + token, std::nullopt).outcome);
    pool.stop(2000);
}

TEST(auth_demo_queue_full_is_busy)
{
    MemoryStore store;
    WorkPool pool("t-pg", 1, 1);
    // not started: the queue fills and stays full
    CHECK(pool.trySubmit([](int) {}));
    AuthenticatorSettings s;
    s.modes = auth_mode::DEMO;
    Authenticator a(s, AuthMaterial{}, &store, &pool);
    const AuthResult r = resolveNow(a, "Bearer " + newDemoToken(), std::nullopt);
    CHECK_MSG(AuthOutcome::BUSY == r.outcome, r.detail);
    // a remembered token is served from the cache without the store
    const std::string token = newDemoToken();
    a.remember(std::string(crypto::view(crypto::sha256(token))), 5, "bob");
    CHECK(AuthOutcome::OK == resolveNow(a, "Bearer " + token, std::nullopt).outcome);
    a.forget(std::string(crypto::view(crypto::sha256(token))));
    CHECK(AuthOutcome::BUSY == resolveNow(a, "Bearer " + token, std::nullopt).outcome);
}

TEST(auth_unknown_token_lookups_have_their_own_budget)
{
    MemoryStore store;
    store.tokenDelayMs = 300;
    WorkPool pool("t-pg", 1, 8);
    pool.start();
    std::atomic<std::int64_t> clock{ 1'000'000 };
    AuthenticatorSettings s;
    s.modes = auth_mode::DEMO;
    s.unknownLookupInflight = 1;
    Authenticator a(s, AuthMaterial{}, &store, &pool, [&] { return clock.load(); });

    // one unknown-token lookup waits on the store; a second one is over the share: BUSY, the store never asked
    Waiter first;
    CredentialHeaders h;
    const std::string auth1 = "Bearer " + newDemoToken();
    h.authorization = auth1;
    a.resolve(h, first.done());
    AuthResult r = resolveNow(a, "Bearer " + newDemoToken(), std::nullopt);
    CHECK_MSG(AuthOutcome::BUSY == r.outcome, r.detail);
    CHECK(1 == a.unknownLookupsRefused());
    CHECK(AuthOutcome::UNAUTHORIZED == first.get().outcome);
    CHECK(1 == store.calls.load()); // only the first lookup reached the store

    // the share is released when the lookup ends; a known token past its TTL is never charged
    const std::string token = newDemoToken();
    const std::string hash(crypto::view(crypto::sha256(token)));
    {
        std::lock_guard<std::mutex> l(store.mutex);
        store.users[100001] = StoredUser{ 100001, "alice", "pbkdf2$x", hash };
    }
    r = resolveNow(a, "Bearer " + token, std::nullopt);
    CHECK_MSG(AuthOutcome::OK == r.outcome, r.detail);
    clock += 10'000; // past the TTL
    std::atomic<int> charged{ 0 };
    CredentialHeaders known;
    const std::string authKnown = "Bearer " + token;
    known.authorization = authKnown;
    known.lookupBudget = [&] {
        charged++;
        return TakeResult{ false, 777 };
    };
    Waiter w;
    a.resolve(known, w.done());
    CHECK(AuthOutcome::OK == w.get().outcome && 0 == charged.load());

    // an unknown token with the caller's budget spent: RATE_LIMITED with the retry time, the store not asked
    CredentialHeaders unknown;
    const std::string authUnknown = "Bearer " + newDemoToken();
    unknown.authorization = authUnknown;
    unknown.lookupBudget = known.lookupBudget;
    const int calls = store.calls.load();
    Waiter u;
    a.resolve(unknown, u.done());
    r = u.get();
    CHECK_MSG(AuthOutcome::RATE_LIMITED == r.outcome && 777 == r.retryAfterMs, r.detail);
    CHECK(1 == charged.load() && store.calls.load() == calls);
    // the refused budget released the share: the next unknown token reaches the store
    CHECK(AuthOutcome::UNAUTHORIZED == resolveNow(a, "Bearer " + newDemoToken(), std::nullopt).outcome);
    pool.stop(2000);
}

TEST(auth_lookup_racing_a_logout_is_not_cached)
{
    MemoryStore store;
    store.tokenDelayMs = 200;
    WorkPool pool("t-pg", 1, 8);
    pool.start();
    std::atomic<std::int64_t> clock{ 1'000'000 };
    AuthenticatorSettings s;
    s.modes = auth_mode::DEMO;
    Authenticator a(s, AuthMaterial{}, &store, &pool, [&] { return clock.load(); });
    const std::string token = newDemoToken();
    const std::string hash(crypto::view(crypto::sha256(token)));
    store.users[100001] = StoredUser{ 100001, "alice", "pbkdf2$x", hash };
    // the lookup reads the store, then the logout here clears the row and forgets the token before the lookup's put
    Waiter w;
    CredentialHeaders h;
    const std::string authorization = "Bearer " + token;
    h.authorization = authorization;
    a.resolve(h, w.done());
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // the worker is inside findByToken (200 ms)
    {
        std::lock_guard<std::mutex> l(store.mutex);
        store.users[100001].tokenHash.reset();
    }
    a.forget(hash);
    CHECK(AuthOutcome::OK == w.get().outcome); // the request that read before the logout is answered
    // the logged-out token never comes back from the cache: with the store down it is 503, never served
    store.tokenDelayMs = 0;
    store.down = true;
    const AuthResult r = resolveNow(a, authorization, std::nullopt);
    CHECK_MSG(AuthOutcome::UNAVAILABLE == r.outcome, std::string("a logged-out token was served: ") + r.detail);
    pool.stop(2000);
}

TEST(pg_store_fails_fast_after_a_failed_connect)
{
    // a closed loopback port: every connect is refused at once
    PgSettings ps;
    ps.url = "postgresql://gw@127.0.0.1:1/gw";
    ps.workers = 1;
    ps.statementTimeoutMs = 1000;
    ps.retryMs = 60'000;
    PgUserStore slow(ps, {});
    StoredUser u;
    for (int i = 0; i < 5; ++i)
    {
        CHECK(StoreStatus::UNAVAILABLE == slow.findByToken(0, std::string(32, 'x'), u));
    }
    CHECK_MSG(1 == slow.connectAttempts(), std::to_string(slow.connectAttempts()) + " connect attempts in the window");
    CHECK(5 == slow.failures());
    ps.retryMs = 1;
    PgUserStore quick(ps, {});
    CHECK(StoreStatus::UNAVAILABLE == quick.findByToken(0, std::string(32, 'x'), u));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    CHECK(StoreStatus::UNAVAILABLE == quick.findByToken(0, std::string(32, 'x'), u));
    CHECK(2 == quick.connectAttempts());
}

// ---- the worker pool and the duty bridge

TEST(work_pool_bounded_and_named)
{
    WorkPool pool("t-hash", 2, 3);
    std::mutex m;
    std::condition_variable cv;
    bool release = false;
    std::set<std::string> names;
    pool.start();
    for (int i = 0; i < 2; ++i)
    {
        CHECK(pool.trySubmit([&](int) {
            char name[16];
            pthread_getname_np(pthread_self(), name, sizeof(name));
            std::unique_lock<std::mutex> l(m);
            names.insert(name);
            cv.wait(l, [&] { return release; });
        }));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK(pool.trySubmit([](int) {}) && pool.trySubmit([](int) {}) && pool.trySubmit([](int) {}));
    CHECK(!pool.trySubmit([](int) {})); // the queue holds 3: the fourth is refused at once
    CHECK(1 == pool.refused());
    {
        std::lock_guard<std::mutex> l(m);
        release = true;
    }
    cv.notify_all();
    CHECK(pool.stop(2000));
    CHECK(names == (std::set<std::string>{ "t-hash-0", "t-hash-1" }));
}

TEST(duty_bridge_ring_and_completions)
{
    DutyBridge bridge(4, 8, "tb");
    bridge.startCompletions();
    std::mutex m;
    std::condition_variable cv;
    std::vector<std::pair<std::uint64_t, std::string>> seen;
    for (int i = 0; i < 4; ++i)
    {
        wire::RequestUserBalances q;
        q.userId = 100 + i;
        CHECK(bridge.submit(session::Command{ q }, [&, i](const session::Outcome &o) {
            char name[16];
            pthread_getname_np(pthread_self(), name, sizeof(name));
            std::lock_guard<std::mutex> l(m);
            seen.emplace_back(static_cast<std::uint64_t>(i) * 1000 + o.ticket, name);
            cv.notify_all();
        }));
    }
    wire::RequestUserBalances extra;
    CHECK(!bridge.submit(session::Command{ extra }, [](const session::Outcome &) {})); // ring of 4 is full
    CHECK(1 == bridge.refused());
    // the duty side: drain in order, complete each
    std::vector<std::int64_t> users;
    const int n = bridge.drain(
        [&](DutyBridge::Request &&r) {
            users.push_back(std::get<wire::RequestUserBalances>(r.command).userId);
            session::Outcome o;
            o.ticket = 7;
            bridge.complete(r.id, std::move(o));
        },
        16);
    CHECK(4 == n);
    CHECK(users == (std::vector<std::int64_t>{ 100, 101, 102, 103 }));
    std::unique_lock<std::mutex> l(m);
    cv.wait_for(l, std::chrono::seconds(5), [&] { return seen.size() == 4; });
    CHECK(4 == seen.size());
    for (const auto &s : seen)
    {
        CHECK(s.second == "tb-done");
    }
    l.unlock();
    CHECK(bridge.stopCompletions(2000));
}

TEST(mpmc_ring_concurrent)
{
    MpmcRing<std::uint64_t> ring(1000); // any capacity
    constexpr int PRODUCERS = 4;
    constexpr std::uint64_t PER = 50'000;
    std::atomic<std::uint64_t> sum{ 0 };
    std::atomic<std::uint64_t> count{ 0 };
    std::vector<std::thread> threads;
    for (int p = 0; p < PRODUCERS; ++p)
    {
        threads.emplace_back([&, p] {
            for (std::uint64_t i = 1; i <= PER; ++i)
            {
                std::uint64_t v = static_cast<std::uint64_t>(p) * PER + i;
                while (!ring.tryPush(std::move(v)))
                {
                    std::this_thread::yield();
                }
            }
        });
    }
    std::thread consumer([&] {
        std::uint64_t v = 0;
        while (count.load() < PRODUCERS * PER)
        {
            if (ring.tryPop(v))
            {
                sum += v;
                count++;
            }
        }
    });
    for (auto &t : threads)
    {
        t.join();
    }
    consumer.join();
    const std::uint64_t n = PRODUCERS * PER;
    CHECK(count.load() == n);
    CHECK(sum.load() == n * (n + 1) / 2);
}
