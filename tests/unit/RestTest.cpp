// The pure pieces of the REST surface (T0): the money decimal, the Idempotency-Key and clientOrderId encodings, the
// derived register-funding id, the create shape rules, and the status table of every core reject reason.
#include <cstdio>
#include <random>
#include <set>
#include <tuple>

#include "Check.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/Rest.h"

using namespace openexchange::gateway;
namespace money = wire::money;

TEST(rest_money_decimal)
{
    struct Good
    {
        const char *text;
        std::int64_t units;
        const char *canonical;
    };
    for (const Good &g : std::initializer_list<Good>{ { "0", 0, "0" }, { "1", 100000000, "1" }, { "1.5", 150000000, "1.5" },
             { "0.00000001", 1, "0.00000001" }, { "12.34000000", 1234000000, "12.34" }, { "007", 700000000, "7" },
             { "92233720368.54775807", INT64_MAX, "92233720368.54775807" } })
    {
        const auto v = rest::parseMoney(g.text);
        CHECK_MSG(v && *v == g.units, g.text);
        CHECK_MSG(rest::formatMoney(g.units) == g.canonical, g.text);
    }
    for (const char *bad : { "", ".", "1.", ".5", "-1", "+1", "1e8", "1E-2", "0.000000001", "1,5", " 1", "1 ", "0x10",
             "92233720368.54775808", "99999999999999999999", "1.2.3", "NaN", "１" })
    {
        CHECK_MSG(!rest::parseMoney(bad), std::string("accepted '") + bad + "'");
    }
    CHECK(rest::formatMoney(-225000000) == "-2.25");
    CHECK(rest::formatMoney(INT64_MIN) == "-92233720368.54775808");
    // property: int64 -> string -> int64 is exact, and the string is the shortest form, for random non-negative values
    std::mt19937_64 rng(20261116);
    for (int i = 0; i < 200000; ++i)
    {
        std::int64_t v = static_cast<std::int64_t>(rng() >> (1 + rng() % 63));
        const std::string t = rest::formatMoney(v);
        const auto back = rest::parseMoney(t);
        if (!back || *back != v)
        {
            CHECK_MSG(false, "round trip of " + std::to_string(v) + " via " + t);
            break;
        }
        CHECK(t.back() != '0' || std::string::npos == t.find('.'));
    }
}

TEST(rest_idempotency_key)
{
    rest::RequestKey k = rest::parseIdempotencyKey(std::nullopt, 0);
    CHECK(rest::KeyStatus::MISSING == k.status);
    k = rest::parseIdempotencyKey(std::string_view("0123456789abcdef-FEDCBA9876543210"), 2);
    CHECK(rest::KeyStatus::INVALID == k.status); // given twice
    k = rest::parseIdempotencyKey(std::string_view("00112233-4455-6677-8899-AABBCCDDEEFF"), 1);
    CHECK(rest::KeyStatus::OK == k.status && 0x0011223344556677LL == k.high &&
        static_cast<std::int64_t>(0x8899AABBCCDDEEFFULL) == k.low); // UUID bits as is
    k = rest::parseIdempotencyKey(std::string_view("00000000-0000-0000-0000-000000000000"), 1);
    CHECK(rest::KeyStatus::NIL == k.status);
    k = rest::parseIdempotencyKey(std::string_view("order-7"), 1);
    const crypto::Digest d = crypto::sha256("order-7");
    std::int64_t high = 0;
    for (int i = 0; i < 8; ++i)
    {
        high = static_cast<std::int64_t>((static_cast<std::uint64_t>(high) << 8) | d[i]);
    }
    CHECK(rest::KeyStatus::OK == k.status && k.high == high); // the first 16 bytes of SHA-256
    for (const std::string &bad : { std::string(""), std::string(65, 'a'), std::string("with space"), std::string("tab\t"),
             std::string("caf\xc3\xa9") })
    {
        CHECK_MSG(rest::KeyStatus::INVALID == rest::parseIdempotencyKey(std::string_view(bad), 1).status, bad);
    }
    CHECK(rest::KeyStatus::OK == rest::parseIdempotencyKey(std::string_view(std::string(64, '~')), 1).status);
    // clientOrderId: a UUID, formatted back in lower case
    const auto u = rest::parseUuid("0FA1B2C3-D4E5-4F60-8172-93A4B5C6D7E8");
    CHECK(u && rest::formatUuid(u->first, u->second) == "0fa1b2c3-d4e5-4f60-8172-93a4b5c6d7e8");
    CHECK(!rest::parseUuid("0fa1b2c3d4e54f60817293a4b5c6d7e8"));
    CHECK(!rest::parseUuid("0fa1b2c3-d4e5-4f60-8172-93a4b5c6d7eg"));
}

TEST(rest_funding_request_id)
{
    std::int64_t h1 = 0, l1 = 0, h2 = 0, l2 = 0, h3 = 0, l3 = 0;
    rest::fundingRequestId(100001, 1, h1, l1);
    rest::fundingRequestId(100001, 1, h2, l2);
    rest::fundingRequestId(100001, 2, h3, l3);
    CHECK(h1 == h2 && l1 == l2);           // derived: the same user and asset give the same id
    CHECK(h1 != h3 || l1 != l3);           // another asset, another id
    CHECK(0 != h1 || 0 != l1);
    // the documented input: "oe-gateway/register-funding/v1" || userId (8 bytes BE) || assetId (4 bytes BE)
    std::string input = "oe-gateway/register-funding/v1";
    input += std::string("\x00\x00\x00\x00\x00\x01\x86\xa1", 8) + std::string("\x00\x00\x00\x01", 4);
    const crypto::Digest d = crypto::sha256(input);
    std::uint64_t high = 0;
    for (int i = 0; i < 8; ++i)
    {
        high = (high << 8) | d[i];
    }
    CHECK(static_cast<std::int64_t>(high) == h1);
}

TEST(rest_create_shape)
{
    wire::CreateOrder c;
    c.orderType = money::OrderType::LIMIT;
    c.timeInForce = money::TimeInForce::GTC;
    c.price = 100;
    c.quantity = 5;
    CHECK(rest::createShapeProblem(c).empty());
    auto problem = [](wire::CreateOrder o) { return rest::createShapeProblem(o); };
    wire::CreateOrder x = c;
    x.quantity = 0;
    CHECK(problem(x).find("quantity") != std::string::npos);
    x = c;
    x.price = 0;
    CHECK(problem(x).find("price") != std::string::npos);
    x = c;
    x.budget = 7;
    CHECK(problem(x).find("budget") != std::string::npos);
    x = c;
    x.orderType = money::OrderType::MARKET;
    CHECK(problem(x).find("price") != std::string::npos); // a MARKET order carries no price
    x.price = 0;
    x.budget = 9;
    CHECK(problem(x).empty());
    x = c;
    x.orderType = money::OrderType::STOP_LIMIT;
    CHECK(problem(x).find("stopPrice") != std::string::npos);
    x.stopPrice = 90;
    CHECK(problem(x).empty());
    x = c;
    x.orderType = money::OrderType::TRAILING_STOP;
    x.price = 0;
    CHECK(problem(x).find("trailingDelta") != std::string::npos);
    x.trailingDelta = 3;
    CHECK(problem(x).empty());
    x = c;
    x.orderType = money::OrderType::ICEBERG;
    x.displayQty = 5;
    CHECK(problem(x).find("displayQuantity") != std::string::npos); // must be below quantity
    x.displayQty = 2;
    CHECK(problem(x).empty());
    x = c;
    x.timeInForce = money::TimeInForce::GTD;
    CHECK(problem(x).find("expiresAt") != std::string::npos);
    x.expiresAtMs = 1;
    CHECK(problem(x).empty());
    x = c;
    x.expiresAtMs = 1;
    CHECK(problem(x).find("expiresAt") != std::string::npos);
}

// The status table, row by row: every CoreRejectReason and RejectReason the schema has, its status, code and keyFinal.
TEST(rest_status_table)
{
    using R = money::CoreRejectReason;
    struct Row
    {
        R::Value reason;
        unsigned status;
        const char *code;
        bool keyFinal;
        bool retryAfter;
    };
    const std::initializer_list<Row> rows = {
        { R::MISSING_REQUEST_ID, 400, "VALIDATION", false, false },
        { R::UNSUPPORTED_FLAGS, 400, "VALIDATION", false, false },
        { R::UNSUPPORTED_TIF, 400, "VALIDATION", false, false },
        { R::UNAUTHORIZED_CLASS, 403, "FORBIDDEN", false, false },
        { R::UNKNOWN_ORDER, 404, "NOT_FOUND", false, false },
        { R::NOT_OWNER, 404, "NOT_FOUND", false, false },
        { R::UNKNOWN_MARKET, 404, "NOT_FOUND", false, false },
        { R::REQUEST_ID_PAYLOAD_MISMATCH, 409, "CONFLICT", false, false },
        { R::DUPLICATE_CLIENT_ORDER_ID, 409, "CONFLICT", false, false },
        { R::NOT_OPEN, 409, "CONFLICT", false, false },
        { R::AMEND_PENDING, 409, "CONFLICT", false, false },
        { R::EXPIRED, 409, "CONFLICT", false, false },
        { R::PRICE_OUT_OF_BAND, 422, "REJECTED", false, false },
        { R::PRICE_OFF_TICK, 422, "REJECTED", false, false },
        { R::QTY_OFF_LOT, 422, "REJECTED", false, false },
        { R::INSUFFICIENT_FUNDS, 422, "REJECTED", true, false },
        { R::SIZE_TOO_SMALL, 422, "REJECTED", true, false },
        { R::SIZE_TOO_LARGE, 422, "REJECTED", true, false },
        { R::NOTIONAL_TOO_SMALL, 422, "REJECTED", true, false },
        { R::NOTIONAL_TOO_LARGE, 422, "REJECTED", true, false },
        { R::PRICE_COLLAR, 422, "REJECTED", true, false },
        { R::OPEN_ORDER_LIMIT, 422, "REJECTED", true, false },
        { R::POSITION_LIMIT, 422, "REJECTED", true, false },
        { R::NO_REFERENCE_PRICE, 422, "REJECTED", true, false },
        { R::STOP_WOULD_TRIGGER_IMMEDIATELY, 422, "REJECTED", true, false },
        { R::INVALID, 422, "REJECTED", true, false }, // on a create or an amend: the late, stored INVALID
        { R::RATE_LIMIT, 429, "RATE_LIMITED", true, true },
        { R::TOO_MANY_PENDING_REQUESTS, 429, "RATE_LIMITED", false, true },
        { R::GATE_CLOSED, 503, "ADMISSION_CLOSED", false, true },
        { R::CIRCUIT_BREAKER, 503, "ADMISSION_CLOSED", true, true },
    };
    std::set<int> covered;
    for (const Row &r : rows)
    {
        for (const std::uint16_t t : { wire::tid::CREATE_ORDER, wire::tid::AMEND_ORDER })
        {
            const rest::Mapping m = rest::mapOrderReject(r.reason, t);
            CHECK_MSG(m.status == r.status && std::string(m.code) == r.code && !m.defect &&
                    (m.retryAfterMs >= 0) == r.retryAfter, R::c_str(r.reason));
            if (m.keyFinal != r.keyFinal)
            {
                CHECK_MSG(false, std::string("keyFinal wrong for ") + R::c_str(r.reason));
            }
        }
        covered.insert(r.reason);
    }
    CHECK(rest::mapOrderReject(R::UNAUTHORIZED_CLASS, wire::tid::CREATE_ORDER).critical);
    // INVALID on a query or a cancel is a gateway defect (the gateway sent a shape the core refused)
    CHECK(rest::mapOrderReject(R::INVALID, wire::tid::REQUEST_ORDER_SNAPSHOT).defect);
    CHECK(rest::mapOrderReject(R::INVALID, wire::tid::CANCEL_ORDER).defect);
    // every other reason cannot reach a gateway as a synchronous answer: 500
    for (int v = 0; v < 255; ++v)
    {
        if (covered.count(v) > 0)
        {
            continue;
        }
        const char *name = nullptr;
        try
        {
            name = R::c_str(static_cast<R::Value>(v));
        }
        catch (...)
        {
            continue; // not a value of the schema
        }
        const rest::Mapping m = rest::mapOrderReject(static_cast<R::Value>(v), wire::tid::CREATE_ORDER);
        CHECK_MSG(m.defect && 500 == m.status, name);
    }

    using M = money::RejectReason;
    for (const auto &[reason, status, code] : std::initializer_list<std::tuple<M::Value, unsigned, const char *>>{
             { M::INSUFFICIENT_FUNDS, 422, "REJECTED" }, { M::INVALID_AMOUNT, 422, "REJECTED" },
             { M::BALANCE_OVERFLOW, 422, "REJECTED" }, { M::REQUEST_ID_MISMATCH, 409, "CONFLICT" } })
    {
        const rest::Mapping m = rest::mapMoneyReject(reason);
        CHECK_MSG(m.status == status && std::string(m.code) == code && !m.defect, M::c_str(reason));
        if (m.keyFinal)
        {
            CHECK_MSG(false, std::string("keyFinal wrong for ") + M::c_str(reason));
        }
    }
    CHECK(rest::mapMoneyReject(M::UNKNOWN_HOLD).defect);
}
