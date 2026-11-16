// The pure pieces of the history read layer (T0): the cursor (round trip, route and filter binding, every malformed
// form), the statements (fixed text, every value a parameter, the keyset predicate and order), and the JSON of a row.
#include <string>

#include "Check.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/History.h"

using namespace openexchange::gateway;
using namespace openexchange::gateway::history;

namespace {

Filter filterOf(std::int32_t marketId, std::string status = "", std::int64_t orderId = 0)
{
    Filter f;
    f.marketId = marketId;
    f.status = std::move(status);
    f.orderId = orderId;
    return f;
}

Row orderRow()
{
    return { "900", std::nullopt, "1", "BUY", "MARKET", "IOC", std::nullopt, std::nullopt, "150000000", "150000000", "0",
        "FILLED", std::nullopt, std::nullopt, "55", "2", "10000000000", "0", "1800000000000", "1800000000500" };
}

}

TEST(history_cursor_round_trip_and_binding)
{
    const Filter f = filterOf(3, "FILLED");
    const Key k{ 1'800'000'000'123, 42, 0 };
    const std::string c = encodeCursor(Kind::ORDERS, f, k);
    Key out;
    CHECK(CursorError::NONE == decodeCursor(c, Kind::ORDERS, f, out));
    CHECK(out.a == k.a && out.b == k.b);
    const std::string fc = encodeCursor(Kind::FILLS, filterOf(0, "", 7), Key{ 5, 6, 7 });
    CHECK(CursorError::NONE == decodeCursor(fc, Kind::FILLS, filterOf(0, "", 7), out));
    CHECK(out.a == 5 && out.b == 6 && out.c == 7);
    // bound to the route and to every filter field
    CHECK(CursorError::OTHER_ROUTE == decodeCursor(c, Kind::FILLS, f, out));
    CHECK(CursorError::OTHER_ROUTE == decodeCursor(fc, Kind::ORDERS, filterOf(0, "", 7), out));
    CHECK(CursorError::OTHER_FILTER == decodeCursor(c, Kind::ORDERS, filterOf(4, "FILLED"), out));
    CHECK(CursorError::OTHER_FILTER == decodeCursor(c, Kind::ORDERS, filterOf(3, "CANCELLED"), out));
    CHECK(CursorError::OTHER_FILTER == decodeCursor(c, Kind::ORDERS, filterOf(3), out));
    CHECK(CursorError::OTHER_FILTER == decodeCursor(fc, Kind::FILLS, filterOf(0, "", 8), out));
    // opaque: url-safe characters only
    for (const char ch : c + fc)
    {
        CHECK((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || '-' == ch || '_' == ch);
    }
}

TEST(history_cursor_malformed)
{
    const Filter f;
    Key out;
    auto enc = [](const std::string &raw) { return crypto::base64UrlEncode(raw); };
    const std::string binding = "0000000000000000"; // the length of a binding, not its value
    const std::vector<std::string> bad = {
        "",
        "!!!",
        enc("o1"),
        enc("x1|" + binding + "|1|2"),
        enc("o1|" + binding + "|1"),
        enc("o1|" + binding + "|1|2|3"),
        enc("o1|short|1|2"),
        enc("o1|" + binding + "|a|2"),
        enc("o1|" + binding + "|1|0"),           // order id 0
        enc("o1|" + binding + "|-1|2"),          // a negative time
        enc("o1|" + binding + "|+1|2"),
        enc("o1|" + binding + "|1|99999999999999999999"), // above int64
        enc("f1|" + binding + "|1|2"),
        enc("f1|" + binding + "|1|2|0"),
        enc("o1|" + binding + "|1|2") + "=",     // padding is refused
        std::string(300, 'A'),
    };
    for (const std::string &c : bad)
    {
        CHECK_MSG(CursorError::MALFORMED == decodeCursor(c, Kind::ORDERS, f, out) ||
                (0 == c.rfind(enc("f1"), 0) && CursorError::MALFORMED == decodeCursor(c, Kind::FILLS, f, out)),
            "accepted '" + c + "'");
    }
    CHECK(CursorError::MALFORMED == decodeCursor(enc("f1|" + binding + "|1|2|0"), Kind::FILLS, f, out));
}

TEST(history_statements_carry_values_as_parameters_only)
{
    PageRequest r;
    r.kind = Kind::ORDERS;
    r.userId = 987654321;
    r.filter = filterOf(424242, "PENDING_REPLACE");
    r.after = Key{ 1'777'777'777'777, 13579, 0 };
    r.limit = 31;
    Statement s = pageStatement(r);
    for (const std::string v : { "987654321", "424242", "PENDING_REPLACE", "1777777777777", "13579", "32" })
    {
        CHECK_MSG(s.sql.find(v) == std::string::npos, "value '" + v + "' inside the statement text: " + s.sql);
        bool carried = false;
        for (const std::string &p : s.params)
        {
            carried = carried || p == v;
        }
        CHECK_MSG(carried, "value '" + v + "' is not a parameter");
    }
    CHECK(s.params.size() == 6);
    CHECK_MSG(s.sql.find("FROM gateway_order_history_v1 WHERE user_id = $1::bigint") != std::string::npos, s.sql);
    CHECK_MSG(s.sql.find("(updated_at_ms, order_id) < ($4::bigint, $5::bigint)") != std::string::npos, s.sql);
    CHECK_MSG(s.sql.find("ORDER BY updated_at_ms DESC, order_id DESC LIMIT $6::bigint") != std::string::npos, s.sql);
    CHECK_MSG(s.sql.find("OFFSET") == std::string::npos, s.sql);
    CHECK(s.params[5] == "32"); // limit + 1: one row more tells whether a next page exists

    PageRequest fr;
    fr.kind = Kind::FILLS;
    fr.userId = 7;
    fr.filter = filterOf(0, "", 555);
    fr.after = Key{ 10, 20, 30 };
    fr.limit = 2;
    s = pageStatement(fr);
    CHECK_MSG(s.sql.find("FROM gateway_fills_v1 WHERE user_id = $1::bigint AND order_id = $2::bigint AND "
        "(executed_at_ms, trade_id, order_id) < ($3::bigint, $4::bigint, $5::bigint) ORDER BY executed_at_ms DESC, "
        "trade_id DESC, order_id DESC LIMIT $6::bigint") != std::string::npos, s.sql);
    CHECK(s.params == std::vector<std::string>({ "7", "555", "10", "20", "30", "3" }));

    // no filter, no cursor: user and limit only
    PageRequest first;
    first.userId = 7;
    first.limit = 100;
    s = pageStatement(first);
    CHECK(s.params == std::vector<std::string>({ "7", "101" }));

    s = orderStatement(31337, 271828);
    CHECK(s.sql.find("31337") == std::string::npos && s.sql.find("271828") == std::string::npos);
    CHECK(s.params == std::vector<std::string>({ "31337", "271828" }));
    CHECK_MSG(s.sql.find("WHERE user_id = $1::bigint AND order_id = $2::bigint") != std::string::npos, s.sql);
}

TEST(history_next_cursor_is_the_last_key)
{
    PageRequest r;
    r.kind = Kind::FILLS;
    r.filter = filterOf(2);
    const std::string c = nextCursor(r, Key{ 100, 200, 300 }, 5);
    Key out;
    CHECK(CursorError::NONE == decodeCursor(c, Kind::FILLS, r.filter, out));
    CHECK(100 == out.a && 200 == out.b && 300 == out.c);
}

TEST(history_status_names)
{
    for (const char *s : { "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "EXPIRED", "PENDING_NEW",
             "PENDING_TRIGGER", "PENDING_CANCEL", "PENDING_REPLACE" })
    {
        CHECK(knownStatus(s));
    }
    CHECK(!knownStatus("filled") && !knownStatus("OPEN") && !knownStatus(""));
}

TEST(history_row_json)
{
    std::string json;
    std::string problem;
    Key key;
    CHECK_MSG(orderJson(orderRow(), json, key, problem), problem);
    CHECK_MSG(json == R"({"orderId":"900","clientOrderId":null,"marketId":"1","side":"BUY","type":"MARKET",)"
        R"("timeInForce":"IOC","price":null,"stopPrice":null,"quantity":"1.5","filledQuantity":"1.5",)"
        R"("remainingQuantity":"0","status":"FILLED","reason":null,"meReason":null,"meOrderId":"55",)"
        R"("hold":{"assetId":"2","amount":"100","remaining":"0"},"createdAt":1800000000000,"updatedAt":1800000000500})",
        json);
    CHECK(1'800'000'000'500 == key.a && 900 == key.b);
    Row r = orderRow();
    r[1] = "11111111-2222-4333-8444-555555555555";
    r[6] = "10000000000";
    r[12] = "INSUFFICIENT_FUNDS";
    r[13] = "7";
    r[17] = std::nullopt; // no hold.remaining: the field is left out
    CHECK(orderJson(r, json, key, problem));
    CHECK_MSG(json.find(R"("clientOrderId":"11111111-2222-4333-8444-555555555555")") != std::string::npos &&
            json.find(R"("price":"100")") != std::string::npos &&
            json.find(R"("reason":"INSUFFICIENT_FUNDS","meReason":7)") != std::string::npos &&
            json.find(R"("hold":{"assetId":"2","amount":"100"})") != std::string::npos, json);
    // a value cannot break out of its JSON string
    r[3] = "BUY\",\"x\":\"1";
    CHECK(orderJson(r, json, key, problem));
    CHECK_MSG(json.find(R"("side":"BUY\",\"x\":\"1")") != std::string::npos, json);
    // the contract's shape is checked
    r = orderRow();
    r[8] = std::nullopt;
    // the problem names the row's key as stored, so an operator can find the row
    CHECK_MSG(!orderJson(r, json, key, problem) && problem == "column quantity is NULL (row order_id 900)", problem);
    problem.clear();
    r = orderRow();
    r[19] = "12.5";
    CHECK_MSG(!orderJson(r, json, key, problem) && problem == "column updated_at_ms is not an integer (row order_id 900)",
        problem);
    problem.clear();
    r = orderRow();
    r.pop_back();
    CHECK_MSG(!orderJson(r, json, key, problem) && problem == "column updated_at_ms is missing (row order_id 900)", problem);
    problem.clear();
    r = orderRow();
    r[0] = std::nullopt;
    CHECK_MSG(!orderJson(r, json, key, problem) && problem == "column order_id is NULL (row order_id NULL)", problem);
    problem.clear();

    const Row fill = { "77", "900", "1", "SELL", "10000000000", "50000000", "t", "1800000000600" };
    CHECK_MSG(fillJson(fill, json, key, problem), problem);
    CHECK_MSG(json == R"({"tradeId":"77","orderId":"900","marketId":"1","side":"SELL","price":"100","quantity":"0.5",)"
        R"("maker":true,"executedAt":1800000000600})", json);
    CHECK(1'800'000'000'600 == key.a && 77 == key.b && 900 == key.c);
    Row bad = fill;
    bad[6] = "maybe";
    CHECK_MSG(!fillJson(bad, json, key, problem) &&
            problem == "column maker is not a boolean (row trade_id 77, order_id 900)", problem);
}
