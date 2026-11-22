#include "gateway/common/History.h"

#include <array>
#include <charconv>
#include <cstdint>

#include "gateway/common/Api.h"
#include "gateway/common/Crypto.h"
#include "gateway/common/Rest.h"

namespace openexchange { namespace gateway { namespace history {

namespace {

// A shape check, not a capacity limit: the longest cursor this code writes is base64url of "f1|" + 16 hex + 3 x ("|" +
// an int64 of up to 20 characters), 82 bytes, 110 characters; anything above 256 is not a cursor of ours.
constexpr std::size_t MAX_CURSOR_CHARS = 256;

const char *ORDER_COLUMNS = "order_id, client_order_id::text, market_id, side, type, time_in_force, price, stop_price, "
    "quantity, filled_quantity, remaining_quantity, status, reason, me_reason, me_order_id, hold_asset_id, hold_amount, "
    "hold_remaining, created_at_ms, updated_at_ms";
const char *FILL_COLUMNS = "trade_id, order_id, market_id, side, price, quantity, maker, executed_at_ms";
const char *TRAIL_COLUMNS = "order_id, event_seq, event, status, core_log_position, me_epoch, me_log_position, "
    "me_journal_seq, liquidity, trade_id, last_price, last_qty, hold_remaining, at_ms, me_timestamp_ms";
const char *MOVEMENT_COLUMNS = "journal_seq, leg_no, log_position, kind, asset_id, delta, balance_after, at_ms, "
    "trade_id, market_id";

const std::array<const char *, 10> STATUSES = { "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED", "EXPIRED",
    "PENDING_NEW", "PENDING_TRIGGER", "PENDING_CANCEL", "PENDING_REPLACE" };

const char *tagOf(Kind kind)
{
    return Kind::ORDERS == kind ? "o1" : Kind::FILLS == kind ? "f1" : Kind::TRAIL == kind ? "t1" : "m1";
}

std::string param(std::vector<std::string> &params, std::string value);

// The owner filter of the trail statements: the principal's user id, in the statement itself.
std::string trailOwner(std::vector<std::string> &params, std::int64_t userId)
{
#ifdef GW_NC_TRAIL_NO_OWNER
    (void)params;
    (void)userId;
    return "TRUE"; // test build only: no owner filter
#else
    return "user_id = " + param(params, std::to_string(userId)) + "::bigint";
#endif
}

std::string binding(Kind kind, const Filter &f)
{
    std::string text = std::string(tagOf(kind)) + "|" + std::to_string(f.marketId) + "|" + f.status + "|" +
        std::to_string(f.orderId);
    if (Kind::MOVEMENTS == kind)
    {
        text += "|" + std::to_string(f.assetId); // the other kinds keep the binding they always had
    }
    const crypto::Digest d = crypto::sha256(text);
    return crypto::hex(crypto::view(d).substr(0, 8));
}

// A decimal integer, the whole text (an optional '-' only where `negative` allows it).
bool integer(std::string_view t, std::int64_t &v, bool negative = false)
{
    if (t.empty() || (!negative && '-' == t[0]) || '+' == t[0])
    {
        return false;
    }
    const auto [end, ec] = std::from_chars(t.data(), t.data() + t.size(), v);
    return std::errc() == ec && end == t.data() + t.size();
}

std::vector<std::string_view> split(std::string_view s, char sep)
{
    std::vector<std::string_view> out;
    std::size_t from = 0;
    for (;;)
    {
        const std::size_t at = s.find(sep, from);
        out.push_back(s.substr(from, std::string_view::npos == at ? std::string_view::npos : at - from));
        if (std::string_view::npos == at)
        {
            return out;
        }
        from = at + 1;
    }
}

std::string param(std::vector<std::string> &params, std::string value)
{
    params.push_back(std::move(value));
    return "$" + std::to_string(params.size());
}

// Row readers: each records the first problem.
struct Reader
{
    const Row &row;
    std::string &problem;

    const std::optional<std::string> *cell(std::size_t i, const char *column)
    {
        if (i >= row.size())
        {
            fail(column, "is missing");
            return nullptr;
        }
        return &row[i];
    }
    void fail(const char *column, const char *what)
    {
        if (problem.empty())
        {
            problem = std::string("column ") + column + " " + what;
        }
    }
    std::int64_t number(std::size_t i, const char *column)
    {
        const auto *c = cell(i, column);
        std::int64_t v = 0;
        if (nullptr == c || !c->has_value())
        {
            nullptr == c ? void() : fail(column, "is NULL");
            return 0;
        }
        if (!integer(**c, v, true))
        {
            fail(column, "is not an integer");
        }
        return v;
    }
    std::optional<std::int64_t> optNumber(std::size_t i, const char *column)
    {
        const auto *c = cell(i, column);
        if (nullptr == c || !c->has_value())
        {
            return std::nullopt;
        }
        return number(i, column);
    }
    std::string text(std::size_t i, const char *column)
    {
        const auto *c = cell(i, column);
        if (nullptr == c || !c->has_value())
        {
            nullptr == c ? void() : fail(column, "is NULL");
            return std::string();
        }
        return **c;
    }
    std::optional<std::string> optText(std::size_t i, const char *column)
    {
        const auto *c = cell(i, column);
        return nullptr == c ? std::nullopt : *c;
    }
    // a cell as stored, for a problem line (NULL and a missing cell named as such)
    std::string raw(std::size_t i) const
    {
        return i >= row.size() ? std::string("(missing)") : row[i] ? *row[i] : std::string("NULL");
    }
};

std::string id(std::int64_t v)
{
    return api::quote(std::to_string(v));
}

std::string money(std::int64_t v)
{
    return api::quote(rest::formatMoney(v));
}

std::string optMoney(const std::optional<std::int64_t> &v)
{
    return v ? money(*v) : std::string("null");
}

}

bool knownStatus(std::string_view name)
{
    for (const char *s : STATUSES)
    {
        if (name == s)
        {
            return true;
        }
    }
    return false;
}

// Each placeholder is numbered by its own statement (never two param() calls in one expression: the operands of +
// are unsequenced, and the numbers would follow the compiler's order instead of the text's).
namespace {

// A trail page: the caller's records of one order, oldest first, by event_seq and by nothing else.
Statement trailStatement(const PageRequest &r)
{
    Statement s;
    s.sql = std::string("SELECT ") + TRAIL_COLUMNS + " FROM gateway_order_trail_v1 WHERE ";
    s.sql += trailOwner(s.params, r.userId);
    s.sql += " AND order_id = ";
    s.sql += param(s.params, std::to_string(r.filter.orderId)) + "::bigint";
    if (r.after)
    {
        s.sql += " AND event_seq > ";
        s.sql += param(s.params, std::to_string(r.after->a)) + "::bigint";
    }
    s.sql += " ORDER BY event_seq LIMIT ";
    const std::int64_t fetch = r.limit < INT64_MAX ? r.limit + 1 : r.limit;
    s.sql += param(s.params, std::to_string(fetch)) + "::bigint";
    return s;
}

}

Statement trailExistsStatement(std::int64_t userId, std::int64_t orderId)
{
    Statement s;
    s.sql = "SELECT 1 FROM gateway_order_trail_v1 WHERE ";
    s.sql += trailOwner(s.params, userId);
    s.sql += " AND order_id = ";
    s.sql += param(s.params, std::to_string(orderId)) + "::bigint";
    s.sql += " LIMIT 1";
    return s;
}

namespace {

// A movements page: the caller's legs, newest first by (journal_seq, leg_no), optionally of one asset.
Statement movementsStatement(const PageRequest &r)
{
    Statement s;
    s.sql = std::string("SELECT ") + MOVEMENT_COLUMNS + " FROM gateway_balance_movements_v1 WHERE ";
#ifdef GW_NC_MOVEMENTS_NO_OWNER
    s.sql += "TRUE"; // test build only: no owner filter
#else
    s.sql += "user_id = ";
    s.sql += param(s.params, std::to_string(r.userId)) + "::bigint";
#endif
    if (r.filter.assetId >= 0)
    {
        s.sql += " AND asset_id = ";
        s.sql += param(s.params, std::to_string(r.filter.assetId)) + "::integer";
    }
    if (r.after)
    {
        s.sql += " AND (journal_seq, leg_no) < (";
        s.sql += param(s.params, std::to_string(r.after->a)) + "::bigint, ";
        s.sql += param(s.params, std::to_string(r.after->b)) + "::integer)";
    }
    s.sql += " ORDER BY journal_seq DESC, leg_no DESC LIMIT ";
    s.sql += param(s.params, std::to_string(r.limit < INT64_MAX ? r.limit + 1 : r.limit)) + "::bigint";
    return s;
}

}

Statement pageStatement(const PageRequest &r)
{
    if (Kind::TRAIL == r.kind)
    {
        return trailStatement(r);
    }
    if (Kind::MOVEMENTS == r.kind)
    {
        return movementsStatement(r);
    }
    Statement s;
    const bool orders = Kind::ORDERS == r.kind;
    s.sql = std::string("SELECT ") + (orders ? ORDER_COLUMNS : FILL_COLUMNS) + " FROM " +
        (orders ? "gateway_order_history_v1" : "gateway_fills_v1") + " WHERE user_id = ";
    s.sql += param(s.params, std::to_string(r.userId)) + "::bigint";
    if (0 != r.filter.marketId)
    {
        s.sql += " AND market_id = ";
        s.sql += param(s.params, std::to_string(r.filter.marketId)) + "::integer";
    }
    if (orders && !r.filter.status.empty())
    {
        s.sql += " AND status = ";
        s.sql += param(s.params, r.filter.status) + "::text";
    }
    if (!orders && 0 != r.filter.orderId)
    {
        s.sql += " AND order_id = ";
        s.sql += param(s.params, std::to_string(r.filter.orderId)) + "::bigint";
    }
#ifndef GW_NC_OFFSET_PAGING
    if (r.after)
    {
        s.sql += orders ? " AND (updated_at_ms, order_id) < (" : " AND (executed_at_ms, trade_id, order_id) < (";
        s.sql += param(s.params, std::to_string(r.after->a)) + "::bigint, ";
        s.sql += param(s.params, std::to_string(r.after->b)) + "::bigint";
        if (!orders)
        {
            s.sql += ", ";
            s.sql += param(s.params, std::to_string(r.after->c)) + "::bigint";
        }
        s.sql += ")";
    }
#endif
    s.sql += orders ? " ORDER BY updated_at_ms DESC, order_id DESC" : " ORDER BY executed_at_ms DESC, trade_id DESC, order_id DESC";
    // one row more than the page tells whether another page exists
    const std::int64_t fetch = r.limit < INT64_MAX ? r.limit + 1 : r.limit;
    s.sql += " LIMIT ";
    s.sql += param(s.params, std::to_string(fetch)) + "::bigint";
#ifdef GW_NC_OFFSET_PAGING
    if (r.after)
    {
        s.sql += " OFFSET "; // control: rows served so far
        s.sql += param(s.params, std::to_string(r.after->a)) + "::bigint";
    }
#endif
    return s;
}

Statement orderStatement(std::int64_t userId, std::int64_t orderId)
{
    Statement s;
    s.sql = std::string("SELECT ") + ORDER_COLUMNS + " FROM gateway_order_history_v1 WHERE user_id = ";
    s.sql += param(s.params, std::to_string(userId)) + "::bigint AND order_id = ";
    s.sql += param(s.params, std::to_string(orderId)) + "::bigint";
    return s;
}

const std::string &heartbeatStatement()
{
    // NULL when there is no row: GREATEST ignores NULL, so the empty case is tested before it (else it reads as age 0).
    // applied_order_id and applied_event_seq: the lowest of the rows (one row in the reference DDL; with more, the
    // conservative one, so an order is never answered 404 before every reporter has applied it); two epochs among the
    // rows match no genesis
    static const std::string sql = "SELECT CASE WHEN max(beat_at) IS NULL THEN NULL ELSE GREATEST(0, floor(extract("
        "epoch FROM (clock_timestamp() - max(beat_at))) * 1000))::bigint END, min(applied_order_id), "
        "min(applied_event_seq), min(epoch), max(epoch) FROM gateway_history_heartbeat_v1";
    return sql;
}

std::string encodeCursor(Kind kind, const Filter &filter, const Key &key)
{
    std::string text = std::string(tagOf(kind)) + "|" + binding(kind, filter) + "|" + std::to_string(key.a) + "|" +
        std::to_string(key.b);
    if (Kind::FILLS == kind)
    {
        text += "|" + std::to_string(key.c);
    }
    return crypto::base64UrlEncode(text);
}

CursorError decodeCursor(std::string_view text, Kind kind, const Filter &filter, Key &out)
{
    if (text.empty() || text.size() > MAX_CURSOR_CHARS)
    {
        return CursorError::MALFORMED;
    }
    const auto raw = crypto::base64UrlDecode(text);
    if (!raw)
    {
        return CursorError::MALFORMED;
    }
    const std::vector<std::string_view> parts = split(*raw, '|');
    const std::string_view tag = parts.front();
    if (tag != "o1" && tag != "f1" && tag != "t1" && tag != "m1")
    {
        return CursorError::MALFORMED;
    }
    const Kind of = "o1" == tag ? Kind::ORDERS : "f1" == tag ? Kind::FILLS : "t1" == tag ? Kind::TRAIL : Kind::MOVEMENTS;
    if (parts.size() != (Kind::FILLS == of ? 5u : 4u) || 16 != parts[1].size())
    {
        return CursorError::MALFORMED;
    }
    Key k;
    // a movement's leg_no starts at 0; every other key's second part is an id (1 or above)
    if (!integer(parts[2], k.a) || !integer(parts[3], k.b) || k.b < (Kind::MOVEMENTS == of ? 0 : 1) ||
        (Kind::FILLS == of && (!integer(parts[4], k.c) || k.c < 1)))
    {
        return CursorError::MALFORMED;
    }
    if (of != kind)
    {
        return CursorError::OTHER_ROUTE;
    }
    if (parts[1] != binding(kind, filter))
    {
        return CursorError::OTHER_FILTER;
    }
    out = k;
    return CursorError::NONE;
}

std::string nextCursor(const PageRequest &request, const Key &last, std::size_t served)
{
#ifdef GW_NC_OFFSET_PAGING
    (void)last;
    const std::int64_t offset = (request.after ? request.after->a : 0) + static_cast<std::int64_t>(served);
    return encodeCursor(request.kind, request.filter, Key{ offset, 1, 1 });
#else
    (void)served;
    return encodeCursor(request.kind, request.filter, last);
#endif
}

bool orderJson(const Row &row, std::string &json, Key &key, std::string &problem)
{
    Reader r{ row, problem };
    const std::int64_t orderId = r.number(0, "order_id");
    const auto clientOrderId = r.optText(1, "client_order_id");
    const std::int64_t marketId = r.number(2, "market_id");
    const std::string side = r.text(3, "side");
    const std::string type = r.text(4, "type");
    const std::string tif = r.text(5, "time_in_force");
    const auto price = r.optNumber(6, "price");
    const auto stopPrice = r.optNumber(7, "stop_price");
    const std::int64_t quantity = r.number(8, "quantity");
    const std::int64_t filled = r.number(9, "filled_quantity");
    const std::int64_t remaining = r.number(10, "remaining_quantity");
    const std::string status = r.text(11, "status");
    const auto reason = r.optText(12, "reason");
    const auto meReason = r.optNumber(13, "me_reason");
    const auto meOrderId = r.optNumber(14, "me_order_id");
    const std::int64_t holdAsset = r.number(15, "hold_asset_id");
    const std::int64_t holdAmount = r.number(16, "hold_amount");
    const auto holdRemaining = r.optNumber(17, "hold_remaining");
    const std::int64_t createdAt = r.number(18, "created_at_ms");
    const std::int64_t updatedAt = r.number(19, "updated_at_ms");
    if (!problem.empty())
    {
        problem += " (row order_id " + r.raw(0) + ")";
        return false;
    }
    json = "{\"orderId\":" + id(orderId);
    json += ",\"clientOrderId\":" + (clientOrderId ? api::quote(*clientOrderId) : std::string("null"));
    json += ",\"marketId\":" + id(marketId);
    json += ",\"side\":" + api::quote(side);
    json += ",\"type\":" + api::quote(type);
    json += ",\"timeInForce\":" + api::quote(tif);
    json += ",\"price\":" + optMoney(price);
    json += ",\"stopPrice\":" + optMoney(stopPrice);
    json += ",\"quantity\":" + money(quantity);
    json += ",\"filledQuantity\":" + money(filled);
    json += ",\"remainingQuantity\":" + money(remaining);
    json += ",\"status\":" + api::quote(status);
    json += ",\"reason\":" + (reason ? api::quote(*reason) : std::string("null"));
    json += ",\"meReason\":" + (meReason ? std::to_string(*meReason) : std::string("null"));
    json += ",\"meOrderId\":" + (meOrderId ? id(*meOrderId) : std::string("null"));
    json += ",\"hold\":{\"assetId\":" + id(holdAsset) + ",\"amount\":" + money(holdAmount);
    if (holdRemaining)
    {
        json += ",\"remaining\":" + money(*holdRemaining);
    }
    json += "},\"createdAt\":" + std::to_string(createdAt);
    json += ",\"updatedAt\":" + std::to_string(updatedAt);
    json += "}";
    key = Key{ updatedAt, orderId, 0 };
    return true;
}

bool fillJson(const Row &row, std::string &json, Key &key, std::string &problem)
{
    Reader r{ row, problem };
    const std::int64_t tradeId = r.number(0, "trade_id");
    const std::int64_t orderId = r.number(1, "order_id");
    const std::int64_t marketId = r.number(2, "market_id");
    const std::string side = r.text(3, "side");
    const std::int64_t price = r.number(4, "price");
    const std::int64_t quantity = r.number(5, "quantity");
    const std::string maker = r.text(6, "maker");
    const std::int64_t executedAt = r.number(7, "executed_at_ms");
    if (problem.empty() && "t" != maker && "f" != maker)
    {
        r.fail("maker", "is not a boolean");
    }
    if (!problem.empty())
    {
        problem += " (row trade_id " + r.raw(0) + ", order_id " + r.raw(1) + ")";
        return false;
    }
    json = "{\"tradeId\":" + id(tradeId);
    json += ",\"orderId\":" + id(orderId);
    json += ",\"marketId\":" + id(marketId);
    json += ",\"side\":" + api::quote(side);
    json += ",\"price\":" + money(price);
    json += ",\"quantity\":" + money(quantity);
    json += ",\"maker\":" + std::string("t" == maker ? "true" : "false");
    json += ",\"executedAt\":" + std::to_string(executedAt);
    json += "}";
    key = Key{ executedAt, tradeId, orderId };
    return true;
}

bool movementJson(const Row &row, const std::map<std::int32_t, std::string> &assetCodes, std::string &json, Key &key,
    std::string &problem)
{
    Reader r{ row, problem };
    const std::int64_t journalSeq = r.number(0, "journal_seq");
    const std::int64_t legNo = r.number(1, "leg_no");
    const std::int64_t logPosition = r.number(2, "log_position");
    const std::string kind = r.text(3, "kind");
    const std::int64_t assetId = r.number(4, "asset_id");     // NULL: a settle of a market the database cannot name
    const std::int64_t delta = r.number(5, "delta");
    const std::int64_t balanceAfter = r.number(6, "balance_after");
    const auto at = r.optNumber(7, "at_ms");
    const auto tradeId = r.optNumber(8, "trade_id");
    const auto marketId = r.optNumber(9, "market_id");
    if (problem.empty() && (assetId < INT32_MIN || assetId > INT32_MAX))
    {
        r.fail("asset_id", "is out of range");
    }
    if (!problem.empty())
    {
        problem += " (row journal_seq " + r.raw(0) + ", leg_no " + r.raw(1) + ")";
        return false;
    }
    const auto code = assetCodes.find(static_cast<std::int32_t>(assetId));
    json = "{\"journalSeq\":" + std::to_string(journalSeq);
    json += ",\"legNo\":" + std::to_string(legNo);
    json += ",\"logPosition\":" + std::to_string(logPosition);
    json += ",\"kind\":" + api::quote(kind);
    json += ",\"assetId\":" + id(assetId);
    json += ",\"asset\":" + (code == assetCodes.end() ? std::string("null") : api::quote(code->second));
    json += ",\"delta\":" + money(delta);
    json += ",\"balanceAfter\":" + money(balanceAfter);
    json += ",\"at\":" + (at ? std::to_string(*at) : std::string("null"));
    json += ",\"tradeId\":" + (tradeId ? id(*tradeId) : std::string("null"));
    json += ",\"marketId\":" + (marketId ? id(*marketId) : std::string("null"));
    json += "}";
    key = Key{ journalSeq, legNo, 0 };
    return true;
}

bool trailJson(const Row &row, std::string &json, Key &key, std::string &problem)
{
    Reader r{ row, problem };
    const std::int64_t orderId = r.number(0, "order_id");
    const std::int64_t eventSeq = r.number(1, "event_seq");
    const std::string event = r.text(2, "event");
    const std::string status = r.text(3, "status");
    const std::int64_t coreLogPosition = r.number(4, "core_log_position");
    const std::int64_t meEpoch = r.number(5, "me_epoch");
    const std::int64_t meEgressSeq = r.number(6, "me_log_position");
    const std::int64_t meJournalSeq = r.number(7, "me_journal_seq");
    const std::string liquidity = r.text(8, "liquidity");
    const std::int64_t tradeId = r.number(9, "trade_id");
    const std::int64_t lastPrice = r.number(10, "last_price");
    const std::int64_t lastQty = r.number(11, "last_qty");
    const std::int64_t holdRemaining = r.number(12, "hold_remaining");
    const std::int64_t at = r.number(13, "at_ms");
    const std::int64_t meAt = r.number(14, "me_timestamp_ms");
    if (!problem.empty())
    {
        problem += " (row order_id " + r.raw(0) + ", event_seq " + r.raw(1) + ")";
        return false;
    }
    json = "{\"eventSeq\":" + std::to_string(eventSeq);
    json += ",\"event\":" + api::quote(event);
    json += ",\"status\":" + api::quote(status);
    json += ",\"coreLogPosition\":" + std::to_string(coreLogPosition);
    json += ",\"me\":" + (0 == meEpoch ? std::string("null")
                                         : "{\"epoch\":" + id(meEpoch) + ",\"egressSeq\":" + std::to_string(meEgressSeq) +
                                             ",\"journalSeq\":" + std::to_string(meJournalSeq) + ",\"at\":" +
                                             std::to_string(meAt) + "}");
    json += ",\"liquidity\":" + ("NONE" == liquidity ? std::string("null") : api::quote(liquidity));
    const bool trade = 0 != tradeId;
    json += ",\"tradeId\":" + (trade ? id(tradeId) : std::string("null"));
    json += ",\"lastPrice\":" + (trade ? money(lastPrice) : std::string("null"));
    json += ",\"lastQty\":" + (trade ? money(lastQty) : std::string("null"));
    json += ",\"holdRemaining\":" + money(holdRemaining);
    json += ",\"at\":" + std::to_string(at);
    json += "}";
    key = Key{ eventSeq, orderId, 0 };
    return true;
}

}

const char *nameOf(HistoryCause c) noexcept
{
    switch (c)
    {
        case HistoryCause::NONE: return "none";
        case HistoryCause::UNCONFIGURED: return "unconfigured";
        case HistoryCause::ABSENT: return "absent";
        case HistoryCause::UNREACHABLE: return "unreachable";
        case HistoryCause::TIMEOUT: return "timeout";
        case HistoryCause::NO_HEARTBEAT: return "no_heartbeat";
        case HistoryCause::BAD_ROW: return "bad_row";
        case HistoryCause::ERROR: return "error";
        case HistoryCause::LAGGING: return "lagging";
        case HistoryCause::EPOCH: return "epoch";
        case HistoryCause::STALE: return "stale";
    }
    return "?";
}

const char *groupOf(HistoryCause c) noexcept
{
    switch (c)
    {
        case HistoryCause::NONE: return "";
        case HistoryCause::UNCONFIGURED:
        case HistoryCause::ABSENT:
        case HistoryCause::UNREACHABLE:
        case HistoryCause::TIMEOUT:
        case HistoryCause::BAD_ROW:
        case HistoryCause::ERROR: return "pg";
        case HistoryCause::NO_HEARTBEAT: return "heartbeat";
        case HistoryCause::EPOCH: return "epoch";
        case HistoryCause::STALE: return "stale";
        case HistoryCause::LAGGING: return "lagging";
    }
    return "?";
}

}}
