#include "gateway/common/Rest.h"

#include <cstring>
#include <limits>

#include "gateway/common/Api.h"
#include "gateway/common/Crypto.h"

namespace openexchange { namespace gateway { namespace rest {

namespace {

int hexValue(char h)
{
    if (h >= '0' && h <= '9')
    {
        return h - '0';
    }
    const char l = static_cast<char>(h | 0x20);
    return l >= 'a' && l <= 'f' ? l - 'a' + 10 : -1;
}

std::int64_t bigEndian64(const std::uint8_t *b)
{
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
    {
        v = (v << 8) | b[i];
    }
    return static_cast<std::int64_t>(v);
}

std::string idText(std::int64_t v)
{
    return api::quote(std::to_string(v));
}

std::string moneyText(std::int64_t v)
{
    return api::quote(formatMoney(v));
}

}

std::optional<std::int64_t> parseMoney(std::string_view text)
{
    const std::size_t dot = text.find('.');
    const std::string_view whole = text.substr(0, dot);
    const std::string_view frac = std::string_view::npos == dot ? std::string_view() : text.substr(dot + 1);
    if (whole.empty() || (std::string_view::npos != dot && (frac.empty() || frac.size() > 8)))
    {
        return std::nullopt;
    }
    constexpr std::uint64_t max = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    std::uint64_t units = 0;
    for (const char c : whole)
    {
        if (c < '0' || c > '9')
        {
            return std::nullopt;
        }
        const std::uint64_t d = static_cast<std::uint64_t>(c - '0');
        if (units > (max - d) / 10)
        {
            return std::nullopt;
        }
        units = units * 10 + d;
    }
    for (std::size_t i = 0; i < 8; ++i)
    {
        std::uint64_t d = 0;
        if (i < frac.size())
        {
            const char c = frac[i];
            if (c < '0' || c > '9')
            {
                return std::nullopt;
            }
            d = static_cast<std::uint64_t>(c - '0');
        }
        if (units > (max - d) / 10)
        {
            return std::nullopt;
        }
        units = units * 10 + d;
    }
    return static_cast<std::int64_t>(units);
}

std::string formatMoney(std::int64_t units)
{
    const bool negative = units < 0;
    // the magnitude as unsigned (INT64_MIN included)
    const std::uint64_t magnitude = negative ? 0 - static_cast<std::uint64_t>(units) : static_cast<std::uint64_t>(units);
    const std::uint64_t whole = magnitude / static_cast<std::uint64_t>(SCALE);
    std::uint64_t frac = magnitude % static_cast<std::uint64_t>(SCALE);
    std::string out = (negative ? "-" : "") + std::to_string(whole);
    if (0 != frac)
    {
        std::string digits = std::to_string(frac);
        digits.insert(0, 8 - digits.size(), '0');
        while (!digits.empty() && '0' == digits.back())
        {
            digits.pop_back();
        }
        out += "." + digits;
    }
    return out;
}

std::optional<std::pair<std::int64_t, std::int64_t>> parseUuid(std::string_view text)
{
    if (36 != text.size())
    {
        return std::nullopt;
    }
    std::uint64_t parts[2] = { 0, 0 };
    int nibble = 0;
    for (std::size_t i = 0; i < text.size(); ++i)
    {
        if (8 == i || 13 == i || 18 == i || 23 == i)
        {
            if ('-' != text[i])
            {
                return std::nullopt;
            }
            continue;
        }
        const int v = hexValue(text[i]);
        if (v < 0)
        {
            return std::nullopt;
        }
        std::uint64_t &p = parts[nibble / 16];
        p = (p << 4) | static_cast<std::uint64_t>(v);
        ++nibble;
    }
    return std::make_pair(static_cast<std::int64_t>(parts[0]), static_cast<std::int64_t>(parts[1]));
}

std::string formatUuid(std::int64_t high, std::int64_t low)
{
    static const char *digits = "0123456789abcdef";
    std::string out;
    const std::uint64_t parts[2] = { static_cast<std::uint64_t>(high), static_cast<std::uint64_t>(low) };
    for (int n = 0; n < 32; ++n)
    {
        if (8 == n || 12 == n || 16 == n || 20 == n)
        {
            out += '-';
        }
        const std::uint64_t p = parts[n / 16];
        out += digits[(p >> (60 - 4 * (n % 16))) & 0xF];
    }
    return out;
}

RequestKey parseIdempotencyKey(std::optional<std::string_view> header, std::size_t count)
{
    RequestKey k;
    if (0 == count || !header)
    {
        k.status = KeyStatus::MISSING;
        return k;
    }
    if (count > 1)
    {
        k.status = KeyStatus::INVALID;
        return k;
    }
    const std::string_view text = *header;
    if (const auto uuid = parseUuid(text))
    {
        k.high = uuid->first;
        k.low = uuid->second;
    }
    else
    {
        if (text.empty() || text.size() > 64)
        {
            k.status = KeyStatus::INVALID;
            return k;
        }
        for (const char c : text)
        {
            if (c < 0x21 || c > 0x7E)
            {
                k.status = KeyStatus::INVALID;
                return k;
            }
        }
        const crypto::Digest d = crypto::sha256(text);
        k.high = bigEndian64(d.data());
        k.low = bigEndian64(d.data() + 8);
    }
    k.status = 0 == k.high && 0 == k.low ? KeyStatus::NIL : KeyStatus::OK;
    return k;
}

void fundingRequestId(std::int64_t userId, std::int32_t assetId, std::int64_t &high, std::int64_t &low)
{
    std::string input = "oe-gateway/register-funding/v1";
    for (int i = 7; i >= 0; --i)
    {
        input += static_cast<char>((static_cast<std::uint64_t>(userId) >> (8 * i)) & 0xFF);
    }
    for (int i = 3; i >= 0; --i)
    {
        input += static_cast<char>((static_cast<std::uint32_t>(assetId) >> (8 * i)) & 0xFF);
    }
    const crypto::Digest d = crypto::sha256(input);
    high = bigEndian64(d.data());
    low = bigEndian64(d.data() + 8);
}

std::optional<money::Side::Value> parseSide(std::string_view n)
{
    if ("BUY" == n)
    {
        return money::Side::BUY;
    }
    if ("SELL" == n)
    {
        return money::Side::SELL;
    }
    return std::nullopt;
}

std::optional<money::OrderType::Value> parseOrderType(std::string_view n)
{
    for (const money::OrderType::Value v : { money::OrderType::LIMIT, money::OrderType::MARKET,
             money::OrderType::LIMIT_MAKER, money::OrderType::STOP_LOSS, money::OrderType::STOP_LIMIT,
             money::OrderType::TRAILING_STOP, money::OrderType::ICEBERG })
    {
        if (n == money::OrderType::c_str(v))
        {
            return v;
        }
    }
    return std::nullopt;
}

std::optional<money::TimeInForce::Value> parseTimeInForce(std::string_view n)
{
    for (const money::TimeInForce::Value v :
        { money::TimeInForce::GTC, money::TimeInForce::IOC, money::TimeInForce::FOK, money::TimeInForce::GTD })
    {
        if (n == money::TimeInForce::c_str(v))
        {
            return v;
        }
    }
    return std::nullopt;
}

const char *nameOf(money::Side::Value v)
{
    return money::Side::c_str(v);
}
const char *nameOf(money::OrderType::Value v)
{
    return money::OrderType::c_str(v);
}
const char *nameOf(money::TimeInForce::Value v)
{
    return money::TimeInForce::c_str(v);
}
const char *nameOf(money::OrderStatus::Value v)
{
    return money::OrderStatus::c_str(v);
}

bool priceLess(money::OrderType::Value t)
{
    return money::OrderType::MARKET == t || money::OrderType::STOP_LOSS == t || money::OrderType::TRAILING_STOP == t;
}

std::string createShapeProblem(const wire::CreateOrder &c)
{
    if (c.quantity <= 0)
    {
        return "quantity must be positive";
    }
    if (priceLess(c.orderType))
    {
        if (0 != c.price)
        {
            return std::string("price is not part of a ") + nameOf(c.orderType) + " order";
        }
    }
    else
    {
        if (c.price <= 0)
        {
            return std::string("price is required and positive for a ") + nameOf(c.orderType) + " order";
        }
        if (0 != c.budget)
        {
            return std::string("budget is only part of a MARKET, STOP_LOSS or TRAILING_STOP order");
        }
    }
    const bool stop = money::OrderType::STOP_LOSS == c.orderType || money::OrderType::STOP_LIMIT == c.orderType;
    const bool trailing = money::OrderType::TRAILING_STOP == c.orderType;
    const bool iceberg = money::OrderType::ICEBERG == c.orderType;
    if (stop != (0 != c.stopPrice))
    {
        return stop ? "stopPrice is required and positive for a stop order" : "stopPrice is only part of a STOP_LOSS or STOP_LIMIT order";
    }
    if (trailing != (0 != c.trailingDelta))
    {
        return trailing ? "trailingDelta is required and positive for a TRAILING_STOP order"
                        : "trailingDelta is only part of a TRAILING_STOP order";
    }
    if (iceberg != (0 != c.displayQty))
    {
        return iceberg ? "displayQuantity is required and positive for an ICEBERG order"
                       : "displayQuantity is only part of an ICEBERG order";
    }
    if (iceberg && c.displayQty >= c.quantity)
    {
        return "displayQuantity must be below quantity";
    }
    if (money::TimeInForce::GTD == c.timeInForce ? c.expiresAtMs <= 0 : 0 != c.expiresAtMs)
    {
        return money::TimeInForce::GTD == c.timeInForce ? "expiresAt is required with GTD" : "expiresAt is only part of a GTD order";
    }
    return {};
}

std::string fixedText(const char *bytes, std::size_t size)
{
    std::size_t n = 0;
    while (n < size && '\0' != bytes[n])
    {
        ++n;
    }
    return std::string(bytes, n);
}

std::string orderJson(const wire::OrderSnapshotEntry &e)
{
    auto optMoney = [](std::int64_t v) { return 0 == v ? std::string("null") : moneyText(v); };
    std::string out = "{\"orderId\":" + idText(e.orderId);
    out += ",\"clientOrderId\":" + (0 == e.clientOrderIdHigh && 0 == e.clientOrderIdLow
            ? std::string("null") : api::quote(formatUuid(e.clientOrderIdHigh, e.clientOrderIdLow)));
    out += ",\"marketId\":" + idText(e.marketId);
    out += ",\"side\":" + api::quote(nameOf(e.side));
    out += ",\"type\":" + api::quote(nameOf(e.orderType));
    out += ",\"timeInForce\":" + api::quote(nameOf(e.timeInForce));
    out += ",\"price\":" + (priceLess(e.orderType) ? std::string("null") : moneyText(e.price));
    out += ",\"stopPrice\":" + optMoney(e.stopPrice);
    out += ",\"quantity\":" + moneyText(e.quantity);
    out += ",\"filledQuantity\":" + moneyText(e.filledQty);
    out += ",\"remainingQuantity\":" + moneyText(e.remainingQty);
    out += ",\"status\":" + api::quote(nameOf(e.status));
    out += ",\"reason\":null,\"meReason\":null";
    out += ",\"meOrderId\":" + (0 == e.meOrderId ? std::string("null") : idText(e.meOrderId));
    out += ",\"hold\":{\"assetId\":" + idText(e.holdAssetId) + ",\"amount\":" + moneyText(e.holdAmount) + "}";
    out += ",\"createdAt\":" + std::to_string(e.createdAtMs);
    out += ",\"updatedAt\":" + std::to_string(e.updatedAtMs);
    out += ",\"displayQuantity\":" + optMoney(e.displayQty);
    out += ",\"expiresAt\":" + (0 == e.expiresAtMs ? std::string("null") : std::to_string(e.expiresAtMs));
    out += ",\"budget\":" + optMoney(e.budget);
    out += "}";
    return out;
}

Mapping mapOrderReject(money::CoreRejectReason::Value reason, std::uint16_t refTemplateId)
{
    using R = money::CoreRejectReason;
    Mapping m;
    auto set = [&m](unsigned status, const char *code, bool keyFinal, std::int64_t retryAfterMs = -1) {
        m.status = status;
        m.code = code;
        m.keyFinal = keyFinal;
        m.retryAfterMs = retryAfterMs;
    };
    switch (reason)
    {
        case R::MISSING_REQUEST_ID:
        case R::UNSUPPORTED_FLAGS:
        case R::UNSUPPORTED_TIF:
            set(400, api::code::VALIDATION, false);
            break;
        case R::UNAUTHORIZED_CLASS:
            set(403, api::code::FORBIDDEN, false);
            m.critical = true;
            break;
        case R::UNKNOWN_ORDER:
        case R::NOT_OWNER:
        case R::UNKNOWN_MARKET:
            set(404, api::code::NOT_FOUND, false);
            break;
        case R::REQUEST_ID_PAYLOAD_MISMATCH:
        case R::DUPLICATE_CLIENT_ORDER_ID:
        case R::NOT_OPEN:
        case R::AMEND_PENDING:
        case R::EXPIRED:
            set(409, api::code::CONFLICT, false);
            break;
        case R::PRICE_OUT_OF_BAND:
        case R::PRICE_OFF_TICK:
        case R::QTY_OFF_LOT:
            set(422, api::code::REJECTED, false);
            break;
        case R::INSUFFICIENT_FUNDS:
        case R::SIZE_TOO_SMALL:
        case R::SIZE_TOO_LARGE:
        case R::NOTIONAL_TOO_SMALL:
        case R::NOTIONAL_TOO_LARGE:
        case R::PRICE_COLLAR:
        case R::OPEN_ORDER_LIMIT:
        case R::POSITION_LIMIT:
        case R::NO_REFERENCE_PRICE:
        case R::STOP_WOULD_TRIGGER_IMMEDIATELY:
            set(422, api::code::REJECTED, true);
            break;
        case R::INVALID:
            if (wire::tid::CREATE_ORDER == refTemplateId || wire::tid::AMEND_ORDER == refTemplateId)
            {
                // The late INVALID: stored under the key. This holds only while createShapeProblem refuses, at the
                // gateway, every shape the core answers INVALID without storing it (OrderEngine.syntheticFieldsValid
                // and the early checks of applyCreate, for example expiresAt without GTD). A core change to those
                // checks must be mirrored there, or this keyFinal is wrong for the new case.
                set(422, api::code::REJECTED, true);
            }
            else
            {
                m.defect = true;
            }
            break;
        case R::RATE_LIMIT:
            set(429, api::code::RATE_LIMITED, true, 1000);
            break;
        case R::TOO_MANY_PENDING_REQUESTS:
            set(429, api::code::RATE_LIMITED, false, 1000);
            break;
        case R::GATE_CLOSED:
            set(503, api::code::ADMISSION_CLOSED, false, 1000);
            break;
        case R::CIRCUIT_BREAKER:
            set(503, api::code::ADMISSION_CLOSED, true, 1000);
            break;
        default:
            // HOLD_FAULT, the ME_* family (an ME decision is never synchronous), FEED_HALTED and ME_LANE_INACTIVE
            // (ADMIN acks only), UNREGISTERED_SESSION and NOT_RESUMABLE (the session answers those itself), NONE
            m.defect = true;
            break;
    }
#ifdef GW_NC_KEYFINAL_INVERTED
    // test-only control: keyFinal inverted for every reason
    m.keyFinal = !m.keyFinal;
#endif
    return m;
}

Mapping mapMoneyReject(money::RejectReason::Value reason)
{
    using R = money::RejectReason;
    Mapping m;
    switch (reason)
    {
        case R::INSUFFICIENT_FUNDS:
        case R::INVALID_AMOUNT:
        case R::BALANCE_OVERFLOW:
            m.status = 422;
            m.code = api::code::REJECTED;
            break;
        case R::REQUEST_ID_MISMATCH:
            m.status = 409;
            m.code = api::code::CONFLICT;
            break;
        default:
            m.defect = true; // UNKNOWN_HOLD cannot reach a gateway; NONE is never a reject
            break;
    }
#ifdef GW_NC_KEYFINAL_INVERTED
    m.keyFinal = !m.keyFinal;
#endif
    return m;
}

}}}
