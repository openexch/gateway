#ifndef GATEWAY_COMMON_REST_H
#define GATEWAY_COMMON_REST_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "gateway/common/SessionParts.h"
#include "gateway/common/Wire.h"

// Rest - the pure pieces of the REST surface, shared by the routes and their tests: the money decimal, the
// Idempotency-Key and clientOrderId encodings, the enum names, the derived register-funding request id, the JSON of an
// open order, and the table that maps a core reject to its HTTP status, code and keyFinal.
//
// Money. A decimal string with at most 8 fraction digits ("12", "0.5", "1.00000001"), read as an integer scaled by
// 10^8. No sign, no exponent, no leading '+' or '.', no thousands separator, no whitespace; a value above INT64_MAX in
// units is refused (overflow). A JSON number is refused by the routes before this is reached. Output is the shortest
// exact form: "1.5", "100", "0.00000001", "-2.25" (a negative value is written, never read).
//
// Idempotency-Key. A UUID (8-4-4-4-12 hex digits, either case) gives its 128 bits as is (high = the first 64 bits);
// any other key of 1 to 64 visible ASCII characters (0x21..0x7E) gives the first 16 bytes of its SHA-256, big-endian.
// The nil UUID and a key that encodes 0/0 are refused (0/0 is "no key" on the wire).
//
// clientOrderId. A UUID only (the 128 bits travel in the order); the nil UUID is refused (0/0 means "none").

namespace openexchange { namespace gateway { namespace rest {

namespace money = wire::money;

inline constexpr std::int64_t SCALE = 100'000'000; // 10^8: the money scale on the wire

std::optional<std::int64_t> parseMoney(std::string_view text);
std::string formatMoney(std::int64_t units);

enum class KeyStatus : std::uint8_t
{
    OK,
    MISSING,
    INVALID, // not a UUID and not 1..64 visible ASCII characters, or the header given twice
    NIL      // the nil UUID, or a key that encodes 0/0
};
struct RequestKey
{
    KeyStatus status = KeyStatus::MISSING;
    std::int64_t high = 0;
    std::int64_t low = 0;
};
// `count` is how many Idempotency-Key headers the request carried (more than one is INVALID).
RequestKey parseIdempotencyKey(std::optional<std::string_view> header, std::size_t count);

// A UUID string -> its two halves; nullopt when the text is not 8-4-4-4-12 hex.
std::optional<std::pair<std::int64_t, std::int64_t>> parseUuid(std::string_view text);
std::string formatUuid(std::int64_t high, std::int64_t low);

// The requestId of one register-funding deposit: the first 16 bytes of SHA-256("oe-gateway/register-funding/v1" ||
// userId as 8 bytes big-endian || assetId as 4 bytes big-endian). Never 0/0 (a 0/0 digest is refused by the caller).
void fundingRequestId(std::int64_t userId, std::int32_t assetId, std::int64_t &high, std::int64_t &low);

// Enum names (the core's) and their parsers; nullopt for a name the schema does not have.
std::optional<money::Side::Value> parseSide(std::string_view name);
std::optional<money::OrderType::Value> parseOrderType(std::string_view name);
std::optional<money::TimeInForce::Value> parseTimeInForce(std::string_view name);
const char *nameOf(money::Side::Value v);
const char *nameOf(money::OrderType::Value v);
const char *nameOf(money::TimeInForce::Value v);
const char *nameOf(money::OrderStatus::Value v);
// MARKET, STOP_LOSS and TRAILING_STOP carry no limit price (the core's isPriceLess)
bool priceLess(money::OrderType::Value type);

// The shape rules the core applies to a CreateOrder before it looks at anything else (OrderEngine's structural and
// per-type field checks), mirrored so a shape error is a 400 here and never a stored decision there. Empty = valid,
// else the field and the rule. Business rules (tick, lot, band, balance, risk) are the core's alone.
std::string createShapeProblem(const wire::CreateOrder &c);

// A fixed-width wire string (symbol, asset code): the bytes up to the first NUL.
std::string fixedText(const char *bytes, std::size_t size);

// The JSON object of one open order (a RequestOrderSnapshot row): the Order shape with displayQuantity, expiresAt and
// budget, and no hold.remaining.
std::string orderJson(const wire::OrderSnapshotEntry &e);

// ---- the status table: core reject -> HTTP status, code, keyFinal

struct Mapping
{
    unsigned status = 500;
    const char *code = "INTERNAL";
    bool keyFinal = false;          // a retry with the same Idempotency-Key replays this answer
    std::int64_t retryAfterMs = -1; // >= 0: send Retry-After
    bool critical = false;          // a deployment error: logged CRITICAL
    bool defect = false;            // cannot reach a gateway (or a gateway defect): 500, logged ERROR
};

// An OrderReject (CoreRejectReason) for the template the gateway sent (refTemplateId). INVALID on a create or an amend
// is the stored late INVALID (keyFinal): every shape INVALID the core has is checked here first
// (createShapeProblem, and the amend's own shape), so an INVALID that still arrives is the one the core remembers
// (the position overflow of a create, the leg quantity of an amend). INVALID on a query or a cancel is a gateway defect.
Mapping mapOrderReject(money::CoreRejectReason::Value reason, std::uint16_t refTemplateId);
// A DepositReject or WithdrawReject (RejectReason).
Mapping mapMoneyReject(money::RejectReason::Value reason);

}}}

#endif
