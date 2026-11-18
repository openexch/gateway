#include "JournalRecord.h"

#include <cstring>

#include "com_openexchange_core_infrastructure_generated/OrderEvent.h"

#include "gateway/common/Crypto.h"
#include "gateway/common/Rest.h"

namespace openexchange { namespace gateway { namespace projector {

namespace money = wire::money;

namespace {

// Message offsets of the record (header 8, then the block).
constexpr std::size_t OFF_SEQ = 8;
constexpr std::size_t OFF_EPOCH = 16;
constexpr std::size_t OFF_LOG_POSITION = 24;
constexpr std::size_t OFF_BODY = 32;
constexpr std::size_t BODY_LENGTH = 151;
constexpr std::size_t OFF_LIQUIDITY = OFF_BODY + BODY_LENGTH; // 183
constexpr std::size_t OFF_ME_EPOCH = 184;
constexpr std::size_t OFF_ME_JOURNAL_SEQ = 192;
constexpr std::size_t OFF_ME_LOG_POSITION = 200;
constexpr std::size_t OFF_ME_TIMESTAMP = 208;

// The OrderEvent block the middle bytes come from: eventSeq at 0, logPosition at 8, orderId .. meReason from 16.
constexpr std::size_t EVENT_BODY_OFFSET = 16;
static_assert(money::OrderEvent::eventSeqEncodingOffset() == 0);
static_assert(money::OrderEvent::logPositionEncodingOffset() == 8);
static_assert(money::OrderEvent::orderIdEncodingOffset() == EVENT_BODY_OFFSET);
static_assert(money::OrderEvent::sbeBlockLength() == EVENT_BODY_OFFSET + BODY_LENGTH);
static_assert(money::OrderEvent::meReasonEncodingOffset() == money::OrderEvent::sbeBlockLength() - 1);
static_assert(OFF_ME_TIMESTAMP + 8 == RECORD_LENGTH);
static_assert(wire::HEADER_LENGTH + RECORD_BLOCK_LENGTH == RECORD_LENGTH);

std::int64_t le64(const std::uint8_t *at)
{
    std::uint64_t v = 0;
    for (int i = 7; i >= 0; --i)
    {
        v = (v << 8) | at[i];
    }
    return static_cast<std::int64_t>(v);
}

std::uint16_t le16(const std::uint8_t *at)
{
    return static_cast<std::uint16_t>(at[0] | (at[1] << 8));
}

void put64(std::uint8_t *at, std::int64_t value)
{
    std::uint64_t v = static_cast<std::uint64_t>(value);
    for (int i = 0; i < 8; ++i)
    {
        at[i] = static_cast<std::uint8_t>(v & 0xff);
        v >>= 8;
    }
}

void put16(std::uint8_t *at, std::uint16_t v)
{
    at[0] = static_cast<std::uint8_t>(v & 0xff);
    at[1] = static_cast<std::uint8_t>(v >> 8);
}

class Capture final : public wire::ReplyHandler
{
public:
    void onOrderEvent(const wire::OrderEvent &e) override
    {
        event = e;
        seen = true;
    }
    wire::OrderEvent event;
    bool seen = false;
};

std::string num(std::int64_t v)
{
    return std::to_string(v);
}

}

const char *nameOf(Liquidity l) noexcept
{
    switch (l)
    {
        case Liquidity::NONE: return "NONE";
        case Liquidity::TAKER: return "TAKER";
        case Liquidity::MAKER: return "MAKER";
    }
    return "?";
}

bool decodeRecord(const std::uint8_t *bytes, std::size_t length, JournalRecord &out, std::string &why)
{
    if (nullptr == bytes || RECORD_LENGTH != length)
    {
        why = "record length " + std::to_string(length) + ", the order journal record is " + std::to_string(RECORD_LENGTH);
        return false;
    }
    const std::uint16_t block = le16(bytes);
    const std::uint16_t templateId = le16(bytes + 2);
    const std::uint16_t schemaId = le16(bytes + 4);
    const std::uint16_t version = le16(bytes + 6);
    if (RECORD_SCHEMA_ID != schemaId || RECORD_SCHEMA_VERSION != version)
    {
        why = "schema " + std::to_string(schemaId) + " version " + std::to_string(version) + ", the order journal is " +
            std::to_string(RECORD_SCHEMA_ID) + " version " + std::to_string(RECORD_SCHEMA_VERSION);
        return false;
    }
    if (RECORD_TEMPLATE_ID != templateId || RECORD_BLOCK_LENGTH != block)
    {
        why = "template " + std::to_string(templateId) + " block " + std::to_string(block) + ", the order journal has " +
            "template " + std::to_string(RECORD_TEMPLATE_ID) + " block " + std::to_string(RECORD_BLOCK_LENGTH);
        return false;
    }
    JournalRecord r;
    std::memcpy(r.payload.data(), bytes, RECORD_LENGTH);
    r.seq = le64(bytes + OFF_SEQ);
    r.epoch = le64(bytes + OFF_EPOCH);
    r.logPosition = le64(bytes + OFF_LOG_POSITION);
    if (r.seq < 1)
    {
        why = "journalSeq " + std::to_string(r.seq) + " below 1";
        return false;
    }
    if (0 == r.epoch)
    {
        why = "epoch 0 (a record without a genesis)";
        return false;
    }
    const std::uint8_t liquidity = bytes[OFF_LIQUIDITY];
#ifdef PROJ_NC_NO_ENUM_CHECK
    if (false) // test build only: an unnamed value is not refused
#else
    if (liquidity > static_cast<std::uint8_t>(Liquidity::MAKER))
#endif
    {
        why = "liquidity " + std::to_string(liquidity) + " has no name";
        return false;
    }
    r.liquidity = static_cast<Liquidity>(liquidity);
    r.meEpoch = le64(bytes + OFF_ME_EPOCH);
    r.meJournalSeq = le64(bytes + OFF_ME_JOURNAL_SEQ);
    r.meLogPosition = le64(bytes + OFF_ME_LOG_POSITION);
    r.meTimestampMs = le64(bytes + OFF_ME_TIMESTAMP);

    // the OrderEvent frame the middle bytes came from: the gateway's own decoder reads it, enum rules included
    std::uint8_t frame[wire::HEADER_LENGTH + money::OrderEvent::sbeBlockLength()];
    put16(frame, money::OrderEvent::sbeBlockLength());
    put16(frame + 2, wire::tid::ORDER_EVENT);
    put16(frame + 4, wire::MONEY_SCHEMA_ID);
    put16(frame + 6, wire::MONEY_SCHEMA_VERSION);
    put64(frame + wire::HEADER_LENGTH, r.seq);
    put64(frame + wire::HEADER_LENGTH + 8, r.logPosition);
    std::memcpy(frame + wire::HEADER_LENGTH + EVENT_BODY_OFFSET, bytes + OFF_BODY, BODY_LENGTH);
    wire::Decoder decoder;
    Capture capture;
    const wire::Refusal refusal = decoder.decode(frame, sizeof frame, capture);
#ifdef PROJ_NC_NO_ENUM_CHECK
    if (wire::Refusal::BAD_VALUE == refusal) // test build only: written with a stand-in for the unnamed value
    {
        capture.seen = true;
        capture.event.orderId = le64(bytes + OFF_BODY);
    }
    else
#endif
    if (wire::Refusal::NONE != refusal || !capture.seen)
    {
        const wire::RefusalRecord &refused = decoder.lastRefusal();
        why = std::string("order event ") + wire::nameOf(refused.reason) +
            (*refused.field ? std::string(" (") + refused.field + " has no name)" : std::string());
        return false;
    }
    r.event = capture.event;
    if (r.event.orderId < 1)
    {
        why = "orderId " + std::to_string(r.event.orderId) + " below 1";
        return false;
    }
    out = r;
    return true;
}

Payload encodeRecord(const JournalRecord &record)
{
    Payload out{};
    std::uint8_t frame[wire::HEADER_LENGTH + money::OrderEvent::sbeBlockLength()] = {};
    money::OrderEvent enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char *>(frame), 0, sizeof frame);
    const wire::OrderEvent &e = record.event;
    enc.eventSeq(record.seq).logPosition(record.logPosition).orderId(e.orderId).userId(e.userId).marketId(e.marketId);
    enc.eventKind(e.eventKind).status(e.status).reason(e.reason).side(e.side).orderType(e.orderType);
    enc.timeInForce(e.timeInForce).price(e.price).stopPrice(e.stopPrice).quantity(e.quantity).filledQty(e.filledQty);
    enc.remainingQty(e.remainingQty).lastPrice(e.lastPrice).lastQty(e.lastQty).tradeId(e.tradeId);
    enc.meOrderId(e.meOrderId).holdAssetId(e.holdAssetId).holdAmount(e.holdAmount).holdRemaining(e.holdRemaining);
    enc.clientOrderIdHigh(e.clientOrderIdHigh).clientOrderIdLow(e.clientOrderIdLow).createdAtMs(e.createdAtMs);
    enc.updatedAtMs(e.updatedAtMs).meReason(e.meReason);
    put16(out.data(), RECORD_BLOCK_LENGTH);
    put16(out.data() + 2, RECORD_TEMPLATE_ID);
    put16(out.data() + 4, RECORD_SCHEMA_ID);
    put16(out.data() + 6, RECORD_SCHEMA_VERSION);
    put64(out.data() + OFF_SEQ, record.seq);
    put64(out.data() + OFF_EPOCH, record.epoch);
    put64(out.data() + OFF_LOG_POSITION, record.logPosition);
    std::memcpy(out.data() + OFF_BODY, frame + wire::HEADER_LENGTH + EVENT_BODY_OFFSET, BODY_LENGTH);
    out[OFF_LIQUIDITY] = static_cast<std::uint8_t>(record.liquidity);
    put64(out.data() + OFF_ME_EPOCH, record.meEpoch);
    put64(out.data() + OFF_ME_JOURNAL_SEQ, record.meJournalSeq);
    put64(out.data() + OFF_ME_LOG_POSITION, record.meLogPosition);
    put64(out.data() + OFF_ME_TIMESTAMP, record.meTimestampMs);
    return out;
}

std::string hexOf(const std::uint8_t *bytes, std::size_t n)
{
    return crypto::hex(std::string_view(reinterpret_cast<const char *>(bytes), n));
}

std::string sha256Hex(const std::uint8_t *bytes, std::size_t n)
{
    return crypto::hex(crypto::view(crypto::sha256(std::string_view(reinterpret_cast<const char *>(bytes), n))));
}

Columns eventColumns(const JournalRecord &r)
{
    const wire::OrderEvent &e = r.event;
    return { num(r.seq), num(e.orderId), num(e.userId), std::string(rest::nameOf(e.eventKind)),
        std::string(rest::nameOf(e.status)), num(r.logPosition), std::string(nameOf(r.liquidity)), num(r.meEpoch),
        num(r.meJournalSeq), num(r.meLogPosition), num(r.meTimestampMs), num(e.tradeId), num(e.lastPrice),
        num(e.lastQty), num(e.holdRemaining), num(e.updatedAtMs),
        "\\x" + hexOf(r.payload.data(), r.payload.size()) };
}

Columns orderColumns(const JournalRecord &r)
{
    const wire::OrderEvent &e = r.event;
    auto opt = [](bool isNull, std::int64_t v) { return isNull ? std::optional<std::string>() : std::optional(num(v)); };
    Columns c;
    c.push_back(num(e.orderId));
    c.push_back(num(e.userId));
    c.push_back(0 == e.clientOrderIdHigh && 0 == e.clientOrderIdLow
            ? std::optional<std::string>()
            : std::optional(rest::formatUuid(e.clientOrderIdHigh, e.clientOrderIdLow)));
    c.push_back(num(e.marketId));
#ifdef PROJ_NC_SECOND_NAME_TABLE
    c.push_back(std::string(money::Side::BUY == e.side ? "Buy" : "Sell")); // test build only: a name table of its own
#else
    c.push_back(std::string(rest::nameOf(e.side)));
#endif
    c.push_back(std::string(rest::nameOf(e.orderType)));
    c.push_back(std::string(rest::nameOf(e.timeInForce)));
    c.push_back(opt(rest::priceLess(e.orderType), e.price));
    c.push_back(opt(0 == e.stopPrice, e.stopPrice));
    c.push_back(num(e.quantity));
    c.push_back(num(e.filledQty));
    c.push_back(num(e.remainingQty));
    c.push_back(std::string(rest::nameOf(e.status)));
    c.push_back(money::CoreRejectReason::NONE == e.reason ? std::optional<std::string>()
                                                          : std::optional<std::string>(money::CoreRejectReason::c_str(e.reason)));
    c.push_back(opt(255 == e.meReason, e.meReason));
    c.push_back(opt(0 == e.meOrderId, e.meOrderId));
    c.push_back(num(e.holdAssetId));
    c.push_back(num(e.holdAmount));
    c.push_back(num(e.holdRemaining));
    c.push_back(num(e.createdAtMs));
    c.push_back(num(e.updatedAtMs));
    c.push_back(num(r.seq));
    c.push_back(num(r.logPosition));
    return c;
}

bool hasFill(const JournalRecord &r)
{
#ifdef PROJ_NC_FILL_ZERO_TRADE
    // test build only: the zero-trade filter is off
    return money::EventKind::FILL == r.event.eventKind;
#else
    return money::EventKind::FILL == r.event.eventKind && r.event.tradeId > 0 && r.event.lastQty > 0;
#endif
}

Columns fillColumns(const JournalRecord &r)
{
    const wire::OrderEvent &e = r.event;
    return { num(e.tradeId), num(e.orderId), num(e.userId), num(e.marketId), std::string(rest::nameOf(e.side)),
        num(e.lastPrice), num(e.lastQty), std::string(Liquidity::MAKER == r.liquidity ? "true" : "false"),
        num(e.updatedAtMs), num(r.seq), num(r.logPosition), num(r.meEpoch), num(r.meJournalSeq), num(r.meLogPosition),
        num(r.meTimestampMs) };
}

}}}
