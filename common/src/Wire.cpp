#include "gateway/common/Wire.h"

#include <cstring>
#include <exception>
#include <iterator>
#include <stdexcept>

#include "com_openexchange_core_infrastructure_generated/AdminAck.h"
#include "com_openexchange_core_infrastructure_generated/AdmissionState.h"
#include "com_openexchange_core_infrastructure_generated/AmendOrder.h"
#include "com_openexchange_core_infrastructure_generated/BalanceUpdate.h"
#include "com_openexchange_core_infrastructure_generated/BalanceUpdateBatch.h"
#include "com_openexchange_core_infrastructure_generated/BoolFlag.h"
#include "com_openexchange_core_infrastructure_generated/CancelOrder.h"
#include "com_openexchange_core_infrastructure_generated/ConfigApplied.h"
#include "com_openexchange_core_infrastructure_generated/CoreConfigReport.h"
#include "com_openexchange_core_infrastructure_generated/CreateOrder.h"
#include "com_openexchange_core_infrastructure_generated/Deposit.h"
#include "com_openexchange_core_infrastructure_generated/DepositAck.h"
#include "com_openexchange_core_infrastructure_generated/DepositReject.h"
#include "com_openexchange_core_infrastructure_generated/OrderAck.h"
#include "com_openexchange_core_infrastructure_generated/OrderEvent.h"
#include "com_openexchange_core_infrastructure_generated/OrderEventBatch.h"
#include "com_openexchange_core_infrastructure_generated/OrderEventsEnd.h"
#include "com_openexchange_core_infrastructure_generated/OrderEventsPage.h"
#include "com_openexchange_core_infrastructure_generated/OrderFault.h"
#include "com_openexchange_core_infrastructure_generated/OrderReject.h"
#include "com_openexchange_core_infrastructure_generated/OrderSnapshotBatch.h"
#include "com_openexchange_core_infrastructure_generated/OrderSnapshotEnd.h"
#include "com_openexchange_core_infrastructure_generated/RegisterSession.h"
#include "com_openexchange_core_infrastructure_generated/RequestCoreConfig.h"
#include "com_openexchange_core_infrastructure_generated/RequestOrderEvents.h"
#include "com_openexchange_core_infrastructure_generated/RequestOrderSnapshot.h"
#include "com_openexchange_core_infrastructure_generated/RequestUserBalances.h"
#include "com_openexchange_core_infrastructure_generated/UserBalances.h"
#include "com_openexchange_core_infrastructure_generated/Withdraw.h"
#include "com_openexchange_core_infrastructure_generated/WithdrawAck.h"
#include "com_openexchange_core_infrastructure_generated/WithdrawReject.h"

namespace openexchange { namespace gateway { namespace wire {

#if defined(GW_MONEY_SCHEMA_ID) && defined(GW_MONEY_SCHEMA_VERSION)
static_assert(MONEY_SCHEMA_ID == GW_MONEY_SCHEMA_ID, "the exported money codecs carry another schema id than dependencies.lock");
static_assert(MONEY_SCHEMA_VERSION == GW_MONEY_SCHEMA_VERSION,
    "the exported money codecs carry another schema version than dependencies.lock");
#else
#error "GW_MONEY_SCHEMA_ID and GW_MONEY_SCHEMA_VERSION come from dependencies.lock (CMakeLists.txt)"
#endif

namespace {

// GateReasons: one choice per GateReason value, bits 0..8 in money schema v8.
constexpr std::uint32_t GATE_REASONS_BITS = (1u << 9) - 1;

struct Layout
{
    std::uint16_t id;
    const char *name;
    std::uint64_t blockLength;
    int groups;
    std::uint64_t groupBlockLength[2];
    const char *groupName[2];
};

// The templates this build decodes, with the lengths of money schema v8 (from the exported codecs).
const Layout DECODED[] = {
    { tid::BALANCE_UPDATE, "BalanceUpdate", money::BalanceUpdate::sbeBlockLength(), 0, {}, {} },
    { tid::WITHDRAW_REJECT, "WithdrawReject", money::WithdrawReject::sbeBlockLength(), 0, {}, {} },
    { tid::DEPOSIT_ACK, "DepositAck", money::DepositAck::sbeBlockLength(), 0, {}, {} },
    { tid::WITHDRAW_ACK, "WithdrawAck", money::WithdrawAck::sbeBlockLength(), 0, {}, {} },
    { tid::BALANCE_UPDATE_BATCH, "BalanceUpdateBatch", money::BalanceUpdateBatch::sbeBlockLength(), 1,
        { money::BalanceUpdateBatch::Updates::sbeBlockLength(), 0 }, { "updates", "" } },
    { tid::ORDER_ACK, "OrderAck", money::OrderAck::sbeBlockLength(), 0, {}, {} },
    { tid::ORDER_REJECT, "OrderReject", money::OrderReject::sbeBlockLength(), 0, {}, {} },
    { tid::ORDER_EVENT, "OrderEvent", money::OrderEvent::sbeBlockLength(), 0, {}, {} },
    { tid::ORDER_EVENT_BATCH, "OrderEventBatch", money::OrderEventBatch::sbeBlockLength(), 1,
        { money::OrderEventBatch::Events::sbeBlockLength(), 0 }, { "events", "" } },
    { tid::ORDER_SNAPSHOT_BATCH, "OrderSnapshotBatch", money::OrderSnapshotBatch::sbeBlockLength(), 1,
        { money::OrderSnapshotBatch::Orders::sbeBlockLength(), 0 }, { "orders", "" } },
    { tid::ORDER_SNAPSHOT_END, "OrderSnapshotEnd", money::OrderSnapshotEnd::sbeBlockLength(), 0, {}, {} },
    { tid::ADMISSION_STATE, "AdmissionState", money::AdmissionState::sbeBlockLength(), 0, {}, {} },
    { tid::ADMIN_ACK, "AdminAck", money::AdminAck::sbeBlockLength(), 0, {}, {} },
    { tid::DEPOSIT_REJECT, "DepositReject", money::DepositReject::sbeBlockLength(), 0, {}, {} },
    { tid::CONFIG_APPLIED, "ConfigApplied", money::ConfigApplied::sbeBlockLength(), 0, {}, {} },
    { tid::ORDER_EVENTS_PAGE, "OrderEventsPage", money::OrderEventsPage::sbeBlockLength(), 1,
        { money::OrderEventsPage::Events::sbeBlockLength(), 0 }, { "events", "" } },
    { tid::ORDER_EVENTS_END, "OrderEventsEnd", money::OrderEventsEnd::sbeBlockLength(), 0, {}, {} },
    { tid::USER_BALANCES, "UserBalances", money::UserBalances::sbeBlockLength(), 1,
        { money::UserBalances::Balances::sbeBlockLength(), 0 }, { "balances", "" } },
    { tid::CORE_CONFIG_REPORT, "CoreConfigReport", money::CoreConfigReport::sbeBlockLength(), 2,
        { money::CoreConfigReport::Markets::sbeBlockLength(), money::CoreConfigReport::Assets::sbeBlockLength() },
        { "markets", "assets" } },
};

// The templates this build ignores (IGNORED_TEMPLATES, same order), checked like the decoded ones and never read.
const Layout IGNORED[] = {
    { tid::ORDER_FAULT, "OrderFault", money::OrderFault::sbeBlockLength(), 0, {}, {} },
};
static_assert(std::size(IGNORED) == IGNORED_TEMPLATES.size(), "one layout per ignored template");

const Layout *decodedLayout(std::uint16_t id) noexcept
{
    for (const Layout &layout : DECODED)
    {
        if (layout.id == id)
        {
            return &layout;
        }
    }
    return nullptr;
}

// The position of an ignored template in IGNORED_TEMPLATES, or -1.
int ignoredSlot(std::uint16_t id) noexcept
{
    for (std::size_t k = 0; k < std::size(IGNORED); ++k)
    {
        if (IGNORED[k].id == id && IGNORED_TEMPLATES[k] == id)
        {
            return static_cast<int>(k);
        }
    }
    return -1;
}

std::uint16_t readU16(const std::uint8_t *p) noexcept
{
    // the wire is little endian (SBE); the codecs' own byte order macros decode it
    std::uint16_t v;
    std::memcpy(&v, p, sizeof(v));
    return SBE_LITTLE_ENDIAN_ENCODE_16(v);
}

struct BadValue
{
    const char *field;
};

// An enum field the schema names a value for; NULL_VALUE (255) is not one.
template<typename E>
typename E::Value enumOf(std::uint8_t raw, const char *field)
{
    typename E::Value value;
    try
    {
        value = E::get(raw);
    }
    catch (const std::runtime_error &)
    {
        throw BadValue{ field };
    }
    if (E::NULL_VALUE == value)
    {
        throw BadValue{ field };
    }
    return value;
}

bool flagOf(std::uint8_t raw, const char *field)
{
    return money::BoolFlag::TRUE == enumOf<money::BoolFlag>(raw, field);
}

std::uint32_t gateReasonsOf(std::uint32_t raw, const char *field)
{
    if (0 != (raw & ~GATE_REASONS_BITS))
    {
        throw BadValue{ field };
    }
    return raw;
}

char *bytes(const std::uint8_t *payload)
{
    // the codecs take a mutable buffer; decode only reads through it
    return reinterpret_cast<char *>(const_cast<std::uint8_t *>(payload));
}

template<typename D>
OrderEvent eventOf(D &d)
{
    OrderEvent e;
    e.eventSeq = d.eventSeq();
    e.logPosition = d.logPosition();
    e.orderId = d.orderId();
    e.userId = d.userId();
    e.marketId = d.marketId();
    e.eventKind = enumOf<money::EventKind>(d.eventKindRaw(), "eventKind");
    e.status = enumOf<money::OrderStatus>(d.statusRaw(), "status");
    e.reason = enumOf<money::CoreRejectReason>(d.reasonRaw(), "reason");
    e.side = enumOf<money::Side>(d.sideRaw(), "side");
    e.orderType = enumOf<money::OrderType>(d.orderTypeRaw(), "orderType");
    e.timeInForce = enumOf<money::TimeInForce>(d.timeInForceRaw(), "timeInForce");
#ifdef GW_NC_SWAPPED_DECODE_FIELD
    // test-only control: price and stopPrice swapped on decode; the golden reply row must fail naming OrderEvent
    e.price = d.stopPrice();
    e.stopPrice = d.price();
#else
    e.price = d.price();
    e.stopPrice = d.stopPrice();
#endif
    e.quantity = d.quantity();
    e.filledQty = d.filledQty();
    e.remainingQty = d.remainingQty();
    e.lastPrice = d.lastPrice();
    e.lastQty = d.lastQty();
    e.tradeId = d.tradeId();
    e.meOrderId = d.meOrderId();
    e.holdAssetId = d.holdAssetId();
    e.holdAmount = d.holdAmount();
    e.holdRemaining = d.holdRemaining();
    e.clientOrderIdHigh = d.clientOrderIdHigh();
    e.clientOrderIdLow = d.clientOrderIdLow();
    e.createdAtMs = d.createdAtMs();
    e.updatedAtMs = d.updatedAtMs();
    e.meReason = d.meReason();
    return e;
}

template<typename D>
MoneyAck moneyAckOf(D &d)
{
    MoneyAck a;
    a.correlationId = d.correlationId();
    a.userId = d.userId();
    a.assetId = d.assetId();
    a.amount = d.amount();
    a.newAvailable = d.newAvailable();
    a.duplicate = flagOf(d.duplicateRaw(), "duplicate");
    return a;
}

// Reads a frame the structural checks accepted and calls the handler. Throws BadValue on a value the schema does
// not name; the caller runs it once with a no-op handler first (every value read and checked), and delivers to the
// owner's handler only after that pass completed, so a decoding throw never follows a delivered call.
void deliver(const Layout &layout, const std::uint8_t *payload, std::size_t length, std::uint16_t version,
    ReplyHandler &h)
{
    char *buffer = bytes(payload);
    const std::uint64_t offset = HEADER_LENGTH;
    const std::uint64_t block = layout.blockLength;
    switch (layout.id)
    {
        case tid::ADMIN_ACK:
        {
            money::AdminAck d(buffer, offset, length, block, version);
            AdminAck m;
            m.correlationId = d.correlationId();
            m.refTemplateId = d.refTemplateId();
            m.result = enumOf<money::AdminResult>(d.resultRaw(), "result");
            m.reason = d.reason();
            m.configVersion = d.configVersion();
            h.onAdminAck(m);
            return;
        }
        case tid::ADMISSION_STATE:
        {
            money::AdmissionState d(buffer, offset, length, block, version);
            AdmissionState m;
            m.marketId = d.marketId();
            m.gateState = enumOf<money::GateState>(d.gateStateRaw(), "gateState");
            m.gateReason = enumOf<money::GateReason>(d.gateReasonRaw(), "gateReason");
            m.circuitBreaker = flagOf(d.circuitBreakerRaw(), "circuitBreaker");
            m.atMs = d.atMs();
            m.logPosition = d.logPosition();
            m.gateReasons = gateReasonsOf(d.gateReasons().rawValue(), "gateReasons");
            h.onAdmissionState(m);
            return;
        }
        case tid::CONFIG_APPLIED:
        {
            money::ConfigApplied d(buffer, offset, length, block, version);
            ConfigApplied m;
            m.kind = enumOf<money::ConfigKind>(d.kindRaw(), "kind");
            m.marketId = d.marketId();
            m.configVersion = d.configVersion();
            m.logPosition = d.logPosition();
            h.onConfigApplied(m);
            return;
        }
        case tid::ORDER_ACK:
        {
            money::OrderAck d(buffer, offset, length, block, version);
            OrderAck m;
            m.correlationId = d.correlationId();
            m.requestIdHigh = d.requestIdHigh();
            m.requestIdLow = d.requestIdLow();
            m.orderId = d.orderId();
            m.userId = d.userId();
            m.marketId = d.marketId();
            m.status = enumOf<money::OrderStatus>(d.statusRaw(), "status");
            m.duplicate = flagOf(d.duplicateRaw(), "duplicate");
            m.holdAssetId = d.holdAssetId();
            m.holdAmount = d.holdAmount();
            m.acceptedAtMs = d.acceptedAtMs();
            m.eventSeq = d.eventSeq();
            h.onOrderAck(m);
            return;
        }
        case tid::ORDER_REJECT:
        {
            money::OrderReject d(buffer, offset, length, block, version);
            OrderReject m;
            m.correlationId = d.correlationId();
            m.requestIdHigh = d.requestIdHigh();
            m.requestIdLow = d.requestIdLow();
            m.userId = d.userId();
            m.orderId = d.orderId();
            m.marketId = d.marketId();
            m.refTemplateId = d.refTemplateId();
            m.reason = enumOf<money::CoreRejectReason>(d.reasonRaw(), "reason");
            m.meReason = d.meReason();
            m.detail = d.detail();
            m.rejectedAtMs = d.rejectedAtMs();
            h.onOrderReject(m);
            return;
        }
        case tid::ORDER_EVENT:
        {
            money::OrderEvent d(buffer, offset, length, block, version);
            h.onOrderEvent(eventOf(d));
            return;
        }
        case tid::ORDER_EVENT_BATCH:
        {
            money::OrderEventBatch d(buffer, offset, length, block, version);
            money::OrderEventBatch::Events &events = d.events();
            h.onOrderEventBatch(static_cast<std::uint16_t>(events.count()));
            while (events.hasNext())
            {
                h.onOrderEvent(eventOf(events.next()));
            }
            return;
        }
        case tid::ORDER_EVENTS_PAGE:
        {
            money::OrderEventsPage d(buffer, offset, length, block, version);
            const std::int64_t correlationId = d.correlationId();
            money::OrderEventsPage::Events &events = d.events();
            h.onOrderEventsPage(correlationId, static_cast<std::uint16_t>(events.count()));
            while (events.hasNext())
            {
                h.onPageEvent(correlationId, eventOf(events.next()));
            }
            return;
        }
        case tid::ORDER_EVENTS_END:
        {
            money::OrderEventsEnd d(buffer, offset, length, block, version);
            OrderEventsEnd m;
            m.correlationId = d.correlationId();
            m.count = d.count();
            m.firstAvailableSeq = d.firstAvailableSeq();
            m.lastSeq = d.lastSeq();
            m.nextFromSeq = d.nextFromSeq();
            h.onOrderEventsEnd(m);
            return;
        }
        case tid::ORDER_SNAPSHOT_BATCH:
        {
            money::OrderSnapshotBatch d(buffer, offset, length, block, version);
            money::OrderSnapshotBatch::Orders &orders = d.orders();
            h.onOrderSnapshotBatch(static_cast<std::uint16_t>(orders.count()));
            while (orders.hasNext())
            {
                money::OrderSnapshotBatch::Orders &o = orders.next();
                OrderSnapshotEntry m;
                m.orderId = o.orderId();
                m.userId = o.userId();
                m.marketId = o.marketId();
                m.status = enumOf<money::OrderStatus>(o.statusRaw(), "status");
                m.side = enumOf<money::Side>(o.sideRaw(), "side");
                m.orderType = enumOf<money::OrderType>(o.orderTypeRaw(), "orderType");
                m.timeInForce = enumOf<money::TimeInForce>(o.timeInForceRaw(), "timeInForce");
                m.price = o.price();
                m.quantity = o.quantity();
                m.filledQty = o.filledQty();
                m.remainingQty = o.remainingQty();
                m.stopPrice = o.stopPrice();
                m.displayQty = o.displayQty();
                m.expiresAtMs = o.expiresAtMs();
                m.holdAssetId = o.holdAssetId();
                m.holdAmount = o.holdAmount();
                m.clientOrderIdHigh = o.clientOrderIdHigh();
                m.clientOrderIdLow = o.clientOrderIdLow();
                m.meOrderId = o.meOrderId();
                m.trailingArm = o.trailingArm();
                m.hiddenQty = o.hiddenQty();
                m.sliceSeq = o.sliceSeq();
                m.budget = o.budget();
                m.createdAtMs = o.createdAtMs();
                m.updatedAtMs = o.updatedAtMs();
                h.onOrderSnapshotEntry(m);
            }
            return;
        }
        case tid::ORDER_SNAPSHOT_END:
        {
            money::OrderSnapshotEnd d(buffer, offset, length, block, version);
            OrderSnapshotEnd m;
            m.correlationId = d.correlationId();
            m.entryCount = d.entryCount();
            m.nextOrderId = d.nextOrderId();
            m.asOfLogPosition = d.asOfLogPosition();
            h.onOrderSnapshotEnd(m);
            return;
        }
        case tid::USER_BALANCES:
        {
            money::UserBalances d(buffer, offset, length, block, version);
            UserBalancesHead m;
            m.correlationId = d.correlationId();
            m.userId = d.userId();
            m.known = flagOf(d.knownRaw(), "known");
            m.asOfLogPosition = d.asOfLogPosition();
            money::UserBalances::Balances &lines = d.balances();
            m.lineCount = static_cast<std::uint16_t>(lines.count());
            h.onUserBalances(m);
            while (lines.hasNext())
            {
                money::UserBalances::Balances &l = lines.next();
                BalanceLine line;
                line.assetId = l.assetId();
                line.available = l.available();
                line.locked = l.locked();
                h.onUserBalanceLine(m.correlationId, line);
            }
            return;
        }
        case tid::CORE_CONFIG_REPORT:
        {
            money::CoreConfigReport d(buffer, offset, length, block, version);
            CoreConfigHead m;
            m.correlationId = d.correlationId();
            m.configVersion = d.configVersion();
            m.behaviorVersion = d.behaviorVersion();
            m.limitsVersion = d.limitsVersion();
            m.requestTtlMs = d.requestTtlMs();
            m.requestPerUserCap = d.requestPerUserCap();
            m.orderEventRingCapacity = d.orderEventRingCapacity();
            m.gateState = enumOf<money::GateState>(d.gateStateRaw(), "gateState");
            m.gateReasons = gateReasonsOf(d.gateReasons().rawValue(), "gateReasons");
            m.asOfLogPosition = d.asOfLogPosition();
            money::CoreConfigReport::Markets &markets = d.markets();
            m.marketCount = static_cast<std::uint16_t>(markets.count());
            // the head carries both counts; the assets group starts after the markets group, so a second decoder
            // walks the markets to read the asset count (the frame is validated already: both are plain reads)
            {
                money::CoreConfigReport probe(buffer, offset, length, block, version);
                money::CoreConfigReport::Markets &skip = probe.markets();
                while (skip.hasNext())
                {
                    skip.next();
                }
                m.assetCount = static_cast<std::uint16_t>(probe.assets().count());
            }
            h.onCoreConfigReport(m);
            while (markets.hasNext())
            {
                money::CoreConfigReport::Markets &k = markets.next();
                MarketConfig c;
                c.marketId = k.marketId();
                std::memcpy(c.symbol.data(), k.symbol(), c.symbol.size());
                c.baseAssetId = k.baseAssetId();
                c.quoteAssetId = k.quoteAssetId();
                c.tickSize = k.tickSize();
                c.minPrice = k.minPrice();
                c.maxPrice = k.maxPrice();
                c.lotSize = k.lotSize();
                c.minQuantity = k.minQuantity();
                c.maxQuantity = k.maxQuantity();
                c.minNotional = k.minNotional();
                c.maxNotional = k.maxNotional();
                c.riskConfigVersion = k.riskConfigVersion();
                c.gateState = enumOf<money::GateState>(k.gateStateRaw(), "markets.gateState");
                c.gateReasons = gateReasonsOf(k.gateReasons().rawValue(), "markets.gateReasons");
                c.circuitBreaker = flagOf(k.circuitBreakerRaw(), "markets.circuitBreaker");
                h.onCoreConfigMarket(c);
            }
            money::CoreConfigReport::Assets &assets = d.assets();
            while (assets.hasNext())
            {
                money::CoreConfigReport::Assets &a = assets.next();
                AssetConfig c;
                c.assetId = a.assetId();
                std::memcpy(c.code.data(), a.code(), c.code.size());
                c.scale = a.scale();
                h.onCoreConfigAsset(c);
            }
            return;
        }
        case tid::BALANCE_UPDATE:
        {
            money::BalanceUpdate d(buffer, offset, length, block, version);
            BalanceUpdate m;
            m.userId = d.userId();
            m.assetId = d.assetId();
            m.available = d.available();
            m.locked = d.locked();
            h.onBalanceUpdate(m);
            return;
        }
        case tid::BALANCE_UPDATE_BATCH:
        {
            money::BalanceUpdateBatch d(buffer, offset, length, block, version);
            money::BalanceUpdateBatch::Updates &updates = d.updates();
            h.onBalanceUpdateBatch(static_cast<std::uint16_t>(updates.count()));
            while (updates.hasNext())
            {
                money::BalanceUpdateBatch::Updates &u = updates.next();
                BalanceUpdate m;
                m.userId = u.userId();
                m.assetId = u.assetId();
                m.available = u.available();
                m.locked = u.locked();
                h.onBalanceUpdate(m);
            }
            return;
        }
        case tid::DEPOSIT_ACK:
        {
            money::DepositAck d(buffer, offset, length, block, version);
            h.onDepositAck(moneyAckOf(d));
            return;
        }
        case tid::WITHDRAW_ACK:
        {
            money::WithdrawAck d(buffer, offset, length, block, version);
            h.onWithdrawAck(moneyAckOf(d));
            return;
        }
        case tid::DEPOSIT_REJECT:
        {
            money::DepositReject d(buffer, offset, length, block, version);
            DepositReject m;
            m.correlationId = d.correlationId();
            m.requestIdHigh = d.requestIdHigh();
            m.requestIdLow = d.requestIdLow();
            m.userId = d.userId();
            m.assetId = d.assetId();
            m.amount = d.amount();
            m.reason = enumOf<money::RejectReason>(d.reasonRaw(), "reason");
            h.onDepositReject(m);
            return;
        }
        case tid::WITHDRAW_REJECT:
        {
            money::WithdrawReject d(buffer, offset, length, block, version);
            WithdrawReject m;
            m.correlationId = d.correlationId();
            m.userId = d.userId();
            m.assetId = d.assetId();
            m.amount = d.amount();
            m.reason = enumOf<money::RejectReason>(d.reasonRaw(), "reason");
            m.duplicate = flagOf(d.duplicateRaw(), "duplicate");
            h.onWithdrawReject(m);
            return;
        }
        default:
            throw std::logic_error("deliver: a layout without a reader");
    }
}

template<typename M>
M *wrapCommand(M &encoder, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    if (nullptr == buffer || capacity < HEADER_LENGTH + M::sbeBlockLength())
    {
        return nullptr;
    }
    encoder.wrapAndApplyHeader(reinterpret_cast<char *>(buffer), 0, capacity);
    return &encoder;
}

template<typename M>
std::size_t lengthOf(const M &) noexcept
{
    return HEADER_LENGTH + M::sbeBlockLength();
}

template<typename M>
std::size_t encodeMoney(const MoneyCommand &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    M e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId)
        .userId(c.userId)
        .assetId(c.assetId)
        .amount(c.amount)
        .requestIdHigh(c.requestIdHigh)
        .requestIdLow(c.requestIdLow);
    return lengthOf(e);
}

}

const char *templateName(std::uint16_t templateId) noexcept
{
    if (const Layout *layout = decodedLayout(templateId))
    {
        return layout->name;
    }
    if (const int slot = ignoredSlot(templateId); slot >= 0)
    {
        return IGNORED[slot].name;
    }
    switch (templateId)
    {
        case tid::DEPOSIT: return "Deposit";
        case tid::WITHDRAW: return "Withdraw";
        case tid::CREATE_ORDER: return "CreateOrder";
        case tid::CANCEL_ORDER: return "CancelOrder";
        case tid::AMEND_ORDER: return "AmendOrder";
        case tid::REQUEST_ORDER_SNAPSHOT: return "RequestOrderSnapshot";
        case tid::REQUEST_ORDER_EVENTS: return "RequestOrderEvents";
        case tid::REQUEST_USER_BALANCES: return "RequestUserBalances";
        case tid::REQUEST_CORE_CONFIG: return "RequestCoreConfig";
        case tid::REGISTER_SESSION: return "RegisterSession";
        default: return "unknown";
    }
}

std::size_t encodeDeposit(const MoneyCommand &command, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    return encodeMoney<money::Deposit>(command, buffer, capacity);
}

std::size_t encodeWithdraw(const MoneyCommand &command, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    return encodeMoney<money::Withdraw>(command, buffer, capacity);
}

std::size_t encode(const CreateOrder &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::CreateOrder e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
#ifdef GW_NC_SWAPPED_FIELD
    // test-only control: price and quantity swapped; the golden parity row must fail naming CreateOrder
    const std::int64_t price = c.quantity;
    const std::int64_t quantity = c.price;
#else
    const std::int64_t price = c.price;
    const std::int64_t quantity = c.quantity;
#endif
    e.correlationId(c.correlationId)
        .userId(c.userId)
        .clientOrderIdHigh(c.clientOrderIdHigh)
        .clientOrderIdLow(c.clientOrderIdLow)
        .requestIdHigh(c.requestIdHigh)
        .requestIdLow(c.requestIdLow)
        .marketId(c.marketId)
        .side(c.side)
        .orderType(c.orderType)
        .timeInForce(c.timeInForce)
        .price(price)
        .quantity(quantity)
        .budget(c.budget)
        .stopPrice(c.stopPrice)
        .trailingDelta(c.trailingDelta)
        .displayQty(c.displayQty)
        .expiresAtMs(c.expiresAtMs);
    e.flags().rawValue(c.flags);
    return lengthOf(e);
}

std::size_t encode(const CancelOrder &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::CancelOrder e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId)
        .userId(c.userId)
        .orderId(c.orderId)
        .requestIdHigh(c.requestIdHigh)
        .requestIdLow(c.requestIdLow)
        .origClientOrderIdHigh(c.origClientOrderIdHigh)
        .origClientOrderIdLow(c.origClientOrderIdLow);
    return lengthOf(e);
}

std::size_t encode(const AmendOrder &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::AmendOrder e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId)
        .userId(c.userId)
        .orderId(c.orderId)
        .requestIdHigh(c.requestIdHigh)
        .requestIdLow(c.requestIdLow)
        .newPrice(c.newPrice)
        .newQuantity(c.newQuantity);
    return lengthOf(e);
}

std::size_t encode(const RequestOrderSnapshot &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::RequestOrderSnapshot e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId).userId(c.userId).fromOrderId(c.fromOrderId);
    return lengthOf(e);
}

std::size_t encode(const RequestOrderEvents &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::RequestOrderEvents e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId).fromEventSeq(c.fromEventSeq).maxEvents(c.maxEvents);
    return lengthOf(e);
}

std::size_t encode(const RequestUserBalances &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::RequestUserBalances e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId).userId(c.userId);
    return lengthOf(e);
}

std::size_t encode(const RequestCoreConfig &c, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    money::RequestCoreConfig e;
    if (nullptr == wrapCommand(e, buffer, capacity))
    {
        return 0;
    }
    e.correlationId(c.correlationId);
    return lengthOf(e);
}

std::size_t maxCommandLength() noexcept
{
    std::uint64_t block = 0;
    for (const std::uint64_t b : { money::Deposit::sbeBlockLength(), money::Withdraw::sbeBlockLength(),
             money::CreateOrder::sbeBlockLength(), money::CancelOrder::sbeBlockLength(),
             money::AmendOrder::sbeBlockLength(), money::RequestOrderSnapshot::sbeBlockLength(),
             money::RequestOrderEvents::sbeBlockLength(), money::RequestUserBalances::sbeBlockLength(),
             money::RequestCoreConfig::sbeBlockLength() })
    {
        block = b > block ? b : block;
    }
    return HEADER_LENGTH + block;
}

const char *nameOf(Refusal refusal) noexcept
{
    switch (refusal)
    {
        case Refusal::NONE: return "none";
        case Refusal::TRUNCATED: return "truncated";
        case Refusal::WRONG_SCHEMA: return "wrong_schema";
        case Refusal::WRONG_VERSION: return "wrong_version";
        case Refusal::UNKNOWN_TEMPLATE: return "unknown_template";
        case Refusal::WRONG_BLOCK_LENGTH: return "wrong_block_length";
        case Refusal::WRONG_GROUP_BLOCK_LENGTH: return "wrong_group_block_length";
        case Refusal::TRAILING_BYTES: return "trailing_bytes";
        case Refusal::BAD_VALUE: return "bad_value";
        case Refusal::DECODE_FAULT: return "decode_fault";
        case Refusal::HANDLER_FAULT: return "handler_fault";
    }
    return "?";
}

std::uint64_t DecodeCounters::refusedTotal() const noexcept
{
    std::uint64_t total = 0;
    for (const std::uint64_t n : refused)
    {
        total += n;
    }
    return total;
}

std::uint64_t DecodeCounters::ignoredTotal() const noexcept
{
    std::uint64_t total = 0;
    for (const std::uint64_t n : ignored)
    {
        total += n;
    }
    return total;
}

std::string describe(const RefusalRecord &r)
{
    std::string out = std::string("money frame refused: ") + nameOf(r.reason);
    switch (r.reason)
    {
        case Refusal::WRONG_SCHEMA:
            out += " (schema " + std::to_string(r.schemaId) + ", this build reads schema " +
                std::to_string(MONEY_SCHEMA_ID) + " only)";
            break;
        case Refusal::WRONG_VERSION:
            out += " (version " + std::to_string(r.version) + ", this build reads schema " +
                std::to_string(MONEY_SCHEMA_ID) + " version " + std::to_string(MONEY_SCHEMA_VERSION) + " only)";
            break;
        case Refusal::WRONG_BLOCK_LENGTH:
            out += " (block length " + std::to_string(r.blockLength) + ")";
            break;
        case Refusal::WRONG_GROUP_BLOCK_LENGTH:
        case Refusal::BAD_VALUE:
        case Refusal::HANDLER_FAULT:
            out += std::string(" (") + r.field + ")";
            break;
        default:
            break;
    }
    if (Refusal::TRUNCATED != r.reason || r.length >= HEADER_LENGTH)
    {
        out += ", template " + std::to_string(r.templateId) + " " + templateName(r.templateId);
    }
    out += ", " + std::to_string(r.length) + " bytes";
    return out;
}

Refusal Decoder::refuse(const RefusalRecord &record) noexcept
{
    m_last = record;
    m_counters.refused[static_cast<std::size_t>(record.reason)]++;
    return record.reason;
}

Refusal Decoder::decode(const std::uint8_t *payload, std::size_t length, ReplyHandler &handler) noexcept
{
    RefusalRecord r;
    r.length = length;
    if (nullptr == payload || length < HEADER_LENGTH)
    {
        r.reason = Refusal::TRUNCATED;
        return refuse(r);
    }
    r.blockLength = readU16(payload + 0);
    r.templateId = readU16(payload + 2);
    r.schemaId = readU16(payload + 4);
    r.version = readU16(payload + 6);
    // the schema first: another schema's version number says nothing about this one
    if (MONEY_SCHEMA_ID != r.schemaId)
    {
        r.reason = Refusal::WRONG_SCHEMA;
        return refuse(r);
    }
#ifdef GW_NC_NO_VERSION_GATE
    // test-only control: the exact version gate off; the version rows must fail
    if (false)
#else
    if (MONEY_SCHEMA_VERSION != r.version)
#endif
    {
        r.reason = Refusal::WRONG_VERSION;
        return refuse(r);
    }
    const Layout *layout = decodedLayout(r.templateId);
    const int ignored = nullptr == layout ? ignoredSlot(r.templateId) : -1;
    if (ignored >= 0)
    {
        layout = &IGNORED[ignored];
    }
    if (nullptr == layout)
    {
        r.reason = Refusal::UNKNOWN_TEMPLATE;
        return refuse(r);
    }
    if (layout->blockLength != r.blockLength)
    {
        r.reason = Refusal::WRONG_BLOCK_LENGTH;
        return refuse(r);
    }
    std::uint64_t position = HEADER_LENGTH + layout->blockLength;
    if (position > length)
    {
        r.reason = Refusal::TRUNCATED;
        return refuse(r);
    }
    for (int g = 0; g < layout->groups; ++g)
    {
        constexpr std::uint64_t GROUP_HEADER = 4; // groupSizeEncoding: blockLength uint16, numInGroup uint16
        if (position + GROUP_HEADER > length)
        {
            r.reason = Refusal::TRUNCATED;
            r.field = layout->groupName[g];
            return refuse(r);
        }
        const std::uint64_t entryLength = readU16(payload + position);
        const std::uint64_t count = readU16(payload + position + 2);
        if (entryLength != layout->groupBlockLength[g])
        {
            r.reason = Refusal::WRONG_GROUP_BLOCK_LENGTH;
            r.field = layout->groupName[g];
            return refuse(r);
        }
        position += GROUP_HEADER + count * entryLength;
        if (position > length)
        {
            r.reason = Refusal::TRUNCATED;
            r.field = layout->groupName[g];
            return refuse(r);
        }
    }
    if (position != length)
    {
        r.reason = Refusal::TRAILING_BYTES;
        return refuse(r);
    }
    if (ignored >= 0)
    {
        m_counters.ignored[static_cast<std::size_t>(ignored)]++; // well formed, read by no one
        return Refusal::NONE;
    }
    // Pass 1: the whole frame decoded into a handler that keeps nothing. Every value of every entry is read and
    // checked here; the owner's handler sees no part of the frame unless this pass completed.
    try
    {
        ReplyHandler dryRun;
        deliver(*layout, payload, length, r.version, dryRun);
    }
    catch (const BadValue &bad)
    {
        r.reason = Refusal::BAD_VALUE;
        r.field = bad.field;
        return refuse(r);
    }
    catch (...) // a codec exception, std or not: a defect, never past this noexcept boundary
    {
        r.reason = Refusal::DECODE_FAULT;
        return refuse(r);
    }
    // Pass 2: the same reads, now delivered. The bytes decoded once already, so the only thing that can throw here is
    // the owner's handler, which must not (the handler contract in Wire.h): if it does, delivery stops at that call.
    try
    {
        deliver(*layout, payload, length, r.version, handler);
    }
    catch (const BadValue &bad) // unreachable: pass 1 read the same bytes
    {
        r.reason = Refusal::BAD_VALUE;
        r.field = bad.field;
        return refuse(r);
    }
    catch (...)
    {
        r.reason = Refusal::HANDLER_FAULT;
        r.field = "handler threw";
        return refuse(r);
    }
    m_counters.accepted++;
    return Refusal::NONE;
}

}}}
