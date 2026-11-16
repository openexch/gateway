#ifndef GATEWAY_COMMON_WIRE_H
#define GATEWAY_COMMON_WIRE_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "com_openexchange_core_infrastructure_generated/AdminResult.h"
#include "com_openexchange_core_infrastructure_generated/ConfigKind.h"
#include "com_openexchange_core_infrastructure_generated/CoreRejectReason.h"
#include "com_openexchange_core_infrastructure_generated/EventKind.h"
#include "com_openexchange_core_infrastructure_generated/GateReason.h"
#include "com_openexchange_core_infrastructure_generated/GateState.h"
#include "com_openexchange_core_infrastructure_generated/MessageHeader.h"
#include "com_openexchange_core_infrastructure_generated/OrderStatus.h"
#include "com_openexchange_core_infrastructure_generated/OrderType.h"
#include "com_openexchange_core_infrastructure_generated/RejectReason.h"
#include "com_openexchange_core_infrastructure_generated/Side.h"
#include "com_openexchange_core_infrastructure_generated/TimeInForce.h"

// Wire - the money-schema frames the gateway and the stream put on the core ingress and read from the core egress,
// through the codecs cluster-tools exports (cluster_tools::sbe_codecs, generated from the schema dependencies.lock
// pins). One copy for both binaries.
//
// Encode: one plain struct per command, encode() writes header and body and returns the frame length (0 when the
// buffer is shorter than the frame). RegisterSession is not here: the cluster-tools session registrar encodes it. The
// encoders of the commands that write (50, 51, 52 and the money commands 1, 2) are in OrderEntry.h, outside this file,
// so a binary can be built without them (oe-stream).
//
// Decode: Decoder::decode() checks the frame before any field is read, and refuses it, counted by reason, when
//   - it is shorter than its header, its block or a group it declares (truncated),
//   - its header names another schema id (wrong_schema) or any version other than the exact one this build speaks
//     (wrong_version: a v7 or a v9 frame is refused by name, never read with v8 offsets),
//   - its template is neither one the gateway reads nor one it ignores (unknown_template),
//   - its block length or a group's entry block length differs from the schema's (wrong_block_length,
//     wrong_group_block_length: the version is exact, so the lengths are too),
//   - it carries bytes after its last group (trailing_bytes),
//   - an enum field holds a value the schema does not name, or a set field a bit it does not name (bad_value).
// A frame is delivered to the handler only after it decoded completely once: every value of every entry is read and
// checked before the first handler call, so a refusal never leaves a handler with part of a batch. decode() never
// throws (nothing, std::exception or not, leaves it) and never reads outside [payload, payload + length).
//
// Ignored templates (IGNORED_TEMPLATES): frames the core sends on a channel the gateway subscribes to, or that reach
// it before its channel mask applies, that the gateway has no reader for (OrderFault, a CONTROL broadcast; the
// settlement broadcasts, see below). One that passes the same header, version and length checks is
// counted by template (DecodeCounters::ignored), delivered to no one and not refused; a malformed one is refused like
// any other frame. Any other template stays unknown_template.
//
// Handler contract: a ReplyHandler method must not throw. One that throws anyway stops that frame's delivery at the
// throwing call (the calls before it stand), decode() catches whatever it threw and counts the frame as handler_fault.
//
// The exported session registrar (CoreSessionRegistrar::onEgress) reads an AdminAck at any header version; the owner
// therefore passes an egress frame to it only after decode() accepted the frame.

namespace openexchange { namespace gateway { namespace wire {

namespace money = com::openexchange::core::infrastructure::generated;

// The one money schema this build speaks: the exported codecs' own constants (CMake checks them against
// dependencies.lock at configure).
inline constexpr std::uint16_t MONEY_SCHEMA_ID = money::MessageHeader::sbeSchemaId();
inline constexpr std::uint16_t MONEY_SCHEMA_VERSION = money::MessageHeader::sbeSchemaVersion();
inline constexpr std::size_t HEADER_LENGTH = money::MessageHeader::encodedLength();

namespace tid {
inline constexpr std::uint16_t DEPOSIT = 1;
inline constexpr std::uint16_t WITHDRAW = 2;
inline constexpr std::uint16_t BALANCE_UPDATE = 12;
inline constexpr std::uint16_t WITHDRAW_REJECT = 14;
inline constexpr std::uint16_t DEPOSIT_ACK = 15;
inline constexpr std::uint16_t WITHDRAW_ACK = 16;
inline constexpr std::uint16_t BALANCE_UPDATE_BATCH = 22;
inline constexpr std::uint16_t CREATE_ORDER = 50;
inline constexpr std::uint16_t CANCEL_ORDER = 51;
inline constexpr std::uint16_t AMEND_ORDER = 52;
inline constexpr std::uint16_t REQUEST_ORDER_SNAPSHOT = 53;
inline constexpr std::uint16_t REQUEST_ORDER_EVENTS = 54;
inline constexpr std::uint16_t REQUEST_USER_BALANCES = 55;
inline constexpr std::uint16_t REQUEST_CORE_CONFIG = 56;
inline constexpr std::uint16_t REGISTER_SESSION = 80;
inline constexpr std::uint16_t ORDER_ACK = 100;
inline constexpr std::uint16_t ORDER_REJECT = 101;
inline constexpr std::uint16_t ORDER_EVENT = 102;
inline constexpr std::uint16_t ORDER_EVENT_BATCH = 103;
inline constexpr std::uint16_t ORDER_SNAPSHOT_BATCH = 106;
inline constexpr std::uint16_t ORDER_SNAPSHOT_END = 107;
inline constexpr std::uint16_t ADMISSION_STATE = 109;
inline constexpr std::uint16_t ADMIN_ACK = 110;
inline constexpr std::uint16_t DEPOSIT_REJECT = 111;
inline constexpr std::uint16_t CONFIG_APPLIED = 112;
inline constexpr std::uint16_t ORDER_EVENTS_PAGE = 115;
inline constexpr std::uint16_t ORDER_EVENTS_END = 116;
inline constexpr std::uint16_t USER_BALANCES = 117;
inline constexpr std::uint16_t ORDER_FAULT = 113;
inline constexpr std::uint16_t SETTLEMENT_APPLIED = 13;
inline constexpr std::uint16_t SETTLE_FAULT = 21;
inline constexpr std::uint16_t SETTLEMENT_APPLIED_BATCH = 24;
inline constexpr std::uint16_t CORE_CONFIG_REPORT = 118;
}

// The schema's name of a template id, "unknown" for an id outside the gateway's sets.
const char *templateName(std::uint16_t templateId) noexcept;

// The templates the gateway encodes (RegisterSession through the registrar included) and decodes.
inline constexpr std::array<std::uint16_t, 10> ENCODED_TEMPLATES = { tid::DEPOSIT, tid::WITHDRAW, tid::CREATE_ORDER,
    tid::CANCEL_ORDER, tid::AMEND_ORDER, tid::REQUEST_ORDER_SNAPSHOT, tid::REQUEST_ORDER_EVENTS,
    tid::REQUEST_USER_BALANCES, tid::REQUEST_CORE_CONFIG, tid::REGISTER_SESSION };
inline constexpr std::array<std::uint16_t, 19> DECODED_TEMPLATES = { tid::BALANCE_UPDATE, tid::WITHDRAW_REJECT,
    tid::DEPOSIT_ACK, tid::WITHDRAW_ACK, tid::BALANCE_UPDATE_BATCH, tid::ORDER_ACK, tid::ORDER_REJECT,
    tid::ORDER_EVENT, tid::ORDER_EVENT_BATCH, tid::ORDER_SNAPSHOT_BATCH, tid::ORDER_SNAPSHOT_END,
    tid::ADMISSION_STATE, tid::ADMIN_ACK, tid::DEPOSIT_REJECT, tid::CONFIG_APPLIED, tid::ORDER_EVENTS_PAGE,
    tid::ORDER_EVENTS_END, tid::USER_BALANCES, tid::CORE_CONFIG_REPORT };
// What the core sends on the channels the gateway subscribes to (ACKS and CONTROL; every reply to a command it sends
// is in DECODED_TEMPLATES) that the gateway does not read: CoreEventPublisher.onOrderFault, BROADCAST on CH_CONTROL.
// Also the CH_SETTLEMENTS broadcasts (SettlementApplied, SettleFault, SettlementAppliedBatch): the core opens a new
// session with every legacy channel until its RegisterSession lands, so these can arrive before the session's real
// mask applies.
inline constexpr std::array<std::uint16_t, 4> IGNORED_TEMPLATES = { tid::ORDER_FAULT, tid::SETTLEMENT_APPLIED,
    tid::SETTLE_FAULT, tid::SETTLEMENT_APPLIED_BATCH };

// ---- commands (gateway -> core) ----

struct MoneyCommand // Deposit 1, Withdraw 2 (the treasury lane)
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int32_t assetId = 0;
    std::int64_t amount = 0;
    std::int64_t requestIdHigh = 0;
    std::int64_t requestIdLow = 0;
    bool operator==(const MoneyCommand &) const = default;
};

struct CreateOrder
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int64_t clientOrderIdHigh = 0;
    std::int64_t clientOrderIdLow = 0;
    std::int64_t requestIdHigh = 0;
    std::int64_t requestIdLow = 0;
    std::int32_t marketId = 0;
    money::Side::Value side = money::Side::BUY;
    money::OrderType::Value orderType = money::OrderType::LIMIT;
    money::TimeInForce::Value timeInForce = money::TimeInForce::GTC;
    std::int64_t price = 0;
    std::int64_t quantity = 0;
    std::int64_t budget = 0;
    std::int64_t stopPrice = 0;
    std::int64_t trailingDelta = 0;
    std::int64_t displayQty = 0;
    std::int64_t expiresAtMs = 0;
    std::uint32_t flags = 0; // OrderFlags: every bit reserved in v8, the core refuses a nonzero value
    bool operator==(const CreateOrder &) const = default;
};

struct CancelOrder
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int64_t orderId = 0;
    std::int64_t requestIdHigh = 0; // 0/0 = unkeyed
    std::int64_t requestIdLow = 0;
    std::int64_t origClientOrderIdHigh = 0;
    std::int64_t origClientOrderIdLow = 0;
    bool operator==(const CancelOrder &) const = default;
};

struct AmendOrder
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int64_t orderId = 0;
    std::int64_t requestIdHigh = 0;
    std::int64_t requestIdLow = 0;
    std::int64_t newPrice = 0;
    std::int64_t newQuantity = 0;
    bool operator==(const AmendOrder &) const = default;
};

struct RequestOrderSnapshot
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int64_t fromOrderId = 0;
    bool operator==(const RequestOrderSnapshot &) const = default;
};

struct RequestOrderEvents
{
    std::int64_t correlationId = 0;
    std::int64_t fromEventSeq = 0;
    std::int32_t maxEvents = 0; // 0 = the head probe
    bool operator==(const RequestOrderEvents &) const = default;
};

struct RequestUserBalances
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    bool operator==(const RequestUserBalances &) const = default;
};

struct RequestCoreConfig
{
    std::int64_t correlationId = 0;
    bool operator==(const RequestCoreConfig &) const = default;
};

// The writes (Deposit, Withdraw, CreateOrder, CancelOrder, AmendOrder) are encoded by OrderEntry.h, a library oe-stream
// does not link. The reads:
std::size_t encode(const RequestOrderSnapshot &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encode(const RequestOrderEvents &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encode(const RequestUserBalances &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encode(const RequestCoreConfig &command, std::uint8_t *buffer, std::size_t capacity) noexcept;

// The longest command frame above, from the schema (every command is fixed length): a buffer of this size holds
// any of them. Derived, not a setting.
std::size_t maxCommandLength() noexcept;

// ---- replies and broadcasts (core -> gateway) ----

struct AdminAck
{
    std::int64_t correlationId = 0;
    std::uint16_t refTemplateId = 0;
    money::AdminResult::Value result = money::AdminResult::ACCEPTED;
    std::uint8_t reason = 0;
    std::uint32_t configVersion = 0;
    bool operator==(const AdminAck &) const = default;
};

struct AdmissionState
{
    std::int32_t marketId = 0;
    money::GateState::Value gateState = money::GateState::OPEN;
    money::GateReason::Value gateReason = money::GateReason::ADMIN;
    bool circuitBreaker = false;
    std::int64_t atMs = 0;
    std::int64_t logPosition = 0;
    std::uint32_t gateReasons = 0; // GateReasons bits
    bool operator==(const AdmissionState &) const = default;
};

struct ConfigApplied
{
    money::ConfigKind::Value kind = money::ConfigKind::CORE;
    std::int32_t marketId = 0;
    std::uint32_t configVersion = 0;
    std::int64_t logPosition = 0;
    bool operator==(const ConfigApplied &) const = default;
};

struct OrderAck
{
    std::int64_t correlationId = 0;
    std::int64_t requestIdHigh = 0;
    std::int64_t requestIdLow = 0;
    std::int64_t orderId = 0;
    std::int64_t userId = 0;
    std::int32_t marketId = 0;
    money::OrderStatus::Value status = money::OrderStatus::NEW;
    bool duplicate = false;
    std::int32_t holdAssetId = 0;
    std::int64_t holdAmount = 0;
    std::int64_t acceptedAtMs = 0;
    std::int64_t eventSeq = 0;
    bool operator==(const OrderAck &) const = default;
};

struct OrderReject
{
    std::int64_t correlationId = 0;
    std::int64_t requestIdHigh = 0;
    std::int64_t requestIdLow = 0;
    std::int64_t userId = 0;
    std::int64_t orderId = 0;
    std::int32_t marketId = 0;
    std::uint16_t refTemplateId = 0;
    money::CoreRejectReason::Value reason = money::CoreRejectReason::NONE;
    std::uint8_t meReason = 0;
    std::int64_t detail = 0;
    std::int64_t rejectedAtMs = 0;
    bool operator==(const OrderReject &) const = default;
};

// One order event: OrderEvent 102, an entry of OrderEventBatch 103 or of OrderEventsPage 115 (one layout).
struct OrderEvent
{
    std::int64_t eventSeq = 0;
    std::int64_t logPosition = 0;
    std::int64_t orderId = 0;
    std::int64_t userId = 0;
    std::int32_t marketId = 0;
    money::EventKind::Value eventKind = money::EventKind::ACCEPTED;
    money::OrderStatus::Value status = money::OrderStatus::NEW;
    money::CoreRejectReason::Value reason = money::CoreRejectReason::NONE;
    money::Side::Value side = money::Side::BUY;
    money::OrderType::Value orderType = money::OrderType::LIMIT;
    money::TimeInForce::Value timeInForce = money::TimeInForce::GTC;
    std::int64_t price = 0;
    std::int64_t stopPrice = 0;
    std::int64_t quantity = 0;
    std::int64_t filledQty = 0;
    std::int64_t remainingQty = 0;
    std::int64_t lastPrice = 0;
    std::int64_t lastQty = 0;
    std::int64_t tradeId = 0;
    std::int64_t meOrderId = 0;
    std::int32_t holdAssetId = 0;
    std::int64_t holdAmount = 0;
    std::int64_t holdRemaining = 0;
    std::int64_t clientOrderIdHigh = 0;
    std::int64_t clientOrderIdLow = 0;
    std::int64_t createdAtMs = 0;
    std::int64_t updatedAtMs = 0;
    std::uint8_t meReason = 0; // 255 = not ME-sourced
    bool operator==(const OrderEvent &) const = default;
};

// One entry of OrderSnapshotBatch 106.
struct OrderSnapshotEntry
{
    std::int64_t orderId = 0;
    std::int64_t userId = 0;
    std::int32_t marketId = 0;
    money::OrderStatus::Value status = money::OrderStatus::NEW;
    money::Side::Value side = money::Side::BUY;
    money::OrderType::Value orderType = money::OrderType::LIMIT;
    money::TimeInForce::Value timeInForce = money::TimeInForce::GTC;
    std::int64_t price = 0;
    std::int64_t quantity = 0;
    std::int64_t filledQty = 0;
    std::int64_t remainingQty = 0;
    std::int64_t stopPrice = 0;
    std::int64_t displayQty = 0;
    std::int64_t expiresAtMs = 0;
    std::int32_t holdAssetId = 0;
    std::int64_t holdAmount = 0;
    std::int64_t clientOrderIdHigh = 0;
    std::int64_t clientOrderIdLow = 0;
    std::int64_t meOrderId = 0;
    std::int64_t trailingArm = 0;
    std::int64_t hiddenQty = 0;
    std::int32_t sliceSeq = 0;
    std::int64_t budget = 0;
    std::int64_t createdAtMs = 0;
    std::int64_t updatedAtMs = 0;
    bool operator==(const OrderSnapshotEntry &) const = default;
};

struct OrderSnapshotEnd
{
    std::int64_t correlationId = 0;
    std::int32_t entryCount = 0;
    std::int64_t nextOrderId = 0;
    std::int64_t asOfLogPosition = 0;
    bool operator==(const OrderSnapshotEnd &) const = default;
};

struct OrderEventsEnd
{
    std::int64_t correlationId = 0;
    std::int32_t count = 0;
    std::int64_t firstAvailableSeq = 0;
    std::int64_t lastSeq = 0;
    std::int64_t nextFromSeq = 0;
    bool operator==(const OrderEventsEnd &) const = default;
};

struct UserBalancesHead
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    bool known = false;
    std::int64_t asOfLogPosition = 0;
    std::uint16_t lineCount = 0; // the balance lines that follow, one onUserBalanceLine each
    bool operator==(const UserBalancesHead &) const = default;
};

struct BalanceLine
{
    std::int32_t assetId = 0;
    std::int64_t available = 0;
    std::int64_t locked = 0;
    bool operator==(const BalanceLine &) const = default;
};

// BalanceUpdate 12, or one entry of BalanceUpdateBatch 22.
struct BalanceUpdate
{
    std::int64_t userId = 0;
    std::int32_t assetId = 0;
    std::int64_t available = 0;
    std::int64_t locked = 0;
    bool operator==(const BalanceUpdate &) const = default;
};

struct CoreConfigHead
{
    std::int64_t correlationId = 0;
    std::uint32_t configVersion = 0;
    std::uint32_t behaviorVersion = 0;
    std::uint32_t limitsVersion = 0;
    std::int64_t requestTtlMs = 0;
    std::int32_t requestPerUserCap = 0;
    std::int32_t orderEventRingCapacity = 0;
    money::GateState::Value gateState = money::GateState::OPEN;
    std::uint32_t gateReasons = 0;
    std::int64_t asOfLogPosition = 0;
    std::uint16_t marketCount = 0; // onCoreConfigMarket calls that follow
    std::uint16_t assetCount = 0;  // then onCoreConfigAsset calls
    bool operator==(const CoreConfigHead &) const = default;
};

struct MarketConfig
{
    std::int32_t marketId = 0;
    std::array<char, 16> symbol{}; // the raw 16 bytes, NUL padded unless the symbol fills them
    std::int32_t baseAssetId = 0;
    std::int32_t quoteAssetId = 0;
    std::int64_t tickSize = 0;
    std::int64_t minPrice = 0;
    std::int64_t maxPrice = 0;
    std::int64_t lotSize = 0;
    std::int64_t minQuantity = 0;
    std::int64_t maxQuantity = 0;
    std::int64_t minNotional = 0;
    std::int64_t maxNotional = 0;
    std::uint32_t riskConfigVersion = 0;
    money::GateState::Value gateState = money::GateState::OPEN;
    std::uint32_t gateReasons = 0;
    bool circuitBreaker = false;
    bool operator==(const MarketConfig &) const = default;
};

struct AssetConfig
{
    std::int32_t assetId = 0;
    std::array<char, 8> code{}; // raw 8 bytes
    std::uint8_t scale = 0;
    bool operator==(const AssetConfig &) const = default;
};

// DepositAck 15 and WithdrawAck 16 (one layout).
struct MoneyAck
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int32_t assetId = 0;
    std::int64_t amount = 0;
    std::int64_t newAvailable = 0;
    bool duplicate = false;
    bool operator==(const MoneyAck &) const = default;
};

struct WithdrawReject
{
    std::int64_t correlationId = 0;
    std::int64_t userId = 0;
    std::int32_t assetId = 0;
    std::int64_t amount = 0;
    money::RejectReason::Value reason = money::RejectReason::NONE;
    bool duplicate = false;
    bool operator==(const WithdrawReject &) const = default;
};

struct DepositReject
{
    std::int64_t correlationId = 0;
    std::int64_t requestIdHigh = 0;
    std::int64_t requestIdLow = 0;
    std::int64_t userId = 0;
    std::int32_t assetId = 0;
    std::int64_t amount = 0;
    money::RejectReason::Value reason = money::RejectReason::NONE;
    bool operator==(const DepositReject &) const = default;
};

// What a decoded frame is delivered to. Every method defaults to doing nothing; an owner overrides what it reads.
// Calls for one frame follow its layout: a head, then one call per group entry in wire order.
class ReplyHandler
{
public:
    virtual ~ReplyHandler() = default;
    virtual void onAdminAck(const AdminAck &) {}
    virtual void onAdmissionState(const AdmissionState &) {}
    virtual void onConfigApplied(const ConfigApplied &) {}
    virtual void onOrderAck(const OrderAck &) {}
    virtual void onOrderReject(const OrderReject &) {}
    virtual void onOrderEvent(const OrderEvent &) {}                          // 102, and each entry of 103
    virtual void onOrderEventBatch(std::uint16_t /*eventCount*/) {}           // 103, before its entries
    virtual void onOrderEventsPage(std::int64_t /*correlationId*/, std::uint16_t /*eventCount*/) {} // 115
    virtual void onPageEvent(std::int64_t /*correlationId*/, const OrderEvent &) {} // each entry of 115
    virtual void onOrderEventsEnd(const OrderEventsEnd &) {}
    virtual void onOrderSnapshotBatch(std::uint16_t /*entryCount*/) {}        // 106, before its entries
    virtual void onOrderSnapshotEntry(const OrderSnapshotEntry &) {}
    virtual void onOrderSnapshotEnd(const OrderSnapshotEnd &) {}
    virtual void onUserBalances(const UserBalancesHead &) {}
    virtual void onUserBalanceLine(std::int64_t /*correlationId*/, const BalanceLine &) {}
    virtual void onCoreConfigReport(const CoreConfigHead &) {}
    virtual void onCoreConfigMarket(const MarketConfig &) {}
    virtual void onCoreConfigAsset(const AssetConfig &) {}
    virtual void onBalanceUpdate(const BalanceUpdate &) {}                    // 12, and each entry of 22
    virtual void onBalanceUpdateBatch(std::uint16_t /*updateCount*/) {}       // 22, before its entries
    virtual void onDepositAck(const MoneyAck &) {}
    virtual void onWithdrawAck(const MoneyAck &) {}
    virtual void onDepositReject(const DepositReject &) {}
    virtual void onWithdrawReject(const WithdrawReject &) {}
};

enum class Refusal : std::uint8_t
{
    NONE,
    TRUNCATED,
    WRONG_SCHEMA,
    WRONG_VERSION,
    UNKNOWN_TEMPLATE,
    WRONG_BLOCK_LENGTH,
    WRONG_GROUP_BLOCK_LENGTH,
    TRAILING_BYTES,
    BAD_VALUE,
    DECODE_FAULT, // a codec exception after the checks above passed: a defect, counted, never a crash
    HANDLER_FAULT, // the handler threw while the frame was delivered (see the handler contract): counted, never a crash
};
inline constexpr std::size_t REFUSAL_COUNT = 11;

// snake_case names, as the metrics label them
const char *nameOf(Refusal refusal) noexcept;

struct DecodeCounters
{
    std::uint64_t accepted = 0;
    std::array<std::uint64_t, REFUSAL_COUNT> refused{}; // by Refusal; [NONE] stays 0
    std::array<std::uint64_t, IGNORED_TEMPLATES.size()> ignored{}; // by position in IGNORED_TEMPLATES
    std::uint64_t refusedTotal() const noexcept;
    std::uint64_t ignoredTotal() const noexcept;
    bool operator==(const DecodeCounters &) const = default;
};

// The last refused frame, for the log line.
struct RefusalRecord
{
    Refusal reason = Refusal::NONE;
    std::size_t length = 0;
    std::uint16_t templateId = 0;  // 0 when the header was not readable
    std::uint16_t schemaId = 0;
    std::uint16_t version = 0;
    std::uint16_t blockLength = 0;
    const char *field = "";        // bad_value: the field; wrong_group_block_length: the group; handler_fault: the call
    bool operator==(const RefusalRecord &) const = default;
};

// One line naming the reason and what the frame carried, e.g.
// "money frame refused: wrong_version (version 7, this build reads schema 2 version 8 only), template 110 AdminAck,
// 24 bytes".
std::string describe(const RefusalRecord &record);

// Refusals are logged rate limited, as the core logs its own: the first, then every REFUSAL_LOG_INTERVAL-th, the line
// carrying the running count. refusedSoFar counts the frame being logged.
inline constexpr std::uint64_t REFUSAL_LOG_INTERVAL = 1000;
constexpr bool refusalLogged(std::uint64_t refusedSoFar) noexcept
{
    return 1 == refusedSoFar || 0 == refusedSoFar % REFUSAL_LOG_INTERVAL;
}

class Decoder
{
public:
    // Refusal::NONE = not refused: accepted and delivered to handler, or an ignored template, counted and delivered to
    // no one (counters().ignored). Never throws.
    Refusal decode(const std::uint8_t *payload, std::size_t length, ReplyHandler &handler) noexcept;
    const DecodeCounters &counters() const noexcept { return m_counters; }
    const RefusalRecord &lastRefusal() const noexcept { return m_last; }

private:
    Refusal refuse(const RefusalRecord &record) noexcept;
    DecodeCounters m_counters;
    RefusalRecord m_last;
};

}}}

#endif
