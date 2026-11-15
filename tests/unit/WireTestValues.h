#ifndef GATEWAY_TESTS_WIRE_TEST_VALUES_H
#define GATEWAY_TESTS_WIRE_TEST_VALUES_H

#include <cstdint>
#include <cstring>

#include "gateway/common/Wire.h"

// The field values tests/golden/GoldenGateway.java writes: template t, group entry e, field k (the schema field id).
// The same two formulas on both sides; every field of every golden frame gets its own value.

namespace gwtest { namespace values {

using namespace openexchange::gateway::wire;

inline std::int64_t v(int t, int e, int k)
{
    return (static_cast<std::int64_t>(t) << 40) | (static_cast<std::int64_t>(e) << 32) |
        (static_cast<std::int64_t>(k) << 24) | 0x5A5A;
}

inline std::int32_t i(int t, int e, int k)
{
    return (t << 20) | (e << 12) | (k << 4) | 0xA;
}

enum class EventShape
{
    SINGLE, // OrderEvent 102
    BATCH,  // OrderEventBatch 103
    PAGE,   // OrderEventsPage 115
};

inline OrderEvent expectedEvent(int t, int e, EventShape shape)
{
    OrderEvent x;
    x.eventSeq = v(t, e, 1);
    x.logPosition = v(t, e, 2);
    x.orderId = v(t, e, 3);
    x.userId = v(t, e, 4);
    x.marketId = i(t, e, 5);
    switch (shape)
    {
        case EventShape::SINGLE:
            x.eventKind = money::EventKind::REPLACED;
            x.status = money::OrderStatus::PENDING_REPLACE;
            x.reason = money::CoreRejectReason::ME_RELEASED;
            x.side = money::Side::SELL;
            x.orderType = money::OrderType::ICEBERG;
            x.timeInForce = money::TimeInForce::FOK;
            break;
        case EventShape::BATCH:
            x.eventKind = money::EventKind::FILL;
            x.status = money::OrderStatus::PARTIALLY_FILLED;
            x.reason = money::CoreRejectReason::NONE;
            x.side = money::Side::BUY;
            x.orderType = money::OrderType::LIMIT;
            x.timeInForce = money::TimeInForce::GTC;
            break;
        case EventShape::PAGE:
            x.eventKind = money::EventKind::EXPIRED;
            x.status = money::OrderStatus::EXPIRED;
            x.reason = money::CoreRejectReason::EXPIRED;
            x.side = money::Side::SELL;
            x.orderType = money::OrderType::TRAILING_STOP;
            x.timeInForce = money::TimeInForce::GTD;
            break;
    }
    x.price = v(t, e, 12);
    x.stopPrice = v(t, e, 13);
    x.quantity = v(t, e, 14);
    x.filledQty = v(t, e, 15);
    x.remainingQty = v(t, e, 16);
    x.lastPrice = v(t, e, 17);
    x.lastQty = v(t, e, 18);
    x.tradeId = v(t, e, 19);
    x.meOrderId = v(t, e, 20);
    x.holdAssetId = i(t, e, 21);
    x.holdAmount = v(t, e, 22);
    x.holdRemaining = v(t, e, 23);
    x.clientOrderIdHigh = v(t, e, 24);
    x.clientOrderIdLow = v(t, e, 25);
    x.createdAtMs = v(t, e, 26);
    x.updatedAtMs = v(t, e, 27);
    x.meReason = static_cast<std::uint8_t>(200 + e);
    return x;
}

inline OrderSnapshotEntry expectedSnapshotEntry(int e)
{
    const int t = 106;
    OrderSnapshotEntry x;
    x.orderId = v(t, e, 1);
    x.userId = v(t, e, 2);
    x.marketId = i(t, e, 3);
    x.status = money::OrderStatus::PENDING_TRIGGER;
    x.side = money::Side::BUY;
    x.orderType = money::OrderType::STOP_LOSS;
    x.timeInForce = money::TimeInForce::IOC;
    x.price = v(t, e, 8);
    x.quantity = v(t, e, 9);
    x.filledQty = v(t, e, 10);
    x.remainingQty = v(t, e, 11);
    x.stopPrice = v(t, e, 12);
    x.displayQty = v(t, e, 13);
    x.expiresAtMs = v(t, e, 14);
    x.holdAssetId = i(t, e, 15);
    x.holdAmount = v(t, e, 16);
    x.clientOrderIdHigh = v(t, e, 17);
    x.clientOrderIdLow = v(t, e, 18);
    x.meOrderId = v(t, e, 19);
    x.trailingArm = v(t, e, 20);
    x.hiddenQty = v(t, e, 21);
    x.sliceSeq = i(t, e, 22);
    x.budget = v(t, e, 23);
    x.createdAtMs = v(t, e, 24);
    x.updatedAtMs = v(t, e, 25);
    return x;
}

inline MarketConfig expectedMarket(int e)
{
    const int t = 118;
    static const char *const SYMBOLS[] = { "BTC-USDT", "ETH-USDT-PERP-01" };
    MarketConfig x;
    x.marketId = i(t, e, 1);
    std::memcpy(x.symbol.data(), SYMBOLS[e - 1], std::strlen(SYMBOLS[e - 1]));
    x.baseAssetId = i(t, e, 3);
    x.quoteAssetId = i(t, e, 4);
    x.tickSize = v(t, e, 5);
    x.minPrice = v(t, e, 6);
    x.maxPrice = v(t, e, 7);
    x.lotSize = v(t, e, 8);
    x.minQuantity = v(t, e, 9);
    x.maxQuantity = v(t, e, 10);
    x.minNotional = v(t, e, 11);
    x.maxNotional = v(t, e, 12);
    x.riskConfigVersion = static_cast<std::uint32_t>(i(t, e, 13));
    x.gateState = 1 == e ? money::GateState::OPEN : money::GateState::SUSPENDED;
    x.gateReasons = 1 == e ? 0u : (1u << 4);
    x.circuitBreaker = 2 == e;
    return x;
}

inline AssetConfig expectedAsset(int e)
{
    const int t = 118;
    static const char *const CODES[] = { "BTC", "USDTUSDT" };
    AssetConfig x;
    x.assetId = i(t, e + 4, 1);
    std::memcpy(x.code.data(), CODES[e - 1], std::strlen(CODES[e - 1]));
    x.scale = static_cast<std::uint8_t>(6 + e);
    return x;
}

}}

#endif
