#include "gateway/common/OrderEntry.h"

#include "com_openexchange_core_infrastructure_generated/AmendOrder.h"
#include "com_openexchange_core_infrastructure_generated/CancelOrder.h"
#include "com_openexchange_core_infrastructure_generated/CreateOrder.h"
#include "com_openexchange_core_infrastructure_generated/Deposit.h"
#include "com_openexchange_core_infrastructure_generated/Withdraw.h"

#include "WireEncode.h"

namespace openexchange { namespace gateway {

namespace wire {

using detail::lengthOf;
using detail::wrapCommand;

namespace {

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

}

}}
