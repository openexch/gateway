#include "gateway/common/OrderEntry.h"

#include <type_traits>

namespace openexchange { namespace gateway {

namespace session {

std::size_t encodeCommand(Command &command, std::int64_t correlationId, std::uint8_t *buffer, std::size_t capacity) noexcept
{
    return std::visit(
        [&](auto &c) -> std::size_t {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, Deposit>)
            {
                c.command.correlationId = correlationId;
                return wire::encodeDeposit(c.command, buffer, capacity);
            }
            else if constexpr (std::is_same_v<T, Withdraw>)
            {
                c.command.correlationId = correlationId;
                return wire::encodeWithdraw(c.command, buffer, capacity);
            }
            else if constexpr (std::is_same_v<T, wire::CreateOrder> || std::is_same_v<T, wire::CancelOrder> ||
                std::is_same_v<T, wire::AmendOrder>)
            {
                c.correlationId = correlationId;
                return wire::encode(c, buffer, capacity);
            }
            else
            {
                return encodeReadCommand(command, correlationId, buffer, capacity);
            }
        },
        command);
}

}

}}
