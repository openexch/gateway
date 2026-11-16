// Built with oe-stream's flags (GW_NO_ORDER_ENTRY): must not compile. tests/CMakeLists.txt expects the refusal text of
// OrderEntry.h.
#include <cstdint>

#include "gateway/common/OrderEntry.h"

std::size_t probeEncodesAnOrder(std::uint8_t *buffer, std::size_t capacity)
{
    openexchange::gateway::wire::CreateOrder c;
    return openexchange::gateway::wire::encode(c, buffer, capacity);
}
