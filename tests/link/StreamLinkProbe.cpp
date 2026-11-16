// Declares the CreateOrder encoder itself (around OrderEntry.h) and calls it. Linked like oe-stream (without
// gw_order_entry) it must fail to link; linked with gw_order_entry (the control) it links. tests/CMakeLists.txt.
#include <cstddef>
#include <cstdint>

#include "gateway/common/Wire.h"

namespace openexchange { namespace gateway { namespace wire {
std::size_t encode(const CreateOrder &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
}}}

int main()
{
    std::uint8_t buffer[256];
    openexchange::gateway::wire::CreateOrder c;
    return openexchange::gateway::wire::encode(c, buffer, sizeof(buffer)) > 0 ? 0 : 1;
}
