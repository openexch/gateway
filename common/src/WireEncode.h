#ifndef GATEWAY_COMMON_SRC_WIRE_ENCODE_H
#define GATEWAY_COMMON_SRC_WIRE_ENCODE_H

#include <cstddef>
#include <cstdint>

#include "gateway/common/Wire.h"

// The two pieces every command encoder shares (Wire.cpp: the queries; OrderEntry.cpp: the writes). Private to the
// library sources.

namespace openexchange { namespace gateway { namespace wire { namespace detail {

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

}}}}

#endif
