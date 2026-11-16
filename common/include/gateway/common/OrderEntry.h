#ifndef GATEWAY_COMMON_ORDER_ENTRY_H
#define GATEWAY_COMMON_ORDER_ENTRY_H

#include <cstddef>
#include <cstdint>

#include "gateway/common/SessionParts.h"
#include "gateway/common/Wire.h"

// OrderEntry - the encoders of every command that writes: CreateOrder 50, CancelOrder 51, AmendOrder 52 and the
// treasury lane's Deposit 1 and Withdraw 2, and the session encoder that includes them. They live in their own library
// (gw_order_entry), linked by oe-gateway and the tests and never by oe-stream: the stream's key is a GATEWAY key that
// the core would let create orders, so the stream binary must hold no code path that can encode one. A target built
// with GW_NO_ORDER_ENTRY (oe-stream and its library) cannot include this header, and it does not link the library, so
// a reference from anywhere in it fails the build (compile or link); tests/link checks both, and the symbols of the
// finished binary.

#ifdef GW_NO_ORDER_ENTRY
#error "this target must not encode CreateOrder, CancelOrder, AmendOrder, Deposit or Withdraw (GW_NO_ORDER_ENTRY)"
#endif

namespace openexchange { namespace gateway {

namespace wire {
std::size_t encodeDeposit(const MoneyCommand &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encodeWithdraw(const MoneyCommand &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encode(const CreateOrder &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encode(const CancelOrder &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
std::size_t encode(const AmendOrder &command, std::uint8_t *buffer, std::size_t capacity) noexcept;
}

namespace session {
// Every command: the writes above and the reads (encodeReadCommand). Writes the correlation id into the command; the
// frame length, 0 when the buffer is too short. oe-gateway's sessions use this one (SessionSettings::encoder).
std::size_t encodeCommand(Command &command, std::int64_t correlationId, std::uint8_t *buffer, std::size_t capacity) noexcept;
}

}}

#endif
