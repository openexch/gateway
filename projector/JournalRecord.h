#ifndef GATEWAY_PROJECTOR_JOURNAL_RECORD_H
#define GATEWAY_PROJECTOR_JOURNAL_RECORD_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "gateway/common/Wire.h"

// JournalRecord - one record of the core order journal (schema 5, version 1, template OrderJournalEvent 1): the 216
// message bytes an archive replay delivers (an 8-byte SBE header, then a 208-byte block, little endian):
//   @8   journalSeq      int64   the core eventSeq: dense, from 1
//   @16  epoch           int64   the recorded CoreConfig genesisId
//   @24  logPosition     int64   the core log position of the applying entry
//   @32  the OrderEvent fields orderId .. meReason (151 bytes: bytes 16..166 of the OrderEvent block, template 102)
//   @183 liquidity       uint8   0 NONE, 1 TAKER, 2 MAKER
//   @184 meEpoch         int64   0 when the record is not caused by a matching-engine record
//   @192 meJournalSeq    int64
//   @200 meLogPosition   int64
//   @208 meTimestampMs   int64
// The 151 middle bytes are the OrderEvent body, so they are read by the gateway's own OrderEvent decoder (wire::Decoder
// on a rebuilt OrderEvent frame: journalSeq and logPosition in its first two fields): one codec and one set of enum
// rules for the stream, the history reader and this projector.
//
// The projected rows are a function of these bytes only ("event to columns" below); the JSON of a row is the history
// reader's (History.h), the JSON of a live event the stream's (Rest.h), and the two agree by construction.
//
// Test-only controls (never in a published build): PROJ_NC_SECOND_NAME_TABLE (the side column from a name table of its
// own), PROJ_NC_FILL_ZERO_TRADE (a FILL with tradeId 0 gets a fills row), PROJ_NC_NO_ENUM_CHECK (a record whose enum
// value has no name is accepted).

namespace openexchange { namespace gateway { namespace projector {

inline constexpr std::size_t RECORD_LENGTH = 216; // one message: header 8 + block 208 (a format constant)
inline constexpr std::uint16_t RECORD_SCHEMA_ID = 5;
inline constexpr std::uint16_t RECORD_SCHEMA_VERSION = 1;
inline constexpr std::uint16_t RECORD_TEMPLATE_ID = 1;
inline constexpr std::uint16_t RECORD_BLOCK_LENGTH = 208;

enum class Liquidity : std::uint8_t
{
    NONE = 0,
    TAKER = 1,
    MAKER = 2
};
const char *nameOf(Liquidity l) noexcept;

using Payload = std::array<std::uint8_t, RECORD_LENGTH>;

struct JournalRecord
{
    std::int64_t seq = 0;
    std::int64_t epoch = 0;
    std::int64_t logPosition = 0;
    wire::OrderEvent event; // eventSeq == seq, logPosition == logPosition
    Liquidity liquidity = Liquidity::NONE;
    std::int64_t meEpoch = 0;
    std::int64_t meJournalSeq = 0;
    std::int64_t meLogPosition = 0;
    std::int64_t meTimestampMs = 0;
    Payload payload{};
};

// Decodes one record. False with `why` for anything the projector refuses to write: a wrong header, length, schema,
// version, template or block length; a sequence below 1; epoch 0; an enum value the projector has no name for
// (eventKind, status, reason, side, orderType, timeInForce: the field is named; liquidity above 2); an order id below 1.
bool decodeRecord(const std::uint8_t *bytes, std::size_t length, JournalRecord &out, std::string &why);

// The bytes of a record (the inverse of decodeRecord): the test stories and the fixtures write records with it.
Payload encodeRecord(const JournalRecord &record);

// ---- event to columns (text parameters; nullopt = SQL NULL)

using Columns = std::vector<std::optional<std::string>>;

// oe_order_events: journal_seq, order_id, user_id, event, status, core_log_position, liquidity, me_epoch,
// me_journal_seq, me_log_position, me_timestamp_ms, trade_id, last_price, last_qty, hold_remaining, at_ms, payload (hex)
Columns eventColumns(const JournalRecord &r);
// oe_orders, the read contract's 21 columns in its order (order_id, user_id, client_order_id, market_id, side, type,
// time_in_force, price, stop_price, quantity, filled_quantity, remaining_quantity, status, reason, me_reason,
// me_order_id, hold_asset_id, hold_amount, hold_remaining, created_at_ms, updated_at_ms), then last_event_seq and
// last_log_position. The null rules are the stream's (Rest.cpp orderJson): no price for a price-less type, stop price
// 0, reason NONE, meReason 255 and meOrderId 0 are NULL; client_order_id 0/0 is NULL.
Columns orderColumns(const JournalRecord &r);
// A fills row: a FILL record with tradeId > 0 and lastQty > 0.
bool hasFill(const JournalRecord &r);
// oe_fills: trade_id, order_id, user_id, market_id, side, price, quantity, maker, executed_at_ms, event_seq,
// core_log_position, me_epoch, me_journal_seq, me_log_position, me_timestamp_ms (maker = liquidity MAKER; executed_at_ms
// = updatedAtMs, the stream's executedAt)
Columns fillColumns(const JournalRecord &r);

// Lower-case hex of `n` bytes.
std::string hexOf(const std::uint8_t *bytes, std::size_t n);
// SHA-256 of the bytes, lower-case hex (the conflict line names both values).
std::string sha256Hex(const std::uint8_t *bytes, std::size_t n);

}}}

#endif
