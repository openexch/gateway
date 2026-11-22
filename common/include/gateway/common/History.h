#ifndef GATEWAY_COMMON_HISTORY_H
#define GATEWAY_COMMON_HISTORY_H

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

// History - the read side of the history projection (sql/history_read_contract_v1.sql): the statements oe-gateway
// sends, the opaque page cursor, the JSON of one row, and the store interface the routes call (PgHistoryStore is the
// PostgreSQL one). The gateway only reads these relations; the history projection creates and writes them.
//
// Read contract v1 (the DDL file is the reference):
//   gateway_order_history_v1  the Order columns plus user_id; key (updated_at_ms, order_id), newest first
//   gateway_fills_v1          the Fill columns plus user_id; key (executed_at_ms, trade_id, order_id), newest first
//   gateway_order_trail_v1    one row per journal record of an order (order_id, user_id, event_seq, event, status, the
//                             core and matching-engine positions, the trade, hold_remaining, at_ms); key event_seq,
//                             oldest first. Ordered by event_seq and by nothing else: a log position is not unique (the
//                             FILL records of one trade batch share one)
//   gateway_history_heartbeat_v1 (beat_at timestamptz, applied_order_id bigint, applied_event_seq bigint, epoch
//                             bigint): beat_at is the instant as of which the projection holds every event of its
//                             genesis (it beats only when it has applied the core's head as a cluster probe read it);
//                             applied_order_id the highest order id it has applied, applied_event_seq its cursor (every
//                             event up to it is applied, committed with the rows), epoch the core genesisId it
//                             projects. ageMs = the database clock minus the newest beat_at, in whole milliseconds,
//                             never below 0 (one clock, the database's: no skew between hosts). The projection writes
//                             every order from its creation on, so an order id above applied_order_id without a row may
//                             exist and not be projected yet: GET /orders/{orderId} answers 503 (LAGGING), never 404
//
// Balance movements (GET /api/v2/balances/movements) are read from another database, the core money database, through
// one relation of its own (the core projector's migration V002; sql/money_read_contract_v1.sql is the gateway's
// reference copy):
//   gateway_balance_movements_v1  one row per balance-after leg of the money journal (user_id, journal_seq, leg_no,
//                             log_position, kind, asset_id, delta, balance_after, at_ms, trade_id, market_id); key
//                             (journal_seq, leg_no), newest first. Display only: nothing replays settlement from it
// That database has no heartbeat relation, so a movements page carries no asOf and none of the heartbeat checks below
// apply to it; the owner filter (user_id = the principal) is in its statement as in every other.
//
// The checks of a history read, in this order (the first that fails answers 503 HISTORY_UNAVAILABLE): PostgreSQL
// reachable and answering (cause pg); a heartbeat row (cause heartbeat); its epoch equal to the genesisId of the
// gateway's last CoreConfigReport (cause epoch; a gateway that never read a report cannot know, the same 503); its age
// at most GATEWAY_HISTORY_MAX_AGE_MS when that is set (cause stale); the LAGGING rule of GET /orders/{orderId}; then
// the rows, with asOf {ageMs, stale (ageMs above GATEWAY_HISTORY_STALE_MS), eventSeq (applied_event_seq)}.
// Amounts are BIGINT integer units at the wire's scale 10^8, rendered as the REST decimal strings; enum columns hold
// the core's names; times are BIGINT epoch milliseconds (core cluster time).
//
// Paging is keyset only: a page is the rows strictly below the cursor's key in the key order, LIMIT n + 1 to learn
// whether another page exists. No offset, no total. Every row whose key does not change during a walk is returned
// exactly once, whatever is inserted meanwhile. An order updated during a walk takes a newer key (updated_at_ms): one
// the walk has already served is not served again, and one it has not reached yet moves above the cursor and is not
// served by that walk at all (skipped, not met again); a walk that starts over from the top serves it. This follows
// from the contract's key and is the documented behaviour of the orders route.
//
// Cursor: base64url (unpadded) of "<tag>|<binding>|<key...>": tag o1 (orders), f1 (fills), t1 (an order's trail: key
// event_seq, then the order id) or m1 (balance movements: key journal_seq, then leg_no), binding the first 8 bytes of
// SHA-256 over the tag and the filter (marketId, status, orderId; assetId for movements) as hex. A cursor of another route or another
// filter, or one that does not decode, is refused (the route answers 400 VALIDATION). The cursor carries no user: the
// user is always the principal's, so a cursor only ever positions a walk inside the caller's own rows.
//
// Every statement is a fixed text with $n placeholders; the values travel as parameters, never inside the text.
//
// The trail of an order (GET /api/v2/orders/{orderId}/trail): the caller's own order only, the owner filter in the
// statement (user_id = the principal), never in the gateway after the rows. An empty trail page is checked with
// trailExistsStatement: no row of the caller for that order is NOT_FOUND at or below applied_order_id (unknown and
// another user's order: one answer) and LAGGING above it, as GET /orders/{orderId}.
//
// Test-only controls (never in a published build): GW_NC_OFFSET_PAGING pages with OFFSET (the cursor carries the
// count of rows served) instead of the key; the PG test history_keyset_walk_under_concurrent_inserts must then fail
// with "row skipped or repeated". GW_NC_TRAIL_NO_OWNER drops the owner filter of the trail statements; the PG test
// history_order_trail must then fail with "trail of another user's order". The store's and the routes' controls are
// named in PgHistoryStore.h and GatewayApi.h.

namespace openexchange { namespace gateway { namespace history {

enum class Kind : std::uint8_t
{
    ORDERS,
    FILLS,
    TRAIL,
    MOVEMENTS
};

struct Filter
{
    std::int32_t marketId = 0;  // 0: any
    std::string status;         // "": any (orders only)
    std::int64_t orderId = 0;   // 0: any (fills); the order of a trail (always set)
    std::int32_t assetId = -1;  // -1: any (movements; asset ids start at 0)
};

// orders: a = updated_at_ms, b = order_id; fills: a = executed_at_ms, b = trade_id, c = order_id; trail: a = event_seq,
// b = order_id; movements: a = journal_seq, b = leg_no
struct Key
{
    std::int64_t a = 0;
    std::int64_t b = 0;
    std::int64_t c = 0;
};

struct PageRequest
{
    Kind kind = Kind::ORDERS;
    std::int64_t userId = 0;
    Filter filter;
    std::optional<Key> after; // the decoded cursor
    std::int64_t limit = 0;
    std::map<std::int32_t, std::string> assetCodes{}; // movements: assetId -> code from the core config (rendering only)
};

struct Statement
{
    std::string sql;
    std::vector<std::string> params;
};

// The page statement (LIMIT limit + 1). A trail page is oldest first, by event_seq only.
Statement pageStatement(const PageRequest &request);
// Whether the user has any trail row of the order (one row "1", or none).
Statement trailExistsStatement(std::int64_t userId, std::int64_t orderId);
// One order of the user by id (GET /orders/{orderId} of an order the core does not hold open).
Statement orderStatement(std::int64_t userId, std::int64_t orderId);
// One row: the projection's age in milliseconds, its applied_order_id, applied_event_seq and the lowest and highest
// epoch (all NULL when the heartbeat relation holds no row).
const std::string &heartbeatStatement();

std::string encodeCursor(Kind kind, const Filter &filter, const Key &key);
enum class CursorError : std::uint8_t
{
    NONE,
    MALFORMED,
    OTHER_ROUTE,
    OTHER_FILTER
};
CursorError decodeCursor(std::string_view text, Kind kind, const Filter &filter, Key &out);
// The cursor that follows a page of `served` rows whose last key is `last`.
std::string nextCursor(const PageRequest &request, const Key &last, std::size_t served);

// The OrderStatus names a status filter may name (the core's).
bool knownStatus(std::string_view name);

// One row as libpq gives it (text values; nullopt = SQL NULL), in the column order of the statements above.
using Row = std::vector<std::optional<std::string>>;
// The JSON of one row: the Order shape (hold.remaining only when the row has it) or the Fill shape. False when the row
// does not have the contract's shape (a NULL in a NOT NULL column, a value that is not an integer): `problem` names it
// and the row's key as stored (order_id, or trade_id and order_id), so an operator can find the row.
bool orderJson(const Row &row, std::string &json, Key &key, std::string &problem);
bool fillJson(const Row &row, std::string &json, Key &key, std::string &problem);
// One trail row: eventSeq, event, status, coreLogPosition, me {epoch, egressSeq, journalSeq, at} (null when the record
// has no matching-engine cause, me_epoch 0; egressSeq is the ME egress sequence the journal stores as me_log_position,
// at its me_timestamp_ms), liquidity (null for NONE), tradeId, lastPrice, lastQty (all three null
// when trade_id is 0), holdRemaining, at.
bool trailJson(const Row &row, std::string &json, Key &key, std::string &problem);
// One balance movement: journalSeq, legNo, logPosition, kind, assetId, asset (the code from `assetCodes`, null when the
// config names none), delta (signed), balanceAfter, at (null for opening), tradeId and marketId (null unless settle).
// A row without an asset, a kind or a delta is not the contract's shape (false).
bool movementJson(const Row &row, const std::map<std::int32_t, std::string> &assetCodes, std::string &json, Key &key,
    std::string &problem);

}

// ---- the store the routes call

enum class HistoryStatus : std::uint8_t
{
    OK,
    NOT_FOUND,
    UNAVAILABLE
};

// Why history could not answer: in logs and metrics only, never in the public body (one 503 HISTORY_UNAVAILABLE).
enum class HistoryCause : std::uint8_t
{
    NONE,
    UNCONFIGURED, // GATEWAY_PG_URL unset
    ABSENT,       // a relation or column of the read contract does not exist (SQLSTATE 42P01, 42703)
    UNREACHABLE,  // no connection (connect failed, or inside GATEWAY_PG_RETRY_MS after one), or it was lost
    TIMEOUT,      // the statement timeout or the client deadline ended the statement
    NO_HEARTBEAT, // the heartbeat relation holds no row: the projection never reported
    BAD_ROW,      // a row that does not have the contract's shape
    ERROR,        // any other server error
    LAGGING,      // GET /orders/{orderId}: no row, and the id is above the projection's applied_order_id
    EPOCH,        // the heartbeat's epoch is not the genesisId of the gateway's last CoreConfigReport (or none was read)
    STALE         // the heartbeat is older than GATEWAY_HISTORY_MAX_AGE_MS (only when that is set)
};
const char *nameOf(HistoryCause c) noexcept;
inline constexpr int HISTORY_CAUSES = 11;
// The cause as /health and the catch order name it: pg (unconfigured, absent, unreachable, timeout, bad_row, error),
// heartbeat, epoch, stale, lagging; "" for NONE.
const char *groupOf(HistoryCause c) noexcept;

// What a history read is checked against, taken when the read is queued.
struct HistoryExpect
{
    std::int64_t genesisId = 0; // of the gateway's last CoreConfigReport; 0: none read (every read is cause epoch)
    std::int64_t maxAgeMs = 0;  // GATEWAY_HISTORY_MAX_AGE_MS; 0: off
};

// The heartbeat as one read found it.
struct HistoryAsOf
{
    std::int64_t ageMs = 0;
    std::int64_t eventSeq = 0;      // applied_event_seq: every event up to it is in the rows read after it
    std::int64_t appliedOrderId = 0;
    std::int64_t epoch = 0;
};

struct HistoryAnswer
{
    HistoryStatus status = HistoryStatus::UNAVAILABLE;
    HistoryCause cause = HistoryCause::NONE;
};

struct HistoryPage
{
    std::vector<std::string> items; // JSON objects, newest first
    std::optional<std::string> nextCursor;
    std::int64_t ageMs = 0;
    std::int64_t eventSeq = 0;      // the heartbeat's applied_event_seq, read before the rows
};

// Every call is synchronous and runs on a history worker (WorkPool gw-pg-hist, the index is `worker`): never an I/O
// thread, never the duty thread, never a completion thread. `deadlineNs` (steady clock, nanoseconds) is the one
// deadline of the whole read, stamped when the route queued it: connect, heartbeat and rows together spend only what
// is left of it (UNAVAILABLE, TIMEOUT once it has passed).
class HistoryStore
{
public:
    virtual ~HistoryStore() = default;
    virtual HistoryAnswer page(int worker, const history::PageRequest &request, HistoryPage &out,
        std::int64_t deadlineNs, const HistoryExpect &expect) = 0;
    // A trail page (Kind::TRAIL) answers NOT_FOUND or LAGGING by the rule of order() below when the user has no trail
    // row of that order.
    // NOT_FOUND when the user has no such order (unknown, or another user's: one answer for both) and the id is at or
    // below the projection's applied_order_id; above it, UNAVAILABLE with cause LAGGING. `asOf` is the heartbeat the
    // lookup was checked against (the answer's asOf)
    virtual HistoryAnswer order(int worker, std::int64_t userId, std::int64_t orderId, std::string &json,
        HistoryAsOf &asOf, std::int64_t deadlineNs, const HistoryExpect &expect) = 0;
    // The heartbeat alone, through the same checks as a read (the /health probe)
    virtual HistoryAnswer probe(int worker, HistoryAsOf &out, std::int64_t deadlineNs, const HistoryExpect &expect) = 0;
    // A balance movements page (Kind::MOVEMENTS) from the money database: no heartbeat, no asOf (ageMs and eventSeq of
    // `out` stay 0). A store that does not read that database answers UNAVAILABLE, UNCONFIGURED.
    virtual HistoryAnswer movements(int worker, const history::PageRequest &request, HistoryPage &out,
        std::int64_t deadlineNs)
    {
        (void)worker;
        (void)request;
        (void)out;
        (void)deadlineNs;
        return HistoryAnswer{ HistoryStatus::UNAVAILABLE, HistoryCause::UNCONFIGURED };
    }
    virtual std::string state() const = 0;
};

}}

#endif
