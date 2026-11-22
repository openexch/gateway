#ifndef GATEWAY_MARKETDATA_BOOK_FRAMES_H
#define GATEWAY_MARKETDATA_BOOK_FRAMES_H

#include <cstdint>
#include <string>
#include <vector>

#include "cluster_tools/domain/forwarder/BookLadder.h"
#include "cluster_tools/domain/forwarder/MarketData.h"

// BookFrames - the JSON text of the public market frames this build sends, one function per frame. Pure, no state.
//
// Prices and quantities are decimal strings with at most 8 fraction digits, formatted from the int64 wire values with
// integer arithmetic (rest::formatMoney, the v2 API's convention): no double exists anywhere here. The envelope ids the
// edge relay inspects stay JSON numbers: marketId, bookVersion, fromVersion, timestamp. Neither `market` (a name; the
// names come from the gateway's market list) nor the old snapshot `version` is sent. Field order is fixed, so a frame
// is a byte-for-byte golden value.
//
//   BOOK_SNAPSHOT  {"type","marketId","bids":[level],"asks":[level],"bookVersion","timestamp"[,"stale":true]}
//   level          {"price","quantity","orderCount"}
//   BOOK_DELTA     {"type","marketId","changes":[change],"bidVersion","askVersion","bookVersion","fromVersion","timestamp"}
//   change         {"price","quantity","orderCount","side":"BID"|"ASK","updateType":"NEW_LEVEL"|"UPDATE_LEVEL"|"DELETE_LEVEL"}
//   SUBSCRIPTION_CONFIRMED {"type","status":"subscribed","marketId"}
//   PONG           {"type","timestamp"}
//   ERROR          {"type","code","message"[,"parts":[...]]}

namespace openexchange { namespace gateway { namespace marketdata {

// The ME frame fields the ladder does not keep (the 22 / 25 root block): the ME's timestamp (ms) and the two side
// versions.
struct BookStamps
{
    std::int64_t timestampMs = 0;
    std::int64_t bidVersion = 0;
    std::int64_t askVersion = 0;
};

// `ladder` null or not OK: no level is listed (the levels of a ladder that cannot prove its state are never sent).
std::string renderBookSnapshot(int marketId, const cluster_tools::domain::forwarder::BookLadder *ladder,
    std::int64_t bookVersion, std::int64_t timestampMs, bool stale);
std::string renderBookDelta(int marketId, const cluster_tools::domain::forwarder::BookFrame &frame,
    const BookStamps &stamps);
std::string renderSubscribed(int marketId);
std::string renderPong(std::int64_t timestampMs);
std::string renderError(const std::string &code, const std::string &message,
    const std::vector<std::string> &parts = {});

}}}

#endif
