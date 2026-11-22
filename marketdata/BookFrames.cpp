#include "BookFrames.h"

#include "gateway/common/Admin.h"
#include "gateway/common/Rest.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace fwd = cluster_tools::domain::forwarder;

namespace {

std::string dec(std::int64_t units)
{
#ifdef MD_NC_NUMBER_PRICE
    // control build only (never published): a price or quantity written as a JSON number
    return rest::formatMoney(units);
#else
    return "\"" + rest::formatMoney(units) + "\"";
#endif
}

std::string marketField(int marketId)
{
#ifdef MD_NC_MARKETID_STRING
    // control build only (never published): marketId written as a JSON string
    return "\"marketId\":\"" + std::to_string(marketId) + "\"";
#else
    return "\"marketId\":" + std::to_string(marketId);
#endif
}

std::string bookVersionField(std::int64_t version)
{
#ifdef MD_NC_NO_BOOK_VERSION
    // control build only (never published): bookVersion left out
    (void)version;
    return "";
#else
    return ",\"bookVersion\":" + std::to_string(version);
#endif
}

void level(std::string &out, std::int64_t price, std::int64_t quantity, std::int32_t orderCount)
{
    out += "{\"price\":" + dec(price) + ",\"quantity\":" + dec(quantity) + ",\"orderCount\":" +
        std::to_string(orderCount) + "}";
}

const char *sideName(std::uint8_t raw)
{
    return fwd::BOOK_SIDE_BID == raw ? "BID" : "ASK";
}

const char *updateName(std::uint8_t raw)
{
    switch (raw)
    {
        case fwd::BOOK_UPDATE_NEW: return "NEW_LEVEL";
        case fwd::BOOK_UPDATE_UPDATE: return "UPDATE_LEVEL";
        default: break;
    }
    return "DELETE_LEVEL";
}

}

std::string renderBookSnapshot(int marketId, const fwd::BookLadder *ladder, std::int64_t bookVersion,
    std::int64_t timestampMs, bool stale)
{
    const bool levels = nullptr != ladder && fwd::LadderState::OK == ladder->state();
    std::string out = "{\"type\":\"BOOK_SNAPSHOT\"," + marketField(marketId) + ",\"bids\":[";
    for (int i = 0; levels && i < ladder->bidCount(); ++i)
    {
        out += 0 == i ? "" : ",";
        level(out, ladder->bid(i).price, ladder->bid(i).quantity, ladder->bid(i).orderCount);
    }
    out += "],\"asks\":[";
    for (int i = 0; levels && i < ladder->askCount(); ++i)
    {
        out += 0 == i ? "" : ",";
        level(out, ladder->ask(i).price, ladder->ask(i).quantity, ladder->ask(i).orderCount);
    }
    out += "]" + bookVersionField(bookVersion) + ",\"timestamp\":" + std::to_string(timestampMs);
    if (stale)
    {
        out += ",\"stale\":true";
    }
    return out + "}";
}

std::string renderBookDelta(int marketId, const fwd::BookFrame &f, const BookStamps &stamps)
{
    std::string out = "{\"type\":\"BOOK_DELTA\"," + marketField(marketId) + ",\"changes\":[";
    for (int i = 0; i < f.nChanges; ++i)
    {
        const fwd::BookChange &c = f.changes[i];
        out += (0 == i ? "" : ",") + std::string("{\"price\":") + dec(c.price) + ",\"quantity\":" + dec(c.quantity) +
            ",\"orderCount\":" + std::to_string(c.orderCount) + ",\"side\":\"" + sideName(c.side) +
#ifdef MD_NC_RENAMED_UPDATE_TYPE
            // control build only (never published): the change's updateType under another name
            "\",\"type\":\"" +
#else
            "\",\"updateType\":\"" +
#endif
            updateName(c.updateType) + "\"}";
    }
    out += "],\"bidVersion\":" + std::to_string(stamps.bidVersion) + ",\"askVersion\":" +
        std::to_string(stamps.askVersion) + bookVersionField(f.bookVersion) + ",\"fromVersion\":" +
        std::to_string(f.fromVersion) + ",\"timestamp\":" + std::to_string(stamps.timestampMs) + "}";
    return out;
}

std::string renderSubscribed(int marketId)
{
    return "{\"type\":\"SUBSCRIPTION_CONFIRMED\",\"status\":\"subscribed\"," + marketField(marketId) + "}";
}

std::string renderPong(std::int64_t timestampMs)
{
    return "{\"type\":\"PONG\",\"timestamp\":" + std::to_string(timestampMs) + "}";
}

std::string renderError(const std::string &code, const std::string &message, const std::vector<std::string> &parts)
{
    std::string out = "{\"type\":\"ERROR\",\"code\":\"" + jsonEscape(code) + "\",\"message\":\"" + jsonEscape(message) +
        "\"";
    if (!parts.empty())
    {
        out += ",\"parts\":[";
        for (std::size_t i = 0; i < parts.size(); ++i)
        {
            out += (0 == i ? "\"" : ",\"") + jsonEscape(parts[i]) + "\"";
        }
        out += "]";
    }
    return out + "}";
}

}}}
