#ifndef GATEWAY_MARKETDATA_CANDLE_ROUTE_H
#define GATEWAY_MARKETDATA_CANDLE_ROUTE_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "Candles.h"
#include "MarketHistory.h"

// CandleRoute - GET /api/candles?marketId=&interval=&limit= on oe-marketdata's public listener, answered from the
// market history (md_candles) and nothing else. There is no memory fallback: with the market history down the answer
// is 503 HISTORY_UNAVAILABLE, never a partial series that would look whole.
//
// Parameters, all three required, nothing else accepted (each named at most once): marketId, one of
// MD_EXPECTED_MARKETS; interval, one of 1m 5m 15m 1h 4h 1d; limit, a positive integer, capped at MD_CANDLES_LIMIT_MAX. A
// missing or unknown value, or another parameter, is 400 VALIDATION naming the field (no silent default). The body:
// {"marketId":N,"interval":"5m","candles":[candle]} (BookFrames.h), the newest `limit` buckets oldest first.
// Other methods are 405 (GET and OPTIONS are allowed). Errors use the v2 API's error body (gateway Api.h).
//
// CORS (MD_CORS_ORIGINS, a list or `none`): an answer to a request whose Origin is listed carries
// Access-Control-Allow-Origin with that origin and Vary: Origin; a preflight (OPTIONS) from a listed origin is 204 with
// Access-Control-Allow-Methods GET, from any other origin (or with `none`) 403 FORBIDDEN.
//
// The reads run on md-rest (one thread with its own market history connection, the writer's credentials, no writer
// lock) over a queue of MD_CANDLES_QUEUE requests; a full queue is 503 HISTORY_BUSY. The I/O threads never wait on
// PostgreSQL. A failed read closes the connection; the next request connects again.
// Test-only control (never in a published build): MD_NC_REST_FROM_MEMORY answers a failed read with 200 and the
// candles in hand (none) instead of 503.

namespace openexchange { namespace gateway { class WorkPool; } }

namespace openexchange { namespace gateway { namespace marketdata {

struct RestAnswer
{
    unsigned status = 200;
    std::string body;
    std::vector<std::pair<std::string, std::string>> headers;
};

struct CandleQuery
{
    std::int32_t marketId = 0;
    std::string interval;
    std::int32_t intervalS = 0;
    std::int64_t limit = 0;
};

inline constexpr const char *CANDLES_PATH = "/api/candles";

// The interval's width in seconds, or 0 when it is not one of 1m 5m 15m 1h 4h 1d.
std::int32_t intervalSeconds(std::string_view interval) noexcept;

// `query`: the text after '?' (may be empty). False with `bad` the 400 to answer.
bool parseCandleQuery(std::string_view query, const std::vector<int> &expectedMarkets, std::int64_t limitMax,
    CandleQuery &out, RestAnswer &bad);

struct CandleRouteSettings
{
    HistoryOptions pg;
    std::vector<std::string> corsOrigins; // empty: none
    std::vector<int> expectedMarkets;
    std::int64_t limitMax = 500;
    std::size_t queue = 64;
};

class CandleRoute
{
public:
    // A read of the newest `limit` buckets (oldest first); false with `error` when the market history did not answer.
    using Read = std::function<bool(const CandleQuery &query, std::vector<CandleRow> &out, std::string &error)>;
    using Done = std::function<void(RestAnswer)>;

    // `read` empty: the market history read on md-rest's own connection.
    explicit CandleRoute(CandleRouteSettings settings, Read read = {});
    ~CandleRoute();
    CandleRoute(const CandleRoute &) = delete;
    CandleRoute &operator=(const CandleRoute &) = delete;

    void start();
    bool stop(std::int64_t boundMs);

    // True when the request target's path is this route's.
    static bool owns(std::string_view target) noexcept;
    // Answers through `done` exactly once (on the caller's thread, or on md-rest after the read).
    void handle(std::string_view method, std::string_view target, const std::optional<std::string> &origin, Done done);

    // md_requests_total{route,code}
    std::map<std::pair<std::string, unsigned>, std::uint64_t> requests() const;

private:
    void finish(const char *route, const std::optional<std::string> &origin, RestAnswer answer, const Done &done);
    bool pgRead(const CandleQuery &query, std::vector<CandleRow> &out, std::string &error);

    CandleRouteSettings m_settings;
    Read m_read;
    std::unique_ptr<WorkPool> m_pool;
    std::unique_ptr<PgConn> m_conn; // md-rest's only
    mutable std::mutex m_countMutex;
    std::map<std::pair<std::string, unsigned>, std::uint64_t> m_requests;
};

}}}

#endif
