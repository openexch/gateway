#include "CandleRoute.h"

#include <algorithm>

#include "gateway/common/Api.h"
#include "gateway/common/WorkPool.h"

#include "BookFrames.h"

namespace openexchange { namespace gateway { namespace marketdata {

namespace {

RestAnswer fromApi(ApiResponse r)
{
    RestAnswer a;
    a.status = r.status;
    a.body = std::move(r.body);
    a.headers = std::move(r.headers);
    return a;
}

RestAnswer invalid(const std::string &field, const std::string &text)
{
    return fromApi(api::error(400, api::code::VALIDATION, text, field));
}

// digits only, at most 18 of them (no sign, no space)
std::optional<std::int64_t> digits(std::string_view v)
{
    if (v.empty() || v.size() > 18)
    {
        return std::nullopt;
    }
    std::int64_t out = 0;
    for (const char c : v)
    {
        if (c < '0' || c > '9')
        {
            return std::nullopt;
        }
        out = out * 10 + (c - '0');
    }
    return out;
}

}

std::int32_t intervalSeconds(std::string_view interval) noexcept
{
    static constexpr std::pair<std::string_view, std::int32_t> TABLE[] = { { "1m", 60 }, { "5m", 300 }, { "15m", 900 },
        { "1h", 3600 }, { "4h", 14'400 }, { "1d", 86'400 } };
    for (const auto &e : TABLE)
    {
        if (e.first == interval)
        {
            return e.second;
        }
    }
    return 0;
}

bool parseCandleQuery(std::string_view query, const std::vector<int> &expectedMarkets, std::int64_t limitMax,
    CandleQuery &out, RestAnswer &bad)
{
    std::map<std::string, std::string, std::less<>> values;
    while (!query.empty())
    {
        const std::size_t amp = query.find('&');
        const std::string_view part = query.substr(0, amp);
        query = std::string_view::npos == amp ? std::string_view() : query.substr(amp + 1);
        if (part.empty())
        {
            continue;
        }
        const std::size_t eq = part.find('=');
        const std::string name(part.substr(0, eq));
        const std::string value(std::string_view::npos == eq ? std::string_view() : part.substr(eq + 1));
        if ("marketId" != name && "interval" != name && "limit" != name)
        {
            bad = invalid(name, "unknown query parameter '" + name + "': marketId, interval and limit are accepted");
            return false;
        }
        if (!values.emplace(name, value).second)
        {
            bad = invalid(name, name + " is given more than once");
            return false;
        }
    }
    const auto field = [&](const char *name) -> std::optional<std::string> {
        const auto it = values.find(name);
        return values.end() == it ? std::nullopt : std::optional<std::string>(it->second);
    };
    const std::optional<std::string> market = field("marketId");
    if (!market || market->empty())
    {
        bad = invalid("marketId", "marketId is required");
        return false;
    }
    const std::optional<std::int64_t> id = digits(*market);
    if (!id || expectedMarkets.end() == std::find(expectedMarkets.begin(), expectedMarkets.end(), *id))
    {
        bad = invalid("marketId", "marketId '" + *market + "' is not a market served here");
        return false;
    }
    const std::optional<std::string> interval = field("interval");
    if (!interval || interval->empty())
    {
        bad = invalid("interval", "interval is required: 1m, 5m, 15m, 1h, 4h or 1d");
        return false;
    }
    const std::int32_t width = intervalSeconds(*interval);
    if (0 == width)
    {
        bad = invalid("interval", "interval '" + *interval + "' is not one of 1m, 5m, 15m, 1h, 4h, 1d");
        return false;
    }
    const std::optional<std::string> limit = field("limit");
    if (!limit || limit->empty())
    {
        bad = invalid("limit", "limit is required: a positive integer");
        return false;
    }
    const std::optional<std::int64_t> n = digits(*limit);
    if (!n || *n < 1)
    {
        bad = invalid("limit", "limit '" + *limit + "' is not a positive integer");
        return false;
    }
    out.marketId = static_cast<std::int32_t>(*id);
    out.interval = *interval;
    out.intervalS = width;
    out.limit = std::min(*n, limitMax);
    return true;
}

CandleRoute::CandleRoute(CandleRouteSettings settings, Read read) :
    m_settings(std::move(settings)), m_read(std::move(read)),
    m_pool(std::make_unique<WorkPool>("md-rest", 1, m_settings.queue)), m_conn(std::make_unique<PgConn>())
{
}

CandleRoute::~CandleRoute()
{
    stop(2000);
}

void CandleRoute::start()
{
    m_pool->start();
}

bool CandleRoute::stop(std::int64_t boundMs)
{
    return m_pool->stop(boundMs);
}

bool CandleRoute::owns(std::string_view target) noexcept
{
    return target.substr(0, target.find('?')) == CANDLES_PATH;
}

bool CandleRoute::pgRead(const CandleQuery &q, std::vector<CandleRow> &out, std::string &error)
{
    if (!m_conn->ok() && !connectHistory(*m_conn, m_settings.pg, error))
    {
        return false;
    }
    if (!readCandles(*m_conn, m_settings.pg.statementTimeoutMs, q.marketId, q.intervalS, q.limit, out, error))
    {
        m_conn->close();
        return false;
    }
    return true;
}

void CandleRoute::finish(const char *route, const std::optional<std::string> &origin, RestAnswer answer,
    const Done &done)
{
    if (origin)
    {
        for (const std::string &allowed : m_settings.corsOrigins)
        {
            if (*origin == allowed)
            {
                answer.headers.emplace_back("Access-Control-Allow-Origin", allowed);
                answer.headers.emplace_back("Vary", "Origin");
                break;
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_countMutex);
        m_requests[{ route, answer.status }]++;
    }
    done(std::move(answer));
}

void CandleRoute::handle(std::string_view method, std::string_view target, const std::optional<std::string> &origin,
    Done done)
{
    if ("OPTIONS" == method)
    {
        for (const std::string &allowed : m_settings.corsOrigins)
        {
            if (origin && *origin == allowed)
            {
                RestAnswer a = fromApi(api::noContent());
                a.headers.emplace_back("Access-Control-Allow-Methods", "GET");
                a.headers.emplace_back("Access-Control-Max-Age", "600");
                finish("preflight", origin, std::move(a), done);
                return;
            }
        }
        finish("preflight", std::nullopt, fromApi(api::error(403, api::code::FORBIDDEN, "origin not allowed",
            std::string("the Origin is not listed in MD_CORS_ORIGINS"))), done);
        return;
    }
    if ("GET" != method)
    {
        RestAnswer a = fromApi(api::error(405, api::code::METHOD_NOT_ALLOWED, "only GET is served on /api/candles"));
        a.headers.emplace_back("Allow", "GET, OPTIONS");
        finish("candles", origin, std::move(a), done);
        return;
    }
    const std::size_t q = target.find('?');
    CandleQuery query;
    RestAnswer bad;
    if (!parseCandleQuery(std::string_view::npos == q ? std::string_view() : target.substr(q + 1),
            m_settings.expectedMarkets, m_settings.limitMax, query, bad))
    {
        finish("candles", origin, std::move(bad), done);
        return;
    }
    auto shared = std::make_shared<Done>(std::move(done));
    const bool taken = m_pool->trySubmit([this, query, origin, shared](int) {
        std::vector<CandleRow> rows;
        std::string error;
        const bool ok = m_read ? m_read(query, rows, error) : pgRead(query, rows, error);
        if (!ok)
        {
#ifdef MD_NC_REST_FROM_MEMORY
            // control build only (never published): the candles in hand, as if they were the whole series
            finish("candles", origin, RestAnswer{ 200, renderCandlesBody(query.marketId, query.interval, {}), {} },
                *shared);
            return;
#endif
            api::ErrorSpec spec;
            spec.status = 503;
            spec.code = api::code::HISTORY_UNAVAILABLE;
            spec.text = "the market history is not available";
            spec.retryAfterMs = 1000;
            finish("candles", origin, fromApi(api::error(std::move(spec))), *shared);
            return;
        }
        finish("candles", origin, RestAnswer{ 200, renderCandlesBody(query.marketId, query.interval, rows), {} },
            *shared);
    });
    if (!taken)
    {
        api::ErrorSpec spec;
        spec.status = 503;
        spec.code = api::code::HISTORY_BUSY;
        spec.text = "too many candle requests are waiting";
        spec.retryAfterMs = 1000;
        finish("candles", origin, fromApi(api::error(std::move(spec))), *shared);
    }
}

std::map<std::pair<std::string, unsigned>, std::uint64_t> CandleRoute::requests() const
{
    std::lock_guard<std::mutex> lock(m_countMutex);
    return m_requests;
}

}}}
