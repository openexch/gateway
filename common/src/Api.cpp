#include "gateway/common/Api.h"

#include <optional>
#include <set>

#include <boost/json.hpp>

namespace openexchange { namespace gateway { namespace api {

namespace json = boost::json;

namespace {

bool ieq(std::string_view a, std::string_view b)
{
    if (a.size() != b.size())
    {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i)
    {
        if ((a[i] | 0x20) != (b[i] | 0x20))
        {
            return false;
        }
    }
    return true;
}

// Parameter names that would carry a credential in a URL: refused by name so a client learns at once.
bool credentialName(std::string_view name)
{
    for (const char *n : { "token", "access_token", "apikey", "api_key", "api-key", "jwt", "authorization", "bearer" })
    {
        if (ieq(name, n))
        {
            return true;
        }
    }
    return false;
}

// %XX and '+' decoded (a parameter name cannot hide from the checks by encoding); nullopt for a bad escape
std::optional<std::string> percentDecoded(std::string_view text)
{
    std::string out;
    for (std::size_t i = 0; i < text.size(); ++i)
    {
        const char c = text[i];
        if ('+' == c)
        {
            out += ' ';
        }
        else if ('%' == c)
        {
            auto hexValue = [](char h) -> int {
                return h >= '0' && h <= '9' ? h - '0' : (h | 0x20) >= 'a' && (h | 0x20) <= 'f' ? (h | 0x20) - 'a' + 10 : -1;
            };
            if (i + 2 >= text.size())
            {
                return std::nullopt;
            }
            const int hi = hexValue(text[i + 1]);
            const int lo = hexValue(text[i + 2]);
            if (hi < 0 || lo < 0)
            {
                return std::nullopt;
            }
            out += static_cast<char>(hi * 16 + lo);
            i += 2;
        }
        else
        {
            out += c;
        }
    }
    return out;
}

bool isUserIdKey(std::string_view key)
{
    return ieq(key, "userId") || ieq(key, "user_id");
}

}

std::string quote(std::string_view text)
{
    return json::serialize(json::string(text));
}

ApiResponse error(ErrorSpec s)
{
    ApiResponse r;
    r.status = s.status;
    r.body = "{\"error\":" + quote(s.text) + ",\"code\":" + quote(s.code) + ",\"reason\":" +
        (s.reason ? quote(*s.reason) : "null") + ",\"detail\":" + (s.detail ? quote(*s.detail) : "null") +
        ",\"keyFinal\":" + (s.keyFinal ? "true" : "false") + "}";
    if (s.retryAfterMs >= 0)
    {
        const std::int64_t seconds = std::max<std::int64_t>(1, (s.retryAfterMs + 999) / 1000);
        r.headers.emplace_back("Retry-After", std::to_string(seconds));
    }
    return r;
}

ApiResponse error(unsigned status, const char *c, std::string text, std::optional<std::string> detail)
{
    ErrorSpec s;
    s.status = status;
    s.code = c;
    s.text = std::move(text);
    s.detail = std::move(detail);
    return error(std::move(s));
}

ApiResponse jsonBody(unsigned status, std::string body)
{
    ApiResponse r;
    r.status = status;
    r.body = std::move(body);
    return r;
}

ApiResponse noContent()
{
    ApiResponse r;
    r.status = 204;
    return r;
}

bool namesAUser(const json::value &v, std::string &where)
{
    if (v.is_object())
    {
        for (const auto &kv : v.as_object())
        {
            if (isUserIdKey(kv.key()))
            {
                where = std::string(kv.key());
                return true;
            }
            if (namesAUser(kv.value(), where))
            {
                where = std::string(kv.key()) + "." + where;
                return true;
            }
        }
    }
    else if (v.is_array())
    {
        std::size_t i = 0;
        for (const json::value &item : v.as_array())
        {
            if (namesAUser(item, where))
            {
                where = "[" + std::to_string(i) + "]." + where;
                return true;
            }
            ++i;
        }
    }
    return false;
}

std::optional<json::object> readBody(const HttpExchange &ex, std::initializer_list<std::string_view> allowed,
    ApiResponse &bad)
{
    const auto type = ex.header("Content-Type");
    const std::string_view t = type ? *type : std::string_view();
    const std::string_view media = t.substr(0, t.find(';'));
    if (!ieq(media, "application/json"))
    {
        bad = error(400, code::VALIDATION, "the body must be JSON", "Content-Type must be application/json");
        return std::nullopt;
    }
    boost::system::error_code ec;
    json::value v = json::parse(ex.body(), ec);
    if (ec)
    {
        bad = error(400, code::VALIDATION, "the body is not valid JSON", ec.message());
        return std::nullopt;
    }
    if (!v.is_object())
    {
        bad = error(400, code::VALIDATION, "the body must be a JSON object");
        return std::nullopt;
    }
    std::string where;
#ifndef GW_NC_BODY_USER
    if (namesAUser(v, where))
    {
        bad = error(400, code::VALIDATION, "a request cannot name a user: the credential decides it",
            "field '" + where + "' is refused");
        return std::nullopt;
    }
#endif
    for (const auto &kv : v.as_object())
    {
#ifdef GW_NC_BODY_USER
        if (isUserIdKey(kv.key()))
        {
            continue; // test-only control: a userId in the body is accepted (the route reads it)
        }
#endif
        bool known = false;
        for (const std::string_view a : allowed)
        {
            known = known || kv.key() == a;
        }
        if (!known)
        {
            bad = error(400, code::VALIDATION, "unknown field", "field '" + std::string(kv.key()) + "' is not part of this request");
            return std::nullopt;
        }
    }
    return std::move(v.as_object());
}

bool readQuery(const HttpExchange &ex, std::initializer_list<std::string_view> allowed, ApiResponse &bad)
{
    std::string_view q = ex.query();
    std::set<std::string> seen;
    while (!q.empty())
    {
        const std::size_t amp = q.find('&');
        const std::string_view item = q.substr(0, amp);
        q = std::string_view::npos == amp ? std::string_view() : q.substr(amp + 1);
        if (item.empty())
        {
            continue;
        }
        const auto decoded = percentDecoded(item.substr(0, item.find('=')));
        if (!decoded)
        {
            bad = error(400, code::VALIDATION, "malformed query string", "a parameter name has a bad %-escape");
            return false;
        }
        const std::string &name = *decoded;
        if (isUserIdKey(name))
        {
            bad = error(400, code::VALIDATION, "a request cannot name a user: the credential decides it",
                "query parameter '" + std::string(name) + "' is refused");
            return false;
        }
        if (credentialName(name))
        {
            bad = error(400, code::VALIDATION, "a credential is never read from a URL",
                "query parameter '" + std::string(name) + "' is refused: send Authorization or X-API-Key");
            return false;
        }
        bool known = false;
        for (const std::string_view a : allowed)
        {
            known = known || name == a;
        }
        if (!known)
        {
            bad = error(400, code::VALIDATION, "unknown query parameter",
                "query parameter '" + std::string(name) + "' is not part of this request");
            return false;
        }
        if (!seen.insert(name).second)
        {
            bad = error(400, code::VALIDATION, "a query parameter is repeated",
                "query parameter '" + std::string(name) + "' is given twice");
            return false;
        }
    }
    return true;
}

}}}
