#include "gateway/common/Jwt.h"

#include <cmath>
#include <limits>
#include <optional>

#include <boost/json.hpp>

#include "gateway/common/Crypto.h"

namespace openexchange { namespace gateway { namespace jwt {

namespace json = boost::json;

namespace {

std::optional<json::object> objectOf(std::string_view b64)
{
    const auto bytes = crypto::base64UrlDecode(b64);
    if (!bytes)
    {
        return std::nullopt;
    }
    boost::system::error_code ec;
    json::value v = json::parse(*bytes, ec);
    if (ec || !v.is_object())
    {
        return std::nullopt;
    }
    return std::move(v.as_object());
}

// A NumericDate: an integer or a finite number of seconds.
std::optional<double> numericDate(const json::value &v)
{
    if (v.is_int64())
    {
        return static_cast<double>(v.as_int64());
    }
    if (v.is_uint64())
    {
        return static_cast<double>(v.as_uint64());
    }
    if (v.is_double() && std::isfinite(v.as_double()))
    {
        return v.as_double();
    }
    return std::nullopt;
}

}

const char *nameOf(Verdict v) noexcept
{
    switch (v)
    {
        case Verdict::OK: return "OK";
        case Verdict::MALFORMED: return "MALFORMED";
        case Verdict::BAD_ALGORITHM: return "BAD_ALGORITHM";
        case Verdict::BAD_SIGNATURE: return "BAD_SIGNATURE";
        case Verdict::MISSING_EXP: return "MISSING_EXP";
        case Verdict::EXPIRED: return "EXPIRED";
        case Verdict::NOT_YET_VALID: return "NOT_YET_VALID";
        case Verdict::BAD_SUBJECT: return "BAD_SUBJECT";
    }
    return "?";
}

Result verifyHs256(std::string_view token, std::string_view secret, std::int64_t nowSeconds)
{
    Result r;
    const std::size_t d1 = token.find('.');
    const std::size_t d2 = std::string_view::npos == d1 ? d1 : token.find('.', d1 + 1);
    if (std::string_view::npos == d2 || std::string_view::npos != token.find('.', d2 + 1))
    {
        return r;
    }
    const std::string_view h64 = token.substr(0, d1);
    const std::string_view p64 = token.substr(d1 + 1, d2 - d1 - 1);
    const std::string_view s64 = token.substr(d2 + 1);
    const auto header = objectOf(h64);
    const auto signature = crypto::base64UrlDecode(s64);
    if (!header || !signature)
    {
        return r;
    }
    const json::value *alg = header->if_contains("alg");
    const json::value *typ = header->if_contains("typ");
    if (nullptr == alg || !alg->is_string() || "HS256" != alg->as_string() || header->contains("crit") ||
        (nullptr != typ && (!typ->is_string() || "JWT" != typ->as_string())))
    {
        r.verdict = Verdict::BAD_ALGORITHM;
        return r;
    }
    const crypto::Digest expected = crypto::hmacSha256(secret, token.substr(0, d2));
    if (!crypto::constantTimeEqual(crypto::view(expected), *signature))
    {
        r.verdict = Verdict::BAD_SIGNATURE;
        return r;
    }
    const auto payload = objectOf(p64);
    if (!payload)
    {
        return r;
    }
    const json::value *exp = payload->if_contains("exp");
    if (nullptr == exp)
    {
        r.verdict = Verdict::MISSING_EXP;
        return r;
    }
    const auto expAt = numericDate(*exp);
    if (!expAt)
    {
        r.verdict = Verdict::MISSING_EXP;
        return r;
    }
    if (static_cast<double>(nowSeconds) >= *expAt)
    {
        r.verdict = Verdict::EXPIRED;
        return r;
    }
    if (const json::value *nbf = payload->if_contains("nbf"))
    {
        const auto nbfAt = numericDate(*nbf);
        if (!nbfAt || static_cast<double>(nowSeconds) < *nbfAt)
        {
            r.verdict = Verdict::NOT_YET_VALID;
            return r;
        }
    }
    const json::value *sub = payload->if_contains("sub");
    if (nullptr == sub || !sub->is_string())
    {
        r.verdict = Verdict::BAD_SUBJECT;
        return r;
    }
    const json::string &s = sub->as_string();
    if (s.empty() || s.size() > 19 || '0' == s.front())
    {
        r.verdict = Verdict::BAD_SUBJECT;
        return r;
    }
    std::uint64_t value = 0;
    for (const char c : s)
    {
        if (c < '0' || c > '9')
        {
            r.verdict = Verdict::BAD_SUBJECT;
            return r;
        }
        value = value * 10 + static_cast<std::uint64_t>(c - '0');
    }
    if (value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    {
        r.verdict = Verdict::BAD_SUBJECT;
        return r;
    }
    r.verdict = Verdict::OK;
    r.userId = static_cast<std::int64_t>(value);
    return r;
}

std::string signHs256(std::string_view headerJson, std::string_view payloadJson, std::string_view secret)
{
    const std::string signingInput = crypto::base64UrlEncode(headerJson) + "." + crypto::base64UrlEncode(payloadJson);
    const crypto::Digest sig = crypto::hmacSha256(secret, signingInput);
    return signingInput + "." + crypto::base64UrlEncode(crypto::view(sig));
}

}}}
