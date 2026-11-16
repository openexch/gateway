#ifndef GATEWAY_COMMON_API_H
#define GATEWAY_COMMON_API_H

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>

#include <boost/json/object.hpp>

#include "gateway/common/HttpServer.h"

// Api - the shapes every REST answer shares: the error body, the codes of the status table, and the strict
// reading of a JSON body and a query string.
//
// Error body: {"error": <text>, "code": <CODE>, "reason": <core reason name or null>, "detail": <text or null>,
// "keyFinal": <bool>}. keyFinal is false wherever the table says n/a (nothing is stored under a key).
//
// Strict input: a body must be one JSON object (Content-Type application/json); a field the route does not
// name is 400 VALIDATION naming it; a "userId" key anywhere, at any depth, is 400 VALIDATION (the principal decides the
// user, nothing in a request can name one); a query parameter the route does not name, a "userId" parameter and a
// credential-looking parameter are 400 VALIDATION naming it. The gateway never reads a credential from a URL.

namespace openexchange { namespace gateway { namespace api {

namespace code {
inline constexpr const char *VALIDATION = "VALIDATION";
inline constexpr const char *UNAUTHORIZED = "UNAUTHORIZED";
inline constexpr const char *FORBIDDEN = "FORBIDDEN";
inline constexpr const char *NOT_FOUND = "NOT_FOUND";
inline constexpr const char *FEATURE_DISABLED = "FEATURE_DISABLED";
inline constexpr const char *CONFLICT = "CONFLICT";
inline constexpr const char *RATE_LIMITED = "RATE_LIMITED";
inline constexpr const char *CORE_UNAVAILABLE = "CORE_UNAVAILABLE";
inline constexpr const char *OVERLOADED = "OVERLOADED";
inline constexpr const char *AUTH_UNAVAILABLE = "AUTH_UNAVAILABLE";
inline constexpr const char *AUTH_BUSY = "AUTH_BUSY";
inline constexpr const char *IDENTITY_STORE_INCONSISTENT = "IDENTITY_STORE_INCONSISTENT";
inline constexpr const char *OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN";
inline constexpr const char *INTERNAL = "INTERNAL";
inline constexpr const char *METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED";
inline constexpr const char *MISSING_IDEMPOTENCY_KEY = "MISSING_IDEMPOTENCY_KEY";
inline constexpr const char *REJECTED = "REJECTED";
inline constexpr const char *ADMISSION_CLOSED = "ADMISSION_CLOSED";
inline constexpr const char *HISTORY_UNAVAILABLE = "HISTORY_UNAVAILABLE";
inline constexpr const char *HISTORY_BUSY = "HISTORY_BUSY";
}

struct ErrorSpec
{
    unsigned status = 500;
    const char *code = code::INTERNAL;
    std::string text;
    std::optional<std::string> reason;
    std::optional<std::string> detail;
    bool keyFinal = false;
    std::int64_t retryAfterMs = -1; // >= 0: a Retry-After header in whole seconds, rounded up (at least 1)
};

ApiResponse error(ErrorSpec spec);
ApiResponse error(unsigned status, const char *code, std::string text, std::optional<std::string> detail = std::nullopt);
ApiResponse jsonBody(unsigned status, std::string body);
ApiResponse noContent();

std::string quote(std::string_view text); // a JSON string literal

// The parsed body or nullopt with `bad` set to the 400 to answer.
std::optional<boost::json::object> readBody(const HttpExchange &ex, std::initializer_list<std::string_view> allowed,
    ApiResponse &bad);
// True when the query names only `allowed` parameters (each at most once); else `bad` is the 400 to answer.
bool readQuery(const HttpExchange &ex, std::initializer_list<std::string_view> allowed, ApiResponse &bad);
// A "userId" key anywhere in the value (any depth, any case of the key's letters).
bool namesAUser(const boost::json::value &v, std::string &where);

}}}

#endif
