#ifndef GATEWAY_COMMON_JWT_H
#define GATEWAY_COMMON_JWT_H

#include <cstdint>
#include <string>
#include <string_view>

// Jwt - verification of an HS256 compact JWS (RFC 7515, 7519), the only JWT form the gateway accepts:
//   - three unpadded base64url parts; the header is a JSON object with "alg":"HS256", "typ" absent or "JWT", and no
//     "crit" (an extension the verifier would have to understand);
//   - the signature is HMAC-SHA256(secret, header "." payload), compared in constant time;
//   - the payload is a JSON object with "exp" required (a NumericDate; now >= exp is expired), "nbf" honoured when
//     present (now < nbf is not yet valid), and "sub" required: a JSON string of decimal digits, a positive int64,
//     which is the userId. Every other claim is ignored; no claim can name another user.
// Times are wall-clock seconds; there is no leeway.

namespace openexchange { namespace gateway { namespace jwt {

enum class Verdict : std::uint8_t
{
    OK,
    MALFORMED,       // not three base64url parts of JSON objects
    BAD_ALGORITHM,   // alg is not HS256, typ is not JWT, or crit is present
    BAD_SIGNATURE,
    MISSING_EXP,
    EXPIRED,
    NOT_YET_VALID,
    BAD_SUBJECT      // sub missing, not a string of digits, or not a positive int64
};
const char *nameOf(Verdict v) noexcept;

struct Result
{
    Verdict verdict = Verdict::MALFORMED;
    std::int64_t userId = 0;
};

Result verifyHs256(std::string_view token, std::string_view secret, std::int64_t nowSeconds);

// Test support: a signed token with the given header and payload JSON (no validation).
std::string signHs256(std::string_view headerJson, std::string_view payloadJson, std::string_view secret);

}}}

#endif
