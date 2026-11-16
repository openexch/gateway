#ifndef GATEWAY_COMMON_CRYPTO_H
#define GATEWAY_COMMON_CRYPTO_H

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

// Crypto - the primitives the auth layer uses, all from the pinned OpenSSL libcrypto: SHA-256, HMAC-SHA256,
// PBKDF2-HMAC-SHA256, a constant-time compare, the CSPRNG, and unpadded base64url (RFC 4648 section 5) in both
// directions. Every function is thread-safe and allocation-light; none of them is ever called on a duty thread.

namespace openexchange { namespace gateway { namespace crypto {

using Digest = std::array<std::uint8_t, 32>;

Digest sha256(std::string_view data);
Digest hmacSha256(std::string_view key, std::string_view data);
// PBKDF2-HMAC-SHA256 with `iterations` rounds, `length` bytes of output. Empty on an OpenSSL failure.
std::string pbkdf2Sha256(std::string_view password, std::string_view salt, std::uint32_t iterations, std::size_t length);

// Equal length and equal bytes, in time that depends only on the length.
bool constantTimeEqual(std::string_view a, std::string_view b) noexcept;

// `n` bytes from the OpenSSL CSPRNG; throws std::runtime_error when the generator fails (never returns weak bytes).
std::string randomBytes(std::size_t n);

std::string base64UrlEncode(std::string_view bytes);
// Strict: the unpadded alphabet only ('=' refused), no whitespace, and no length that cannot occur (len % 4 == 1).
// Trailing bits that an encoder would have written as zero must be zero, so one value has one encoding.
std::optional<std::string> base64UrlDecode(std::string_view text);

std::string hex(std::string_view bytes);
inline std::string_view view(const Digest &d) noexcept
{
    return std::string_view(reinterpret_cast<const char *>(d.data()), d.size());
}

}}}

#endif
