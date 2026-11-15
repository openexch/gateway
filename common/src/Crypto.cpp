#include "gateway/common/Crypto.h"

#include <stdexcept>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

namespace openexchange { namespace gateway { namespace crypto {

namespace {

const char B64URL[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int b64Value(char c) noexcept
{
    if (c >= 'A' && c <= 'Z')
    {
        return c - 'A';
    }
    if (c >= 'a' && c <= 'z')
    {
        return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9')
    {
        return c - '0' + 52;
    }
    if ('-' == c)
    {
        return 62;
    }
    if ('_' == c)
    {
        return 63;
    }
    return -1;
}

}

Digest sha256(std::string_view data)
{
    Digest out{};
    SHA256(reinterpret_cast<const unsigned char *>(data.data()), data.size(), out.data());
    return out;
}

Digest hmacSha256(std::string_view key, std::string_view data)
{
    Digest out{};
    unsigned int len = 0;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()), reinterpret_cast<const unsigned char *>(data.data()),
        data.size(), out.data(), &len);
    return out;
}

std::string pbkdf2Sha256(std::string_view password, std::string_view salt, std::uint32_t iterations, std::size_t length)
{
    std::string out(length, '\0');
    if (1 != PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                 reinterpret_cast<const unsigned char *>(salt.data()), static_cast<int>(salt.size()),
                 static_cast<int>(iterations), EVP_sha256(), static_cast<int>(length),
                 reinterpret_cast<unsigned char *>(out.data())))
    {
        return {};
    }
    return out;
}

bool constantTimeEqual(std::string_view a, std::string_view b) noexcept
{
    return a.size() == b.size() && 0 == CRYPTO_memcmp(a.data(), b.data(), a.size());
}

std::string randomBytes(std::size_t n)
{
    std::string out(n, '\0');
    if (1 != RAND_bytes(reinterpret_cast<unsigned char *>(out.data()), static_cast<int>(n)))
    {
        throw std::runtime_error("the OpenSSL random generator failed");
    }
    return out;
}

std::string base64UrlEncode(std::string_view bytes)
{
    std::string out;
    out.reserve((bytes.size() * 4 + 2) / 3);
    std::size_t i = 0;
    for (; i + 3 <= bytes.size(); i += 3)
    {
        const std::uint32_t v = (static_cast<std::uint8_t>(bytes[i]) << 16) |
            (static_cast<std::uint8_t>(bytes[i + 1]) << 8) | static_cast<std::uint8_t>(bytes[i + 2]);
        out += B64URL[(v >> 18) & 63];
        out += B64URL[(v >> 12) & 63];
        out += B64URL[(v >> 6) & 63];
        out += B64URL[v & 63];
    }
    const std::size_t rest = bytes.size() - i;
    if (1 == rest)
    {
        const std::uint32_t v = static_cast<std::uint8_t>(bytes[i]) << 16;
        out += B64URL[(v >> 18) & 63];
        out += B64URL[(v >> 12) & 63];
    }
    else if (2 == rest)
    {
        const std::uint32_t v = (static_cast<std::uint8_t>(bytes[i]) << 16) | (static_cast<std::uint8_t>(bytes[i + 1]) << 8);
        out += B64URL[(v >> 18) & 63];
        out += B64URL[(v >> 12) & 63];
        out += B64URL[(v >> 6) & 63];
    }
    return out;
}

std::optional<std::string> base64UrlDecode(std::string_view text)
{
    if (1 == text.size() % 4)
    {
        return std::nullopt;
    }
    std::string out;
    out.reserve(text.size() * 3 / 4);
    std::uint32_t acc = 0;
    int bits = 0;
    for (const char c : text)
    {
        const int v = b64Value(c);
        if (v < 0)
        {
            return std::nullopt;
        }
        acc = (acc << 6) | static_cast<std::uint32_t>(v);
        bits += 6;
        if (bits >= 8)
        {
            bits -= 8;
            out += static_cast<char>((acc >> bits) & 0xFF);
        }
    }
    if (0 != (acc & ((1u << bits) - 1u)))
    {
        return std::nullopt; // non-zero padding bits: a second encoding of the same bytes
    }
    return out;
}

std::string hex(std::string_view bytes)
{
    static const char *digits = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (const char c : bytes)
    {
        out += digits[(static_cast<std::uint8_t>(c) >> 4) & 15];
        out += digits[static_cast<std::uint8_t>(c) & 15];
    }
    return out;
}

}}}
