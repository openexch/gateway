#include "gateway/common/Password.h"

#include <atomic>
#include <charconv>
#include <vector>

#include "gateway/common/Crypto.h"

namespace openexchange { namespace gateway { namespace password {

namespace {

constexpr std::size_t SALT_BYTES = 16;
constexpr std::size_t HASH_BYTES = 32;

std::atomic<void (*)()> g_observer{ nullptr };

struct Parsed
{
    std::uint32_t iterations = 0;
    std::string salt;
    std::string hash;
};

bool parse(std::string_view stored, Parsed &out)
{
    std::vector<std::string_view> parts;
    std::size_t start = 0;
    for (;;)
    {
        const std::size_t dollar = stored.find('$', start);
        parts.push_back(stored.substr(start, std::string_view::npos == dollar ? std::string_view::npos : dollar - start));
        if (std::string_view::npos == dollar)
        {
            break;
        }
        start = dollar + 1;
    }
    if (4 != parts.size() || "pbkdf2" != parts[0] || parts[1].empty() || '0' == parts[1].front())
    {
        return false;
    }
    std::uint32_t iterations = 0;
    const auto r = std::from_chars(parts[1].data(), parts[1].data() + parts[1].size(), iterations);
    if (r.ec != std::errc() || r.ptr != parts[1].data() + parts[1].size() || 0 == iterations)
    {
        return false;
    }
    auto salt = crypto::base64UrlDecode(parts[2]);
    auto hash = crypto::base64UrlDecode(parts[3]);
    if (!salt || !hash || salt->empty() || HASH_BYTES != hash->size())
    {
        return false;
    }
    out.iterations = iterations;
    out.salt = std::move(*salt);
    out.hash = std::move(*hash);
    return true;
}

std::string derive(std::string_view password, std::string_view salt, std::uint32_t iterations)
{
    if (auto observer = g_observer.load(std::memory_order_acquire))
    {
        observer();
    }
    return crypto::pbkdf2Sha256(password, salt, iterations, HASH_BYTES);
}

}

std::string hash(std::string_view password, std::uint32_t iterations)
{
    const std::string salt = crypto::randomBytes(SALT_BYTES);
    const std::string derived = derive(password, salt, iterations);
    return "pbkdf2$" + std::to_string(iterations) + "$" + crypto::base64UrlEncode(salt) + "$" +
        crypto::base64UrlEncode(derived);
}

bool verify(std::string_view password, std::string_view stored, bool *malformed)
{
    Parsed p;
    if (!parse(stored, p))
    {
        if (nullptr != malformed)
        {
            *malformed = true;
        }
        return false;
    }
    if (nullptr != malformed)
    {
        *malformed = false;
    }
    const std::string derived = derive(password, p.salt, p.iterations);
    return !derived.empty() && crypto::constantTimeEqual(derived, p.hash);
}

bool wellFormed(std::string_view stored, std::uint32_t *iterations)
{
    Parsed p;
    if (!parse(stored, p))
    {
        return false;
    }
    if (nullptr != iterations)
    {
        *iterations = p.iterations;
    }
    return true;
}

void setHashObserver(void (*observer)())
{
    g_observer.store(observer, std::memory_order_release);
}

}}}
