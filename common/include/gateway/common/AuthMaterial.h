#ifndef GATEWAY_COMMON_AUTH_MATERIAL_H
#define GATEWAY_COMMON_AUTH_MATERIAL_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "gateway/common/Settings.h"

// AuthMaterial - the credential files both binaries read at boot, in the same pass as the settings (any problem is
// listed with the others and the process exits 78 before anything starts):
//   GATEWAY_API_KEYS_FILE (api-key mode): one "key:userId" per line; blank lines and lines starting with '#' are
//     skipped. A key is 16 or more visible ASCII characters without ':' (the validity floor: a shorter key is guessable
//     online inside the request rates); userId is a positive int64. A repeated key, a malformed line (named by its line
//     number, never by its content) and a file with no key are problems. No role field: no principal can act as
//     another user, so a role would mean nothing.
//   GATEWAY_JWT_SECRET_FILE (jwt mode): the HS256 secret, the file's bytes with one trailing newline removed; at least
//     32 bytes (RFC 7518 section 3.2: an HS256 key is at least the hash size).
// A missing or unreadable file is a problem that names the setting and the path.

namespace openexchange { namespace gateway {

// Lookup of an API key: by SHA-256 of the key, then a constant-time compare of the key itself.
class ApiKeyTable
{
public:
    void add(const std::string &key, std::int64_t userId);
    std::optional<std::int64_t> find(std::string_view key) const;
    std::size_t size() const noexcept { return m_byHash.size(); }

private:
    struct Entry
    {
        std::string key;
        std::int64_t userId;
    };
    std::unordered_map<std::string, Entry> m_byHash;
};

struct AuthMaterial
{
    ApiKeyTable apiKeys;    // api-key mode
    std::string jwtSecret;  // jwt mode
};

inline constexpr std::size_t API_KEY_MIN_CHARS = 16;
inline constexpr std::size_t JWT_SECRET_MIN_BYTES = 32;

// Reads the files the enabled modes need; every problem is appended to `errors`.
AuthMaterial loadAuthMaterial(const CommonSettings &settings, std::vector<std::string> &errors);

// The parsers alone (tests).
void parseApiKeys(const std::string &text, const std::string &source, ApiKeyTable &out, std::vector<std::string> &errors);
bool parseJwtSecret(const std::string &text, const std::string &source, std::string &out, std::vector<std::string> &errors);

}}

#endif
