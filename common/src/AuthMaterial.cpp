#include "gateway/common/AuthMaterial.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>

#include "gateway/common/Crypto.h"

namespace openexchange { namespace gateway {

namespace {

bool readFile(const std::string &path, std::string &out, std::string &why)
{
    std::ifstream in(path, std::ios::binary);
    if (!in)
    {
        why = std::strerror(errno);
        return false;
    }
    std::ostringstream buffer;
    buffer << in.rdbuf();
    if (in.bad())
    {
        why = "read error";
        return false;
    }
    out = buffer.str();
    return true;
}

bool positiveInt64(const std::string &text, std::int64_t &out)
{
    return parsePositiveDecimal(text, out);
}

}

void ApiKeyTable::add(const std::string &key, std::int64_t userId)
{
    const crypto::Digest h = crypto::sha256(key);
    m_byHash[std::string(crypto::view(h))] = Entry{ key, userId };
}

std::optional<std::int64_t> ApiKeyTable::find(std::string_view key) const
{
    const crypto::Digest h = crypto::sha256(key);
    const auto it = m_byHash.find(std::string(crypto::view(h)));
    if (it == m_byHash.end() || !crypto::constantTimeEqual(it->second.key, key))
    {
        return std::nullopt;
    }
    return it->second.userId;
}

void parseApiKeys(const std::string &text, const std::string &source, ApiKeyTable &out, std::vector<std::string> &errors)
{
    std::istringstream lines(text);
    std::string line;
    int number = 0;
    std::size_t entries = 0;
    while (std::getline(lines, line))
    {
        ++number;
        if (!line.empty() && '\r' == line.back())
        {
            line.pop_back();
        }
        if (line.empty() || '#' == line.front())
        {
            continue;
        }
        const std::size_t colon = line.rfind(':');
        const std::string where = source + " line " + std::to_string(number);
        if (std::string::npos == colon)
        {
            errors.push_back(where + ": expected key:userId");
            continue;
        }
        const std::string key = line.substr(0, colon);
        const std::string user = line.substr(colon + 1);
        bool visible = !key.empty();
        for (const char c : key)
        {
            visible = visible && c > 0x20 && c < 0x7f && ':' != c;
        }
        std::int64_t userId = 0;
        if (!visible || key.size() < API_KEY_MIN_CHARS)
        {
            errors.push_back(where + ": the key must be " + std::to_string(API_KEY_MIN_CHARS) +
                " or more visible ASCII characters without ':'");
            continue;
        }
        if (!positiveInt64(user, userId))
        {
            errors.push_back(where + ": the userId must be a positive integer");
            continue;
        }
        if (out.find(key))
        {
            errors.push_back(where + ": the key is listed twice");
            continue;
        }
        out.add(key, userId);
        ++entries;
    }
    if (0 == entries && 0 == out.size())
    {
        errors.push_back(source + " holds no key:userId entry");
    }
}

bool parseJwtSecret(const std::string &text, const std::string &source, std::string &out, std::vector<std::string> &errors)
{
    std::string secret = text;
    // one trailing line end is not part of the secret, in either form ("\n" or "\r\n" from an editor on Windows)
    if (!secret.empty() && '\n' == secret.back())
    {
        secret.pop_back();
        if (!secret.empty() && '\r' == secret.back())
        {
            secret.pop_back();
        }
    }
    if (secret.size() < JWT_SECRET_MIN_BYTES)
    {
        errors.push_back(source + " holds " + std::to_string(secret.size()) + " bytes; an HS256 secret needs at least " +
            std::to_string(JWT_SECRET_MIN_BYTES));
        return false;
    }
    out = std::move(secret);
    return true;
}

AuthMaterial loadAuthMaterial(const CommonSettings &s, std::vector<std::string> &errors)
{
    AuthMaterial m;
    if (0 != (s.authModes & auth_mode::API_KEY) && s.apiKeysFile)
    {
        std::string text;
        std::string why;
        if (!readFile(*s.apiKeysFile, text, why))
        {
            errors.push_back("GATEWAY_API_KEYS_FILE: cannot read " + *s.apiKeysFile + ": " + why);
        }
        else
        {
            parseApiKeys(text, "GATEWAY_API_KEYS_FILE " + *s.apiKeysFile, m.apiKeys, errors);
        }
    }
    if (0 != (s.authModes & auth_mode::JWT) && s.jwtSecretFile)
    {
        std::string text;
        std::string why;
        if (!readFile(*s.jwtSecretFile, text, why))
        {
            errors.push_back("GATEWAY_JWT_SECRET_FILE: cannot read " + *s.jwtSecretFile + ": " + why);
        }
        else
        {
            parseJwtSecret(text, "GATEWAY_JWT_SECRET_FILE " + *s.jwtSecretFile, m.jwtSecret, errors);
        }
    }
    return m;
}

}}
