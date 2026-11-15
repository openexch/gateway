#ifndef GATEWAY_COMMON_USER_STORE_H
#define GATEWAY_COMMON_USER_STORE_H

#include <cstdint>
#include <optional>
#include <string>

// UserStore - the demo users table (db/migration/V001__users.sql) as the auth layer needs it. Every call is
// synchronous and runs on a store worker thread (WorkPool gw-pg-auth, the index is `worker`): never an I/O thread,
// never the duty thread. Token values never reach the store: only their SHA-256 (32 raw bytes).
// UNAVAILABLE covers every backend failure (no connection, a timeout, a missing or mismatched schema); the caller
// answers 503, never 400 or 401.

namespace openexchange { namespace gateway {

enum class StoreStatus : std::uint8_t
{
    OK,
    NOT_FOUND,
    CONFLICT,    // the username is taken (or a unique key collided)
    UNAVAILABLE
};
const char *nameOf(StoreStatus s) noexcept;

struct StoredUser
{
    std::int64_t userId = 0;
    std::string username;
    std::string passwordHash;
    std::optional<std::string> tokenHash; // 32 raw bytes, absent after logout
};

class UserStore
{
public:
    virtual ~UserStore() = default;
    // The next userId for a new account, or CONFLICT when the username is taken (no id is consumed then).
    virtual StoreStatus reserveUserId(int worker, const std::string &username, std::int64_t &userId) = 0;
    virtual StoreStatus insertUser(int worker, std::int64_t userId, const std::string &username,
        const std::string &passwordHash, const std::string &tokenHash) = 0;
    virtual StoreStatus findByUsername(int worker, const std::string &username, StoredUser &out) = 0;
    virtual StoreStatus findByToken(int worker, const std::string &tokenHash, StoredUser &out) = 0;
    // Replaces the user's token (login rotation).
    virtual StoreStatus setToken(int worker, std::int64_t userId, const std::string &tokenHash) = 0;
    // Clears the token only while it is still this one (logout); NOT_FOUND when it was already rotated or cleared.
    virtual StoreStatus clearToken(int worker, std::int64_t userId, const std::string &tokenHash) = 0;
    // A short state for /health ("ready", "connecting", "schema mismatch: ...").
    virtual std::string state() const = 0;
};

}}

#endif
