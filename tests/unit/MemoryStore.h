#ifndef GATEWAY_TESTS_MEMORY_STORE_H
#define GATEWAY_TESTS_MEMORY_STORE_H

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "gateway/common/UserStore.h"

namespace gwtest {

using namespace openexchange::gateway;

// In-memory store with switchable failure (UNAVAILABLE) and call counting.
class MemoryStore final : public UserStore
{
public:
    std::mutex mutex;
    std::map<std::int64_t, StoredUser> users;
    std::int64_t nextId = 100000;
    std::atomic<bool> down{ false };
    std::atomic<int> calls{ 0 };
    std::atomic<int> tokenDelayMs{ 0 }; // a slow token lookup (a PG round trip under load)

    StoreStatus reserveUserId(int, const std::string &username, std::int64_t &userId) override
    {
        calls++;
        if (down)
        {
            return StoreStatus::UNAVAILABLE;
        }
        std::lock_guard<std::mutex> lock(mutex);
        for (const auto &[id, u] : users)
        {
            if (u.username == username)
            {
                return StoreStatus::CONFLICT;
            }
        }
        userId = nextId++;
        return StoreStatus::OK;
    }
    StoreStatus insertUser(int, std::int64_t userId, const std::string &username, const std::string &passwordHash,
        const std::string &tokenHash) override
    {
        calls++;
        if (down)
        {
            return StoreStatus::UNAVAILABLE;
        }
        std::lock_guard<std::mutex> lock(mutex);
        for (const auto &[id, u] : users)
        {
            if (u.username == username || id == userId)
            {
                return StoreStatus::CONFLICT;
            }
        }
        users[userId] = StoredUser{ userId, username, passwordHash, tokenHash };
        return StoreStatus::OK;
    }
    StoreStatus findByUsername(int, const std::string &username, StoredUser &out) override
    {
        calls++;
        if (down)
        {
            return StoreStatus::UNAVAILABLE;
        }
        std::lock_guard<std::mutex> lock(mutex);
        for (const auto &[id, u] : users)
        {
            if (u.username == username)
            {
                out = u;
                return StoreStatus::OK;
            }
        }
        return StoreStatus::NOT_FOUND;
    }
    StoreStatus findByToken(int, const std::string &tokenHash, StoredUser &out) override
    {
        calls++;
        if (down)
        {
            return StoreStatus::UNAVAILABLE;
        }
        StoreStatus st = StoreStatus::NOT_FOUND;
        {
            std::lock_guard<std::mutex> lock(mutex);
            for (const auto &[id, u] : users)
            {
                if (u.tokenHash && *u.tokenHash == tokenHash)
                {
                    out = u;
                    st = StoreStatus::OK;
                    break;
                }
            }
        }
        // the answer was read; it reaches the caller this much later (a round trip under load)
        if (tokenDelayMs > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(tokenDelayMs.load()));
        }
        return st;
    }
    StoreStatus setToken(int, std::int64_t userId, const std::string &tokenHash) override
    {
        calls++;
        if (down)
        {
            return StoreStatus::UNAVAILABLE;
        }
        std::lock_guard<std::mutex> lock(mutex);
        auto it = users.find(userId);
        if (it == users.end())
        {
            return StoreStatus::NOT_FOUND;
        }
        it->second.tokenHash = tokenHash;
        return StoreStatus::OK;
    }
    StoreStatus clearToken(int, std::int64_t userId, const std::string &tokenHash) override
    {
        calls++;
        if (down)
        {
            return StoreStatus::UNAVAILABLE;
        }
        std::lock_guard<std::mutex> lock(mutex);
        auto it = users.find(userId);
        if (it == users.end() || !it->second.tokenHash || *it->second.tokenHash != tokenHash)
        {
            return StoreStatus::NOT_FOUND;
        }
        it->second.tokenHash.reset();
        return StoreStatus::OK;
    }
    std::string state() const override { return down ? "down" : "ready"; }
};

}

#endif
