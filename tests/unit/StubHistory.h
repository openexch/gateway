#ifndef GATEWAY_TESTS_STUB_HISTORY_H
#define GATEWAY_TESTS_STUB_HISTORY_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "gateway/common/History.h"

namespace gwtest {

using namespace openexchange::gateway;

// A HistoryStore in memory: orders by (userId, orderId) as Order JSON, a canned page, a switchable answer, and a gate
// that holds every call until it is opened (a slow or blocked database).
class StubHistory final : public HistoryStore
{
public:
    std::mutex mutex;
    std::condition_variable cv;
    std::map<std::pair<std::int64_t, std::int64_t>, std::string> orders;
    HistoryPage canned;
    HistoryAnswer answer{ HistoryStatus::OK, HistoryCause::NONE }; // UNAVAILABLE: every call answers it
    std::vector<history::PageRequest> requests;
    std::vector<std::int64_t> deadlines; // the deadline each call was given
    bool held = false; // calls wait while true
    std::atomic<int> waiting{ 0 };
    std::atomic<int> calls{ 0 };

    void hold(bool on)
    {
        std::lock_guard<std::mutex> l(mutex);
        held = on;
        cv.notify_all();
    }

    HistoryAnswer page(int, const history::PageRequest &request, HistoryPage &out, std::int64_t deadlineNs) override
    {
        std::unique_lock<std::mutex> l(mutex);
        wait(l);
        calls++;
        requests.push_back(request);
        deadlines.push_back(deadlineNs);
        if (HistoryStatus::OK != answer.status)
        {
            return answer;
        }
        out = canned;
        return answer;
    }

    HistoryAnswer order(int, std::int64_t userId, std::int64_t orderId, std::string &json,
        std::int64_t deadlineNs) override
    {
        std::unique_lock<std::mutex> l(mutex);
        wait(l);
        calls++;
        deadlines.push_back(deadlineNs);
        if (HistoryStatus::OK != answer.status)
        {
            return answer;
        }
        const auto it = orders.find({ userId, orderId });
        if (it == orders.end())
        {
            return HistoryAnswer{ HistoryStatus::NOT_FOUND, HistoryCause::NONE };
        }
        json = it->second;
        return answer;
    }

    std::string state() const override { return "stub"; }

private:
    void wait(std::unique_lock<std::mutex> &l)
    {
        waiting++;
        cv.wait(l, [this] { return !held; });
        waiting--;
    }
};

}

#endif
