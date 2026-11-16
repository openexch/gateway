#ifndef GATEWAY_TESTS_STREAM_RIG_H
#define GATEWAY_TESTS_STREAM_RIG_H

// StreamRig - StreamCore against a model of the core (FakeCore) and model clients (FakeOutlet + Client).
//
// FakeCore applies commands one log entry at a time (position += 100): a create, a fill, a cancel, a deposit or a query.
// Every order event gets the next eventSeq and the entry's position; the core's egress to the stream's session is one
// queue in apply order (the per-session FIFO of the real core), holding live events, balance lines and query replies.
// The test can drop live frames (a hole), shrink the core's event ring, and decide when queries are answered. RequestOrderSnapshot
// pages are `pageSize` orders, as of the query's own position; RequestOrderEvents follows the core's rules (the head
// probe, the overrun, the page, the clamp); RequestUserBalances lists the nonzero assets.
//
// Client reads one connection's frames in order and keeps the state a UI would: the open orders (snapshot, then order
// frames), the fills, the balances, the resyncs, the last eventSeq seen. It flags a duplicate (an order frame at or below
// the snapshot's asOf, or a seq seen twice).

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <boost/json.hpp>

#include "Check.h"
#include "StreamCore.h"
#include "gateway/common/Rest.h"

namespace gwtest { namespace stream_rig {

namespace st = openexchange::gateway::stream;
namespace gs = openexchange::gateway::session;
namespace wire = openexchange::gateway::wire;
namespace money = openexchange::gateway::wire::money;
namespace json = boost::json;

constexpr std::int64_t MS = 1'000'000LL;
constexpr std::int64_t SCALE = 100'000'000LL;

class FakeCore : public st::QueryPort
{
public:
    struct Query
    {
        std::uint64_t ticket = 0;
        gs::Command command;
    };
    struct Item
    {
        int kind = 0; // 0 event, 1 balance line, 2 reply
        wire::OrderEvent event;
        wire::BalanceUpdate line;
        gs::Outcome outcome;
    };

    gs::SubmitResult submit(gs::Command command) override
    {
        gs::SubmitResult r;
        r.accepted = true;
        r.ticket = ++nextTicket;
        const std::uint16_t t = gs::templateOf(command);
        submittedByTemplate[t]++;
        if (wire::tid::REQUEST_ORDER_EVENTS == t && std::get<wire::RequestOrderEvents>(command).maxEvents > 0)
        {
            replayQueries++;
        }
        pending.push_back(Query{ r.ticket, std::move(command) });
        std::size_t userQueries = 0;
        for (const Query &q : pending)
        {
            const std::uint16_t qt = gs::templateOf(q.command);
            userQueries += (wire::tid::REQUEST_ORDER_SNAPSHOT == qt || wire::tid::REQUEST_USER_BALANCES == qt) ? 1 : 0;
        }
        maxOutstandingUserQueries = std::max(maxOutstandingUserQueries, userQueries);
        return r;
    }

    // ---- commands (one log entry each)
    std::int64_t create(std::int64_t userId, std::int64_t quantity = 10 * SCALE)
    {
        position += 100;
        wire::OrderEvent e;
        e.orderId = ++nextOrderId;
        e.userId = userId;
        e.marketId = 1;
        e.eventKind = money::EventKind::ACCEPTED;
        e.status = money::OrderStatus::PENDING_NEW;
        e.side = money::Side::BUY;
        e.orderType = money::OrderType::LIMIT;
        e.timeInForce = money::TimeInForce::GTC;
        e.price = 100 * SCALE;
        e.quantity = quantity;
        e.remainingQty = quantity;
        e.holdAssetId = 0;
        e.holdAmount = quantity;
        e.holdRemaining = quantity;
        e.createdAtMs = position;
        e.updatedAtMs = position;
        e.meReason = 255;
        open[e.orderId] = e;
        emit(e);
        return e.orderId;
    }
    void fill(std::int64_t orderId, std::int64_t quantity)
    {
        position += 100;
        wire::OrderEvent e = open.at(orderId);
        e.eventKind = money::EventKind::FILL;
        e.filledQty += quantity;
        e.remainingQty -= quantity;
        e.lastQty = quantity;
        e.lastPrice = e.price;
        e.tradeId = ++nextTradeId;
        e.status = 0 == e.remainingQty ? money::OrderStatus::FILLED : money::OrderStatus::PARTIALLY_FILLED;
        e.updatedAtMs = position;
        fills[e.userId].push_back(e.tradeId);
        if (0 == e.remainingQty)
        {
            open.erase(orderId);
        }
        else
        {
            open[orderId] = e;
        }
        emit(e);
    }
    void cancel(std::int64_t orderId)
    {
        position += 100;
        wire::OrderEvent e = open.at(orderId);
        e.eventKind = money::EventKind::CANCELLED;
        e.status = money::OrderStatus::CANCELLED;
        e.updatedAtMs = position;
        open.erase(orderId);
        emit(e);
    }
    void deposit(std::int64_t userId, std::int32_t assetId, std::int64_t amount)
    {
        position += 100;
        auto &b = balances[{ userId, assetId }];
        b.first += amount;
        Item i;
        i.kind = 1;
        i.line.userId = userId;
        i.line.assetId = assetId;
        i.line.available = b.first;
        i.line.locked = b.second;
        if (!dropBalances)
        {
            egress.push_back(i);
        }
    }

    // ---- queries
    void answerOne()
    {
        if (pending.empty())
        {
            return;
        }
        Query q = std::move(pending.front());
        pending.pop_front();
        position += 100; // every query is a log entry
        Item i;
        i.kind = 2;
        i.outcome.ticket = q.ticket;
        i.outcome.kind = gs::OutcomeKind::REPLY;
        i.outcome.templateId = gs::templateOf(q.command);
        if (auto *s = std::get_if<wire::RequestOrderSnapshot>(&q.command))
        {
            gs::SnapshotReply r;
            std::int64_t last = 0;
            bool more = false;
            for (const auto &[id, e] : open)
            {
                if (e.userId != s->userId || id <= s->fromOrderId)
                {
                    continue;
                }
                if (static_cast<int>(r.entries.size()) == pageSize)
                {
                    more = true;
                    break;
                }
                r.entries.push_back(entryOf(e));
                last = id;
            }
            r.end.entryCount = static_cast<std::int32_t>(r.entries.size());
            r.end.nextOrderId = more ? last : 0;
            r.end.asOfLogPosition = position;
            i.outcome.reply = std::move(r);
        }
        else if (auto *ev = std::get_if<wire::RequestOrderEvents>(&q.command))
        {
            gs::EventsReply r;
            const std::int64_t lastSeq = seq;
            const std::int64_t firstAvailable = std::max<std::int64_t>(1, lastSeq - ringCapacity + 1);
            r.end.firstAvailableSeq = 0 == ringCapacity ? lastSeq + 1 : firstAvailable;
            r.end.lastSeq = lastSeq;
            r.end.nextFromSeq = ev->fromEventSeq;
            if (ev->maxEvents > 0 && !(ev->fromEventSeq + 1 < r.end.firstAvailableSeq) && ev->fromEventSeq >= 0)
            {
                const std::int64_t max = std::min<std::int64_t>(ev->maxEvents, 4096);
                for (const wire::OrderEvent &e : events)
                {
                    if (e.eventSeq > ev->fromEventSeq && static_cast<std::int64_t>(r.events.size()) < max)
                    {
                        r.events.push_back(e);
                    }
                }
                r.end.count = static_cast<std::int32_t>(r.events.size());
                r.end.nextFromSeq = ev->fromEventSeq + r.end.count;
            }
            i.outcome.reply = std::move(r);
        }
        else if (auto *b = std::get_if<wire::RequestUserBalances>(&q.command))
        {
            gs::BalancesReply r;
            r.head.userId = b->userId;
            r.head.known = true;
            r.head.asOfLogPosition = position;
            for (const auto &[key, v] : balances)
            {
                if (key.first == b->userId && (0 != v.first || 0 != v.second))
                {
                    r.lines.push_back(wire::BalanceLine{ key.second, v.first, v.second });
                }
            }
            r.head.lineCount = static_cast<std::uint16_t>(r.lines.size());
            i.outcome.reply = std::move(r);
        }
        egress.push_back(std::move(i));
    }
    void answerAll()
    {
        while (!pending.empty())
        {
            answerOne();
        }
    }
    // every pending query ends unanswered at its deadline (OUTCOME_UNKNOWN)
    void failAll()
    {
        while (!pending.empty())
        {
            Query q = std::move(pending.front());
            pending.pop_front();
            Item i;
            i.kind = 2;
            i.outcome.ticket = q.ticket;
            i.outcome.kind = gs::OutcomeKind::OUTCOME_UNKNOWN;
            i.outcome.templateId = gs::templateOf(q.command);
            egress.push_back(std::move(i));
        }
    }
    // answers only the queries of one template (the others stay pending, in order)
    void answerOnly(std::uint16_t templateId)
    {
        std::deque<Query> keep;
        while (!pending.empty())
        {
            if (gs::templateOf(pending.front().command) == templateId)
            {
                answerOne();
            }
            else
            {
                keep.push_back(std::move(pending.front()));
                pending.pop_front();
            }
        }
        pending = std::move(keep);
    }

    void deliver(st::StreamCore &core, std::int64_t now)
    {
        while (!egress.empty())
        {
            Item i = std::move(egress.front());
            egress.pop_front();
            if (0 == i.kind)
            {
                core.onLiveOrderEvent(i.event, now);
            }
            else if (1 == i.kind)
            {
                core.onLiveBalance(i.line, now);
            }
            else
            {
                core.onOutcome(i.outcome, now);
            }
        }
    }

    std::vector<std::int64_t> openOf(std::int64_t userId) const
    {
        std::vector<std::int64_t> out;
        for (const auto &[id, e] : open)
        {
            if (e.userId == userId)
            {
                out.push_back(id);
            }
        }
        return out;
    }

    static wire::OrderSnapshotEntry entryOf(const wire::OrderEvent &e)
    {
        wire::OrderSnapshotEntry s;
        s.orderId = e.orderId;
        s.userId = e.userId;
        s.marketId = e.marketId;
        s.status = e.status;
        s.side = e.side;
        s.orderType = e.orderType;
        s.timeInForce = e.timeInForce;
        s.price = e.price;
        s.quantity = e.quantity;
        s.filledQty = e.filledQty;
        s.remainingQty = e.remainingQty;
        s.holdAssetId = e.holdAssetId;
        s.holdAmount = e.holdAmount;
        s.createdAtMs = e.createdAtMs;
        s.updatedAtMs = e.updatedAtMs;
        return s;
    }

    std::int64_t position = 1000;
    std::int64_t seq = 0;
    std::int64_t ringCapacity = 1 << 20;
    int pageSize = 3;
    bool dropLive = false;
    bool dropBalances = false;
    std::vector<wire::OrderEvent> events;
    std::map<std::int64_t, wire::OrderEvent> open;
    std::map<std::pair<std::int64_t, std::int32_t>, std::pair<std::int64_t, std::int64_t>> balances;
    std::map<std::int64_t, std::vector<std::int64_t>> fills; // userId -> tradeIds
    std::deque<Query> pending;
    std::deque<Item> egress;
    std::map<std::uint16_t, std::uint64_t> submittedByTemplate;
    std::uint64_t replayQueries = 0;
    std::size_t maxOutstandingUserQueries = 0;
    std::uint64_t nextTicket = 0;
    std::int64_t nextOrderId = 100;
    std::int64_t nextTradeId = 0;

private:
    void emit(wire::OrderEvent &e)
    {
        e.eventSeq = ++seq;
        e.logPosition = position;
        events.push_back(e);
        if (!dropLive)
        {
            Item i;
            i.event = e;
            egress.push_back(i);
        }
    }
};

class FakeOutlet : public st::Outlet
{
public:
    bool offer(const st::Text &frame) override
    {
        if (isClosed || static_cast<std::int64_t>(frames.size() - read) >= capacity)
        {
            return false;
        }
        frames.push_back(*frame);
        if (!paused)
        {
            read = frames.size(); // a reading client takes each frame off the socket as it is written
        }
        return true;
    }
    std::int64_t unwrittenMessages() const override { return static_cast<std::int64_t>(frames.size() - read); }
    std::int64_t unwrittenBytes() const override
    {
        std::int64_t n = 0;
        for (std::size_t i = read; i < frames.size(); ++i)
        {
            n += static_cast<std::int64_t>(frames[i].size());
        }
        return n;
    }
    void close(std::uint16_t code, const char *) override
    {
        closeCode = code;
        isClosed = true;
    }
    bool closed() const override { return isClosed; }

    std::vector<std::string> frames;
    std::size_t read = 0;            // frames the client took off the socket
    std::int64_t capacity = 1 << 20; // frames the writer holds unwritten at most
    bool paused = false;             // the client stopped reading
    std::uint16_t closeCode = 0;
    bool isClosed = false;
};

// One client: its state as a UI would keep it from the frames.
class Client
{
public:
    explicit Client(std::shared_ptr<FakeOutlet> o) : out(std::move(o)) {}

    void take()
    {
        if (out->paused)
        {
            return;
        }
        for (; cursor < out->frames.size(); ++cursor)
        {
            onFrame(out->frames[cursor]);
        }
        out->read = std::max(out->read, out->frames.size());
    }

    void onFrame(const std::string &text)
    {
        const json::object o = json::parse(text).as_object();
        const std::string type(o.at("type").as_string());
        types.push_back(type);
        if ("snapshot" == type && "orders" == o.at("channel").as_string())
        {
            orders.clear();
            for (const json::value &v : o.at("orders").as_array())
            {
                orders[std::stoll(std::string(v.at("orderId").as_string()))] = std::string(v.at("filledQuantity").as_string());
            }
            snapshotAsOf = o.at("asOf").at("eventSeq").as_int64();
            lastSeq = std::max(lastSeq, snapshotAsOf);
            snapshots++;
            seenOrderSeqs.clear();
        }
        else if ("snapshot" == type)
        {
            balances.clear();
            for (const json::value &v : o.at("assets").as_array())
            {
                balances[std::string(v.at("assetId").as_string())] = std::string(v.at("available").as_string());
            }
            balanceSnapshots++;
        }
        else if ("order" == type)
        {
            const std::int64_t s = o.at("eventSeq").as_int64();
            if ((snapshots > 0 && s <= snapshotAsOf) || !seenOrderSeqs.insert(s).second)
            {
                duplicates.push_back(s);
            }
            lastSeq = std::max(lastSeq, s);
            const json::object &ord = o.at("order").as_object();
            const std::int64_t id = std::stoll(std::string(ord.at("orderId").as_string()));
            const std::string status(ord.at("status").as_string());
            if ("FILLED" == status || "CANCELLED" == status || "REJECTED" == status || "EXPIRED" == status)
            {
                orders.erase(id);
            }
            else
            {
                orders[id] = std::string(ord.at("filledQuantity").as_string());
            }
            orderFrames++;
        }
        else if ("fill" == type)
        {
            lastSeq = std::max(lastSeq, o.at("eventSeq").as_int64());
            const std::int64_t trade = std::stoll(std::string(o.at("fill").at("tradeId").as_string()));
            if (!fills.insert(trade).second)
            {
                duplicateFills.push_back(trade);
            }
        }
        else if ("balance" == type)
        {
            balances[std::string(o.at("assetId").as_string())] = std::string(o.at("available").as_string());
        }
        else if ("heartbeat" == type || "pong" == type)
        {
            if (o.at("eventSeq").is_int64())
            {
                lastSeq = std::max(lastSeq, o.at("eventSeq").as_int64());
            }
        }
        else if ("resync" == type)
        {
            resyncs.push_back(std::string(o.at("reason").as_string()));
        }
    }

    // the open orders and their filled quantities equal the core's
    bool matches(const FakeCore &core, std::int64_t userId, std::string &why) const
    {
        std::map<std::int64_t, std::string> expect;
        for (const auto &[id, e] : core.open)
        {
            if (e.userId == userId)
            {
                expect[id] = openexchange::gateway::rest::formatMoney(e.filledQty);
            }
        }
        if (expect == orders)
        {
            return true;
        }
        why = "core has " + std::to_string(expect.size()) + " open, client " + std::to_string(orders.size());
        for (const auto &[id, f] : expect)
        {
            const auto it = orders.find(id);
            if (it == orders.end())
            {
                why += "; missing " + std::to_string(id);
            }
            else if (it->second != f)
            {
                why += "; order " + std::to_string(id) + " filled " + it->second + " expected " + f;
            }
        }
        for (const auto &[id, f] : orders)
        {
            if (0 == expect.count(id))
            {
                why += "; extra " + std::to_string(id);
            }
        }
        return false;
    }

    std::size_t count(const std::string &type) const
    {
        std::size_t n = 0;
        for (const std::string &t : types)
        {
            n += t == type ? 1 : 0;
        }
        return n;
    }

    std::shared_ptr<FakeOutlet> out;
    std::size_t cursor = 0;
    std::vector<std::string> types;
    std::map<std::int64_t, std::string> orders; // orderId -> filledQuantity
    std::set<std::int64_t> fills;
    std::map<std::string, std::string> balances; // assetId -> available
    std::vector<std::string> resyncs;
    std::vector<std::int64_t> duplicates;
    std::vector<std::int64_t> duplicateFills;
    std::set<std::int64_t> seenOrderSeqs;
    std::int64_t snapshotAsOf = -1;
    std::int64_t lastSeq = -1;
    int snapshots = 0;
    int balanceSnapshots = 0;
    int orderFrames = 0;
};

inline st::StreamCoreSettings settings()
{
    st::StreamCoreSettings s;
    s.headProbeMs = 1'000'000; // only the first probe unless a test says otherwise
    s.heartbeatMs = 1'000'000;
    s.userLingerMs = 30'000;
    return s;
}

class Rig
{
public:
    explicit Rig(st::StreamCoreSettings s = settings()) :
        core(s, fake, [this] { return config; })
    {
        auto c = std::make_shared<gs::CoreConfig>();
        wire::AssetConfig usd;
        usd.assetId = 0;
        usd.code = { 'U', 'S', 'D' };
        wire::AssetConfig btc;
        btc.assetId = 1;
        btc.code = { 'B', 'T', 'C' };
        c->assets = { usd, btc };
        config = c;
    }

    // registration and the first head probe: the watermark is the core's head
    void start()
    {
        core.onRegistered(1, 1, now);
        step(3);
    }

    // one duty cycle: the core answers what was asked (unless holding), the egress arrives, the clients read
    void step(int n = 1)
    {
        for (int i = 0; i < n; ++i)
        {
            now += MS;
            core.doWork(now);
            if (autoAnswer)
            {
                fake.answerAll();
            }
            fake.deliver(core, now);
            core.doWork(now);
            for (Client &c : clients)
            {
                c.take();
            }
        }
    }

    std::size_t connect(std::int64_t userId, unsigned channels, std::optional<std::int64_t> resume = std::nullopt)
    {
        auto out = std::make_shared<FakeOutlet>();
        const std::uint64_t id = ++nextConn;
        core.open(id, userId, out, now);
        core.subscribe(id, channels, resume, now);
        clients.emplace_back(out);
        connIds.push_back(id);
        return clients.size() - 1;
    }

    void disconnect(std::size_t client)
    {
        clients[client].out->isClosed = true;
        core.gone(connIds[client], now);
    }

    FakeCore fake;
    std::shared_ptr<const gs::CoreConfig> config;
    st::StreamCore core;
    std::deque<Client> clients;
    std::vector<std::uint64_t> connIds;
    std::uint64_t nextConn = 0;
    std::int64_t now = 1'000'000'000;
    bool autoAnswer = true;
};

}}

#endif
