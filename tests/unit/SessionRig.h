#ifndef GATEWAY_TESTS_SESSION_RIG_H
#define GATEWAY_TESTS_SESSION_RIG_H

// SessionRig - the core session wrapper over the cluster-tools FakeTransport (client/tests/FakeTransport.h of the
// verified cluster-tools tree) with a scripted core behind it. The fake core reads every money frame the session
// offered, in offer order, and answers like the core would: RegisterSession with an AdminAck, CreateOrder with an
// OrderAck that remembers the request id (a repeated key is answered duplicate with the stored orderId), the queries
// with their reports. Answers are queued with a delay of whole duty cycles (never in the cycle that offered), and
// the test can mute, drop or hold them.
//
// Time: one duty cycle = 1 ms of the fake clock.

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "FakeTransport.h"
#include "cluster_tools/client/ClusterClient.h"

#include "com_openexchange_core_infrastructure_generated/AdminAck.h"
#include "com_openexchange_core_infrastructure_generated/AdmissionState.h"
#include "com_openexchange_core_infrastructure_generated/ConfigApplied.h"
#include "com_openexchange_core_infrastructure_generated/CoreConfigReport.h"
#include "com_openexchange_core_infrastructure_generated/CreateOrder.h"
#include "com_openexchange_core_infrastructure_generated/CancelOrder.h"
#include "com_openexchange_core_infrastructure_generated/MessageHeader.h"
#include "com_openexchange_core_infrastructure_generated/OrderAck.h"
#include "com_openexchange_core_infrastructure_generated/OrderFault.h"
#include "com_openexchange_core_infrastructure_generated/OrderReject.h"
#include "com_openexchange_core_infrastructure_generated/OrderSnapshotEnd.h"
#include "com_openexchange_core_infrastructure_generated/RegisterSession.h"
#include "com_openexchange_core_infrastructure_generated/RequestCoreConfig.h"
#include "com_openexchange_core_infrastructure_generated/RequestOrderSnapshot.h"
#include "com_openexchange_core_infrastructure_generated/RequestUserBalances.h"
#include "com_openexchange_core_infrastructure_generated/UserBalances.h"

#include "gateway/common/CoreSession.h"
#include "gateway/common/OrderEntry.h"

namespace gwtest { namespace session_rig {

namespace cl = cluster_tools::client;
namespace cc = io::aeron::cluster::codecs;
namespace money = com::openexchange::core::infrastructure::generated;
namespace gs = openexchange::gateway::session;
using cl::test::FakeTransport;
using Client = cl::ClusterClient<FakeTransport>;
using Session = gs::CoreSession<Client>;

constexpr std::int64_t MS = 1'000'000LL;
inline const char *ENDPOINTS = "0=10.0.0.1:9302,1=10.0.0.2:9302,2=10.0.0.3:9302";

inline cl::ClientSettings clientSettings()
{
    cl::ClientSettings s;
    s.ingressEndpoints = ENDPOINTS;
    s.ingressChannel = "aeron:udp?term-length=64k|mtu=8192";
    s.responseChannel = "aeron:udp?endpoint=127.0.0.1:0|term-length=64k|mtu=8192";
    s.credentials = std::vector<std::uint8_t>(38, 1);
    return s;
}

inline gs::SessionSettings sessionSettings()
{
    gs::SessionSettings s;
    s.identity = 41;
    s.requestTimeoutMs = 5000;
    s.registerResendMs = 300;
    s.reconnectDelayMs = 100;
    s.encoder = gs::encodeCommand;
    return s;
}

// One money frame the session offered (session header stripped).
struct Offered
{
    std::uint16_t templateId = 0;
    std::vector<std::uint8_t> bytes;
    std::size_t publication = 0;
};

template<typename Codec>
Codec decodeAs(std::vector<std::uint8_t> &bytes)
{
    char *b = reinterpret_cast<char *>(bytes.data());
    money::MessageHeader h(b, 0, bytes.size(), 0);
    Codec codec;
    codec.wrapForDecode(b, money::MessageHeader::encodedLength(), h.blockLength(), h.version(), bytes.size());
    return codec;
}

class Rig
{
public:
    explicit Rig(gs::SessionSettings settings = sessionSettings()) :
        client(fake, clientSettings()),
        session(client, settings, [this](const gs::Outcome &o) { outcomes.push_back(o); })
    {
    }

    // ---- driving

    void cycle(int n = 1)
    {
        for (int i = 0; i < n; ++i)
        {
            fake.now += MS;
            cycles++;
            deliverDue();
            session.doWork(pollEgress);
            readOffers();
        }
    }

    // start, answer the connect, and (by default) the registration and the config read
    void connect(bool registerToo = true)
    {
        if (!session.start())
        {
            throw std::runtime_error("start refused: " + client.lastDetail());
        }
        cycle(20);
        fake.pushSessionEvent(fake.lastConnectCorrelation(), sessionId, term, leader, cc::EventCode::OK, "", image);
        cycle(3);
        if (registerToo)
        {
            cycle(10);
        }
    }

    // the leader changes: NewLeaderEvent on the current image; the client re-points ingress at the new leader
    void newLeader(std::int32_t newLeaderId)
    {
        term++;
        leader = newLeaderId;
        fake.pushNewLeader(sessionId, term, leader, ENDPOINTS, image);
    }

    // the cluster closes the session; the session reconnects after its delay and the core answers the new connect
    void sessionClosedByCluster(int cyclesUntilReconnect = 150)
    {
        fake.pushSessionEvent(0, sessionId, term, leader, cc::EventCode::CLOSED, "session timed out", image);
        cycle(cyclesUntilReconnect);
        sessionId++;
        image++;
        fake.pushSessionEvent(fake.lastConnectCorrelation(), sessionId, term, leader, cc::EventCode::OK, "", image);
        cycle(3);
    }

    // ---- the fake core

    struct Order
    {
        std::int64_t orderId = 0;
        std::int64_t correlationId = 0; // of the first create that made it
    };

    void push(std::vector<std::uint8_t> bytes, int delayCycles = 1)
    {
        if (muted)
        {
            held.push_back(std::move(bytes));
            return;
        }
        due.emplace_back(cycles + std::max(1, delayCycles), std::move(bytes));
    }

    // the replies held while muted, released now (in order)
    void releaseHeld()
    {
        for (auto &b : held)
        {
            due.emplace_back(cycles + 1, std::move(b));
        }
        held.clear();
    }
    void dropHeld() { held.clear(); }

    std::vector<std::uint8_t> adminAck(std::int64_t correlationId, money::AdminResult::Value result)
    {
        std::vector<std::uint8_t> b(64, 0);
        money::AdminAck a;
        a.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .correlationId(correlationId).refTemplateId(80).result(result).reason(0).configVersion(0);
        b.resize(money::MessageHeader::encodedLength() + a.encodedLength());
        return b;
    }

    std::vector<std::uint8_t> orderAck(std::int64_t correlationId, std::int64_t rh, std::int64_t rl, std::int64_t orderId,
        bool duplicate)
    {
        std::vector<std::uint8_t> b(128, 0);
        money::OrderAck a;
        a.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .correlationId(correlationId).requestIdHigh(rh).requestIdLow(rl).orderId(orderId).userId(1001).marketId(1)
            .status(money::OrderStatus::PENDING_NEW).duplicate(duplicate ? money::BoolFlag::TRUE : money::BoolFlag::FALSE)
            .holdAssetId(0).holdAmount(100).acceptedAtMs(1).eventSeq(orderId);
        b.resize(money::MessageHeader::encodedLength() + a.encodedLength());
        return b;
    }

    std::vector<std::uint8_t> orderReject(std::int64_t correlationId, std::int64_t rh, std::int64_t rl,
        money::CoreRejectReason::Value reason, std::uint16_t refTemplateId)
    {
        std::vector<std::uint8_t> b(128, 0);
        money::OrderReject r;
        r.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .correlationId(correlationId).requestIdHigh(rh).requestIdLow(rl).userId(1001).orderId(0).marketId(1)
            .refTemplateId(refTemplateId).reason(reason).meReason(255).detail(0).rejectedAtMs(1);
        b.resize(money::MessageHeader::encodedLength() + r.encodedLength());
        return b;
    }

    std::vector<std::uint8_t> configReport(std::int64_t correlationId)
    {
        std::vector<std::uint8_t> b(1024, 0);
        money::CoreConfigReport r;
        r.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .correlationId(correlationId).configVersion(configVersion).behaviorVersion(4).limitsVersion(1)
            .requestTtlMs(requestTtlMs).requestPerUserCap(256).orderEventRingCapacity(65536)
            .gateState(globalGate);
        r.gateReasons().clear().admin(money::GateState::SUSPENDED == globalGate);
        r.asOfLogPosition(logPosition);
        auto &m = r.marketsCount(configVersion > 0 ? 1 : 0);
        if (configVersion > 0)
        {
            m.next().marketId(1).putSymbol(std::string_view("BTC-USD")).baseAssetId(1).quoteAssetId(0).tickSize(1).minPrice(1)
                .maxPrice(1'000'000'000'000'000LL).lotSize(1).minQuantity(1).maxQuantity(1'000'000).minNotional(0)
                .maxNotional(1'000'000'000'000LL).riskConfigVersion(1).gateState(money::GateState::OPEN);
            m.gateReasons().clear();
            m.circuitBreaker(money::BoolFlag::FALSE);
        }
        auto &a = r.assetsCount(configVersion > 0 ? 2 : 0);
        if (configVersion > 0)
        {
            a.next().assetId(0).putCode(std::string_view("USD")).scale(8);
            a.next().assetId(1).putCode(std::string_view("BTC")).scale(8);
        }
        b.resize(money::MessageHeader::encodedLength() + r.encodedLength());
        return b;
    }

    std::vector<std::uint8_t> userBalances(std::int64_t correlationId, std::int64_t userId)
    {
        std::vector<std::uint8_t> b(256, 0);
        money::UserBalances u;
        u.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .correlationId(correlationId).userId(userId).known(money::BoolFlag::TRUE).asOfLogPosition(logPosition);
        u.balancesCount(1).next().assetId(0).available(1000).locked(0);
        b.resize(money::MessageHeader::encodedLength() + u.encodedLength());
        return b;
    }

    std::vector<std::uint8_t> admission(std::int32_t marketId, money::GateState::Value state, std::int64_t position)
    {
        std::vector<std::uint8_t> b(128, 0);
        money::AdmissionState a;
        a.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .marketId(marketId).gateState(state).gateReason(money::GateReason::ADMIN).circuitBreaker(money::BoolFlag::FALSE)
            .atMs(1).logPosition(position);
        a.gateReasons().clear().admin(money::GateState::SUSPENDED == state);
        b.resize(money::MessageHeader::encodedLength() + a.encodedLength());
        return b;
    }

    std::vector<std::uint8_t> configApplied(std::int64_t position)
    {
        std::vector<std::uint8_t> b(64, 0);
        money::ConfigApplied c;
        c.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .kind(money::ConfigKind::CORE).marketId(0).configVersion(configVersion).logPosition(position);
        b.resize(money::MessageHeader::encodedLength() + c.encodedLength());
        return b;
    }

    // the core's CONTROL broadcast the session ignores
    std::vector<std::uint8_t> orderFault(std::int64_t position)
    {
        std::vector<std::uint8_t> b(128, 0);
        money::OrderFault f;
        f.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
            .kind(money::OrderFaultKind::FEED_HALT).orderId(0).userId(0).marketId(0).tradeId(7).amount(0)
            .logPosition(position);
        b.resize(money::MessageHeader::encodedLength() + f.encodedLength());
        return b;
    }

    // ---- what the session sent

    std::vector<Offered> frames(std::uint16_t templateId = 0) const
    {
        std::vector<Offered> out;
        for (const Offered &o : log)
        {
            if (0 == templateId || o.templateId == templateId)
            {
                out.push_back(o);
            }
        }
        return out;
    }

    std::size_t countOutcomes(gs::OutcomeKind kind) const
    {
        std::size_t n = 0;
        for (const gs::Outcome &o : outcomes)
        {
            n += o.kind == kind ? 1 : 0;
        }
        return n;
    }

    const gs::Outcome *outcomeOf(std::uint64_t ticket) const
    {
        for (const gs::Outcome &o : outcomes)
        {
            if (o.ticket == ticket)
            {
                return &o;
            }
        }
        return nullptr;
    }

    FakeTransport fake;
    Client client;
    std::vector<gs::Outcome> outcomes;
    Session session;

    std::int64_t sessionId = 77;
    std::int64_t term = 5;
    std::int32_t leader = 0;
    std::int32_t image = 900;
    std::int64_t cycles = 0;
    bool pollEgress = true;

    // core behaviour
    bool muted = false;                 // replies are held (released or dropped by the test)
    int registerAcksToDrop = 0;         // the next N AdminAcks of RegisterSession are lost
    bool answerRegistrations = true;
    money::AdminResult::Value registrationResult = money::AdminResult::ACCEPTED;
    bool answerCreates = true;
    bool dropCreateAcks = false;        // a create is applied (its order exists) but its ack is lost
    std::function<int()> replyDelay = [] { return 1; };
    std::function<bool(std::int64_t correlationId)> rejectUnregistered = [](std::int64_t) { return false; };
    bool answerQueries = true;
    std::uint32_t configVersion = 3;
    money::GateState::Value globalGate = money::GateState::OPEN;
    std::int64_t requestTtlMs = 900'000;
    std::int64_t logPosition = 1000;

    // what the core saw
    std::vector<Offered> log;                                            // every money frame, in offer order
    std::map<std::pair<std::int64_t, std::int64_t>, Order> orders;      // by request id
    std::int64_t nextOrderId = 5000;
    std::vector<std::int64_t> queryTimesNs;                              // when each query reached the core
    bool awaitingRegistrationAck = false;
    std::uint64_t commandsBeforeAck = 0;                                 // commands offered between a RegisterSession and its ack
    std::vector<std::int64_t> createCorrelations;                        // every create offer's correlation id, in order

private:
    void deliverDue()
    {
        for (auto it = due.begin(); it != due.end();)
        {
            if (it->first <= cycles)
            {
                if (isRegistrationAck(it->second))
                {
                    awaitingRegistrationAck = false;
                }
                fake.pushAppMessage(sessionId, it->second, image, term);
                it = due.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    static bool isRegistrationAck(std::vector<std::uint8_t> &bytes)
    {
        money::MessageHeader h(reinterpret_cast<char *>(bytes.data()), 0, bytes.size(), 0);
        if (h.templateId() != money::AdminAck::sbeTemplateId())
        {
            return false;
        }
        return 80 == decodeAs<money::AdminAck>(bytes).refTemplateId();
    }

    void readOffers()
    {
        for (std::size_t p = 0; p < fake.publications.size(); ++p)
        {
            auto &offered = fake.publications[p].offered;
            std::size_t &seen = cursor[p];
            for (; seen < offered.size(); ++seen)
            {
                const auto &frame = offered[seen];
                if (frame.size() <= 32 + money::MessageHeader::encodedLength())
                {
                    continue;
                }
                cc::MessageHeader session(const_cast<char *>(reinterpret_cast<const char *>(frame.data())), 0, frame.size(), 0);
                if (session.schemaId() != cc::MessageHeader::sbeSchemaId() ||
                    session.templateId() != cc::SessionMessageHeader::sbeTemplateId())
                {
                    continue; // a cluster protocol frame (connect, keepalive, close), not an application message
                }
                std::vector<std::uint8_t> bytes(frame.begin() + 32, frame.end());
                money::MessageHeader h(reinterpret_cast<char *>(bytes.data()), 0, bytes.size(), 0);
                if (h.schemaId() != money::MessageHeader::sbeSchemaId())
                {
                    continue;
                }
                Offered o;
                o.templateId = h.templateId();
                o.bytes = bytes;
                o.publication = p;
                log.push_back(o);
                onCommand(o);
            }
        }
    }

    void onCommand(Offered &o)
    {
        const std::uint16_t t = o.templateId;
        if (80 == t)
        {
            awaitingRegistrationAck = true;
            const std::int64_t c = decodeAs<money::RegisterSession>(o.bytes).correlationId();
            if (!answerRegistrations)
            {
                return;
            }
            if (registerAcksToDrop > 0)
            {
                registerAcksToDrop--;
                return;
            }
            due.emplace_back(cycles + std::max(1, replyDelay()), adminAck(c, registrationResult)); // never muted
            return;
        }
        if (50 == t || 51 == t || 52 == t || 1 == t || 2 == t)
        {
            if (awaitingRegistrationAck)
            {
                commandsBeforeAck++;
            }
        }
        if (50 == t)
        {
            auto create = decodeAs<money::CreateOrder>(o.bytes);
            const std::int64_t c = create.correlationId();
            const auto key = std::make_pair(create.requestIdHigh(), create.requestIdLow());
            createCorrelations.push_back(c);
            if (rejectUnregistered(c))
            {
                push(orderReject(c, key.first, key.second, money::CoreRejectReason::UNREGISTERED_SESSION, 50), replyDelay());
                return;
            }
            if (!answerCreates)
            {
                return;
            }
            auto found = orders.find(key);
            const bool duplicate = found != orders.end();
            if (!duplicate)
            {
                orders[key] = Order{ nextOrderId++, c };
            }
            if (dropCreateAcks)
            {
                return;
            }
            push(orderAck(c, key.first, key.second, orders[key].orderId, duplicate), replyDelay());
            return;
        }
        if (56 == t)
        {
            queryTimesNs.push_back(fake.now);
            if (answerQueries)
            {
                push(configReport(decodeAs<money::RequestCoreConfig>(o.bytes).correlationId()), replyDelay());
            }
            return;
        }
        if (55 == t)
        {
            queryTimesNs.push_back(fake.now);
            auto q = decodeAs<money::RequestUserBalances>(o.bytes);
            if (answerQueries)
            {
                push(userBalances(q.correlationId(), q.userId()), replyDelay());
            }
            return;
        }
    }

    std::map<std::size_t, std::size_t> cursor;
    std::deque<std::pair<std::int64_t, std::vector<std::uint8_t>>> due;
    std::vector<std::vector<std::uint8_t>> held;
};

inline openexchange::gateway::wire::CreateOrder create(std::int64_t keyLow, std::int64_t userId = 1001)
{
    openexchange::gateway::wire::CreateOrder c;
    c.userId = userId;
    c.clientOrderIdHigh = 0;
    c.clientOrderIdLow = keyLow;
    c.requestIdHigh = 0x0102030405060708LL;
    c.requestIdLow = keyLow;
    c.marketId = 1;
    c.side = money::Side::BUY;
    c.orderType = money::OrderType::LIMIT;
    c.timeInForce = money::TimeInForce::GTC;
    c.price = 100;
    c.quantity = 1;
    return c;
}

}}

#endif
