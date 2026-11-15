#ifndef GATEWAY_COMMON_SESSION_PARTS_H
#define GATEWAY_COMMON_SESSION_PARTS_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "cluster_tools/client/CoreSessionRegistrar.h"
#include "cluster_tools/client/Credentials.h"

#include "gateway/common/Wire.h"

// SessionParts - the pure pieces of the core session wrapper (CoreSession.h): the settings, the commands and their
// replies, the outcome of a request, the query token bucket and the core config cache. No Aeron, no clock of their own:
// every call takes the time it runs at.

namespace openexchange { namespace gateway { namespace session {

namespace cl = cluster_tools::client;

// ---- settings

struct SessionSettings
{
    cl::SessionClass sessionClass = cl::SessionClass::GATEWAY;
    cl::SessionClass credentialClass = cl::SessionClass::GATEWAY; // the class of the client's credential
    std::uint32_t channels = cl::core_channel::ACKS | cl::core_channel::CONTROL;
    std::int64_t identity = 0;              // the registration identity (<P>_INSTANCE_ID)
    std::int64_t maxInflight = 4096;        // GATEWAY_MAX_INFLIGHT: requests held, offered or waiting to be offered
    std::int64_t requestTimeoutMs = 5000;   // GATEWAY_REQUEST_TIMEOUT_MS
    std::int64_t queryRateCount = 100;      // GATEWAY_QUERY_RATE: tokens per period
    std::int64_t queryRatePeriodMs = 1000;
    std::int64_t queryBurst = 200;          // GATEWAY_QUERY_BURST
    std::int64_t reconnectDelayMs = 1000;   // <P>_RECONNECT_DELAY_MS: a closed or failed session is replaced after this
    std::int64_t registerResendMs = 5000;   // <P>_REGISTER_RESEND_MS: a RegisterSession without its ack is re-sent
    int pollFragments = 64;                 // egress messages per poll (a batching step, not a capacity)
    bool readConfig = true;                 // read CoreConfigReport after every registration and ConfigApplied
    std::int64_t correlationIdBase = 0x4757'0000'0000'0000LL; // request correlation ids count up from here
};

// Empty = valid; otherwise every problem, one per line.
std::string validate(const SessionSettings &settings);

// ---- commands

struct Deposit
{
    wire::MoneyCommand command;
};
struct Withdraw
{
    wire::MoneyCommand command;
};

using Command = std::variant<wire::CreateOrder, wire::CancelOrder, wire::AmendOrder, Deposit, Withdraw,
    wire::RequestOrderSnapshot, wire::RequestOrderEvents, wire::RequestUserBalances, wire::RequestCoreConfig>;

std::uint16_t templateOf(const Command &command) noexcept;
// 53, 54, 55 and 56: reads that pass the query budget
bool isQuery(std::uint16_t templateId) noexcept;
// the command's requestId (0/0 when it has none or is unkeyed)
void requestIdOf(const Command &command, std::int64_t &high, std::int64_t &low) noexcept;
// Writes the correlation id into the command and encodes it; the frame length, 0 when the buffer is too short.
std::size_t encodeCommand(Command &command, std::int64_t correlationId, std::uint8_t *buffer, std::size_t capacity) noexcept;

// ---- replies

struct DepositAckReply
{
    wire::MoneyAck ack;
};
struct WithdrawAckReply
{
    wire::MoneyAck ack;
};
struct SnapshotReply
{
    std::vector<wire::OrderSnapshotEntry> entries;
    wire::OrderSnapshotEnd end;
};
struct BalancesReply
{
    wire::UserBalancesHead head;
    std::vector<wire::BalanceLine> lines;
};
struct EventsReply
{
    std::vector<wire::OrderEvent> events;
    wire::OrderEventsEnd end;
};

struct GateView
{
    wire::money::GateState::Value state = wire::money::GateState::SUSPENDED;
    std::uint32_t reasons = 0;
    bool circuitBreaker = false;
    std::int64_t logPosition = -1; // the log position this view is as of; -1 = never reported
    bool known() const noexcept { return logPosition >= 0; }
};

// The core's config as this session last read it (CoreConfigReport) with the gate and breaker states the
// AdmissionState frames moved since.
struct CoreConfig
{
    wire::CoreConfigHead head;
    std::vector<wire::MarketConfig> markets; // ascending marketId
    std::vector<wire::AssetConfig> assets;
    GateView global;
    std::vector<GateView> marketGates;        // index-aligned with markets
    bool reported = false;                    // a CoreConfigReport was read

    // a recorded CoreConfig exists (configVersion 0 with empty groups = none yet)
    bool loaded() const noexcept { return reported && head.configVersion > 0; }
    bool gateOpen() const noexcept { return global.known() && wire::money::GateState::OPEN == global.state; }
    // how long the core remembers an Idempotency-Key (the request TTL of the limits record)
    std::int64_t idempotencyWindowMs() const noexcept { return head.requestTtlMs; }
    const wire::MarketConfig *market(std::int32_t marketId) const noexcept;
    const GateView *marketGate(std::int32_t marketId) const noexcept;
};

struct ConfigReply
{
    std::shared_ptr<const CoreConfig> config;
};

using Reply = std::variant<std::monostate, wire::OrderAck, wire::OrderReject, DepositAckReply, WithdrawAckReply,
    wire::DepositReject, wire::WithdrawReject, SnapshotReply, BalancesReply, EventsReply, ConfigReply>;

// ---- outcome of one request

enum class OutcomeKind : std::uint8_t
{
    REPLY,            // the core answered (reply holds it)
    OUTCOME_UNKNOWN,  // 504: offered and unanswered at the deadline; it may or may not have been applied
    CORE_UNAVAILABLE, // 503: never offered before the deadline (no registered session); nothing was sent
    OVERLOADED,       // 503: the request table was full, or a query waited for the budget until its deadline
    INTERNAL,         // 500: the transport refused the frame (TRANSPORT_ERROR), a defect; not retried
};
const char *nameOf(OutcomeKind kind) noexcept;

struct Outcome
{
    std::uint64_t ticket = 0;
    OutcomeKind kind = OutcomeKind::REPLY;
    std::uint16_t templateId = 0;
    std::int64_t correlationId = 0;
    std::uint32_t offers = 0;        // successful offers of this request (> 1: the session re-offered it)
    std::int64_t submittedNs = 0;
    std::int64_t completedNs = 0;
    std::int64_t budgetWaitNs = 0;   // time a query waited for a budget token
    Reply reply;
};

struct SubmitResult
{
    bool accepted = false;
    std::uint64_t ticket = 0;                               // accepted: the outcome carries it
    OutcomeKind refusal = OutcomeKind::CORE_UNAVAILABLE;    // not accepted: OVERLOADED (table full) or CORE_UNAVAILABLE
};

// ---- the query budget

// A token bucket: `count` tokens per `periodMs`, holding at most `burst`, full at the first call. Time is the
// caller's monotonic ns.
class TokenBucket
{
public:
    TokenBucket(std::int64_t count, std::int64_t periodMs, std::int64_t burst) noexcept;
    bool tryTake(std::int64_t nowNs) noexcept;
    void refund() noexcept; // a token taken for an offer that sent nothing
    double tokens(std::int64_t nowNs) noexcept;

private:
    void refill(std::int64_t nowNs) noexcept;
    double m_perNs;
    double m_burst;
    double m_tokens;
    std::int64_t m_lastNs = 0;
    bool m_started = false;
};

// ---- the config cache

// Reads CoreConfigReport frames and AdmissionState and ConfigApplied broadcasts. Gate states are ordered by log
// position: a report is as of its asOfLogPosition, an AdmissionState as of its logPosition, and the newer one wins per
// gate, whatever order they arrive in. An AdmissionState for a market the current view does not hold (it arrives
// before the report that lists the market) is kept, the newest per market, and applied by the next report if it is
// newer than that report; the next report drops the ones it makes obsolete. The current view is also published for
// other threads (snapshot()).
class ConfigCache
{
public:
    void beginReport(const wire::CoreConfigHead &head);
    void addMarket(const wire::MarketConfig &market);
    void addAsset(const wire::AssetConfig &asset);
    // the report is complete: it becomes the current view (gates newer than its asOfLogPosition are kept)
    void endReport();
    // true when the view changed
    bool onAdmission(const wire::AdmissionState &admission);
    // true when the record is newer than the current report: read the config again
    bool staleAfter(const wire::ConfigApplied &applied) const noexcept;
    // a re-read is needed until a report as of at least this position arrives
    std::int64_t asOfLogPosition() const noexcept { return m_current.reported ? m_current.head.asOfLogPosition : -1; }

    const CoreConfig &current() const noexcept { return m_current; }
    std::shared_ptr<const CoreConfig> snapshot() const noexcept { return m_published.load(std::memory_order_acquire); }

private:
    void publish();
    CoreConfig m_current;
    CoreConfig m_building;
    std::map<std::int32_t, GateView> m_pending; // by marketId: admissions for markets the current view does not hold
    std::atomic<std::shared_ptr<const CoreConfig>> m_published{ std::make_shared<const CoreConfig>() };
};

}}}

#endif
