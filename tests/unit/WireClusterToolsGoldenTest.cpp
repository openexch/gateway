// The gateway decoder against cluster-tools' own golden corpus (adapters/sbe/tests/golden/golden_frames.inc of the
// pinned cluster-tools tree, written by the core's Java encoders at the same core commit): every frame of a template
// the gateway reads decodes to the values cluster-tools' generator wrote. CMake refuses a tree whose corpus names
// another core commit than dependencies.lock.
#include <cstdint>
#include <string>
#include <vector>

#include "Check.h"
#include "gateway/common/Wire.h"

namespace ct_golden {
#include "golden_frames.inc"
}

using namespace openexchange::gateway::wire;

namespace {

struct Last : ReplyHandler
{
    int calls = 0;
    AdminAck ack;
    AdmissionState admission;
    OrderReject reject;
    void onAdminAck(const AdminAck &m) override { calls++; ack = m; }
    void onAdmissionState(const AdmissionState &m) override { calls++; admission = m; }
    void onOrderReject(const OrderReject &m) override { calls++; reject = m; }
};

bool decodeOne(const char *name, const std::vector<std::uint8_t> &frame, Last &last)
{
    Decoder decoder;
    const Refusal r = decoder.decode(frame.data(), frame.size(), last);
    if (Refusal::NONE != r || 1 != last.calls)
    {
        gwtest::fail(__FILE__, __LINE__, std::string("golden mismatch ") + name + ": " + describe(decoder.lastRefusal()));
        return false;
    }
    return true;
}

}

TEST(wire_cluster_tools_golden_admin_acks)
{
    Last a;
    if (decodeOne("AdminAck (cluster-tools JAVA_ADMIN_ACK)", ct_golden::JAVA_ADMIN_ACK, a))
    {
        CHECK_MSG((a.ack == AdminAck{ 0x0102030405060708LL, 80, money::AdminResult::DUPLICATE, 7, 9 }),
            "golden mismatch AdminAck (cluster-tools JAVA_ADMIN_ACK)");
    }
    Last b;
    if (decodeOne("AdminAck (cluster-tools JAVA_ADMIN_ACK_66)", ct_golden::JAVA_ADMIN_ACK_66, b))
    {
        CHECK_MSG((b.ack == AdminAck{ 0x0A0B0C0D0E0F1011LL, 66, money::AdminResult::REJECTED, 4, 0 }),
            "golden mismatch AdminAck (cluster-tools JAVA_ADMIN_ACK_66)");
    }
}

TEST(wire_cluster_tools_golden_admission_state)
{
    Last a;
    if (decodeOne("AdmissionState (cluster-tools)", ct_golden::JAVA_ADMISSION_STATE_V7, a))
    {
        CHECK_MSG((a.admission == AdmissionState{ 0, money::GateState::SUSPENDED, money::GateReason::BRIDGE_HALT, false,
                      1'700'000'000'777LL, 0x4142434445464748LL, (1u << 2) | (1u << 4) }),
            "golden mismatch AdmissionState (cluster-tools)");
    }
}

TEST(wire_cluster_tools_golden_order_rejects)
{
    struct Case
    {
        const char *name;
        const std::vector<std::uint8_t> *frame;
        OrderReject expected;
    };
    const Case cases[] = {
        { "OrderReject (cluster-tools 65 INVALID)", &ct_golden::JAVA_ORDER_REJECT_65_INVALID,
            OrderReject{ 0x6501, 0, 0, 0, 0, 0, 65, money::CoreRejectReason::INVALID, 255, -1, 1'700'000'000'888LL } },
        { "OrderReject (cluster-tools 65 UNREGISTERED)", &ct_golden::JAVA_ORDER_REJECT_65_UNREGISTERED,
            OrderReject{ 0x6502, 0, 0, 0, 0, 0, 65, money::CoreRejectReason::UNREGISTERED_SESSION, 255, -1,
                1'700'000'000'888LL } },
        { "OrderReject (cluster-tools 61 UNREGISTERED)", &ct_golden::JAVA_ORDER_REJECT_61_UNREGISTERED,
            OrderReject{ 0x0102030405060708LL, 0, 0, 0, 0, 0, 61, money::CoreRejectReason::UNREGISTERED_SESSION, 255,
                0, 0x1112131415161718LL } },
        { "OrderReject (cluster-tools 7 INVALID)", &ct_golden::JAVA_ORDER_REJECT_7_INVALID,
            OrderReject{ 0x2122232425262728LL, 0, 0, 0, 0, 0, 7, money::CoreRejectReason::INVALID, 255, 7,
                0x3132333435363738LL } },
    };
    for (const Case &c : cases)
    {
        Last last;
        if (decodeOne(c.name, *c.frame, last))
        {
            CHECK_MSG(last.reject == c.expected, std::string("golden mismatch ") + c.name);
        }
    }
}
