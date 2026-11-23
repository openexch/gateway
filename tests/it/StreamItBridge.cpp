// gw_stream_it_bridge - test-only (never installed): stands in for the matching engine's settlement bridge in the
// stream's T1 rows, so that orders fill against a real core with no matching engine. One BRIDGE session registers and
// sends MeJournalTrade (61) frames from stdin; the core (behaviour 1 or 2, where a trade is applied as it comes) settles
// the trade against the two orders' holds and emits ACK_NEW and FILL events for both orders.
//
// usage: gw_stream_it_bridge --mode embedded|external --dir <driver parent dir | AERON_DIR> --ingress <id=host:port,...>
//            --host <response host> [--instance N]
//        credential: CORE_AUTH_CREDENTIAL (a BRIDGE key)
// stdin:  trade <tradeId> <market> <takerOrderId> <takerUser> <makerOrderId> <makerUser> <price> <qty> <takerIsBuy 0|1>
//         | status | quit
// stdout: "REGISTERED", "SENT <tradeId>", "STATUS registered=<0|1>", "ERROR ..."
#include <poll.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/CoreSessionRegistrar.h"
#include "cluster_tools/client/Credentials.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/client/aeron/FatalErrors.h"
#include "cluster_tools/runtime/EmbeddedDriver.h"

#include "com_openexchange_core_infrastructure_generated/BoolFlag.h"
#include "com_openexchange_core_infrastructure_generated/MeJournalTrade.h"
#include "com_openexchange_core_infrastructure_generated/MessageHeader.h"

namespace cl = cluster_tools::client;
namespace at = cluster_tools::client::aeron_transport;
namespace rt = cluster_tools::runtime;
namespace money = com::openexchange::core::infrastructure::generated;

namespace {

using AeronClient = cl::ClusterClient<at::AeronTransport>;

void out(const std::string &line)
{
    std::fputs(line.c_str(), stdout);
    std::fputc('\n', stdout);
    std::fflush(stdout);
}

std::string arg(const std::vector<std::string> &args, const std::string &name, const std::string &fallback = "")
{
    for (std::size_t i = 0; i + 1 < args.size(); ++i)
    {
        if (args[i] == name)
        {
            return args[i + 1];
        }
    }
    if (fallback.empty())
    {
        std::fprintf(stderr, "gw_stream_it_bridge: %s is required\n", name.c_str());
        std::exit(64);
    }
    return fallback;
}

}

int main(int argc, char **argv)
{
    const std::vector<std::string> args(argv + 1, argv + argc);
    const std::string mode = arg(args, "--mode");
    const std::string dir = arg(args, "--dir");
    const std::int64_t instance = std::stoll(arg(args, "--instance", "71"));
    const cl::CredentialsResult credentials = cl::credentialsFromEnvironment(cl::SessionClass::BRIDGE);
    if (!credentials.ok())
    {
        std::fprintf(stderr, "gw_stream_it_bridge: %s\n", credentials.error.c_str());
        return 78;
    }
    std::unique_ptr<rt::EmbeddedDriver> driver;
    std::string aeronDir = dir;
    if ("embedded" == mode)
    {
        rt::EmbeddedDriverOptions options;
        options.processName = "gw_stream_it_bridge";
        options.dirPrefix = "gw-stream-it-bridge-";
        options.instanceKey = "--instance";
        aeronDir = rt::EmbeddedDriver::dirFor(dir, instance, options);
        driver = std::make_unique<rt::EmbeddedDriver>(options, [](const std::string &why) {
            std::fprintf(stderr, "gw_stream_it_bridge: embedded driver terminated: %s\n", why.c_str());
            std::_Exit(70);
        });
        std::string error;
        if (!driver->start(aeronDir, false, error))
        {
            std::fprintf(stderr, "gw_stream_it_bridge: embedded driver: %s\n", error.c_str());
            return 70;
        }
    }
    auto monitor = std::make_shared<at::AeronFatalMonitor>();
    std::shared_ptr<aeron::Aeron> aeron;
    try
    {
        at::AeronClientOptions options;
        options.aeronDir = aeronDir;
        options.clientName = "gw_stream_it_bridge";
        options.driverTimeoutMs = 10'000;
        aeron = at::connectAeron(options, monitor);
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "gw_stream_it_bridge: no media driver at %s: %s\n", aeronDir.c_str(), e.what());
        return 70;
    }
    cl::ClientSettings clientSettings;
    clientSettings.ingressEndpoints = arg(args, "--ingress");
    clientSettings.ingressChannel = "aeron:udp?term-length=4m|mtu=8192";
    clientSettings.responseChannel = "aeron:udp?endpoint=" + arg(args, "--host") + ":0|term-length=4m|mtu=8192";
    clientSettings.credentials = credentials.credentials.encode();
    clientSettings.clientName = "gw_stream_it_bridge";
    cl::RegistrarSettings rs;
    rs.sessionClass = cl::SessionClass::BRIDGE;
    rs.credentialClass = credentials.credentials.sessionClass;
    rs.channels = cl::core_channel::ACKS;
    rs.identity = instance;
    rs.resendIntervalNs = 2'000'000'000LL;
    {
        at::AeronTransport transport(aeron);
        AeronClient client(transport, clientSettings);
        cl::CoreSessionRegistrar<AeronClient> registrar(client, rs);
        if (!client.connect())
        {
            std::fprintf(stderr, "gw_stream_it_bridge: connect refused: %s\n", client.lastDetail().c_str());
            return 78;
        }
        std::deque<std::vector<std::uint8_t>> queue;
        std::deque<std::int64_t> queueIds;
        bool announced = false;
        std::string pendingLine;
        bool quit = false;
        while (!quit)
        {
            const cl::ClientState s = client.state();
            if (cl::ClientState::CLOSED == s || cl::ClientState::FAILED == s)
            {
                client.connect();
            }
            client.doWork();
            registrar.doWork();
            client.poll([&](const cl::EgressMessage &m) { registrar.onEgress(m); }, 32);
            if (registrar.isRegistered() && !announced)
            {
                announced = true;
                out("REGISTERED");
            }
            while (registrar.isRegistered() && !queue.empty())
            {
                const cl::OfferResult r = client.tryOffer(queue.front().data(), queue.front().size());
                if (cl::OfferStatus::OK != r.status)
                {
                    break;
                }
                out("SENT " + std::to_string(queueIds.front()));
                queue.pop_front();
                queueIds.pop_front();
            }
            pollfd p{ 0, POLLIN, 0 };
            if (::poll(&p, 1, 0) > 0)
            {
                char buf[4096];
                const ssize_t n = ::read(0, buf, sizeof(buf));
                if (n <= 0)
                {
                    break;
                }
                pendingLine.append(buf, static_cast<std::size_t>(n));
                std::size_t nl;
                while (std::string::npos != (nl = pendingLine.find('\n')))
                {
                    const std::string line = pendingLine.substr(0, nl);
                    pendingLine.erase(0, nl + 1);
                    std::istringstream in(line);
                    std::string verb;
                    in >> verb;
                    if ("quit" == verb)
                    {
                        quit = true;
                    }
                    else if ("status" == verb)
                    {
                        out(std::string("STATUS registered=") + (registrar.isRegistered() ? "1" : "0"));
                    }
                    else if ("trade" == verb)
                    {
                        std::int64_t tradeId, takerOrder, takerUser, makerOrder, makerUser, price, qty;
                        int market, buy;
                        in >> tradeId >> market >> takerOrder >> takerUser >> makerOrder >> makerUser >> price >> qty >> buy;
                        std::vector<std::uint8_t> b(256, 0);
                        // the ME's own clock (epoch ms), as the matching engine stamps a trade; never 0
                        const std::int64_t meNowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
                        money::MeJournalTrade t;
                        t.wrapAndApplyHeader(reinterpret_cast<char *>(b.data()), 0, b.size())
                            .egressSeq(tradeId)
                            .tradeId(tradeId)
                            .marketId(market)
                            .takerOrderId(1'000'000 + takerOrder)
                            .takerUserId(takerUser)
                            .makerOrderId(1'000'000 + makerOrder)
                            .makerUserId(makerUser)
                            .price(price)
                            .quantity(qty)
                            .takerIsBuy(buy ? money::BoolFlag::TRUE : money::BoolFlag::FALSE)
                            .meTimestamp(meNowMs)
                            .takerOmsOrderId(takerOrder)
                            .makerOmsOrderId(makerOrder)
                            .meEpoch(1)
                            .journalSeq(tradeId);
                        b.resize(money::MessageHeader::encodedLength() + t.encodedLength());
                        queue.push_back(std::move(b));
                        queueIds.push_back(tradeId);
                    }
                    else if (!verb.empty())
                    {
                        out("ERROR unknown verb " + verb);
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        client.close();
    }
    return 0;
}
