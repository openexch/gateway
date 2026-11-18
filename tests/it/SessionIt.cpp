// gw_session_it - test-only (never installed): the core session wrapper over the real Aeron transport against a real
// core cluster, driven line by line from stdin by tests/it/session_it.py. One thread: the duty loop reads stdin without
// blocking, so every command runs between two duty cycles, never inside one.
//
// usage: gw_session_it --mode embedded|external --dir <driver parent dir | AERON_DIR> --ingress <id=host:port,...>
//            --host <response host> [--timeout-ms N] [--query-rate N] [--query-burst N] [--instance N]
//        credential: CORE_AUTH_CREDENTIAL (a GATEWAY key)
// stdin:  status | create <count> <user> <market> <price> <qty> <keyBase> [BUY|SELL] | balances <count> <user> | mute on|off
//         | drop-session <cycles> | quit
//         drop-session: <cycles> duty cycles later the session is closed (the client's close, as a lost session), and the
//         egress is polled again (unmuted) from the next session on; the session reconnects and registers by itself
// stdout: one line per event: "STATUS {...}", "SUBMIT ticket=.. key=..", "OUTCOME ticket=.. kind=.. ...", "ERROR ..."
#include <poll.h>
#include <unistd.h>

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cluster_tools/client/ClusterClient.h"
#include "cluster_tools/client/Credentials.h"
#include "cluster_tools/client/aeron/AeronTransport.h"
#include "cluster_tools/client/aeron/FatalErrors.h"
#include "cluster_tools/runtime/EmbeddedDriver.h"
#include "cluster_tools/runtime/LogRing.h"

#include "gateway/common/CoreSession.h"
#include "gateway/common/OrderEntry.h"
#include "gateway/common/Readiness.h"

namespace cl = cluster_tools::client;
namespace at = cluster_tools::client::aeron_transport;
namespace rt = cluster_tools::runtime;
namespace gs = openexchange::gateway::session;
namespace gw = openexchange::gateway;
namespace wire = openexchange::gateway::wire;

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
        std::fprintf(stderr, "gw_session_it: %s is required\n", name.c_str());
        std::exit(64);
    }
    return fallback;
}

std::string joined(const std::vector<std::string> &names)
{
    std::string s = "[";
    for (std::size_t i = 0; i < names.size(); ++i)
    {
        s += (i ? ",\"" : "\"") + names[i] + "\"";
    }
    return s + "]";
}

}

int main(int argc, char **argv)
{
    const std::vector<std::string> args(argv + 1, argv + argc);
    const std::string mode = arg(args, "--mode");
    const std::string dir = arg(args, "--dir");
    const std::int64_t instance = std::stoll(arg(args, "--instance", "61"));

    const cl::CredentialsResult credentials = cl::credentialsFromEnvironment(cl::SessionClass::GATEWAY);
    if (!credentials.ok())
    {
        std::fprintf(stderr, "gw_session_it: %s\n", credentials.error.c_str());
        return 78;
    }

    std::unique_ptr<rt::EmbeddedDriver> driver;
    std::string aeronDir = dir;
    if ("embedded" == mode)
    {
        rt::EmbeddedDriverOptions options;
        options.processName = "gw_session_it";
        options.dirPrefix = "gw-session-it-";
        options.instanceKey = "--instance";
        aeronDir = rt::EmbeddedDriver::dirFor(dir, instance, options);
        driver = std::make_unique<rt::EmbeddedDriver>(options, [](const std::string &why) {
            std::fprintf(stderr, "gw_session_it: embedded driver terminated: %s\n", why.c_str());
            std::_Exit(70);
        });
        std::string error;
        if (!driver->start(aeronDir, false, error))
        {
            std::fprintf(stderr, "gw_session_it: embedded driver: %s\n", error.c_str());
            return 70;
        }
    }
    else if ("external" != mode)
    {
        std::fprintf(stderr, "gw_session_it: --mode must be embedded or external\n");
        return 64;
    }

    auto monitor = std::make_shared<at::AeronFatalMonitor>();
    std::shared_ptr<aeron::Aeron> aeron;
    try
    {
        at::AeronClientOptions options;
        options.aeronDir = aeronDir;
        options.clientName = "gw_session_it";
        options.driverTimeoutMs = 10'000;
        aeron = at::connectAeron(options, monitor);
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "gw_session_it: no media driver at %s: %s\n", aeronDir.c_str(), e.what());
        return 70;
    }
    out("DRIVER mode=" + mode + " dir=" + aeronDir);

    cl::ClientSettings clientSettings;
    clientSettings.ingressEndpoints = arg(args, "--ingress");
    clientSettings.ingressChannel = "aeron:udp?term-length=4m|mtu=8192";
    clientSettings.responseChannel = "aeron:udp?endpoint=" + arg(args, "--host") + ":0|term-length=4m|mtu=8192";
    clientSettings.credentials = credentials.credentials.encode();
    clientSettings.clientName = "gw_session_it identity=" + std::to_string(instance);

    gs::SessionSettings settings;
    settings.identity = instance;
    settings.credentialClass = credentials.credentials.sessionClass;
    settings.requestTimeoutMs = std::stoll(arg(args, "--timeout-ms", "30000"));
    settings.queryRateCount = std::stoll(arg(args, "--query-rate", "100"));
    settings.queryRatePeriodMs = 1000;
    settings.queryBurst = std::stoll(arg(args, "--query-burst", "200"));
    settings.reconnectDelayMs = 500;
    settings.registerResendMs = 2000;
    settings.encoder = gs::encodeCommand;

    rt::LogRing log;
    int exitCode = 0;
    {
        at::AeronTransport transport(aeron);
        AeronClient client(transport, clientSettings);
        std::map<std::uint64_t, std::int64_t> keys;
        gs::CoreSession<AeronClient> session(client, settings, [&](const gs::Outcome &o) {
            std::ostringstream line;
            line << "OUTCOME ticket=" << o.ticket << " kind=" << gs::nameOf(o.kind) << " template=" << o.templateId
                 << " corr=" << o.correlationId << " offers=" << o.offers << " key=" << keys[o.ticket]
                 << " waitms=" << o.budgetWaitNs / 1'000'000 << " ms=" << (o.completedNs - o.submittedNs) / 1'000'000;
            if (const auto *ack = std::get_if<wire::OrderAck>(&o.reply))
            {
                line << " order=" << ack->orderId << " dup=" << (ack->duplicate ? 1 : 0) << " acorr=" << ack->correlationId;
            }
            else if (const auto *reject = std::get_if<wire::OrderReject>(&o.reply))
            {
                line << " reject=" << wire::money::CoreRejectReason::c_str(reject->reason) << " detail=" << reject->detail;
            }
            else if (const auto *balances = std::get_if<gs::BalancesReply>(&o.reply))
            {
                line << " known=" << (balances->head.known ? 1 : 0) << " lines=" << balances->lines.size();
            }
            out(line.str());
        }, &log);
        if (const std::string e = session.validate(); !e.empty())
        {
            std::fprintf(stderr, "gw_session_it: %s\n", e.c_str());
            return 78;
        }
        session.start();
        bool pollEgress = true;
        std::int64_t dropInCycles = -1; // -1 = no drop pending
        std::uint64_t sessionDownCycles = 0;
        bool running = true;
        std::string pending;
        while (running && 0 == session.exitCode())
        {
            const int work = session.doWork(pollEgress);
            if (dropInCycles > 0)
            {
                dropInCycles--;
            }
            else if (0 == dropInCycles)
            {
                dropInCycles = -1;
                const std::uint64_t epoch = client.sessionEpoch();
                client.close();
                pollEgress = true;
                out("DROPPED epoch=" + std::to_string(epoch));
            }
            {
                // every duty cycle without a registered session to a leader, counted between two status lines (an
                // election can be shorter than the status poll)
                gw::Status probe;
                session.fillStatus(probe);
                sessionDownCycles += (probe.connected && probe.registered) ? 0 : 1;
            }
            log.drain([](const rt::LogRecord &r) { std::fprintf(stderr, "%s\n", r.text); });

            pollfd fd{ 0, POLLIN, 0 };
            if (::poll(&fd, 1, 0) > 0 && (fd.revents & (POLLIN | POLLHUP)))
            {
                char buffer[4096];
                const ssize_t n = ::read(0, buffer, sizeof(buffer));
                if (n <= 0)
                {
                    running = false;
                }
                else
                {
                    pending.append(buffer, static_cast<std::size_t>(n));
                }
            }
            for (std::size_t nl; running && std::string::npos != (nl = pending.find('\n'));)
            {
                std::istringstream line(pending.substr(0, nl));
                pending.erase(0, nl + 1);
                std::string verb;
                line >> verb;
                if ("quit" == verb)
                {
                    running = false;
                }
                else if ("mute" == verb)
                {
                    std::string onOff;
                    line >> onOff;
                    pollEgress = "on" != onOff;
                    out("MUTE " + onOff);
                }
                else if ("drop-session" == verb)
                {
                    std::int64_t cycles = 1;
                    line >> cycles;
                    dropInCycles = (cycles < 1 ? 1 : cycles) - 1; // the drop runs after this many more cycles plus one
                    out("DROP in=" + std::to_string(dropInCycles + 1));
                }
                else if ("create" == verb)
                {
                    std::int64_t count = 0, user = 0, market = 0, price = 0, qty = 0, keyBase = 0;
                    std::string side;
                    line >> count >> user >> market >> price >> qty >> keyBase >> side; // side: BUY (default) or SELL
                    for (std::int64_t i = 0; i < count; ++i)
                    {
                        wire::CreateOrder c;
                        c.userId = user;
                        c.clientOrderIdHigh = 0;
                        c.clientOrderIdLow = keyBase + i;
                        c.requestIdHigh = 0x5E55'17E5'7000'0000LL;
                        c.requestIdLow = keyBase + i;
                        c.marketId = static_cast<std::int32_t>(market);
                        c.side = "SELL" == side ? wire::money::Side::SELL : wire::money::Side::BUY;
                        c.orderType = wire::money::OrderType::LIMIT;
                        c.timeInForce = wire::money::TimeInForce::GTC;
                        c.price = price;
                        c.quantity = qty;
                        const gs::SubmitResult r = session.submit(gs::Command{ c });
                        if (r.accepted)
                        {
                            keys[r.ticket] = keyBase + i;
                            out("SUBMIT ticket=" + std::to_string(r.ticket) + " key=" + std::to_string(keyBase + i));
                        }
                        else
                        {
                            out(std::string("REFUSED ") + gs::nameOf(r.refusal));
                        }
                    }
                }
                else if ("balances" == verb)
                {
                    std::int64_t count = 0, user = 0;
                    line >> count >> user;
                    for (std::int64_t i = 0; i < count; ++i)
                    {
                        const gs::SubmitResult r = session.submit(gs::Command{ wire::RequestUserBalances{ 0, user } });
                        out(r.accepted ? "SUBMIT ticket=" + std::to_string(r.ticket) + " key=0"
                                       : std::string("REFUSED ") + gs::nameOf(r.refusal));
                    }
                }
                else if ("status" == verb)
                {
                    gw::Status s;
                    session.fillStatus(s);
                    s.driverConnected = true;
                    s.publicListenerAccepting = true;
                    const std::int64_t now = client.nanoTime();
                    gw::GatewayReadyBounds bounds;
                    const gs::SessionCounters &c = session.counters();
                    const gs::CoreConfig &config = session.config();
                    std::ostringstream j;
                    j << "STATUS {\"state\":\"" << cl::nameOf(client.state()) << "\",\"connected\":" << s.connected
                      << ",\"registered\":" << s.registered << ",\"epoch\":" << s.sessionEpoch
                      << ",\"term\":" << client.leadershipTermId() << ",\"leader\":" << s.leaderMemberId
                      << ",\"configLoaded\":" << s.configLoaded << ",\"configVersion\":" << config.head.configVersion
                      << ",\"windowMs\":" << config.idempotencyWindowMs() << ",\"gateOpen\":" << s.gateOpen
                      << ",\"held\":" << session.held() << ",\"inflight\":" << session.inFlight()
                      << ",\"offers\":" << c.offers << ",\"reoffers\":" << c.reoffers << ",\"late\":" << c.lateReplies
                      << ",\"unknown\":" << c.outcomeUnknown << ",\"streak\":" << s.timeoutStreak
                      << ",\"registrations\":" << c.registrations << ",\"configReads\":" << c.configReads
                      << ",\"configReports\":" << c.configReports << ",\"reconnects\":" << c.reconnects
                      << ",\"offeredUnregistered\":" << c.commandsOfferedUnregistered
                      << ",\"decodeRefused\":" << s.decodeRefused << ",\"wireIgnored\":" << s.wireIgnored[0]
                      << ",\"storageStarved\":" << s.storageStarved
                      << ",\"keepaliveAgeMs\":" << (s.lastKeepaliveOfferNs < 0 ? -1 : (now - s.lastKeepaliveOfferNs) / 1'000'000)
                      << ",\"notReady\":" << joined(gw::failing(gw::gatewayReadyConditions(s, now, bounds)))
                      << ",\"sessionNotReady\":" << joined(gw::failing(gw::gatewaySessionConditions(s)))
                      << ",\"sessionDownCycles\":" << sessionDownCycles << "}";
                    sessionDownCycles = 0;
                    out(j.str());
                }
                else if (!verb.empty())
                {
                    out("ERROR unknown command " + verb);
                }
            }
            if (0 == work)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        }
        exitCode = session.exitCode();
        if (0 != exitCode)
        {
            out("EXIT " + std::to_string(exitCode) + " " + session.exitDetail());
        }
        session.close();
        log.drain([](const rt::LogRecord &r) { std::fprintf(stderr, "%s\n", r.text); });
    }
    monitor->beginDeliberateClose();
    aeron.reset();
    if (driver)
    {
        driver->close(5000);
    }
    return exitCode;
}
