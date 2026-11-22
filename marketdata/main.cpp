#include "cluster_tools/runtime/Supervisor.h"
#include "gateway/common/Version.h"

#include "MarketDataApp.h"

int main(int argc, char **argv)
{
    if (openexchange::gateway::versionRequested(argc, argv))
    {
        return openexchange::gateway::printVersion("oe-marketdata");
    }
    cluster_tools::runtime::Supervisor::blockSignals(); // before any thread (the embedded driver and Aeron included)
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    return openexchange::gateway::marketdata::marketDataMain();
}
