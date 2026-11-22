#include "cluster_tools/runtime/Supervisor.h"

#include "MarketDataApp.h"

int main()
{
    cluster_tools::runtime::Supervisor::blockSignals(); // before any thread (the embedded driver and Aeron included)
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    return openexchange::gateway::marketdata::marketDataMain();
}
