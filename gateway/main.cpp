#include "cluster_tools/runtime/Supervisor.h"

#include "GatewayApp.h"

int main()
{
    cluster_tools::runtime::Supervisor::blockSignals(); // before any thread (the embedded driver and Aeron included)
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    return openexchange::gateway::gatewayMain({});
}
