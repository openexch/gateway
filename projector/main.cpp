#include "cluster_tools/runtime/Supervisor.h"

#include "ProjectorApp.h"

int main(int argc, char **argv)
{
    cluster_tools::runtime::Supervisor::blockSignals(); // before any thread (the embedded driver and Aeron included)
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    static const openexchange::gateway::ProjectorHooks hooks;
    return openexchange::gateway::projectorMain(argc, argv, hooks);
}
