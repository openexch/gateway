// Test build only, never published: the production main of oe-projector with the driver-loss exits switched off
// (ProcessHooks::ignoreDriverLoss). The control of the external-driver case: killed under it, the copy stays alive.
#include "cluster_tools/runtime/Supervisor.h"

#include "ProjectorApp.h"

int main(int argc, char **argv)
{
    cluster_tools::runtime::Supervisor::blockSignals();
    cluster_tools::runtime::Supervisor::ignoreSigpipe();
    static openexchange::gateway::ProjectorHooks hooks;
    hooks.process.ignoreDriverLoss = true;
    return openexchange::gateway::projectorMain(argc, argv, hooks);
}
