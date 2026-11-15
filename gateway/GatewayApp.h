#ifndef GATEWAY_GATEWAY_APP_H
#define GATEWAY_GATEWAY_APP_H

#include "gateway/common/ProcessShell.h"

namespace openexchange { namespace gateway {

// oe-gateway: parse the settings (every problem in one pass, 78), then run the process. Returns only on a
// configuration refusal.
int gatewayMain(const ProcessHooks &hooks);

}}

#endif
