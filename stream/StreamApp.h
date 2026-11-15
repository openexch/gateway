#ifndef GATEWAY_STREAM_APP_H
#define GATEWAY_STREAM_APP_H

#include "gateway/common/ProcessShell.h"

namespace openexchange { namespace gateway {

// oe-stream: parse the settings (every problem in one pass, 78), then run the process. Returns only on a
// configuration refusal.
int streamMain(const ProcessHooks &hooks);

}}

#endif
