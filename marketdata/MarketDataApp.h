#ifndef GATEWAY_MARKETDATA_APP_H
#define GATEWAY_MARKETDATA_APP_H

namespace openexchange { namespace gateway { namespace marketdata {

// oe-marketdata: parse the settings (every problem in one pass, 78), then run the process. Returns only on a
// configuration refusal.
int marketDataMain();

}}}

#endif
