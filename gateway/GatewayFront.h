#ifndef GATEWAY_GATEWAY_FRONT_H
#define GATEWAY_GATEWAY_FRONT_H

#include <memory>
#include <string>

#include "gateway/common/AuthMaterial.h"
#include "gateway/common/Authenticator.h"
#include "gateway/common/DutyBridge.h"
#include "gateway/common/HttpServer.h"
#include "gateway/common/PgHistoryStore.h"
#include "gateway/common/PgUserStore.h"
#include "gateway/common/ProcessShell.h"
#include "gateway/common/WorkPool.h"

#include "GatewayApi.h"

// GatewayFront - oe-gateway's public side as the process shell runs it (FrontWork): the REST listener
// (GATEWAY_HTTP_BIND, gw-io-<n>), the password hash pool (gw-hash-<n>, demo mode), the users store pool
// (gw-pg-auth-<n>, demo mode), the history pool (gw-pg-hist-<n>, with GATEWAY_PG_URL; its own connections) and the
// duty bridge's completion thread (gw-done). None of them is the duty thread, and
// the duty thread waits on none of them.

namespace openexchange { namespace gateway {

class GatewayFront final : public FrontWork
{
public:
    GatewayFront(const GatewaySettings &settings, AuthMaterial material, DutyBridge &bridge);
    ~GatewayFront() override;

    bool bind(std::string &error) override;
    void start() override;
    bool stop(std::int64_t boundMs) override;
    bool accepting() const override { return m_server->accepting(); }
    std::string healthJson() const override;
    std::string metrics(const std::string &prefix) const override;

    std::uint16_t boundPort() const noexcept { return m_server->boundPort(); }

private:
    const GatewaySettings &m_settings;
    DutyBridge &m_bridge;
    std::unique_ptr<PgUserStore> m_store;
    std::unique_ptr<WorkPool> m_storePool;
    std::unique_ptr<WorkPool> m_hashPool;
    std::unique_ptr<PgHistoryStore> m_history;
    std::unique_ptr<WorkPool> m_historyPool;
    std::unique_ptr<Authenticator> m_auth;
    std::unique_ptr<GatewayApi> m_api;
    std::unique_ptr<HttpServer> m_server;
};

}}

#endif
