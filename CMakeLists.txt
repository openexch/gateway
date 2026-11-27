# openexch/gateway - oe-gateway (REST) and oe-stream (WebSocket user streams), two stateless C++20 processes in front
# of the core, oe-projector (the history projector: the core's order journal into the history relations the
# gateway reads, the only binary here that writes a database), and oe-marketdata (the public market data: the ME's
# books on a public WebSocket), sharing one static library, gw_common.
#
# Build:
#   scripts/build-cluster-tools.sh <cache-dir> <prefix>     (fetches, verifies and installs the pinned cluster-tools)
#   scripts/build-deps.sh <cache-dir> <deps-prefix>          (Boost headers, OpenSSL, libpq from the pinned tarballs)
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=<prefix> -DGW_DEPS_PREFIX=<deps-prefix> && cmake --build build && ctest --test-dir build
# Options:
#   GW_DEPS_PREFIX   the Boost, OpenSSL and libpq prefix scripts/build-deps.sh built from the lock (required)
#   GW_CLUSTER_TOOLS_SOURCE_DIR  the verified cluster-tools source tree (scripts/fetch-cluster-tools.sh prints it as
#                    -DCLUSTER_TOOLS_SOURCE_DIR); required with tests: the codec tests read its golden corpus
#   GW_TEST_BUILDS   the never-published test binaries (duty-stall builds, the test media driver); ON for ctest
#   GW_BUILD_SOURCE_SHA  the commit of a build from a source tarball (no .git); /health "build"
cmake_minimum_required(VERSION 3.30)
project(openexchange_gateway LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "build type" FORCE)
endif()

option(GW_TEST_BUILDS "Build the never-published test binaries and register the process tests" ON)
set(GW_VERSION "dev" CACHE STRING "the version string of this build")
set(GW_BUILD_SOURCE_SHA "" CACHE STRING "the commit of a build without .git (source tarball)")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(ClusterToolsPin)
gw_read_lock("${CMAKE_CURRENT_SOURCE_DIR}/dependencies.lock")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/dependencies.lock")

find_package(Threads REQUIRED)
include(GatewayDeps)
set(GW_DEPS_PREFIX "" CACHE PATH "the prefix scripts/build-deps.sh installed Boost, OpenSSL and libpq to")
gw_check_deps("${CMAKE_CURRENT_SOURCE_DIR}/dependencies.lock" "${GW_DEPS_PREFIX}")
find_package(cluster_tools CONFIG REQUIRED COMPONENTS client_core runtime client client_aeron runtime_aeron sbe_codecs
    runtime_embedded_driver archive_core archive_aeron aeron_archive_client forwarder_domain forwarder_codec journal_codec)
gw_check_cluster_tools_package()

# OE_PROJECTOR_CONTROL: a test build of oe-projector (never published) with one control define, or several joined by
# "+" (PROJ_NC_* or PROJ_TC_*), compiled into every projector source of this build tree. The binary keeps its name, so
# a fixture takes the install tree as a slot artifact; the version string carries the control (GW_VERSION+control.x).
#   cmake -S . -B build-nc -DOE_PROJECTOR_CONTROL=PROJ_NC_DROP_ONE -DBUILD_TESTING=OFF -DGW_TEST_BUILDS=OFF ...
#   cmake --build build-nc --target oe-projector && cmake --install build-nc --prefix <slot>
set(OE_PROJECTOR_CONTROL "" CACHE STRING "test build only: the PROJ_NC_*/PROJ_TC_* define(s) oe-projector is built with")
set(GW_PROJECTOR_CONTROL_DEFINITIONS "")
if(OE_PROJECTOR_CONTROL)
    string(REPLACE "+" ";" GW_PROJECTOR_CONTROL_DEFINITIONS "${OE_PROJECTOR_CONTROL}")
    foreach(_define IN LISTS GW_PROJECTOR_CONTROL_DEFINITIONS)
        if(NOT _define MATCHES "^PROJ_(NC|TC)_[A-Z0-9_]+$")
            message(FATAL_ERROR "OE_PROJECTOR_CONTROL=${OE_PROJECTOR_CONTROL}: each define is PROJ_NC_<NAME> or PROJ_TC_<NAME>")
        endif()
    endforeach()
    string(TOLOWER "${OE_PROJECTOR_CONTROL}" _control_tag)
    string(REPLACE "_" "-" _control_tag "${_control_tag}")
    set(GW_VERSION "${GW_VERSION}+control.${_control_tag}")
    message(STATUS "oe-projector control build (test only, never published): ${OE_PROJECTOR_CONTROL}, version ${GW_VERSION}")
endif()

if(GW_BUILD_SOURCE_SHA)
    set(GW_SOURCE_SHA "${GW_BUILD_SOURCE_SHA}")
else()
    execute_process(COMMAND git -C "${CMAKE_CURRENT_SOURCE_DIR}" rev-parse HEAD
        OUTPUT_VARIABLE GW_SOURCE_SHA OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET RESULT_VARIABLE _git)
    if(NOT _git EQUAL 0 OR GW_SOURCE_SHA STREQUAL "")
        set(GW_SOURCE_SHA "unknown")
    endif()
endif()
message(STATUS "gateway source sha (/health build): ${GW_SOURCE_SHA}")
configure_file(cmake/build_info.h.in "${CMAKE_CURRENT_BINARY_DIR}/generated/gateway/build_info.h" @ONLY)

# The users-store migrations (db/migration/V<n>__<name>.sql), embedded with each file's sha256.
file(GLOB GW_MIGRATION_FILES "${CMAKE_CURRENT_SOURCE_DIR}/db/migration/*.sql")
list(SORT GW_MIGRATION_FILES)
set(_gw_migrations "")
foreach(_file IN LISTS GW_MIGRATION_FILES)
    get_filename_component(_name "${_file}" NAME_WE)
    if(NOT _name MATCHES "^V([0-9]+)__([a-z0-9_]+)$")
        message(FATAL_ERROR "${_file}: a migration is named V<number>__<name>.sql (lowercase name)")
    endif()
    set(_label "${CMAKE_MATCH_2}")
    string(REGEX REPLACE "^0+" "" _version "${CMAKE_MATCH_1}")
    file(READ "${_file}" _sql)
    file(SHA256 "${_file}" _sha)
    if(_sql MATCHES "\\)gwsql\"")
        message(FATAL_ERROR "${_file} contains the embedding delimiter )gwsql\"")
    endif()
    string(APPEND _gw_migrations "{ ${_version}, \"${_label}\", R\"gwsql(${_sql})gwsql\", \"${_sha}\" },\n")
endforeach()
file(CONFIGURE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/generated/gateway/migrations.inc" CONTENT "${_gw_migrations}" @ONLY)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${GW_MIGRATION_FILES})

# The history projection schema: its steps (sql/history_projection_v<n>.sql, version 1 the base, every later one applied
# on top of the one before), each embedded with its own sha256. oe-projector applies the steps a database does not
# record yet and records each one with its sha256; a recorded sha256 that differs from the binary's file refuses the
# start.
set(GW_PROJECTION_SCHEMA_STEPS "1|history_projection|sql/history_projection_v1.sql"
    "2|history_retention|sql/history_projection_v2.sql")
set(_projection_steps "")
set(_projection_steps_nc_wallclock "")
foreach(_step IN LISTS GW_PROJECTION_SCHEMA_STEPS)
    string(REPLACE "|" ";" _parts "${_step}")
    list(GET _parts 0 _version)
    list(GET _parts 1 _name)
    list(GET _parts 2 _path)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/${_path}" _sql)
    file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/${_path}" _sha)
    if(_sql MATCHES "\\)gwsql\"")
        message(FATAL_ERROR "${_path} contains the embedding delimiter )gwsql\"")
    endif()
    string(APPEND _projection_steps "{ ${_version}, \"${_name}\", \"${_path}\", R\"gwsql(${_sql})gwsql\", \"${_sha}\" },\n")
    # The schema of the PROJ_NC_WALLCLOCK_COLUMN test build (never published): version 1 plus a wall-clock column on
    # oe_orders, pinned by the sha256 of the text it applies, so its database records that sha256 and a published binary
    # refuses to open it.
    if(_version STREQUAL "1")
        string(APPEND _sql "
-- test build only (PROJ_NC_WALLCLOCK_COLUMN): a wall-clock column, so two projections of one journal differ
ALTER TABLE oe_orders ADD COLUMN projected_at TIMESTAMPTZ NOT NULL DEFAULT now();
")
        string(SHA256 _sha "${_sql}")
    endif()
    string(APPEND _projection_steps_nc_wallclock
        "{ ${_version}, \"${_name}\", \"${_path}\", R\"gwsql(${_sql})gwsql\", \"${_sha}\" },\n")
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${_path}")
endforeach()
file(CONFIGURE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/generated/gateway/projection_schema.inc"
    CONTENT "${_projection_steps}" @ONLY)
file(CONFIGURE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/generated/gateway/projection_schema_nc_wallclock_column.inc"
    CONTENT "${_projection_steps_nc_wallclock}" @ONLY)


# The market history schema (sql/market_history_v1.sql), embedded with its sha256: oe-marketdata applies it to an empty
# database and refuses one whose relations differ.
set(GW_MARKET_HISTORY_SCHEMA_FILE "${CMAKE_CURRENT_SOURCE_DIR}/sql/market_history_v1.sql")
file(READ "${GW_MARKET_HISTORY_SCHEMA_FILE}" _market_history_sql)
file(SHA256 "${GW_MARKET_HISTORY_SCHEMA_FILE}" _market_history_sha)
if(_market_history_sql MATCHES "\\)gwsql\"")
    message(FATAL_ERROR "${GW_MARKET_HISTORY_SCHEMA_FILE} contains the embedding delimiter )gwsql\"")
endif()
# The trade tape's retention function (sql/market_history_retention_v1.sql): applied at every start after the schema
# (CREATE OR REPLACE), so a database an older build wrote gets it in place.
set(GW_MARKET_RETENTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/sql/market_history_retention_v1.sql")
file(READ "${GW_MARKET_RETENTION_FILE}" _market_retention_sql)
file(SHA256 "${GW_MARKET_RETENTION_FILE}" _market_retention_sha)
if(_market_retention_sql MATCHES "\\)gwsql\"")
    message(FATAL_ERROR "${GW_MARKET_RETENTION_FILE} contains the embedding delimiter )gwsql\"")
endif()
file(CONFIGURE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/generated/gateway/market_history_schema.inc"
    CONTENT "constexpr const char *MARKET_HISTORY_SCHEMA_SQL = R\"gwsql(${_market_history_sql})gwsql\";\nconstexpr const char *MARKET_HISTORY_SCHEMA_SHA256 = \"${_market_history_sha}\";\nconstexpr const char *MARKET_RETENTION_SQL = R\"gwsql(${_market_retention_sql})gwsql\";\nconstexpr const char *MARKET_RETENTION_SHA256 = \"${_market_retention_sha}\";\n"
    @ONLY)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${GW_MARKET_HISTORY_SCHEMA_FILE}" "${GW_MARKET_RETENTION_FILE}")

set(GW_WARNINGS -Wall -Wextra -Wpedantic -Werror)

# gw_common: the one library both binaries link. Never links the embedded driver (a library must not: only a final
# executable lists cluster_tools::runtime_embedded_driver, first).
add_library(gw_common STATIC
    common/src/Admin.cpp
    common/src/Api.cpp
    common/src/AuthMaterial.cpp
    common/src/Authenticator.cpp
    common/src/Crypto.cpp
    common/src/DutyBridge.cpp
    common/src/Environment.cpp
    common/src/FrontLog.cpp
    common/src/History.cpp
    common/src/HttpServer.cpp
    common/src/JsonImpl.cpp
    common/src/Jwt.cpp
    common/src/Migrations.cpp
    common/src/Password.cpp
    common/src/PgConn.cpp
    common/src/PgHistoryStore.cpp
    common/src/PgUserStore.cpp
    common/src/ProcessShell.cpp
    common/src/RateLimit.cpp
    common/src/Rest.cpp
    common/src/Readiness.cpp
    common/src/Settings.cpp
    common/src/SessionParts.cpp
    common/src/SettingsReader.cpp
    common/src/Wire.cpp
    common/src/WorkPool.cpp)
target_include_directories(gw_common PUBLIC common/include "${CMAKE_CURRENT_BINARY_DIR}/generated")
target_link_libraries(gw_common PUBLIC cluster_tools::runtime_aeron cluster_tools::client_aeron cluster_tools::client
    cluster_tools::client_core cluster_tools::runtime cluster_tools::sbe_codecs gw::boost gw::pq gw::crypto Threads::Threads)
target_compile_options(gw_common PRIVATE ${GW_WARNINGS})
# the money schema of dependencies.lock: Wire.cpp asserts that the exported codecs carry exactly this id and version
set(GW_MONEY_SCHEMA_DEFINITIONS GW_MONEY_SCHEMA_ID=${GW_LOCK_money_schema_id}
    GW_MONEY_SCHEMA_VERSION=${GW_LOCK_money_schema_version})
target_compile_definitions(gw_common PRIVATE ${GW_MONEY_SCHEMA_DEFINITIONS})

# gw_order_entry: the encoders of every command that writes (CreateOrder, CancelOrder, AmendOrder, Deposit, Withdraw)
# and the session encoder that includes them. oe-gateway links it; oe-stream never does (its key could create orders):
# the stream's sources are built with GW_NO_ORDER_ENTRY, under which OrderEntry.h refuses to compile, and tests/build
# checks that a reference fails to compile and to link and that the finished binary holds none of these symbols.
add_library(gw_order_entry STATIC common/src/OrderEntry.cpp common/src/OrderEntryWire.cpp)
target_link_libraries(gw_order_entry PUBLIC gw_common)
target_compile_options(gw_order_entry PRIVATE ${GW_WARNINGS})

add_library(oe_gateway_app STATIC gateway/GatewayApp.cpp gateway/GatewayApi.cpp gateway/GatewayFront.cpp
    gateway/GatewayRoutes.cpp)
target_include_directories(oe_gateway_app PUBLIC gateway)
target_link_libraries(oe_gateway_app PUBLIC gw_order_entry gw_common)
target_compile_options(oe_gateway_app PRIVATE ${GW_WARNINGS})

set(GW_STREAM_SOURCES stream/StreamCore.cpp stream/WsServer.cpp stream/EngineStatus.cpp stream/EnginePoller.cpp)
add_library(oe_stream_app STATIC stream/StreamApp.cpp ${GW_STREAM_SOURCES})
target_include_directories(oe_stream_app PUBLIC stream)
target_link_libraries(oe_stream_app PUBLIC gw_common)
target_compile_options(oe_stream_app PRIVATE ${GW_WARNINGS})
target_compile_definitions(oe_stream_app PRIVATE GW_NO_ORDER_ENTRY)

# oe-projector: built like the stream (GW_NO_ORDER_ENTRY, no gw_order_entry): it holds a GATEWAY session for its head
# probe only and cannot encode an order or money command.
# It follows the core's order journal with the cluster-tools archive follow library (archive_aeron, on the Aeron archive
# client) and reads each member node's /health with the stream's HTTP poller (stream/EnginePoller.cpp).
set(GW_PROJECTOR_SOURCES projector/ProjectorApp.cpp projector/ProjectorSettings.cpp projector/ProjectionStore.cpp
    projector/JournalRecord.cpp projector/FollowRules.cpp projector/HistoryWork.cpp projector/FollowDuty.cpp
    stream/EnginePoller.cpp stream/EngineStatus.cpp)
add_library(oe_projector_app STATIC ${GW_PROJECTOR_SOURCES})
target_include_directories(oe_projector_app PUBLIC projector PRIVATE stream)
target_link_libraries(oe_projector_app PUBLIC gw_common cluster_tools::archive_aeron cluster_tools::archive_core)
target_compile_options(oe_projector_app PRIVATE ${GW_WARNINGS})
target_compile_definitions(oe_projector_app PRIVATE GW_NO_ORDER_ENTRY ${GW_PROJECTOR_CONTROL_DEFINITIONS})

# oe-marketdata: built like the stream (GW_NO_ORDER_ENTRY, no gw_order_entry): it holds a GATEWAY-class ME session that
# sends no ingress but its keepalive. The book ladder and the ME book egress decode are the cluster-tools package's
# (forwarder_domain, forwarder_codec); the ME journal follower and its trade decode (journal_codec, archive_aeron) are
# the trade path's.
set(GW_MARKETDATA_SOURCES marketdata/MarketDataApp.cpp marketdata/MarketDataSettings.cpp marketdata/BookCore.cpp
    marketdata/BookFrames.cpp marketdata/MdWsServer.cpp marketdata/Candles.cpp marketdata/TradePath.cpp
    marketdata/MarketHistory.cpp marketdata/HistoryWriter.cpp marketdata/JournalFollow.cpp marketdata/TradeCore.cpp
    marketdata/CandleRoute.cpp)
add_library(oe_marketdata_app STATIC ${GW_MARKETDATA_SOURCES})
target_include_directories(oe_marketdata_app PUBLIC marketdata)
target_link_libraries(oe_marketdata_app PUBLIC gw_common cluster_tools::forwarder_codec cluster_tools::forwarder_domain
    cluster_tools::journal_codec cluster_tools::archive_aeron cluster_tools::archive_core)
target_compile_options(oe_marketdata_app PRIVATE ${GW_WARNINGS})
target_compile_definitions(oe_marketdata_app PRIVATE GW_NO_ORDER_ENTRY)

# The binaries: the embedded driver first in the link list (its archive resolves the C client symbols before
# aeron_static is scanned).
add_executable(oe-gateway gateway/main.cpp)
target_link_libraries(oe-gateway PRIVATE cluster_tools::runtime_embedded_driver oe_gateway_app)
add_executable(oe-stream stream/main.cpp)
target_link_libraries(oe-stream PRIVATE cluster_tools::runtime_embedded_driver oe_stream_app)
target_compile_definitions(oe-stream PRIVATE GW_NO_ORDER_ENTRY)
add_executable(oe-projector projector/main.cpp)
target_link_libraries(oe-projector PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app)
target_compile_definitions(oe-projector PRIVATE GW_NO_ORDER_ENTRY ${GW_PROJECTOR_CONTROL_DEFINITIONS})
add_executable(oe-marketdata marketdata/main.cpp)
target_link_libraries(oe-marketdata PRIVATE cluster_tools::runtime_embedded_driver oe_marketdata_app)
target_compile_definitions(oe-marketdata PRIVATE GW_NO_ORDER_ENTRY)
if(OE_PROJECTOR_CONTROL)
    install(TARGETS oe-projector RUNTIME DESTINATION bin) # a control tree installs its oe-projector alone
else()
    install(TARGETS oe-gateway oe-stream oe-projector oe-marketdata RUNTIME DESTINATION bin)
endif()

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
