# T0: unit tests of the shared library (settings, readiness, admin rendering, the money-frame codec against the
# golden corpora) and the lock verification.
# Process tests: the real binaries, both media driver modes, the exit codes; they need GW_TEST_BUILDS (the duty-stall
# builds and the test media driver, never published).

find_package(Python3 REQUIRED COMPONENTS Interpreter)
option(GW_PG_TESTS "Run the users-store tests against PostgreSQL in a docker container" OFF)
set(GW_PG_TEST_IMAGE "postgres:16.9-alpine@sha256:7c688148e5e156d0e86df7ba8ae5a05a2386aaec1e2ad8e6d11bdf10504b1fb7"
    CACHE STRING "the PostgreSQL image of the users-store tests (pinned by digest)")

# The codec tests read two golden corpora written by the core's Java encoders at the commit dependencies.lock pins:
# the gateway's own (tests/golden/gateway_frames.inc, tests/golden/regen.sh) and cluster-tools' (golden_frames.inc of
# the verified cluster-tools tree). A tree whose corpus or lock names another core commit is refused here, by name.
set(GW_CLUSTER_TOOLS_SOURCE_DIR "" CACHE PATH
    "the verified cluster-tools source tree (scripts/fetch-cluster-tools.sh <cache> --verify-only prints it)")
if(GW_CLUSTER_TOOLS_SOURCE_DIR STREQUAL "")
    message(FATAL_ERROR "GW_CLUSTER_TOOLS_SOURCE_DIR is required with tests: the codec tests read the cluster-tools "
        "golden corpus. scripts/fetch-cluster-tools.sh <cache-dir> --verify-only prints the tree as "
        "-DCLUSTER_TOOLS_SOURCE_DIR=<dir>; pass -DGW_CLUSTER_TOOLS_SOURCE_DIR=<dir> (or -DBUILD_TESTING=OFF)")
endif()
set(GW_CT_GOLDEN_DIR "${GW_CLUSTER_TOOLS_SOURCE_DIR}/adapters/sbe/tests/golden")
if(NOT EXISTS "${GW_CT_GOLDEN_DIR}/golden_frames.inc" OR NOT EXISTS "${GW_CLUSTER_TOOLS_SOURCE_DIR}/dependencies.lock")
    message(FATAL_ERROR "GW_CLUSTER_TOOLS_SOURCE_DIR '${GW_CLUSTER_TOOLS_SOURCE_DIR}' is not a cluster-tools tree "
        "(no adapters/sbe/tests/golden/golden_frames.inc or dependencies.lock)")
endif()
file(READ "${GW_CLUSTER_TOOLS_SOURCE_DIR}/dependencies.lock" _ct_lock)
string(JSON _ct_money_ref ERROR_VARIABLE _err GET "${_ct_lock}" money_schema_ref)
if(_err OR NOT _ct_money_ref STREQUAL GW_LOCK_money_schema_ref)
    message(FATAL_ERROR "the cluster-tools tree at ${GW_CLUSTER_TOOLS_SOURCE_DIR} pins money schema "
        "'${_ct_money_ref}'; dependencies.lock pins ${GW_LOCK_money_schema_ref}")
endif()
file(STRINGS "${GW_CT_GOLDEN_DIR}/golden_frames.inc" _ct_golden_core REGEX "^// core: Build-Sha " LIMIT_COUNT 1)
string(REGEX REPLACE "^// core: Build-Sha ([0-9a-f]+).*" "\\1" _ct_golden_core "${_ct_golden_core}")
string(LENGTH "${_ct_golden_core}" _ct_golden_core_length)
if(_ct_golden_core_length LESS 7)
    message(FATAL_ERROR "${GW_CT_GOLDEN_DIR}/golden_frames.inc names no core Build-Sha")
endif()
string(SUBSTRING "${GW_LOCK_money_schema_ref}" 0 ${_ct_golden_core_length} _lock_prefix)
if(NOT _ct_golden_core STREQUAL _lock_prefix)
    message(FATAL_ERROR "${GW_CT_GOLDEN_DIR}/golden_frames.inc was written by core ${_ct_golden_core}; "
        "dependencies.lock pins ${GW_LOCK_money_schema_ref}")
endif()

set(GW_WIRE_TEST_DEFINITIONS ${GW_MONEY_SCHEMA_DEFINITIONS} GW_MONEY_SCHEMA_REF="${GW_LOCK_money_schema_ref}")

# The session tests drive the wrapper over the cluster-tools FakeTransport (client/tests of the same verified tree).
set(GW_CT_FAKE_TRANSPORT_DIR "${GW_CLUSTER_TOOLS_SOURCE_DIR}/client/tests")
if(NOT EXISTS "${GW_CT_FAKE_TRANSPORT_DIR}/FakeTransport.h")
    message(FATAL_ERROR "${GW_CT_FAKE_TRANSPORT_DIR}/FakeTransport.h not found: the session tests need the cluster-tools "
        "test transport of the verified tree")
endif()

add_executable(gw_unit_tests unit/TestMain.cpp unit/SettingsTest.cpp unit/AdminTest.cpp unit/WireGoldenTest.cpp
    unit/WireRefusalTest.cpp unit/WireClusterToolsGoldenTest.cpp unit/SessionTest.cpp unit/AuthTest.cpp
    unit/GatewayApiTest.cpp unit/RestTest.cpp unit/RestRoutesTest.cpp unit/HistoryTest.cpp)
target_include_directories(gw_unit_tests PRIVATE unit golden "${GW_CT_GOLDEN_DIR}" "${GW_CT_FAKE_TRANSPORT_DIR}")
target_compile_definitions(gw_unit_tests PRIVATE ${GW_WIRE_TEST_DEFINITIONS})
target_link_libraries(gw_unit_tests PRIVATE oe_gateway_app gw_common)
target_compile_options(gw_unit_tests PRIVATE ${GW_WARNINGS})
add_test(NAME unit COMMAND gw_unit_tests)

# Control for the codec parity row: the codec built with CreateOrder's price and quantity swapped (GW_NC_SWAPPED_FIELD,
# test-only, never linked into a binary) must fail the golden case and name the template.
add_executable(gw_wire_nc_swapped_field unit/TestMain.cpp unit/WireGoldenTest.cpp
    "${PROJECT_SOURCE_DIR}/common/src/Wire.cpp"
    "${PROJECT_SOURCE_DIR}/common/src/OrderEntryWire.cpp")
target_include_directories(gw_wire_nc_swapped_field PRIVATE unit golden "${PROJECT_SOURCE_DIR}/common/include")
target_compile_definitions(gw_wire_nc_swapped_field PRIVATE ${GW_WIRE_TEST_DEFINITIONS} GW_NC_SWAPPED_FIELD)
target_link_libraries(gw_wire_nc_swapped_field PRIVATE cluster_tools::client_core cluster_tools::sbe_codecs)
target_compile_options(gw_wire_nc_swapped_field PRIVATE ${GW_WARNINGS})
add_test(NAME wire_nc_swapped_field COMMAND gw_wire_nc_swapped_field wire_golden_commands)
set_tests_properties(wire_nc_swapped_field PROPERTIES PASS_REGULAR_EXPRESSION "golden mismatch CreateOrder")

# Control for the decode parity rows: the codec built with OrderEvent's price and stopPrice swapped on decode
# (GW_NC_SWAPPED_DECODE_FIELD, test-only, never linked into a binary) must fail the golden reply case and name it.
add_executable(gw_wire_nc_swapped_decode_field unit/TestMain.cpp unit/WireGoldenTest.cpp
    "${PROJECT_SOURCE_DIR}/common/src/Wire.cpp"
    "${PROJECT_SOURCE_DIR}/common/src/OrderEntryWire.cpp")
target_include_directories(gw_wire_nc_swapped_decode_field PRIVATE unit golden "${PROJECT_SOURCE_DIR}/common/include")
target_compile_definitions(gw_wire_nc_swapped_decode_field PRIVATE ${GW_WIRE_TEST_DEFINITIONS} GW_NC_SWAPPED_DECODE_FIELD)
target_link_libraries(gw_wire_nc_swapped_decode_field PRIVATE cluster_tools::client_core cluster_tools::sbe_codecs)
target_compile_options(gw_wire_nc_swapped_decode_field PRIVATE ${GW_WARNINGS})
add_test(NAME wire_nc_swapped_decode_field COMMAND gw_wire_nc_swapped_decode_field wire_golden_replies)
set_tests_properties(wire_nc_swapped_decode_field PROPERTIES PASS_REGULAR_EXPRESSION
    "golden mismatch OrderEvent: decoded values")

# Control for the version gate: the decoder built without its exact-version check (GW_NC_NO_VERSION_GATE, test-only)
# must fail the version row: a v8 frame read with v9 offsets.
add_executable(gw_wire_nc_no_version_gate unit/TestMain.cpp unit/WireRefusalTest.cpp
    "${PROJECT_SOURCE_DIR}/common/src/Wire.cpp"
    "${PROJECT_SOURCE_DIR}/common/src/OrderEntryWire.cpp")
target_include_directories(gw_wire_nc_no_version_gate PRIVATE unit golden "${PROJECT_SOURCE_DIR}/common/include")
target_compile_definitions(gw_wire_nc_no_version_gate PRIVATE ${GW_WIRE_TEST_DEFINITIONS} GW_NC_NO_VERSION_GATE)
target_link_libraries(gw_wire_nc_no_version_gate PRIVATE cluster_tools::client_core cluster_tools::sbe_codecs)
target_compile_options(gw_wire_nc_no_version_gate PRIVATE ${GW_WARNINGS})
add_test(NAME wire_nc_no_version_gate COMMAND gw_wire_nc_no_version_gate wire_refuses_every_other_version_by_name)
set_tests_properties(wire_nc_no_version_gate PROPERTIES PASS_REGULAR_EXPRESSION "expected wrong_version, got none")

# Controls for the session rows: each build switches one rule of CoreSession.h off (test-only defines, never in a
# binary) and runs the case that guards it; the case must fail with the named text.
set(GW_SESSION_CONTROLS
    "GW_NC_REGISTERED_AT_OFFER|session_registered_means_acked|command offered before registration ack"
    "GW_NC_NOT_CONNECTED_FATAL|session_offer_table|request failed during election"
    "GW_NC_NO_REOFFER|session_reoffers_after_a_new_leader|request [0-9]+ unanswered after election"
    "GW_NC_FRESH_KEY|session_ack_loss_same_key|second order for one Idempotency-Key"
    "GW_NC_LATE_REPLY_BY_KEY|session_504_and_the_late_reply|reply delivered to the wrong request"
    "GW_NC_NO_QUERY_BUDGET|session_query_budget|query rate above the bound"
    "GW_NC_ADMINACK_ANY_VERSION|session_adminack_of_another_version_never_registers|registered on a v7 AdminAck"
    "GW_NC_NO_STORAGE_STARVATION|session_storage_starvation_exits_70|storage starvation retried forever")
foreach(control IN LISTS GW_SESSION_CONTROLS)
    string(REPLACE "|" ";" parts "${control}")
    list(GET parts 0 define)
    list(GET parts 1 case)
    list(GET parts 2 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "gw_nc_" "gw_session_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp unit/SessionTest.cpp)
    target_include_directories(${target} PRIVATE unit "${GW_CT_FAKE_TRANSPORT_DIR}")
    target_compile_definitions(${target} PRIVATE ${define})
    target_link_libraries(${target} PRIVATE gw_order_entry gw_common)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
    add_test(NAME ${target} COMMAND ${target} ${case})
    set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

# Controls for the auth rows: each build compiles one source with one rule switched off (test-only defines, never in a
# binary) and runs the case that guards it; the case must fail with the named text.
#   GW_NC_QUERY_TOKEN        the principal resolver also reads ?token= from the URL
#   GW_NC_HASH_ON_IO         PBKDF2 runs on the request's I/O strand instead of the hash pool
#   GW_NC_NO_IDENTITY_CHECK  register writes the row without asking the core
set(GW_AUTH_CONTROLS
    "GW_NC_QUERY_TOKEN|unit/AuthTest.cpp|${PROJECT_SOURCE_DIR}/common/src/Authenticator.cpp|auth_token_never_read_from_url|token accepted from URL"
    "GW_NC_HASH_ON_IO|unit/GatewayApiTest.cpp|${PROJECT_SOURCE_DIR}/gateway/GatewayApi.cpp|api_unauthenticated_budget|order request delayed by login burst"
    "GW_NC_NO_IDENTITY_CHECK|unit/GatewayApiTest.cpp|${PROJECT_SOURCE_DIR}/gateway/GatewayApi.cpp|api_identity_check|new user inherited an existing account")
foreach(control IN LISTS GW_AUTH_CONTROLS)
    string(REPLACE "|" ";" parts "${control}")
    list(GET parts 0 define)
    list(GET parts 1 test_source)
    list(GET parts 2 rule_source)
    list(GET parts 3 case)
    list(GET parts 4 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "gw_nc_" "gw_auth_nc_" target "${target}")
    set(extra_sources "")
    if(rule_source MATCHES "GatewayApi.cpp$")
        set(extra_sources "${PROJECT_SOURCE_DIR}/gateway/GatewayRoutes.cpp") # the rest of GatewayApi's members
    endif()
    add_executable(${target} unit/TestMain.cpp ${test_source} ${rule_source} ${extra_sources})
    target_include_directories(${target} PRIVATE unit "${PROJECT_SOURCE_DIR}/gateway")
    target_compile_definitions(${target} PRIVATE ${define})
    target_link_libraries(${target} PRIVATE gw_order_entry gw_common)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
    add_test(NAME ${target} COMMAND ${target} ${case})
    set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

# Controls for the REST rows: each build compiles the routes (and the status table) with one rule switched off
# (test-only defines, never in a binary) and runs the case that guards it; the case must fail with the named text.
#   GW_NC_KEYFINAL_INVERTED       the status table answers keyFinal inverted for every reason
#   GW_NC_NO_CANCEL_REOFFER_RULE  a re-offered unkeyed cancel is answered by its UNKNOWN_ORDER or NOT_OPEN reject
#   GW_NC_NO_ASSET_CHECK          a deposit is sent with an assetId the core's asset list does not hold
#   GW_NC_RANDOM_FUNDING_ID       register funding uses a fresh random requestId per attempt
#   GW_NC_BODY_USER               a userId in a request body is accepted, and the bulk cancel uses it
set(GW_REST_CONTROLS
    "GW_NC_KEYFINAL_INVERTED|unit/RestTest.cpp|rest_status_table|keyFinal wrong for [A-Z_]+"
    "GW_NC_NO_CANCEL_REOFFER_RULE|unit/RestRoutesTest.cpp|routes_cancel_reoffer_rule|successful cancel answered NOT_FOUND"
    "GW_NC_NO_ASSET_CHECK|unit/RestRoutesTest.cpp|routes_treasury_unknown_asset|request unanswered: decode drop"
    "GW_NC_RANDOM_FUNDING_ID|unit/RestRoutesTest.cpp|routes_register_funding|registration funded twice"
    "GW_NC_BODY_USER|unit/RestRoutesTest.cpp|routes_cancel_owner_binding|cancel of another user's order accepted")
foreach(control IN LISTS GW_REST_CONTROLS)
    string(REPLACE "|" ";" parts "${control}")
    list(GET parts 0 define)
    list(GET parts 1 test_source)
    list(GET parts 2 case)
    list(GET parts 3 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "gw_nc_" "gw_rest_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp ${test_source} "${PROJECT_SOURCE_DIR}/common/src/Rest.cpp"
        "${PROJECT_SOURCE_DIR}/common/src/Api.cpp" "${PROJECT_SOURCE_DIR}/gateway/GatewayApi.cpp"
        "${PROJECT_SOURCE_DIR}/gateway/GatewayRoutes.cpp")
    target_include_directories(${target} PRIVATE unit "${PROJECT_SOURCE_DIR}/gateway")
    target_compile_definitions(${target} PRIVATE ${define})
    target_link_libraries(${target} PRIVATE gw_order_entry gw_common)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
    add_test(NAME ${target} COMMAND ${target} ${case})
    set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

# oe-stream T0: StreamCore over the model core (tests/unit/StreamRig.h) and the WebSocket edge on loopback. Built like
# the stream: GW_NO_ORDER_ENTRY, no gw_order_entry.
add_executable(gw_stream_tests unit/TestMain.cpp unit/StreamCoreTest.cpp unit/WsServerTest.cpp unit/EngineStatusTest.cpp)
target_include_directories(gw_stream_tests PRIVATE unit)
target_compile_definitions(gw_stream_tests PRIVATE GW_NO_ORDER_ENTRY)
target_link_libraries(gw_stream_tests PRIVATE oe_stream_app gw_common)
target_link_libraries(gw_stream_tests PRIVATE ${CMAKE_DL_LIBS})
target_compile_options(gw_stream_tests PRIVATE ${GW_WARNINGS})
# The slow name lookup of engine_poller_slow_dns_is_bounded: a test-only getaddrinfo shim, preloaded into this test only.
add_library(gw_test_slow_dns SHARED unit/SlowDnsShim.cpp)
target_link_libraries(gw_test_slow_dns PRIVATE ${CMAKE_DL_LIBS})
add_dependencies(gw_stream_tests gw_test_slow_dns)
add_test(NAME stream COMMAND gw_stream_tests)
set_tests_properties(stream PROPERTIES ENVIRONMENT "LD_PRELOAD=$<TARGET_FILE:gw_test_slow_dns>")

# Controls for the stream rows: each build compiles StreamCore and WsServer with one rule switched off (test-only
# defines, never in a binary) and runs the case that guards it; the case must fail with the named text.
#   GW_NC_STITCH_INCLUSIVE      the live frames after a stitched snapshot start at asOf.eventSeq instead of above it
#   GW_NC_SINGLE_PAGE_ASOF      the folded span starts at the last page's position instead of the first page's
#   GW_NC_NO_REPLAY             a hole in the event stream is skipped, never replayed
#   GW_NC_NO_HEARTBEAT          no watermark frames (heartbeat, pong without eventSeq)
#   GW_NC_BALANCE_REPLY_LAST    the UserBalances reply is applied after the newer lines that followed it
#   GW_NC_NO_RESNAPSHOT_PACING  every re-snapshot is submitted at once, none held back
#   GW_NC_UNBOUNDED_QUEUE       the outbound queue has no bound (no 4001)
#   GW_NC_COALESCE_FILLS        fill frames of one order coalesce like order frames
#   GW_NC_ROUTE_BY_FRAME_USER   a userId in a client frame rebinds the connection instead of closing it 4003
#   GW_NC_FILL_ZERO_TRADE       a FILL with tradeId 0 goes out as a fill frame
#   GW_NC_ANON_USER_BOUND       an anonymous connection is held to the signed-in outbound bound, not its own
#   GW_NC_CLOSE_WAITS_FOR_READ  a close the server starts releases the connection only through the read loop
set(GW_STREAM_CONTROLS
    "GW_NC_STITCH_INCLUSIVE|stream_stitch_across_pages|duplicate event [0-9]+"
    "GW_NC_SINGLE_PAGE_ASOF|stream_stitch_across_pages|client state differs from oracle"
    "GW_NC_NO_REPLAY|stream_gap_is_replayed|client state differs from oracle"
    "GW_NC_NO_HEARTBEAT|stream_resume_from_heartbeat|quiet user forced to resync"
    "GW_NC_BALANCE_REPLY_LAST|stream_balance_stitch|stale balance after snapshot"
    "GW_NC_NO_RESNAPSHOT_PACING|stream_resnapshot_is_paced|snapshot query rate above the bound"
    "GW_NC_UNBOUNDED_QUEUE|stream_slow_client_bound|queue exceeded bound"
    "GW_NC_COALESCE_FILLS|stream_fills_never_coalesce|fill [0-9]+ missing"
    "GW_NC_ROUTE_BY_FRAME_USER|ws_no_foreign_event|foreign event delivered"
    "GW_NC_FILL_ZERO_TRADE|stream_no_fill_frame_without_a_trade|fill frame with tradeId 0"
    "GW_NC_ANON_USER_BOUND|stream_anonymous_slow_client_bound|a slow anonymous client is closed 4001 at its bound"
    "GW_NC_CLOSE_WAITS_FOR_READ|ws_server_close_frees_the_slot|a server close held its connection")
foreach(control IN LISTS GW_STREAM_CONTROLS)
    string(REPLACE "|" ";" parts "${control}")
    list(GET parts 0 define)
    list(GET parts 1 case)
    list(GET parts 2 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "gw_nc_" "gw_stream_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp unit/StreamCoreTest.cpp unit/WsServerTest.cpp
        "${PROJECT_SOURCE_DIR}/stream/StreamCore.cpp" "${PROJECT_SOURCE_DIR}/stream/WsServer.cpp")
    target_include_directories(${target} PRIVATE unit "${PROJECT_SOURCE_DIR}/stream")
    target_compile_definitions(${target} PRIVATE ${define} GW_NO_ORDER_ENTRY)
    target_link_libraries(${target} PRIVATE gw_common)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
    add_test(NAME ${target} COMMAND ${target} ${case})
    set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

# Controls for the engine status feed: EngineStatus built with one rule switched off (test-only defines, never in a
# binary) and the case that guards it; the case must fail with the named text.
#   GW_NC_ENGINE_INFER_FROM_LEADER  a node that did not answer is shown as the leader's follower
#   GW_NC_ENGINE_NO_RATE_CAP        every changed engine frame goes out at once
#   GW_NC_ENGINE_ERROR_TEXT         a node's failure text (host names, addresses) goes into the frame instead of its code
set(GW_ENGINE_CONTROLS
    "GW_NC_ENGINE_INFER_FROM_LEADER|engine_unreachable_node_from_its_own_poll|follower health inferred from the leader"
    "GW_NC_ENGINE_NO_RATE_CAP|engine_rate_cap|engine frame rate above the cap"
    "GW_NC_ENGINE_ERROR_TEXT|engine_node_error_names_no_host|a host name reached the frame")
foreach(control IN LISTS GW_ENGINE_CONTROLS)
    string(REPLACE "|" ";" parts "${control}")
    list(GET parts 0 define)
    list(GET parts 1 case)
    list(GET parts 2 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "gw_nc_" "gw_stream_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp unit/EngineStatusTest.cpp "${PROJECT_SOURCE_DIR}/stream/EngineStatus.cpp"
        "${PROJECT_SOURCE_DIR}/stream/EnginePoller.cpp" "${PROJECT_SOURCE_DIR}/stream/StreamCore.cpp")
    target_include_directories(${target} PRIVATE unit "${PROJECT_SOURCE_DIR}/stream")
    target_compile_definitions(${target} PRIVATE ${define} GW_NO_ORDER_ENTRY)
    target_link_libraries(${target} PRIVATE gw_common)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
    add_test(NAME ${target} COMMAND ${target} ${case})
    set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

# No order entry in oe-stream: it cannot reference the encoders of CreateOrder, CancelOrder, AmendOrder (nor Deposit,
# Withdraw). A probe that includes OrderEntry.h, built with the stream's flags, must fail to compile with OrderEntry.h's
# refusal; a probe that declares the encoder itself and links like oe-stream must fail to link (the same probe linked
# with gw_order_entry is the control: it links); and the finished oe-stream holds none of the symbols that oe-gateway
# holds.
add_library(gw_stream_compile_probe OBJECT EXCLUDE_FROM_ALL link/StreamOrderEntryProbe.cpp)
target_compile_definitions(gw_stream_compile_probe PRIVATE GW_NO_ORDER_ENTRY)
target_link_libraries(gw_stream_compile_probe PRIVATE gw_common)
add_executable(gw_stream_link_probe EXCLUDE_FROM_ALL link/StreamLinkProbe.cpp)
target_compile_definitions(gw_stream_link_probe PRIVATE GW_NO_ORDER_ENTRY)
target_link_libraries(gw_stream_link_probe PRIVATE cluster_tools::runtime_embedded_driver oe_stream_app)
add_executable(gw_stream_link_probe_control EXCLUDE_FROM_ALL link/StreamLinkProbe.cpp)
target_link_libraries(gw_stream_link_probe_control PRIVATE cluster_tools::runtime_embedded_driver oe_stream_app
    gw_order_entry)
add_test(NAME build.stream_no_order_entry_compile
    COMMAND "${CMAKE_COMMAND}" --build "${PROJECT_BINARY_DIR}" --target gw_stream_compile_probe)
set_tests_properties(build.stream_no_order_entry_compile PROPERTIES RUN_SERIAL TRUE
    PASS_REGULAR_EXPRESSION "must not encode CreateOrder, CancelOrder, AmendOrder")
add_test(NAME build.stream_no_order_entry_link
    COMMAND "${CMAKE_COMMAND}" --build "${PROJECT_BINARY_DIR}" --target gw_stream_link_probe)
set_tests_properties(build.stream_no_order_entry_link PROPERTIES RUN_SERIAL TRUE
    PASS_REGULAR_EXPRESSION "undefined reference to .openexchange::gateway::wire::encode")
add_test(NAME build.stream_link_probe_control
    COMMAND "${CMAKE_COMMAND}" --build "${PROJECT_BINARY_DIR}" --target gw_stream_link_probe_control)
set_tests_properties(build.stream_link_probe_control PROPERTIES RUN_SERIAL TRUE)
add_test(NAME build.stream_no_order_entry_symbols
    COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/link/no_order_entry_symbols.sh" "$<TARGET_FILE:oe-stream>"
        "$<TARGET_FILE:oe-gateway>")

# The same for oe-projector, which is built like the stream. Its probes take the definitions oe_projector_app is
# compiled with, so the compile probe fails only while the projector's own sources are built with GW_NO_ORDER_ENTRY.
add_library(gw_projector_compile_probe OBJECT EXCLUDE_FROM_ALL link/StreamOrderEntryProbe.cpp)
target_compile_definitions(gw_projector_compile_probe PRIVATE
    $<TARGET_PROPERTY:oe_projector_app,COMPILE_DEFINITIONS>)
target_link_libraries(gw_projector_compile_probe PRIVATE gw_common)
add_executable(gw_projector_link_probe EXCLUDE_FROM_ALL link/StreamLinkProbe.cpp)
target_compile_definitions(gw_projector_link_probe PRIVATE $<TARGET_PROPERTY:oe_projector_app,COMPILE_DEFINITIONS>)
target_link_libraries(gw_projector_link_probe PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app)
add_executable(gw_projector_link_probe_control EXCLUDE_FROM_ALL link/StreamLinkProbe.cpp)
target_link_libraries(gw_projector_link_probe_control PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app
    gw_order_entry)
add_test(NAME build.projector_no_order_entry_compile
    COMMAND "${CMAKE_COMMAND}" --build "${PROJECT_BINARY_DIR}" --target gw_projector_compile_probe)
set_tests_properties(build.projector_no_order_entry_compile PROPERTIES RUN_SERIAL TRUE
    PASS_REGULAR_EXPRESSION "must not encode CreateOrder, CancelOrder, AmendOrder")
add_test(NAME build.projector_no_order_entry_link
    COMMAND "${CMAKE_COMMAND}" --build "${PROJECT_BINARY_DIR}" --target gw_projector_link_probe)
set_tests_properties(build.projector_no_order_entry_link PROPERTIES RUN_SERIAL TRUE
    PASS_REGULAR_EXPRESSION "undefined reference to .openexchange::gateway::wire::encode")
add_test(NAME build.projector_link_probe_control
    COMMAND "${CMAKE_COMMAND}" --build "${PROJECT_BINARY_DIR}" --target gw_projector_link_probe_control)
set_tests_properties(build.projector_link_probe_control PROPERTIES RUN_SERIAL TRUE)
add_test(NAME build.projector_no_order_entry_symbols
    COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/link/no_order_entry_symbols.sh" "$<TARGET_FILE:oe-projector>"
        "$<TARGET_FILE:oe-gateway>")

# The users store against a real PostgreSQL in a throwaway container (docker; the image pinned by digest). Built
# always; registered with GW_PG_TESTS=ON.
add_executable(gw_pg_tests unit/TestMain.cpp pg/PgStoreTest.cpp)
target_include_directories(gw_pg_tests PRIVATE unit pg)
target_link_libraries(gw_pg_tests PRIVATE gw_common)
target_compile_options(gw_pg_tests PRIVATE ${GW_WARNINGS})
# The history read layer against the same container, seeded from the read contract's reference DDL
# (sql/history_read_contract_v1.sql): the keyset walk under concurrent inserts, the routes over the real users and
# history stores, pool exhaustion while login answers, and the causes of 503 HISTORY_UNAVAILABLE.
set(GW_HISTORY_CONTRACT_DEFINITION GW_HISTORY_CONTRACT_SQL="${PROJECT_SOURCE_DIR}/sql/history_read_contract_v1.sql"
    GW_HISTORY_PROJECTION_SQL="${PROJECT_SOURCE_DIR}/sql/history_projection_v1.sql"
    GW_MONEY_CONTRACT_SQL="${PROJECT_SOURCE_DIR}/sql/money_read_contract_v1.sql")
add_executable(gw_pg_history_tests unit/TestMain.cpp pg/HistoryPgTest.cpp)
target_include_directories(gw_pg_history_tests PRIVATE unit pg)
target_compile_definitions(gw_pg_history_tests PRIVATE ${GW_HISTORY_CONTRACT_DEFINITION})
target_link_libraries(gw_pg_history_tests PRIVATE oe_gateway_app gw_common)
target_compile_options(gw_pg_history_tests PRIVATE ${GW_WARNINGS})
# Control for the keyset walk (history_keyset_walk_under_concurrent_inserts): the history statements built with OFFSET
# paging (GW_NC_OFFSET_PAGING, test-only, never in a binary) must fail the walk with "row skipped or repeated".
add_executable(gw_pg_nc_offset_paging unit/TestMain.cpp pg/HistoryPgTest.cpp "${PROJECT_SOURCE_DIR}/common/src/History.cpp")
target_include_directories(gw_pg_nc_offset_paging PRIVATE unit pg)
target_compile_definitions(gw_pg_nc_offset_paging PRIVATE ${GW_HISTORY_CONTRACT_DEFINITION} GW_NC_OFFSET_PAGING)
target_link_libraries(gw_pg_nc_offset_paging PRIVATE oe_gateway_app gw_common)
target_compile_options(gw_pg_nc_offset_paging PRIVATE ${GW_WARNINGS})
# Controls for the freshness rows of the history read (stale label, epoch check, health probe, read order) and the
# trail's owner filter: the test built with one rule off in the source named (test-only defines, never in a binary);
# the case must fail with the text.
#   GW_NC_NO_STALE_LABEL          asOf carries no stale field
#   GW_NC_NO_EPOCH_CHECK          the heartbeat's epoch is not compared with the gateway's last CoreConfigReport
#   GW_NC_NO_HISTORY_PROBE        no probe: /health shows what the last successful read found
#   GW_NC_ROWS_BEFORE_HEARTBEAT   a page reads its rows first and the heartbeat after them
#   GW_NC_TRAIL_NO_OWNER          the trail statements have no owner filter (user_id)
#   GW_NC_MOVEMENTS_NO_OWNER      the balance movements statement has no owner filter (user_id)
set(GW_HISTORY_PG_CONTROLS
    "GW_NC_NO_STALE_LABEL|gateway/GatewayRoutes.cpp|history_stale_label|stale label missing"
    "GW_NC_NO_EPOCH_CHECK|common/src/PgHistoryStore.cpp|history_epoch_check|history of another genesis served"
    "GW_NC_NO_HISTORY_PROBE|gateway/GatewayRoutes.cpp|history_health_probe|health shows fresh history without a probe"
    "GW_NC_ROWS_BEFORE_HEARTBEAT|common/src/PgHistoryStore.cpp|history_read_order_under_concurrent_apply|asOf.eventSeq claims rows the page lacks"
    "GW_NC_TRAIL_NO_OWNER|common/src/History.cpp|history_order_trail|trail of another user's order"
    "GW_NC_MOVEMENTS_NO_OWNER|common/src/History.cpp|money_movements_over_postgres|movement of another user")
set(GW_HISTORY_PG_CONTROL_TESTS "")
foreach(entry IN LISTS GW_HISTORY_PG_CONTROLS)
    string(REPLACE "|" ";" parts "${entry}")
    list(GET parts 0 define)
    list(GET parts 1 source)
    list(GET parts 2 case)
    list(GET parts 3 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "gw_nc_" "gw_pg_history_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp pg/HistoryPgTest.cpp "${PROJECT_SOURCE_DIR}/${source}")
    target_include_directories(${target} PRIVATE unit pg)
    target_compile_definitions(${target} PRIVATE ${GW_HISTORY_CONTRACT_DEFINITION} ${define})
    target_link_libraries(${target} PRIVATE oe_gateway_app gw_common)
    list(APPEND GW_HISTORY_PG_CONTROL_TESTS "${target}|${case}|${text}")
endforeach()
if(GW_PG_TESTS)
    foreach(entry IN LISTS GW_HISTORY_PG_CONTROL_TESTS)
        string(REPLACE "|" ";" parts "${entry}")
        list(GET parts 0 target)
        list(GET parts 1 case)
        list(GET parts 2 text)
        add_test(NAME pg.${target} COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
            --binary "$<TARGET_FILE:${target}>" --image "${GW_PG_TEST_IMAGE}" ${case})
        set_tests_properties(pg.${target} PROPERTIES TIMEOUT 300 LABELS pg RUN_SERIAL TRUE
            PASS_REGULAR_EXPRESSION "${text}")
    endforeach()
    add_test(NAME pg.users COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
        --binary "$<TARGET_FILE:gw_pg_tests>" --image "${GW_PG_TEST_IMAGE}")
    set_tests_properties(pg.users PROPERTIES TIMEOUT 300 LABELS pg RUN_SERIAL TRUE)
    add_test(NAME pg.history COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
        --binary "$<TARGET_FILE:gw_pg_history_tests>" --image "${GW_PG_TEST_IMAGE}")
    set_tests_properties(pg.history PROPERTIES TIMEOUT 300 LABELS pg RUN_SERIAL TRUE)
    add_test(NAME pg.history_nc_offset_paging COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
        --binary "$<TARGET_FILE:gw_pg_nc_offset_paging>" --image "${GW_PG_TEST_IMAGE}"
        history_keyset_walk_under_concurrent_inserts)
    set_tests_properties(pg.history_nc_offset_paging PROPERTIES TIMEOUT 300 LABELS pg RUN_SERIAL TRUE
        PASS_REGULAR_EXPRESSION "row skipped or repeated")
endif()

# oe-projector T0: settings, readiness, admin rendering.
add_executable(gw_projector_tests unit/TestMain.cpp unit/ProjectorTest.cpp)
target_include_directories(gw_projector_tests PRIVATE unit)
target_link_libraries(gw_projector_tests PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app gw_common)
target_compile_options(gw_projector_tests PRIVATE ${GW_WARNINGS})
target_compile_definitions(gw_projector_tests PRIVATE
    GW_PROJECTION_STORE_SOURCE="${PROJECT_SOURCE_DIR}/projector/ProjectionStore.cpp")
add_test(NAME projector COMMAND gw_projector_tests)

# oe-projector T0: the follow rules and the record mapping (tests/unit/ProjectorFollowTest.cpp), and their controls: each
# compiles one rule off (PROJ_NC_*, in the projector source named) and must fail its case with the named text.
add_executable(gw_projector_follow_tests unit/TestMain.cpp unit/ProjectorFollowTest.cpp)
target_include_directories(gw_projector_follow_tests PRIVATE unit)
target_link_libraries(gw_projector_follow_tests PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app gw_common)
target_compile_options(gw_projector_follow_tests PRIVATE ${GW_WARNINGS})
add_test(NAME projector.follow COMMAND gw_projector_follow_tests)
set(GW_PROJECTOR_FOLLOW_CONTROLS
    "PROJ_NC_HOLE_FROM_MEMO|FollowRules.cpp|forward_rule_table|hole believed from a memo read"
    "PROJ_NC_READ_THE_LEADER|FollowRules.cpp|source_choice_avoids_the_leader|the leader's archive read while a follower could be"
    "PROJ_NC_NO_BACKOFF|FollowRules.cpp|backoff_doubles_to_its_cap|replay requests flood the source archive"
    "PROJ_NC_BEHIND_SILENT_ONLY|FollowRules.cpp|behind_window|attached to a lagging source while another was current"
    "PROJ_NC_NO_ENUM_CHECK|JournalRecord.cpp|record_refuses_unnamed_enums|row written with an unnamed enum value"
    "PROJ_NC_SECOND_NAME_TABLE|JournalRecord.cpp|one_mapping_order_and_fill_json|history row renders differently from the stream")
foreach(entry IN LISTS GW_PROJECTOR_FOLLOW_CONTROLS)
    string(REPLACE "|" ";" parts "${entry}")
    list(GET parts 0 define)
    list(GET parts 1 source)
    list(GET parts 2 case)
    list(GET parts 3 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "proj_nc_" "gw_projector_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp unit/ProjectorFollowTest.cpp "${PROJECT_SOURCE_DIR}/projector/${source}")
    target_include_directories(${target} PRIVATE unit)
    target_compile_definitions(${target} PRIVATE ${define})
    target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app gw_common)
    add_test(NAME projector.${target} COMMAND ${target} ${case})
    set_tests_properties(projector.${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()
# the property's non-vacuity control: faults that never clear must leave no recovery in the seed set
add_test(NAME projector.fuzz_faults_never_clear COMMAND gw_projector_follow_tests follow_fuzz_recovers)
set_tests_properties(projector.fuzz_faults_never_clear PROPERTIES ENVIRONMENT GW_FUZZ_FAULTS_NEVER_CLEAR=1
    PASS_REGULAR_EXPRESSION "no recovery in the seed set")

# oe-projector T1 against PostgreSQL (tests/pg/ProjectorPgTest.cpp through pg_it.py): the writer's batches over the
# shipped lease and fence, and the controls (PROJ_NC_* in the projector source named, GW_NC_* in the test itself).
add_executable(gw_projector_pg_tests unit/TestMain.cpp pg/ProjectorPgTest.cpp)
target_include_directories(gw_projector_pg_tests PRIVATE unit pg)
target_compile_definitions(gw_projector_pg_tests PRIVATE ${GW_HISTORY_CONTRACT_DEFINITION})
target_link_libraries(gw_projector_pg_tests PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app gw_common)
target_compile_options(gw_projector_pg_tests PRIVATE ${GW_WARNINGS})
set(GW_PROJECTOR_PG_CONTROLS
    "PROJ_NC_FILL_ZERO_TRADE|JournalRecord.cpp|apply_story|fill row with tradeId 0"
    "PROJ_NC_FILLS_FIRST|HistoryWork.cpp|fk_order_first_records|fill before its order"
    "PROJ_NC_CURSOR_OWN_TX|HistoryWork.cpp|crash_points|cursor ahead of the rows"
    "PROJ_NC_NO_COMPARE|HistoryWork.cpp|overlap_compare|conflicting duplicate skipped"
    "PROJ_NC_NO_DENSITY_CHECK|HistoryWork.cpp|dense_apply|applied a record past a gap"
    "PROJ_NC_SECOND_NAME_TABLE|JournalRecord.cpp|one_mapping_from_postgres|history row renders differently from the stream"
    "PROJ_NC_BEAT_ALWAYS|ProjectionStore.cpp|beat_rule|beat without a fresh head"
    "PROJ_NC_BEAT_NO_EPOCH|ProjectionStore.cpp|beat_rule|beat across genesis ids"
    "PROJ_NC_REBUILD_KEEPS_BEAT|ProjectionStore.cpp|rebuild_reads|rebuilding history served as complete"
    "PROJ_NC_DROP_ONE|HistoryWork.cpp|projection_dense|journalSeq missing in projection: 2 [(]applied_seq [0-9]+[)]"
    "PROJ_NC_WALLCLOCK_COLUMN|ProjectionStore.cpp|projection_byte_equal|projection dump differs: oe_orders .*without the column projected_at the dumps are equal"
    "GW_NC_VIEW_REMAINING_FROM_QUANTITY||read_contract_compat|projected history differs from the read contract"
    "GW_NC_VIEW_COLUMN_RENAMED||read_contract_compat|the gateway's absent"
    "GW_NC_SHIFTED_COLUMN||reference_equivalence|projected history differs from the reference DDL")
set(GW_PROJECTOR_PG_CONTROL_TESTS "")
foreach(entry IN LISTS GW_PROJECTOR_PG_CONTROLS)
    string(REPLACE "|" ";" parts "${entry}")
    list(GET parts 0 define)
    list(GET parts 1 source)
    list(GET parts 2 case)
    list(GET parts 3 text)
    string(TOLOWER "${define}" target)
    string(REGEX REPLACE "^(proj|gw)_nc_" "gw_projector_pg_nc_" target "${target}")
    set(extra "")
    if(NOT source STREQUAL "")
        set(extra "${PROJECT_SOURCE_DIR}/projector/${source}")
    endif()
    add_executable(${target} unit/TestMain.cpp pg/ProjectorPgTest.cpp ${extra})
    target_include_directories(${target} PRIVATE unit pg)
    target_compile_definitions(${target} PRIVATE ${GW_HISTORY_CONTRACT_DEFINITION} ${define})
    target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app gw_common)
    list(APPEND GW_PROJECTOR_PG_CONTROL_TESTS "${target}|${case}|${text}")
endforeach()
if(GW_PG_TESTS)
    foreach(case apply_story fk_order_first_records crash_points overlap_compare dense_apply one_mapping_from_postgres
            read_contract_compat reference_equivalence beat_rule rebuild_reads reader_role_prepared projection_dense
            projection_byte_equal)
        add_test(NAME pg.projector.${case} COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
            --binary "$<TARGET_FILE:gw_projector_pg_tests>" --image "${GW_PG_TEST_IMAGE}" ${case})
        set_tests_properties(pg.projector.${case} PROPERTIES TIMEOUT 300 LABELS "pg;projector" RUN_SERIAL TRUE)
    endforeach()
    foreach(entry IN LISTS GW_PROJECTOR_PG_CONTROL_TESTS)
        string(REPLACE "|" ";" parts "${entry}")
        list(GET parts 0 target)
        list(GET parts 1 case)
        list(GET parts 2 text)
        add_test(NAME pg.${target} COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
            --binary "$<TARGET_FILE:${target}>" --image "${GW_PG_TEST_IMAGE}" ${case})
        set_tests_properties(pg.${target} PROPERTIES TIMEOUT 300 LABELS "pg;projector" RUN_SERIAL TRUE
            PASS_REGULAR_EXPRESSION "${text}")
    endforeach()
endif()

# oe-marketdata T0: settings, the book side over the engines' golden egress bytes, the golden output frames
# (tests/golden/marketdata), the relay's fields, the frame shapes, readiness on the expected set, the admin routes.
set(GW_MD_TEST_SOURCES "${PROJECT_SOURCE_DIR}/marketdata/MarketDataSettings.cpp"
    "${PROJECT_SOURCE_DIR}/marketdata/BookCore.cpp" "${PROJECT_SOURCE_DIR}/marketdata/BookFrames.cpp"
    "${PROJECT_SOURCE_DIR}/marketdata/Candles.cpp" "${PROJECT_SOURCE_DIR}/marketdata/TradePath.cpp"
    "${PROJECT_SOURCE_DIR}/marketdata/TradeCore.cpp" "${PROJECT_SOURCE_DIR}/marketdata/CandleRoute.cpp"
    "${PROJECT_SOURCE_DIR}/marketdata/MarketHistory.cpp" "${PROJECT_SOURCE_DIR}/marketdata/MdWsServer.cpp")
# gw_md_test(target [defines...]): a define of the readiness rules (MD_NC_READY_LIVENESS, MD_NC_SEEN_MARKETS) also
# compiles common/src/Readiness.cpp into the target, ahead of gw_common's copy.
function(gw_md_test target)
    set(extra "")
    if("${ARGN}" MATCHES "MD_NC_READY_LIVENESS|MD_NC_SEEN_MARKETS")
        set(extra "${PROJECT_SOURCE_DIR}/common/src/Readiness.cpp")
    endif()
    add_executable(${target} unit/TestMain.cpp unit/MarketDataTest.cpp unit/MarketDataTradeTest.cpp
        unit/MarketDataLiveTest.cpp unit/MdWsServerTest.cpp ${GW_MD_TEST_SOURCES} ${extra})
    target_include_directories(${target} PRIVATE unit "${GW_CT_GOLDEN_DIR}" "${PROJECT_SOURCE_DIR}/marketdata")
    target_compile_definitions(${target} PRIVATE GW_NO_ORDER_ENTRY
        GW_MD_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/golden/marketdata" ${ARGN})
    target_link_libraries(${target} PRIVATE gw_common cluster_tools::forwarder_codec cluster_tools::forwarder_domain
        cluster_tools::journal_codec cluster_tools::archive_core)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
endfunction()
gw_md_test(gw_md_tests)
add_test(NAME marketdata COMMAND gw_md_tests)
# the build whose WebSocket sender drops every frame: clients_served must clear and the sent counter stay flat
gw_md_test(gw_md_tc_send_drops MD_TC_SEND_DROPS)
add_test(NAME marketdata.gw_md_tc_send_drops COMMAND gw_md_tc_send_drops md_ws_clients_served)
# Controls for the market data rows: each build compiles the marketdata sources with one rule switched off (test-only
# defines, never in a binary) and runs the case that guards it; the case must fail with the named text.
#   MD_NC_NO_CHAIN_CHECK         every decoded delta is sent, whatever the ladder made of it
#   MD_NC_DEFAULT_PUBLIC_BIND    MD_PUBLIC_BIND falls back to a default port
#   MD_NC_NO_EXPECTED_SET_CHECK  every market of the library is served, MD_EXPECTED_MARKETS or not
#   MD_NC_MARKETID_STRING        marketId written as a JSON string
#   MD_NC_NO_BOOK_VERSION        bookVersion left out of the book frames
#   MD_NC_NUMBER_PRICE           prices and quantities written as JSON numbers
#   MD_NC_RENAMED_UPDATE_TYPE    a change's updateType written under another name
# and of the trade path (the pure part; the PostgreSQL and process parts are registered below):
#   MD_NC_WALLCLOCK_BUCKET       a candle bucket from the time the trade arrived, not from its journal timestamp
#   MD_NC_QUOTE_64               the quote volume as a 64-bit product
#   MD_NC_NO_MARKET_CHECK        a trade of a market outside MD_EXPECTED_MARKETS taken like any other
#   MD_NC_NO_EPOCH_CHECK         a record of another meEpoch taken like any other
#   MD_NC_NO_PREFIX_CHECK        a jump over the next record taken as if the records in between did not exist
#   MD_NC_NO_LIVE_EMIT_CURSOR    a trade read again is handed to the live path again
# and of the live side:
#   MD_NC_EMPTY_SERIES           empty tape and candle series sent while the market history was down at the start
#   MD_NC_REST_FROM_MEMORY       GET /api/candles answers a failed history read with 200 and the candles in hand
# and of the public surface, the clients and readiness:
#   MD_NC_TAKER_IN_TRADE         the trade entry carries the taker's user id
#   MD_NC_DECODE_TRADE_EGRESS    a 26 egress frame is taken into the book decode
#   MD_NC_NO_QUEUE_BOUND         a client queue without its MD_WS_QUEUE_BYTES bound
#   MD_NC_READY_LIVENESS         /ready and /ready/book report process liveness
#   MD_NC_SEEN_MARKETS           book_fresh judged on the markets seen so far
#   MD_NC_HEARTBEAT_ONLY_LIVENESS  me_session judged on ClusterHeartbeats only (they stop under traffic)
#   MD_TC_SEND_DROPS+MD_NC_NO_CLIENTS_SERVED   a sender that drops its frames, with clients_served not judged
# Defines joined by '+' go into one build.
set(GW_MD_CONTROLS
    "MD_NC_TAKER_IN_TRADE|md_live_no_party_fields|user field on the public surface: takerUserId"
    "MD_NC_DECODE_TRADE_EGRESS|md_egress_templates_dropped_unread|non-market template was decoded: 26"
    "MD_NC_NO_QUEUE_BOUND|md_ws_slow_client|client queue grew past its setting"
    "MD_NC_READY_LIVENESS|md_readiness_egress_paused|ready without a fresh book"
    "MD_NC_SEEN_MARKETS|md_readiness_expected_set|ready with no market snapshot"
    "MD_NC_HEARTBEAT_ONLY_LIVENESS|md_readiness_book_frames_are_liveness|me_session lost under traffic with no heartbeat"
    "MD_TC_SEND_DROPS+MD_NC_NO_CLIENTS_SERVED|md_ws_clients_served|ready while no frame reaches a client"
    "MD_NC_EMPTY_SERIES|md_live_history_down_at_start|empty series sent without the history"
    "MD_NC_REST_FROM_MEMORY|md_candles_route_history_down|REST candles answered without the history"
    "MD_NC_WALLCLOCK_BUCKET|md_candles_function_of_trade_set|candle bucket depends on arrival time"
    "MD_NC_QUOTE_64|md_candles_function_of_trade_set|quote volume overflow"
    "MD_NC_NO_MARKET_CHECK|md_trade_take_record|trade stored for a market with no book"
    "MD_NC_NO_EPOCH_CHECK|md_trade_forward_rule|trades of two genesis epochs in one history"
    "MD_NC_NO_PREFIX_CHECK|md_trade_forward_rule|trades skipped over a purged prefix"
    "MD_NC_NO_LIVE_EMIT_CURSOR|md_trade_live_emit_cursor|trade [0-9]+ sent twice to one connection"
    "MD_NC_NO_CHAIN_CHECK|md_book_chain_break_marks_stale|book served across a version break"
    "MD_NC_DEFAULT_PUBLIC_BIND|md_settings_required_keys|service started without MD_PUBLIC_BIND"
    "MD_NC_NO_EXPECTED_SET_CHECK|md_expected_set|book served for a market outside the expected set"
    "MD_NC_MARKETID_STRING|md_relay_fields|relay field has the wrong JSON type: marketId"
    "MD_NC_NO_BOOK_VERSION|md_relay_fields|relay reads book version 0"
    "MD_NC_NUMBER_PRICE|md_frame_shapes|price is not a decimal string"
    "MD_NC_RENAMED_UPDATE_TYPE|md_frame_shapes|simulator cannot decode BOOK_DELTA")
foreach(control IN LISTS GW_MD_CONTROLS)
    string(REPLACE "|" ";" parts "${control}")
    list(GET parts 0 define)
    list(GET parts 1 case)
    list(GET parts 2 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "+" "_" target "${target}")
    string(REGEX REPLACE "^md_" "gw_md_" target "${target}")
    string(REPLACE "+" ";" define "${define}")
    gw_md_test(${target} ${define})
    add_test(NAME marketdata.${target} COMMAND ${target} ${case})
    set_tests_properties(marketdata.${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

# oe-marketdata's market history against PostgreSQL (tests/pg/MarketHistoryPgTest.cpp through pg_it.py): the start
# checks, the one-transaction commit with the writer killed, the batch splits, the writer
# thread through two outages; and the controls (the source named is compiled into the test with the define):
#   MD_NC_COMMIT_SPLIT     the cursor commits in a transaction of its own before the rows
#   MD_NC_NO_WRITER_LOCK   the start takes no writer lock
#   MD_NC_WALLCLOCK_BUCKET / MD_NC_QUOTE_64   as in the T0 controls, against the stored rows
#   MD_NC_REST_FROM_MEMORY  GET /api/candles answers a failed read with 200
add_executable(gw_md_pg_tests unit/TestMain.cpp pg/MarketHistoryPgTest.cpp)
target_include_directories(gw_md_pg_tests PRIVATE unit pg)
target_link_libraries(gw_md_pg_tests PRIVATE cluster_tools::runtime_embedded_driver oe_marketdata_app gw_common)
target_compile_options(gw_md_pg_tests PRIVATE ${GW_WARNINGS})
set(GW_MD_PG_CONTROLS
    "MD_NC_COMMIT_SPLIT|MarketHistory.cpp|md_history_exactly_once|missing trade [0-9]+"
    "MD_NC_NO_WRITER_LOCK|MarketHistory.cpp|md_history_start|two market history writers"
    "MD_NC_WALLCLOCK_BUCKET|Candles.cpp|md_candles_batch_splits|candle bucket depends on arrival time"
    "MD_NC_QUOTE_64|Candles.cpp|md_candles_batch_splits|quote volume overflow"
    "MD_NC_REST_FROM_MEMORY|CandleRoute.cpp|md_live_start_and_candles_route|REST candles answered without the history")
set(GW_MD_PG_CONTROL_TESTS "")
foreach(entry IN LISTS GW_MD_PG_CONTROLS)
    string(REPLACE "|" ";" parts "${entry}")
    list(GET parts 0 define)
    list(GET parts 1 source)
    list(GET parts 2 case)
    list(GET parts 3 text)
    string(TOLOWER "${define}" target)
    string(REPLACE "md_nc_" "gw_md_pg_nc_" target "${target}")
    add_executable(${target} unit/TestMain.cpp pg/MarketHistoryPgTest.cpp "${PROJECT_SOURCE_DIR}/marketdata/${source}")
    target_include_directories(${target} PRIVATE unit pg)
    target_compile_definitions(${target} PRIVATE ${define})
    target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_marketdata_app gw_common)
    list(APPEND GW_MD_PG_CONTROL_TESTS "${target}|${case}|${text}")
endforeach()
if(GW_PG_TESTS)
    foreach(case md_history_start md_history_exactly_once md_history_another_writer md_candles_batch_splits
            md_writer_outage md_live_start_and_candles_route)
        add_test(NAME pg.marketdata.${case} COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
            --binary "$<TARGET_FILE:gw_md_pg_tests>" --image "${GW_PG_TEST_IMAGE}" ${case})
        set_tests_properties(pg.marketdata.${case} PROPERTIES TIMEOUT 300 LABELS "pg;marketdata" RUN_SERIAL TRUE)
    endforeach()
    foreach(entry IN LISTS GW_MD_PG_CONTROL_TESTS)
        string(REPLACE "|" ";" parts "${entry}")
        list(GET parts 0 target)
        list(GET parts 1 case)
        list(GET parts 2 text)
        add_test(NAME pg.${target} COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/pg/pg_it.py"
            --binary "$<TARGET_FILE:${target}>" --image "${GW_PG_TEST_IMAGE}" ${case})
        set_tests_properties(pg.${target} PROPERTIES TIMEOUT 300 LABELS "pg;marketdata" RUN_SERIAL TRUE
            PASS_REGULAR_EXPRESSION "${text}")
    endforeach()
endif()

add_test(NAME lock COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/lock/lock_test.sh" "${PROJECT_SOURCE_DIR}" "${CMAKE_COMMAND}"
    "${CMAKE_CURRENT_BINARY_DIR}/lock-work")

if(GW_TEST_BUILDS)
    # T1 port bases: every T1 case gets a block of GW_IT_PORT_SPAN ports of its own, handed out in order from
    # GW_IT_PORT_BASE, so no two cases share a port. A three-member core (stride 100) uses base+1..5, +101..105,
    # +201..205 and its node ports base+90..92. The defaults keep every block below this host's ephemeral range (Linux:
    # 32768 and up), where an outgoing connection or a socket of any process could hold a member's port; a block that
    # reaches it is named by a configure warning (both are settings: a host with other listeners moves them).
    set(GW_IT_PORT_BASE "9800" CACHE STRING "first port of the T1 port blocks")
    set(GW_IT_PORT_SPAN "300" CACHE STRING "ports per T1 case (a three-member core needs 206)")
    foreach(_setting GW_IT_PORT_BASE GW_IT_PORT_SPAN)
        if(NOT "${${_setting}}" MATCHES "^[0-9]+$")
            message(FATAL_ERROR "${_setting} must be a port number, got '${${_setting}}'")
        endif()
    endforeach()
    if(GW_IT_PORT_SPAN LESS 206)
        message(WARNING "GW_IT_PORT_SPAN ${GW_IT_PORT_SPAN} is below the 206 ports a three-member core uses, so "
            "neighbouring T1 cases share core ports and can collide when they run together")
    endif()
    set(_gw_it_ephemeral 32768)
    if(EXISTS /proc/sys/net/ipv4/ip_local_port_range)
        file(READ /proc/sys/net/ipv4/ip_local_port_range _range)
        if(_range MATCHES "^([0-9]+)")
            set(_gw_it_ephemeral ${CMAKE_MATCH_1})
        endif()
    endif()
    set(_gw_it_next_port ${GW_IT_PORT_BASE})
    function(gw_it_port out name)
        math(EXPR next "${_gw_it_next_port} + ${GW_IT_PORT_SPAN}")
        if(next GREATER _gw_it_ephemeral)
            message(WARNING "T1 case ${name}: ports ${_gw_it_next_port}..${next} reach this host's ephemeral range "
                "(${_gw_it_ephemeral} and up); lower GW_IT_PORT_BASE or GW_IT_PORT_SPAN")
        endif()
        set(${out} ${_gw_it_next_port} PARENT_SCOPE)
        set(_gw_it_next_port ${next} PARENT_SCOPE)
    endfunction()

    add_executable(gw_test_media_driver support/TestMediaDriver.cpp)
    target_link_libraries(gw_test_media_driver PRIVATE cluster_tools::runtime_embedded_driver cluster_tools::runtime)

    add_executable(oe-gateway-tc-dutystall support/DutyStallMain.cpp)
    target_link_libraries(oe-gateway-tc-dutystall PRIVATE cluster_tools::runtime_embedded_driver oe_gateway_app)
    add_executable(oe-stream-tc-dutystall support/DutyStallMain.cpp)
    target_compile_definitions(oe-stream-tc-dutystall PRIVATE GW_TC_STREAM)
    target_link_libraries(oe-stream-tc-dutystall PRIVATE cluster_tools::runtime_embedded_driver oe_stream_app)

    # T1: the session wrapper against a real three-member core cluster (tests/it/session_it.py, a heavy job). The
    # program is always built; the test is registered when the core jar of the pinned money schema and a CoreProbe jar
    # built against it are given.
    add_executable(gw_session_it it/SessionIt.cpp)
    target_link_libraries(gw_session_it PRIVATE cluster_tools::runtime_embedded_driver gw_order_entry gw_common)
    target_compile_options(gw_session_it PRIVATE ${GW_WARNINGS})
    # T1 of oe-stream: a BRIDGE session that feeds MeJournalTrade frames so that orders fill without a matching engine
    add_executable(gw_stream_it_bridge it/StreamItBridge.cpp)
    target_link_libraries(gw_stream_it_bridge PRIVATE cluster_tools::runtime_embedded_driver cluster_tools::client_aeron
        cluster_tools::client cluster_tools::client_core cluster_tools::runtime_aeron cluster_tools::runtime
        cluster_tools::sbe_codecs)
    target_compile_options(gw_stream_it_bridge PRIVATE ${GW_WARNINGS})
    set(GW_IT_CORE_JAR "" CACHE FILEPATH "core-cluster.jar built at the money schema commit of dependencies.lock (T1)")
    set(GW_IT_PROBE_JAR "" CACHE FILEPATH "CoreProbe.jar compiled against GW_IT_CORE_JAR (T1)")
    if(GW_IT_CORE_JAR AND GW_IT_PROBE_JAR)
        foreach(mode embedded external)
            gw_it_port(_port it.session.${mode})
            add_test(NAME it.session.${mode}
                COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/it/session_it.py"
                    --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                    --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/session.${mode}" --port-base ${_port})
            set_tests_properties(it.session.${mode} PROPERTIES TIMEOUT 900 LABELS t1 RUN_SERIAL TRUE)
        endforeach()
        # the REST routes through the real oe-gateway (dev principals, no users database), the treasury lane off and on
        foreach(mode embedded external)
            gw_it_port(_port it.rest.${mode})
            add_test(NAME it.rest.${mode}
                COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/it/rest_it.py"
                    --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                    --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/rest.${mode}" --port-base ${_port})
            set_tests_properties(it.rest.${mode} PROPERTIES TIMEOUT 900 LABELS t1 RUN_SERIAL TRUE)
        endforeach()
        # oe-stream through the real binary: the stitch, gaps, resume, balances, the cache, the slow client, the storm
        # (GW_IT_STREAM_CONNECTIONS connections, 2000 by default), readiness and the principal binding
        set(GW_IT_STREAM_CONNECTIONS "2000" CACHE STRING "connections of the stream's resync storm row (T1)")
        foreach(mode embedded external)
            gw_it_port(_port it.stream.${mode})
            add_test(NAME it.stream.${mode}
                COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/it/stream_it.py"
                    --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                    --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/stream.${mode}" --port-base ${_port}
                    --connections ${GW_IT_STREAM_CONNECTIONS})
            set_tests_properties(it.stream.${mode} PROPERTIES TIMEOUT 1800 LABELS t1 RUN_SERIAL TRUE)
        endforeach()
        # the engine status feed of oe-stream: each core member's own endpoint polled; a follower killed, the leader killed
        foreach(mode embedded external)
            gw_it_port(_port it.engine.${mode})
            add_test(NAME it.engine.${mode}
                COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/it/engine_it.py"
                    --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                    --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/engine.${mode}" --port-base ${_port})
            set_tests_properties(it.engine.${mode} PROPERTIES TIMEOUT 900 LABELS t1 RUN_SERIAL TRUE)
        endforeach()
        # the auth routes through the real oe-gateway: the core cluster plus a throwaway PostgreSQL container
        if(GW_PG_TESTS)
            foreach(mode embedded external)
                gw_it_port(_port it.auth.${mode})
                add_test(NAME it.auth.${mode}
                    COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/it/auth_it.py"
                        --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                        --image "${GW_PG_TEST_IMAGE}" --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/auth.${mode}"
                        --port-base ${_port})
                set_tests_properties(it.auth.${mode} PROPERTIES TIMEOUT 900 LABELS t1 RUN_SERIAL TRUE)
            endforeach()
        endif()
    endif()

    # oe-projector test builds (never published): the probe writer over the shipped fence, and the controls that compile
    # one rule of the lease off (tests/support/ProjectorFenceMain.cpp, PROJ_NC_* in projector/ProjectionStore.cpp and
    # ProcessHooks::ignoreDriverLoss):
    #   PROJ_NC_NO_FENCE            the takeover ignores the lease and the state update ignores the token
    #   PROJ_NC_SELECT_FENCE        the token is checked by a SELECT before the writes, the state update is unconditional
    #   ignore-driver-loss          the production main with the driver-loss exits switched off by a process hook
    add_executable(oe-projector-tc-fence support/ProjectorFenceMain.cpp)
    target_link_libraries(oe-projector-tc-fence PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app)
    foreach(define PROJ_NC_NO_FENCE PROJ_NC_SELECT_FENCE)
        string(TOLOWER "${define}" target)
        string(REPLACE "proj_nc_" "oe-projector-nc-" target "${target}")
        string(REPLACE "_" "-" target "${target}")
        add_executable(${target} support/ProjectorFenceMain.cpp "${PROJECT_SOURCE_DIR}/projector/ProjectionStore.cpp")
        target_compile_definitions(${target} PRIVATE ${define})
        target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app)
    endforeach()
    add_executable(oe-projector-nc-ignore-driver-loss support/ProjectorNoDriverLossMain.cpp)
    target_link_libraries(oe-projector-nc-ignore-driver-loss PRIVATE cluster_tools::runtime_embedded_driver
        oe_projector_app)
    # T1: the real binaries against a throwaway PostgreSQL container, both driver modes, no core cluster
    # (tests/projector/projector_it.py); each control runs the case its rule guards and must fail with the named text.
    if(GW_PG_TESTS)
        set(GW_PROJECTOR_IT Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/projector/projector_it.py"
            --bin-dir "${PROJECT_BINARY_DIR}/bin" --image "${GW_PG_TEST_IMAGE}" --source-dir "${PROJECT_SOURCE_DIR}")
        set(GW_PROJECTOR_CASES
            "config|oe-projector|"
            "schema|oe-projector|"
            "rebuild|oe-projector|"
            "driver_embedded|oe-projector|"
            "driver_external_killed|oe-projector|"
            "lease_takeover|oe-projector-tc-fence|"
            "lease_race|oe-projector-tc-fence|"
            "slow_commit|oe-projector-tc-fence|"
            "hung_writer|oe-projector-tc-fence|"
            "lease_takeover|oe-projector-nc-no-fence|two writers committed"
            "hung_writer|oe-projector-nc-select-fence|two writers committed"
            "driver_external_killed|oe-projector-nc-ignore-driver-loss|projector alive with a dead driver")
        foreach(entry IN LISTS GW_PROJECTOR_CASES)
            string(REPLACE "|" ";" parts "${entry}")
            list(GET parts 0 case)
            list(GET parts 1 binary)
            list(GET parts 2 text)
            if(text STREQUAL "")
                set(name projector.${case})
            else()
                string(REPLACE "oe-projector-" "" nc "${binary}")
                string(REPLACE "-" "_" nc "${nc}")
                set(name projector.${nc})
            endif()
            add_test(NAME ${name} COMMAND ${GW_PROJECTOR_IT} --work "${CMAKE_CURRENT_BINARY_DIR}/projector-work/${name}"
                --binary ${binary} ${case})
            set_tests_properties(${name} PROPERTIES TIMEOUT 300 LABELS "pg;projector" RUN_SERIAL TRUE)
            if(NOT text STREQUAL "")
                set_tests_properties(${name} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
            endif()
        endforeach()
    endif()

    # oe-projector follow side against a real three-member core cluster (tests/projector/follow_it.py, T1, a heavy job),
    # both driver modes of the projector (the core members run their own embedded drivers), and the follow controls:
    # each test build compiles one rule off (PROJ_NC_*, in the projector source named) and runs the case it guards.
    foreach(entry "PROJ_NC_NO_STOP_REPLAY|FollowDuty.cpp" "PROJ_NC_NO_BACKOFF|FollowRules.cpp"
            "PROJ_NC_READ_THE_LEADER|FollowRules.cpp"
            "PROJ_NC_NO_EPOCH_CHECK|FollowDuty.cpp,HistoryWork.cpp"
            "PROJ_NC_EPOCH_FROM_FIRST_RECORD|FollowDuty.cpp,HistoryWork.cpp" "PROJ_NC_NO_MTU_CHECK|FollowDuty.cpp"
            "PROJ_NC_COMPARES_NOT_DELIVERY|FollowDuty.cpp"
            "PROJ_NC_BEAT_NO_EPOCH|FollowDuty.cpp,ProjectionStore.cpp" "PROJ_NC_PIN_RECORDING|FollowDuty.cpp")
        string(REPLACE "|" ";" parts "${entry}")
        list(GET parts 0 define)
        list(GET parts 1 sources)
        string(REPLACE "," ";" sources "${sources}")
        list(TRANSFORM sources PREPEND "${PROJECT_SOURCE_DIR}/projector/")
        string(TOLOWER "${define}" target)
        string(REPLACE "proj_nc_" "oe-projector-nc-" target "${target}")
        string(REPLACE "_" "-" target "${target}")
        add_executable(${target} "${PROJECT_SOURCE_DIR}/projector/main.cpp" ${sources})
        target_include_directories(${target} PRIVATE "${PROJECT_SOURCE_DIR}/stream" "${PROJECT_SOURCE_DIR}/projector")
        target_compile_definitions(${target} PRIVATE ${define} GW_NO_ORDER_ENTRY)
        target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app)
    endforeach()
    # the fault build (PROJ_TC_SOURCE_FAULTS, test-only, never published): the frame sink drops or delays records of a
    # configured source before the forward rule, and the writer fails one batch on request (PROJ_TC_RELOAD_AT); and the
    # same build with one rule off for each control (defines joined by +)
    foreach(entry "oe-projector-tc-faults|PROJ_TC_SOURCE_FAULTS|FollowDuty.cpp,HistoryWork.cpp"
            "oe-projector-tc-faults-nc-hole-marks-all|PROJ_TC_SOURCE_FAULTS+PROJ_NC_HOLE_MARKS_ALL|FollowDuty.cpp,HistoryWork.cpp"
            "oe-projector-tc-faults-nc-start-at-first|PROJ_TC_SOURCE_FAULTS+PROJ_NC_START_AT_FIRST|FollowDuty.cpp,HistoryWork.cpp"
            "oe-projector-tc-faults-nc-behind-silent-only|PROJ_TC_SOURCE_FAULTS+PROJ_NC_BEHIND_SILENT_ONLY|FollowDuty.cpp,HistoryWork.cpp,FollowRules.cpp"
            "oe-projector-tc-faults-nc-beat-always|PROJ_TC_SOURCE_FAULTS+PROJ_NC_BEAT_ALWAYS|FollowDuty.cpp,HistoryWork.cpp,ProjectionStore.cpp"
            "oe-projector-tc-faults-nc-halt-latch|PROJ_TC_SOURCE_FAULTS+PROJ_NC_HALT_LATCH|FollowDuty.cpp,HistoryWork.cpp")
        string(REPLACE "|" ";" parts "${entry}")
        list(GET parts 0 target)
        list(GET parts 1 defines)
        list(GET parts 2 sources)
        string(REPLACE "+" ";" defines "${defines}")
        string(REPLACE "," ";" sources "${sources}")
        list(TRANSFORM sources PREPEND "${PROJECT_SOURCE_DIR}/projector/")
        add_executable(${target} "${PROJECT_SOURCE_DIR}/projector/main.cpp" ${sources})
        target_include_directories(${target} PRIVATE "${PROJECT_SOURCE_DIR}/stream" "${PROJECT_SOURCE_DIR}/projector")
        target_compile_definitions(${target} PRIVATE ${defines} GW_NO_ORDER_ENTRY)
        target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_projector_app)
    endforeach()
    # the gateway with asOf.eventSeq left out (test-only, never published): the control of the resync fills rule
    add_executable(oe-gateway-nc-no-asof-event-seq "${PROJECT_SOURCE_DIR}/gateway/main.cpp"
        "${PROJECT_SOURCE_DIR}/gateway/GatewayRoutes.cpp")
    target_compile_definitions(oe-gateway-nc-no-asof-event-seq PRIVATE GW_NC_NO_ASOF_EVENT_SEQ)
    target_link_libraries(oe-gateway-nc-no-asof-event-seq PRIVATE cluster_tools::runtime_embedded_driver oe_gateway_app)
    if(GW_IT_CORE_JAR AND GW_IT_PROBE_JAR AND GW_PG_TESTS)
        # the fills gap of a stream resync closed with GET /fills (tests/projector/fills_it.py): both driver modes, and
        # the control (asOf without eventSeq)
        foreach(entry "oe-gateway|" "oe-gateway-nc-no-asof-event-seq|resync fill gap not closable")
            foreach(mode embedded external)
                string(REPLACE "|" ";" parts "${entry}")
                list(GET parts 0 binary)
                list(GET parts 1 text)
                if(text STREQUAL "")
                    set(name it.fills_gap.${mode})
                else()
                    set(name it.fills_gap.nc_no_asof_event_seq.${mode})
                endif()
                gw_it_port(_port ${name})
                add_test(NAME ${name}
                    COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/projector/fills_it.py"
                        --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                        --image "${GW_PG_TEST_IMAGE}" --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/${name}"
                        --port-base ${_port} --gateway-binary ${binary})
                set_tests_properties(${name} PROPERTIES TIMEOUT 600 LABELS t1 RUN_SERIAL TRUE)
                if(NOT text STREQUAL "")
                    set_tests_properties(${name} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
                endif()
            endforeach()
        endforeach()
        # case|binary|failure text of a control|core driver mode
        foreach(entry "follow|oe-projector||embedded" "follow|oe-projector||external" "successor|oe-projector||embedded"
                "replays|oe-projector||embedded" "epoch|oe-projector||embedded" "mtu|oe-projector||embedded"
                "replay_cap|oe-projector||embedded"
                "long_overlap|oe-projector||embedded" "regenesis|oe-projector||embedded"
                "holes|oe-projector-tc-faults||embedded" "above_one|oe-projector-tc-faults||embedded"
                "slow|oe-projector-tc-faults||embedded" "writer_reset|oe-projector-tc-faults||embedded"
                "holes|oe-projector-tc-faults-nc-hole-marks-all|halted on a hole another source could fill|embedded"
                "above_one|oe-projector-tc-faults-nc-start-at-first|projection started above 1|embedded"
                "slow|oe-projector-tc-faults-nc-behind-silent-only|attached to a lagging source while another was current|embedded"
                "replays|oe-projector-nc-no-stop-replay|replay left open on the source|embedded"
                "replay_cap|oe-projector-nc-no-backoff|replay requests flood the source archive|embedded"
                "follow|oe-projector-nc-read-the-leader|the leader's archive read while a follower could be|embedded"
                "epoch|oe-projector-nc-no-epoch-check|rows of two genesis ids in one projection|embedded"
                "regenesis|oe-projector-nc-epoch-from-first-record|projection built from a previous genesis|embedded"
                "mtu|oe-projector-nc-no-mtu-check|replay opened with a different MTU|embedded"
                "long_overlap|oe-projector-nc-compares-not-delivery|BEHIND left a source that was comparing the overlap|embedded"
                "beat|oe-projector-tc-faults||embedded" "beat_epoch|oe-projector||embedded"
                "beat|oe-projector-tc-faults-nc-beat-always|beat without a fresh head|embedded"
                "beat_epoch|oe-projector-nc-beat-no-epoch|beat across genesis ids|embedded"
                "halt_restart|oe-projector-tc-faults||embedded"
                "halt_restart|oe-projector-tc-faults-nc-halt-latch|FAIL halt latched across a restart|embedded"
                # the pin: caught up on the recording first read, the member restarted, then never past it
                "successor|oe-projector-nc-pin-recording|PASS caught up on member [0-9] alone.*PASS round 1: member [0-9] rejoined.*PASS round 1: orders acked.*FAIL round 1: projector did not follow the successor recording|embedded")
            foreach(mode embedded external)
                string(REPLACE "|" ";" parts "${entry}")
                list(GET parts 0 case)
                list(GET parts 1 binary)
                list(GET parts 2 text)
                list(GET parts 3 core)
                if(text STREQUAL "")
                    set(name it.projector.${case}.${mode})
                else()
                    string(REPLACE "oe-projector-" "" nc "${binary}")
                    string(REPLACE "-" "_" nc "${nc}")
                    set(name it.projector.${nc}.${mode})
                endif()
                if(core STREQUAL "external")
                    # the mixed combination's other half: members on external drivers (each its own), the projector
                    # in ${mode}
                    set(name ${name}.core_external)
                endif()
                gw_it_port(_port ${name})
                add_test(NAME ${name}
                    COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/projector/follow_it.py"
                        --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                        --image "${GW_PG_TEST_IMAGE}" --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/${name}"
                        --port-base ${_port} --binary ${binary} --core-driver ${core} ${case})
                set_tests_properties(${name} PROPERTIES TIMEOUT 600 LABELS t1 RUN_SERIAL TRUE)
                if(NOT text STREQUAL "")
                    set_tests_properties(${name} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
                endif()
            endforeach()
        endforeach()
    endif()

    # oe-marketdata test builds (never published): the commit pause the trade-path T1 kills the writer in
    # (MD_TC_COMMIT_PAUSE, HistoryWriter.cpp), and the controls of the trade path, one rule off each (the sources named
    # are compiled into the binary with the defines):
    #   MD_NC_COMMIT_SPLIT       (with the pause) the cursor commits apart from the rows
    #   MD_NC_NO_EPOCH_CHECK     a record of another genesis taken like any other
    #   MD_NC_NO_PREFIX_CHECK    a jump over the next record taken
    #   MD_NC_NO_PARTIAL_FLAG    a history that starts above journalSeq 1 recorded as starting at 1
    #   MD_NC_NO_SCHEMA_CHECK    a frame the gate refused for its schema decoded as a trade
    #   MD_NC_NO_WRITER_LOCK     no writer lock at the start
    #   MD_NC_NO_MARKET_CHECK    a trade of a market outside MD_EXPECTED_MARKETS stored
    #   MD_NC_EMPTY_SERIES       empty tape and candle series sent while the history was down at the start
    #   MD_NC_REST_FROM_MEMORY   GET /api/candles answers a failed history read with 200
    foreach(entry "oe-marketdata-tc-commit-pause|MD_TC_COMMIT_PAUSE|HistoryWriter.cpp"
            "oe-marketdata-tc-commit-pause-nc-commit-split|MD_TC_COMMIT_PAUSE+MD_NC_COMMIT_SPLIT|HistoryWriter.cpp,MarketHistory.cpp"
            "oe-marketdata-nc-no-epoch-check|MD_NC_NO_EPOCH_CHECK|TradePath.cpp"
            "oe-marketdata-nc-no-prefix-check|MD_NC_NO_PREFIX_CHECK|TradePath.cpp"
            "oe-marketdata-nc-no-partial-flag|MD_NC_NO_PARTIAL_FLAG|JournalFollow.cpp"
            "oe-marketdata-nc-no-schema-check|MD_NC_NO_SCHEMA_CHECK|JournalFollow.cpp"
            "oe-marketdata-nc-no-writer-lock|MD_NC_NO_WRITER_LOCK|MarketHistory.cpp"
            "oe-marketdata-nc-no-market-check|MD_NC_NO_MARKET_CHECK|TradePath.cpp"
            "oe-marketdata-nc-empty-series|MD_NC_EMPTY_SERIES|BookCore.cpp"
            "oe-marketdata-nc-rest-from-memory|MD_NC_REST_FROM_MEMORY|CandleRoute.cpp")
        string(REPLACE "|" ";" parts "${entry}")
        list(GET parts 0 target)
        list(GET parts 1 defines)
        list(GET parts 2 sources)
        string(REPLACE "+" ";" defines "${defines}")
        string(REPLACE "," ";" sources "${sources}")
        list(TRANSFORM sources PREPEND "${PROJECT_SOURCE_DIR}/marketdata/")
        add_executable(${target} "${PROJECT_SOURCE_DIR}/marketdata/main.cpp" ${sources})
        target_include_directories(${target} PRIVATE "${PROJECT_SOURCE_DIR}/marketdata")
        target_compile_definitions(${target} PRIVATE ${defines} GW_NO_ORDER_ENTRY)
        target_link_libraries(${target} PRIVATE cluster_tools::runtime_embedded_driver oe_marketdata_app)
    endforeach()
    # T1: the trade path through the real binary against an ME-journal-shaped archive (the cluster-tools
    # JournalArchiveFixture on the pinned aeron-all jar) and a throwaway PostgreSQL container
    # (tests/marketdata/md_trade_it.py); both driver modes, and the controls.
    set(GW_IT_AERON_ALL_JAR "" CACHE FILEPATH "aeron-all jar of the pinned Aeron version (the archive fixture of the T1 trade path)")
    if(GW_IT_AERON_ALL_JAR AND GW_PG_TESTS)
        set(GW_MD_TRADE_IT Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/marketdata/md_trade_it.py"
            --bin-dir "${PROJECT_BINARY_DIR}/bin" --image "${GW_PG_TEST_IMAGE}" --aeron-jar "${GW_IT_AERON_ALL_JAR}"
            --fixture-src "${GW_CLUSTER_TOOLS_SOURCE_DIR}/archive/it/JournalArchiveFixture.java"
            --fixture-dir "${GW_CLUSTER_TOOLS_SOURCE_DIR}/archive/tests/fixtures/mejournal")
        # case|binary|failure text of a control (empty: the case must pass)
        foreach(entry "live|oe-marketdata|" "live|oe-marketdata-nc-empty-series|empty series sent without the history"
                "live|oe-marketdata-nc-rest-from-memory|REST candles answered without the history"
                "exactly_once|oe-marketdata-tc-commit-pause|" "epoch|oe-marketdata|" "purged|oe-marketdata|"
                "schema|oe-marketdata|" "second_instance|oe-marketdata|" "unknown_market|oe-marketdata|"
                "exactly_once|oe-marketdata-tc-commit-pause-nc-commit-split|missing trade [0-9]+"
                "epoch|oe-marketdata-nc-no-epoch-check|trades of two genesis epochs in one history"
                "purged|oe-marketdata-nc-no-prefix-check|trades skipped over a purged prefix"
                "purged|oe-marketdata-nc-no-partial-flag|partial history reported as whole"
                "schema|oe-marketdata-nc-no-schema-check|journal decoded under the wrong schema"
                "second_instance|oe-marketdata-nc-no-writer-lock|two market history writers"
                "unknown_market|oe-marketdata-nc-no-market-check|trade stored for a market with no book")
            string(REPLACE "|" ";" parts "${entry}")
            list(GET parts 0 case)
            list(GET parts 1 binary)
            list(GET parts 2 text)
            set(modes embedded external)
            if(NOT text STREQUAL "")
                set(modes embedded) # a control runs in one mode: the rule it switches off does not depend on the driver
                string(REPLACE "oe-marketdata-" "" nc "${binary}")
                string(REPLACE "-" "_" nc "${nc}")
                set(base it.marketdata.${case}.${nc})
            else()
                set(base it.marketdata.${case})
            endif()
            foreach(mode IN LISTS modes)
                set(name ${base}.${mode})
                add_test(NAME ${name} COMMAND ${GW_MD_TRADE_IT} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/${name}"
                    --mode ${mode} --binary ${binary} ${case})
                set_tests_properties(${name} PROPERTIES TIMEOUT 300 LABELS "t1;marketdata" RUN_SERIAL TRUE)
                if(NOT text STREQUAL "")
                    set_tests_properties(${name} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
                endif()
            endforeach()
        endforeach()
    endif()

    set(GW_PROCESS_CASES config_refused config_bad_values config_auth_files embedded_start_stop external_start_stop
        external_without_driver external_aeron_dir_missing external_driver_killed embedded_same_instance_refused admin_bind_in_use duty_stall)
    foreach(role gateway stream)
        foreach(case IN LISTS GW_PROCESS_CASES)
            add_test(NAME process.${role}.${case}
                COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/process/process_test.py"
                    --bin-dir "${PROJECT_BINARY_DIR}/bin" --work "${CMAKE_CURRENT_BINARY_DIR}/process-work/${role}.${case}"
                    ${role}.${case})
            set_tests_properties(process.${role}.${case} PROPERTIES TIMEOUT 120 LABELS process)
        endforeach()
    endforeach()
    add_test(NAME process.gateway.public_bind_in_use
        COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/process/process_test.py"
            --bin-dir "${PROJECT_BINARY_DIR}/bin" --work "${CMAKE_CURRENT_BINARY_DIR}/process-work/gateway.public_bind_in_use"
            gateway.public_bind_in_use)
    set_tests_properties(process.gateway.public_bind_in_use PROPERTIES TIMEOUT 120 LABELS process)
    # oe-marketdata: the same shell, both driver modes, and its public WebSocket with no credential
    foreach(case config_refused config_bad_values embedded_start_stop external_start_stop external_without_driver
            external_driver_killed)
        add_test(NAME process.marketdata.${case}
            COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/process/md_process_test.py"
                --bin-dir "${PROJECT_BINARY_DIR}/bin" --work "${CMAKE_CURRENT_BINARY_DIR}/process-work/marketdata.${case}"
                ${case})
        set_tests_properties(process.marketdata.${case} PROPERTIES TIMEOUT 120 LABELS process)
    endforeach()
endif()
