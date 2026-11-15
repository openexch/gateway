# T0: unit tests of the shared library (settings, readiness, admin rendering, the money-frame codec against the
# golden corpora) and the lock verification.
# Process tests: the real binaries, both media driver modes, the exit codes; they need GW_TEST_BUILDS (the duty-stall
# builds and the test media driver, never published).

find_package(Python3 REQUIRED COMPONENTS Interpreter)

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
    unit/WireRefusalTest.cpp unit/WireClusterToolsGoldenTest.cpp unit/SessionTest.cpp)
target_include_directories(gw_unit_tests PRIVATE unit golden "${GW_CT_GOLDEN_DIR}" "${GW_CT_FAKE_TRANSPORT_DIR}")
target_compile_definitions(gw_unit_tests PRIVATE ${GW_WIRE_TEST_DEFINITIONS})
target_link_libraries(gw_unit_tests PRIVATE gw_common)
target_compile_options(gw_unit_tests PRIVATE ${GW_WARNINGS})
add_test(NAME unit COMMAND gw_unit_tests)

# Control for the codec parity row: the codec built with CreateOrder's price and quantity swapped (GW_NC_SWAPPED_FIELD,
# test-only, never linked into a binary) must fail the golden case and name the template.
add_executable(gw_wire_nc_swapped_field unit/TestMain.cpp unit/WireGoldenTest.cpp
    "${PROJECT_SOURCE_DIR}/common/src/Wire.cpp")
target_include_directories(gw_wire_nc_swapped_field PRIVATE unit golden "${PROJECT_SOURCE_DIR}/common/include")
target_compile_definitions(gw_wire_nc_swapped_field PRIVATE ${GW_WIRE_TEST_DEFINITIONS} GW_NC_SWAPPED_FIELD)
target_link_libraries(gw_wire_nc_swapped_field PRIVATE cluster_tools::client_core cluster_tools::sbe_codecs)
target_compile_options(gw_wire_nc_swapped_field PRIVATE ${GW_WARNINGS})
add_test(NAME wire_nc_swapped_field COMMAND gw_wire_nc_swapped_field wire_golden_commands)
set_tests_properties(wire_nc_swapped_field PROPERTIES PASS_REGULAR_EXPRESSION "golden mismatch CreateOrder")

# Control for the decode parity rows: the codec built with OrderEvent's price and stopPrice swapped on decode
# (GW_NC_SWAPPED_DECODE_FIELD, test-only, never linked into a binary) must fail the golden reply case and name it.
add_executable(gw_wire_nc_swapped_decode_field unit/TestMain.cpp unit/WireGoldenTest.cpp
    "${PROJECT_SOURCE_DIR}/common/src/Wire.cpp")
target_include_directories(gw_wire_nc_swapped_decode_field PRIVATE unit golden "${PROJECT_SOURCE_DIR}/common/include")
target_compile_definitions(gw_wire_nc_swapped_decode_field PRIVATE ${GW_WIRE_TEST_DEFINITIONS} GW_NC_SWAPPED_DECODE_FIELD)
target_link_libraries(gw_wire_nc_swapped_decode_field PRIVATE cluster_tools::client_core cluster_tools::sbe_codecs)
target_compile_options(gw_wire_nc_swapped_decode_field PRIVATE ${GW_WARNINGS})
add_test(NAME wire_nc_swapped_decode_field COMMAND gw_wire_nc_swapped_decode_field wire_golden_replies)
set_tests_properties(wire_nc_swapped_decode_field PROPERTIES PASS_REGULAR_EXPRESSION
    "golden mismatch OrderEvent: decoded values")

# Control for the version gate: the decoder built without its exact-version check (GW_NC_NO_VERSION_GATE, test-only)
# must fail the version row: a v7 frame read with v8 offsets.
add_executable(gw_wire_nc_no_version_gate unit/TestMain.cpp unit/WireRefusalTest.cpp
    "${PROJECT_SOURCE_DIR}/common/src/Wire.cpp")
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
    target_link_libraries(${target} PRIVATE gw_common)
    target_compile_options(${target} PRIVATE ${GW_WARNINGS})
    add_test(NAME ${target} COMMAND ${target} ${case})
    set_tests_properties(${target} PROPERTIES PASS_REGULAR_EXPRESSION "${text}")
endforeach()

add_test(NAME lock COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/lock/lock_test.sh" "${PROJECT_SOURCE_DIR}" "${CMAKE_COMMAND}"
    "${CMAKE_CURRENT_BINARY_DIR}/lock-work")

if(GW_TEST_BUILDS)
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
    target_link_libraries(gw_session_it PRIVATE cluster_tools::runtime_embedded_driver gw_common)
    target_compile_options(gw_session_it PRIVATE ${GW_WARNINGS})
    set(GW_IT_CORE_JAR "" CACHE FILEPATH "core-cluster.jar built at the money schema commit of dependencies.lock (T1)")
    set(GW_IT_PROBE_JAR "" CACHE FILEPATH "CoreProbe.jar compiled against GW_IT_CORE_JAR (T1)")
    if(GW_IT_CORE_JAR AND GW_IT_PROBE_JAR)
        set(_port 26000)
        foreach(mode embedded external)
            add_test(NAME it.session.${mode}
                COMMAND Python3::Interpreter -I "${CMAKE_CURRENT_SOURCE_DIR}/it/session_it.py"
                    --bin-dir "${PROJECT_BINARY_DIR}/bin" --core-jar "${GW_IT_CORE_JAR}" --probe-jar "${GW_IT_PROBE_JAR}"
                    --mode ${mode} --work "${CMAKE_CURRENT_BINARY_DIR}/it-work/session.${mode}" --port-base ${_port})
            set_tests_properties(it.session.${mode} PROPERTIES TIMEOUT 900 LABELS t1 RUN_SERIAL TRUE)
            math(EXPR _port "${_port} + 1000")
        endforeach()
    endif()

    set(GW_PROCESS_CASES config_refused config_bad_values embedded_start_stop external_start_stop
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
endif()
