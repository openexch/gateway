# T0: unit tests of the shared library (settings, readiness, admin rendering) and the lock verification.
# Process tests: the real binaries, both media driver modes, the exit codes; they need GW_TEST_BUILDS (the duty-stall
# builds and the test media driver, never published).

find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_executable(gw_unit_tests unit/TestMain.cpp unit/SettingsTest.cpp unit/AdminTest.cpp)
target_include_directories(gw_unit_tests PRIVATE unit)
target_link_libraries(gw_unit_tests PRIVATE gw_common)
target_compile_options(gw_unit_tests PRIVATE ${GW_WARNINGS})
add_test(NAME unit COMMAND gw_unit_tests)

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
