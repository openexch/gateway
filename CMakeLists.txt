# openexch/gateway - oe-gateway (REST) and oe-stream (WebSocket user streams), two stateless C++20 processes in front
# of the core, sharing one static library, gw_common.
#
# Build:
#   scripts/build-cluster-tools.sh <cache-dir> <prefix>     (fetches, verifies and installs the pinned cluster-tools)
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=<prefix> && cmake --build build && ctest --test-dir build
# Options:
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
find_package(cluster_tools CONFIG REQUIRED COMPONENTS client_core runtime client client_aeron runtime_aeron sbe_codecs
    runtime_embedded_driver)
gw_check_cluster_tools_package()

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

set(GW_WARNINGS -Wall -Wextra -Wpedantic -Werror)

# gw_common: the one library both binaries link. Never links the embedded driver (a library must not: only a final
# executable lists cluster_tools::runtime_embedded_driver, first).
add_library(gw_common STATIC
    common/src/Admin.cpp
    common/src/Environment.cpp
    common/src/ProcessShell.cpp
    common/src/Readiness.cpp
    common/src/Settings.cpp
    common/src/SettingsReader.cpp)
target_include_directories(gw_common PUBLIC common/include "${CMAKE_CURRENT_BINARY_DIR}/generated")
target_link_libraries(gw_common PUBLIC cluster_tools::runtime_aeron cluster_tools::client_aeron cluster_tools::client
    cluster_tools::client_core cluster_tools::runtime cluster_tools::sbe_codecs Threads::Threads)
target_compile_options(gw_common PRIVATE ${GW_WARNINGS})

add_library(oe_gateway_app STATIC gateway/GatewayApp.cpp)
target_include_directories(oe_gateway_app PUBLIC gateway)
target_link_libraries(oe_gateway_app PUBLIC gw_common)
target_compile_options(oe_gateway_app PRIVATE ${GW_WARNINGS})

add_library(oe_stream_app STATIC stream/StreamApp.cpp)
target_include_directories(oe_stream_app PUBLIC stream)
target_link_libraries(oe_stream_app PUBLIC gw_common)
target_compile_options(oe_stream_app PRIVATE ${GW_WARNINGS})

# The binaries: the embedded driver first in the link list (its archive resolves the C client symbols before
# aeron_static is scanned).
add_executable(oe-gateway gateway/main.cpp)
target_link_libraries(oe-gateway PRIVATE cluster_tools::runtime_embedded_driver oe_gateway_app)
add_executable(oe-stream stream/main.cpp)
target_link_libraries(oe-stream PRIVATE cluster_tools::runtime_embedded_driver oe_stream_app)
install(TARGETS oe-gateway oe-stream RUNTIME DESTINATION bin)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
