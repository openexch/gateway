# cmake -DLOCK=<dependencies.lock> -DPREFIX=<deps prefix> -P deps_check.cmake
# Runs the configure-time check of the Boost, OpenSSL and libpq prefix (cmake/GatewayDeps.cmake) in script mode.
include("${CMAKE_CURRENT_LIST_DIR}/../../cmake/GatewayDeps.cmake")
gw_verify_deps("${LOCK}" "${PREFIX}")
