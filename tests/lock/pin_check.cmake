# cmake -DLOCK=<dependencies.lock> -DPKG_SHA=<sha> -DPKG_MONEY_SHA=<sha256> -DPKG_MONEY_VERSION=<n>
#       (-DPKG_MONEY_ID=<id> | -DPKG_MONEY_INCLUDE=<include-dir>) -P pin_check.cmake
# Runs the configure-time package check (cmake/ClusterToolsPin.cmake) against the given package values. The money schema
# id is given as the package's exported value, or read from the money codec headers under an include dir.
include("${CMAKE_CURRENT_LIST_DIR}/../../cmake/ClusterToolsPin.cmake")
gw_read_lock("${LOCK}")
set(cluster_tools_DIR "<test package>")
set(cluster_tools_SOURCE_SHA "${PKG_SHA}")
set(cluster_tools_MONEY_SCHEMA_SHA256 "${PKG_MONEY_SHA}")
set(cluster_tools_MONEY_SCHEMA_VERSION "${PKG_MONEY_VERSION}")
set(cluster_tools_AERON_VERSION "test")
if(DEFINED PKG_MONEY_ID)
    set(cluster_tools_MONEY_SCHEMA_ID "${PKG_MONEY_ID}")
elseif(DEFINED PKG_MONEY_INCLUDE)
    gw_package_money_schema_id(cluster_tools_MONEY_SCHEMA_ID "${PKG_MONEY_INCLUDE}")
else()
    message(FATAL_ERROR "pin_check.cmake: give PKG_MONEY_ID or PKG_MONEY_INCLUDE")
endif()
gw_check_cluster_tools_package()
