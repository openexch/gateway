# The third-party inputs of gw_common (Boost headers, OpenSSL libcrypto and libssl, libpq) come only from the prefix
# scripts/build-deps.sh builds from the tarballs dependencies.lock pins (version and sha256). Configure refuses, by
# name: a prefix without the stamp, a stamp naming another version or tarball sha256, and installed version headers
# that disagree with the lock. Nothing is taken from the system include or library paths; there is no override.
#
# gw_verify_deps(<lock-file> <prefix>) runs the checks only (script mode: tests/lock/deps_check.cmake);
# gw_check_deps(<lock-file> <prefix>) runs them and defines the imported targets gw::boost, gw::crypto, gw::ssl, gw::pq.

function(gw_deps_fail what)
    message(FATAL_ERROR "GW_DEPS_PREFIX: ${what} (scripts/build-deps.sh <cache-dir> <prefix> builds the pinned set)")
endfunction()

# gw_header_define(<out> <header> <macro>): the value of `#define <macro> <value>` (quotes stripped), "" when absent
function(gw_header_define out header macro)
    set(_value "")
    if(EXISTS "${header}")
        file(STRINGS "${header}" _lines REGEX "^#[ \t]*define[ \t]+${macro}[ \t]+")
        if(_lines)
            list(GET _lines 0 _line)
            string(REGEX REPLACE "^#[ \t]*define[ \t]+${macro}[ \t]+\"?([^\" \t]*)\"?.*" "\\1" _value "${_line}")
        endif()
    endif()
    set(${out} "${_value}" PARENT_SCOPE)
endfunction()

function(gw_verify_deps lock_file prefix)
    if(prefix STREQUAL "")
        gw_deps_fail("not set; pass -DGW_DEPS_PREFIX=<prefix>")
    endif()
    file(READ "${lock_file}" _lock)
    set(_stamp_file "${prefix}/share/gateway-deps/deps.json")
    if(NOT EXISTS "${_stamp_file}")
        gw_deps_fail("${_stamp_file} not found")
    endif()
    file(READ "${_stamp_file}" _stamp)
    foreach(_key boost_version boost_sha256 openssl_version openssl_sha256 libpq_version libpq_sha256)
        string(JSON _want ERROR_VARIABLE _err GET "${_lock}" "${_key}")
        if(_err)
            message(FATAL_ERROR "dependencies.lock: ${_key}: ${_err}")
        endif()
        string(JSON _have ERROR_VARIABLE _err GET "${_stamp}" "${_key}")
        if(_err OR NOT _have STREQUAL _want)
            gw_deps_fail("${_stamp_file} names ${_key} '${_have}'; dependencies.lock pins ${_want}")
        endif()
        set(_lock_${_key} "${_want}")
    endforeach()

    # the installed headers carry the same versions
    gw_header_define(_boost "${prefix}/include/boost/version.hpp" BOOST_VERSION)
    string(REPLACE "." ";" _bv "${_lock_boost_version}")
    list(GET _bv 0 _bmaj)
    list(GET _bv 1 _bmin)
    list(GET _bv 2 _bpat)
    math(EXPR _boost_want "${_bmaj} * 100000 + ${_bmin} * 100 + ${_bpat}")
    if(NOT _boost STREQUAL _boost_want)
        gw_deps_fail("boost/version.hpp says BOOST_VERSION '${_boost}'; dependencies.lock pins ${_lock_boost_version}")
    endif()
    gw_header_define(_openssl "${prefix}/include/openssl/opensslv.h" OPENSSL_VERSION_STR)
    if(NOT _openssl STREQUAL _lock_openssl_version)
        gw_deps_fail("openssl/opensslv.h says '${_openssl}'; dependencies.lock pins ${_lock_openssl_version}")
    endif()
    gw_header_define(_pg "${prefix}/include/pg_config.h" PG_VERSION)
    if(NOT _pg STREQUAL _lock_libpq_version)
        gw_deps_fail("pg_config.h says PG_VERSION '${_pg}'; dependencies.lock pins ${_lock_libpq_version}")
    endif()
    foreach(_lib libcrypto.a libssl.a libpq.a libpgcommon.a libpgport.a)
        if(NOT EXISTS "${prefix}/lib/${_lib}")
            gw_deps_fail("${prefix}/lib/${_lib} not found")
        endif()
    endforeach()
    message(STATUS "gateway deps at ${prefix}: boost ${_lock_boost_version}, openssl ${_lock_openssl_version}, libpq "
        "${_lock_libpq_version}: match dependencies.lock")
endfunction()

function(gw_check_deps lock_file prefix)
    gw_verify_deps("${lock_file}" "${prefix}")

    find_package(Threads REQUIRED)
    add_library(gw::boost INTERFACE IMPORTED GLOBAL)
    # SYSTEM: third-party headers are not held to this repository's -Werror
    set_target_properties(gw::boost PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${prefix}/include"
        INTERFACE_INCLUDE_DIRECTORIES "${prefix}/include"
        INTERFACE_COMPILE_DEFINITIONS "BOOST_ALL_NO_LIB;BOOST_JSON_NO_LIB;BOOST_CONTAINER_NO_LIB;BOOST_ASIO_NO_DEPRECATED;BOOST_BEAST_USE_STD_STRING_VIEW")
    add_library(gw::crypto STATIC IMPORTED GLOBAL)
    set_target_properties(gw::crypto PROPERTIES IMPORTED_LOCATION "${prefix}/lib/libcrypto.a"
        INTERFACE_INCLUDE_DIRECTORIES "${prefix}/include" INTERFACE_LINK_LIBRARIES "Threads::Threads;${CMAKE_DL_LIBS}")
    add_library(gw::ssl STATIC IMPORTED GLOBAL)
    set_target_properties(gw::ssl PROPERTIES IMPORTED_LOCATION "${prefix}/lib/libssl.a"
        INTERFACE_LINK_LIBRARIES "gw::crypto")
    add_library(gw::pq STATIC IMPORTED GLOBAL)
    set_target_properties(gw::pq PROPERTIES IMPORTED_LOCATION "${prefix}/lib/libpq.a"
        INTERFACE_INCLUDE_DIRECTORIES "${prefix}/include"
        INTERFACE_LINK_LIBRARIES "${prefix}/lib/libpgcommon.a;${prefix}/lib/libpgport.a;gw::ssl;gw::crypto;Threads::Threads")
endfunction()
