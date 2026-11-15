# The cluster-tools package must be the one dependencies.lock pins: built from cluster_tools_ref, carrying the money
# schema the lock names (sha256, id and version). Anything else is refused at configure, by name; there is no override.
# The schema id is cluster_tools_MONEY_SCHEMA_ID when the package exports it, else the SBE_SCHEMA_ID its installed money
# codecs were generated with (gw_package_money_schema_id; every codec must agree, none found is a refusal).
# scripts/build-cluster-tools.sh builds the pinned package; CMAKE_PREFIX_PATH points at its install prefix.

function(gw_read_lock lock_file)
    file(READ "${lock_file}" _lock)
    foreach(_key cluster_tools_ref cluster_tools_tarball_sha256 money_schema_ref money_schema_sha256 money_schema_id
            money_schema_version)
        string(JSON _value ERROR_VARIABLE _err GET "${_lock}" "${_key}")
        if(_err)
            message(FATAL_ERROR "dependencies.lock: ${_key}: ${_err}")
        endif()
        set(GW_LOCK_${_key} "${_value}" PARENT_SCOPE)
    endforeach()
endfunction()

# gw_package_money_schema_id(<out-var> <include-dir>...): the SBE schema id of the money codecs under
# <include-dir>/cluster_tools/sbe/money/<package>/*.h, or a FATAL_ERROR naming what is wrong.
function(gw_package_money_schema_id out_var)
    set(_ids "")
    foreach(_dir IN LISTS ARGN)
        file(GLOB _headers "${_dir}/cluster_tools/sbe/money/*/*.h")
        foreach(_header IN LISTS _headers)
            file(STRINGS "${_header}" _lines REGEX "SBE_SCHEMA_ID = static_cast<std::uint16_t>\\([0-9]+\\)")
            foreach(_line IN LISTS _lines)
                string(REGEX REPLACE ".*static_cast<std::uint16_t>\\(([0-9]+)\\).*" "\\1" _id "${_line}")
                list(APPEND _ids "${_id}")
            endforeach()
        endforeach()
    endforeach()
    list(REMOVE_DUPLICATES _ids)
    list(LENGTH _ids _distinct)
    if(_distinct EQUAL 0)
        message(FATAL_ERROR "the cluster_tools package carries no money codec with an SBE_SCHEMA_ID under '${ARGN}' "
            "(cluster_tools/sbe/money/*/*.h): its money schema id cannot be compared with dependencies.lock")
    endif()
    if(NOT _distinct EQUAL 1)
        message(FATAL_ERROR "the cluster_tools package carries money codecs of several schema ids (${_ids}); "
            "dependencies.lock pins one")
    endif()
    set(${out_var} "${_ids}" PARENT_SCOPE)
endfunction()

function(gw_check_cluster_tools_package)
    if(NOT cluster_tools_SOURCE_SHA STREQUAL GW_LOCK_cluster_tools_ref)
        message(FATAL_ERROR "the cluster_tools package at ${cluster_tools_DIR} was built from "
            "'${cluster_tools_SOURCE_SHA}'; dependencies.lock pins ${GW_LOCK_cluster_tools_ref} "
            "(scripts/build-cluster-tools.sh builds the pinned one)")
    endif()
    if(NOT cluster_tools_MONEY_SCHEMA_SHA256 STREQUAL GW_LOCK_money_schema_sha256)
        message(FATAL_ERROR "the cluster_tools package carries money schema sha256 '${cluster_tools_MONEY_SCHEMA_SHA256}'; "
            "dependencies.lock pins ${GW_LOCK_money_schema_sha256} (core ${GW_LOCK_money_schema_ref})")
    endif()
    if(DEFINED cluster_tools_MONEY_SCHEMA_ID)
        set(_money_id "${cluster_tools_MONEY_SCHEMA_ID}")
    else()
        get_target_property(_includes cluster_tools::sbe_codecs INTERFACE_INCLUDE_DIRECTORIES)
        gw_package_money_schema_id(_money_id ${_includes})
    endif()
    if(NOT _money_id STREQUAL GW_LOCK_money_schema_id)
        message(FATAL_ERROR "the cluster_tools package carries money schema id '${_money_id}'; "
            "dependencies.lock pins ${GW_LOCK_money_schema_id}")
    endif()
    if(NOT cluster_tools_MONEY_SCHEMA_VERSION STREQUAL GW_LOCK_money_schema_version)
        message(FATAL_ERROR "the cluster_tools package carries money schema version '${cluster_tools_MONEY_SCHEMA_VERSION}'; "
            "dependencies.lock pins ${GW_LOCK_money_schema_version}")
    endif()
    message(STATUS "cluster_tools ${cluster_tools_SOURCE_SHA} (money schema id ${_money_id} v${cluster_tools_MONEY_SCHEMA_VERSION} "
        "${cluster_tools_MONEY_SCHEMA_SHA256}, aeron ${cluster_tools_AERON_VERSION}): matches dependencies.lock")
endfunction()
