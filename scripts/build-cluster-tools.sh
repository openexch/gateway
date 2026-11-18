#!/bin/bash
# Builds and installs the cluster-tools package this repository pins: scripts/fetch-cluster-tools.sh fetches and
# verifies every input, then the verified tree is configured (WITH_AERON, the embedded media driver, no tests),
# only the package's targets are built, and `cmake --install` writes the package to <prefix>. This repository's
# CMake then finds it with CMAKE_PREFIX_PATH=<prefix> and checks it against the lock again (source sha, money
# schema sha256 and version), so a stale prefix is refused at configure, never used.
#
# Usage: scripts/build-cluster-tools.sh <cache-dir> <prefix>
# Env:   CMAKE (default cmake), JOBS (default nproc), FETCH_OFFLINE (see fetch-cluster-tools.sh)
set -euo pipefail

[ $# -eq 2 ] || { echo "usage: $0 <cache-dir> <prefix>" >&2; exit 64; }
HERE=$(cd "$(dirname "$0")" && pwd)
CACHE=$(mkdir -p "$1" && cd "$1" && pwd)
PREFIX=$(mkdir -p "$2" && cd "$2" && pwd)
CMAKE=${CMAKE:-cmake}
JOBS=${JOBS:-$(nproc)}

ARGS=$("$HERE/fetch-cluster-tools.sh" "$CACHE")
SRC=$(printf '%s\n' "$ARGS" | sed -n 's/^-DCLUSTER_TOOLS_SOURCE_DIR=//p')
mapfile -t CT_ARGS < <(printf '%s\n' "$ARGS" | grep -v '^-DCLUSTER_TOOLS_SOURCE_DIR=')
REF=$(printf '%s\n' "$ARGS" | sed -n 's/^-DCT_BUILD_SOURCE_SHA=//p')
BUILD="$CACHE/build-$REF"

"$CMAKE" -S "$SRC" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release -DWITH_AERON=ON -DCT_BUILD_MEDIA_DRIVER=ON \
    -DBUILD_TESTING=OFF "${CT_ARGS[@]}"
# The package's targets only (cmake/Package.cmake in the pinned tree; ct_sbe_generate makes the codec headers of the
# interface target ct_sbe_codecs): the binaries and tests are not installed.
"$CMAKE" --build "$BUILD" -j"$JOBS" --target ct_client_core ct_runtime ct_archive_core ct_sbe_generate ct_client_aeron ct_runtime_aeron \
    aeron_static aeron_driver_static
"$CMAKE" --install "$BUILD" --prefix "$PREFIX"
CONFIG="$PREFIX/lib/cmake/cluster_tools/cluster_toolsConfig.cmake"
[ -f "$CONFIG" ] || CONFIG=$(find "$PREFIX" -name cluster_toolsConfig.cmake -print -quit)
grep -q "set(cluster_tools_SOURCE_SHA \"$REF\")" "$CONFIG" || {
    echo "FATAL: the installed package at $PREFIX does not name source sha $REF" >&2
    exit 1
}
echo "cluster-tools $REF installed to $PREFIX" >&2
echo "-DCMAKE_PREFIX_PATH=$PREFIX"
