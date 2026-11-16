#!/bin/bash
# Builds the three third-party inputs of gw_common from the source tarballs dependencies.lock pins, and nothing else:
#   Boost    (boost_*): headers only (Beast, Asio, JSON; Boost.JSON is compiled into gw_common from its src.hpp)
#   OpenSSL  (openssl_*): static libcrypto and libssl (libssl only for libpq's TLS to PostgreSQL)
#   libpq    (libpq_*): the PostgreSQL client library, static, with TLS through that OpenSSL
# Every tarball is hashed before it is unpacked; a mismatch deletes the download and stops. The prefix gets a stamp,
# <prefix>/share/gateway-deps/deps.json, with each version and tarball sha256; configure (cmake/GatewayDeps.cmake)
# compares the stamp and the installed version headers with the lock and refuses any difference, by name.
#
# Usage: scripts/build-deps.sh <cache-dir> <prefix>
# Env:   JOBS (default nproc), FETCH_OFFLINE=1 (use only tarballs already in <cache-dir>)
set -euo pipefail

[ $# -eq 2 ] || { echo "usage: $0 <cache-dir> <prefix>" >&2; exit 64; }
HERE=$(cd "$(dirname "$0")" && pwd)
LOCK=${GW_LOCK:-$HERE/../dependencies.lock}
CACHE=$(mkdir -p "$1" && cd "$1" && pwd)
PREFIX=$(mkdir -p "$2" && cd "$2" && pwd)
JOBS=${JOBS:-$(nproc)}

lock() { python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$LOCK" "$1"; }

# fetch <name>: the verified tarball path on stdout
fetch() {
    local url sha file have
    url=$(lock "$1_url")
    sha=$(lock "$1_sha256")
    file="$CACHE/$(basename "$url")"
    if [ ! -f "$file" ]; then
        if [ "${FETCH_OFFLINE:-0}" = 1 ]; then
            echo "FATAL: $file is not in the cache and FETCH_OFFLINE=1" >&2
            exit 1
        fi
        curl -sSfL --retry 3 -o "$file.part" "$url"
        mv "$file.part" "$file"
    fi
    have=$(sha256sum "$file" | cut -d' ' -f1)
    if [ "$have" != "$sha" ]; then
        rm -f "$file"
        echo "FATAL: $(basename "$file") has sha256 $have; dependencies.lock pins $sha (deleted)" >&2
        exit 1
    fi
    printf '%s\n' "$file"
}

BOOST_TGZ=$(fetch boost)
OPENSSL_TGZ=$(fetch openssl)
PG_TGZ=$(fetch libpq)
BOOST_V=$(lock boost_version)
OPENSSL_V=$(lock openssl_version)
PG_V=$(lock libpq_version)
WORK="$CACHE/work"
rm -rf "$WORK"
mkdir -p "$WORK"

# Boost: the header tree only
BOOST_TOP="boost_${BOOST_V//./_}"
tar -xzf "$BOOST_TGZ" -C "$WORK" --no-same-owner "$BOOST_TOP/boost"
rm -rf "$PREFIX/include/boost"
mkdir -p "$PREFIX/include"
mv "$WORK/$BOOST_TOP/boost" "$PREFIX/include/boost"

# OpenSSL: static libraries and headers (install_dev: no binaries, no docs)
tar -xzf "$OPENSSL_TGZ" -C "$WORK" --no-same-owner
(cd "$WORK/openssl-$OPENSSL_V" &&
    ./config --prefix="$PREFIX" --openssldir="$PREFIX/ssl" --libdir=lib no-shared no-tests no-module >/dev/null &&
    make -s -j"$JOBS" build_libs >/dev/null &&
    make -s install_dev >/dev/null)

# libpq: configure against that OpenSSL, build the client library and its two support libraries only
tar -xzf "$PG_TGZ" -C "$WORK" --no-same-owner
(cd "$WORK/postgresql-$PG_V" &&
    ./configure --prefix="$PREFIX" --without-readline --without-zlib --without-icu --without-zstd --without-lz4 \
        --with-ssl=openssl CPPFLAGS="-I$PREFIX/include" LDFLAGS="-L$PREFIX/lib" LIBS="-ldl -pthread" >/dev/null &&
    make -s -j"$JOBS" -C src/port >/dev/null &&
    make -s -j"$JOBS" -C src/common >/dev/null &&
    make -s -j"$JOBS" -C src/interfaces/libpq >/dev/null &&
    make -s -C src/include install >/dev/null &&
    make -s -C src/port install >/dev/null &&
    make -s -C src/common install >/dev/null &&
    make -s -C src/interfaces/libpq install >/dev/null)
# static linking only: the shared libpq would shadow the verified archive for a -lpq link
rm -f "$PREFIX"/lib/libpq.so*

mkdir -p "$PREFIX/share/gateway-deps"
python3 -I - "$LOCK" "$PREFIX/share/gateway-deps/deps.json" <<'PY'
import json, sys
lock = json.load(open(sys.argv[1]))
keys = ["boost_version", "boost_sha256", "openssl_version", "openssl_sha256", "libpq_version", "libpq_sha256"]
with open(sys.argv[2], "w") as f:
    json.dump({k: lock[k] for k in keys}, f, indent=2)
    f.write("\n")
PY
rm -rf "$WORK"
echo "boost $BOOST_V, openssl $OPENSSL_V, libpq $PG_V installed to $PREFIX" >&2
echo "-DGW_DEPS_PREFIX=$PREFIX"
