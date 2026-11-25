#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# smoke.sh: build the gateway image from the published binaries and start each process once.
#
# Usage: deploy/oci/smoke.sh --set <dir> [--tag <tag>]
#   --set  a directory holding the published binaries (oe-gateway, oe-stream, oe-marketdata, oe-projector)
#
# Steps: each process started through worker-launch without its settings refuses with 78 (its own settings check:
# the binary runs in the image, read-only root, non-root uid); a driver mode in the env that contradicts the
# command is refused with 78; no command is 78. Docker builds are heavy on a shared box: run this under the box's
# build lock.
# Exit: 0 every step passed, 1 a step failed.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SET=""
TAG=openexchange/gateway:smoke
while [ $# -gt 0 ]; do
    case "$1" in
        --set) SET=$2; shift 2 ;;
        --tag) TAG=$2; shift 2 ;;
        *) echo "usage: $0 --set <dir> [--tag <tag>]" >&2; exit 64 ;;
    esac
done
PROCESSES="oe-gateway oe-stream oe-marketdata oe-projector"
for f in $PROCESSES; do
    [ -x "$SET/$f" ] || { echo "smoke: $SET/$f is missing (--set <published binaries dir>)" >&2; exit 64; }
done

FAILS=0
fail() { echo "FAIL $1"; FAILS=$((FAILS + 1)); }
pass() { echo "ok   $1"; }
RO=(--read-only --tmpfs "/tmp:rw,size=64m")
BIN=/opt/openexchange/gateway/bin

docker build -q -f "$HERE/oci/Dockerfile" --build-context deploy="$HERE" -t "$TAG" "$SET" >/dev/null
pass "image $TAG built"

expect78() { # what text command...
    local what=$1 text=$2 rc=0 out; shift 2
    out=$(docker run --rm "${RO[@]}" "$@" 2>&1) || rc=$?
    if [ "$rc" = 78 ] && grep -q -- "$text" <<<"$out"; then
        pass "$what: exit 78 ($text)"
    else
        fail "$what: exit $rc, expected 78 naming '$text': $(tail -3 <<<"$out")"
    fi
}
expect78 "no command" "usage: worker-launch" "$TAG"
for p in $PROCESSES; do
    expect78 "$p without settings" "exit 78" "$TAG" "$BIN/worker-launch" embedded "$p"
done
expect78 "env driver mode contradicting the command" "GATEWAY_DRIVER_MODE=embedded" \
    -e GATEWAY_DRIVER_MODE=embedded "$TAG" "$BIN/worker-launch" external oe-gateway

if [ $FAILS -gt 0 ]; then
    echo "smoke: $FAILS step(s) failed"
    exit 1
fi
echo "smoke: all steps passed"
