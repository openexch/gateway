#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# render-verify.sh: render the gateway deploy files on three hosts (each with oe-gateway, oe-stream, oe-marketdata,
# oe-projector and their drivers), then check them.
#
# Usage: deploy/render-verify.sh [--out DIR] [--cluster-tools DIR] [--self-test]
#   --out DIR            render into DIR (default: a temporary dir, removed at exit)
#   --cluster-tools DIR  a cluster-tools checkout: its deploy/systemd/aeronmd@.service and launcher join every host
#                        (default: ../cluster-tools next to this repo when present; otherwise a stand-in, said so)
#   --self-test          also run the negative controls: each check, fed a broken render, must fail with its text
#
# Each host is a root tree: etc/systemd/system (the units), etc/openexchange (the env files), opt/openexchange (the
# launchers and stand-in binaries). Checks:
#   identity  every host carries byte-identical units
#   units     every process unit restarts on failure but not on 78, backs off, and (external) binds to, orders
#             after and is wanted by aeronmd@<same name> with the driver's AERON_DIR default
#   analyze   systemd-analyze verify --root=<host> on every instance (when systemd-analyze exists)
#   launcher  sh -n and shellcheck (local binary, else the pinned koalaman/shellcheck image when docker has it);
#             worker-launch refuses with 78 and execs the expected command
# Exit: 0 all checks pass, 1 a check failed (each failure printed as FAIL <check>: <text>).
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=""
CT=""
SELF_TEST=0
while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT=$2; shift 2 ;;
        --cluster-tools) CT=$2; shift 2 ;;
        --self-test) SELF_TEST=1; shift ;;
        *) echo "usage: $0 [--out DIR] [--cluster-tools DIR] [--self-test]" >&2; exit 64 ;;
    esac
done
if [ -z "$CT" ] && [ -f "$HERE/../../cluster-tools/deploy/systemd/aeronmd@.service" ]; then
    CT=$(cd "$HERE/../../cluster-tools" && pwd)
fi
if [ -z "$OUT" ]; then
    OUT=$(mktemp -d)
    trap 'rm -rf "$OUT"' EXIT
fi
SHELLCHECK_IMAGE=koalaman/shellcheck:v0.10.0@sha256:2097951f02e735b613f4a34de20c40f937a6c8f18ecb170612c88c34517221fb

HOSTS=3
WORKERS="oe-gateway oe-stream oe-marketdata oe-projector"
FAILS=0
fail() { echo "FAIL $1: $2"; FAILS=$((FAILS + 1)); }
pass() { echo "ok   $1: $2"; }

render_host() { # root
    local root=$1 w
    mkdir -p "$root/etc/systemd/system" "$root/etc/openexchange/gateway" "$root/etc/openexchange/aeronmd" \
        "$root/opt/openexchange/gateway/bin"
    cp "$HERE"/systemd/*.service "$root/etc/systemd/system/"
    cp "$HERE"/bin/* "$root/opt/openexchange/gateway/bin/"
    if [ -n "$CT" ]; then
        cp "$CT/deploy/systemd/aeronmd@.service" "$root/etc/systemd/system/"
        mkdir -p "$root/opt/openexchange/cluster-tools/bin"
        cp "$CT/deploy/bin/aeronmd-launch" "$root/opt/openexchange/cluster-tools/bin/"
    else
        printf '[Unit]\nDescription=stand-in driver %%i\n[Service]\nEnvironment=AERON_DIR=/dev/shm/aeron-openexchange-%%i\nExecStart=/bin/true\n' \
            > "$root/etc/systemd/system/aeronmd@.service"
    fi
    for w in $WORKERS; do
        printf '#!/bin/sh\nexit 0\n' > "$root/opt/openexchange/gateway/$w"
        chmod +x "$root/opt/openexchange/gateway/$w"
        # the process's own settings are its to check; the render only proves the unit finds the file
        printf '# %s settings\n' "$w" > "$root/etc/openexchange/gateway/$w.env"
        printf 'AERON_DRIVER_PROFILE=prod\nDRIVER_CPU_SET=\n' > "$root/etc/openexchange/aeronmd/$w.env"
    done
}

render() { # dir
    local dir=$1 i
    rm -rf "$dir"
    for i in $(seq 0 $((HOSTS - 1))); do render_host "$dir/host-$i"; done
}

check_identity() { # dir
    local dir=$1 ok=1 i u
    for i in $(seq 1 $((HOSTS - 1))); do
        for u in "$dir"/host-0/etc/systemd/system/*.service; do
            cmp -s "$u" "$dir/host-$i/etc/systemd/system/$(basename "$u")" ||
                { fail identity "$(basename "$u") differs between host-0 and host-$i"; ok=0; }
        done
    done
    [ $ok = 1 ] && pass identity "units identical on $HOSTS hosts${CT:+ (with the cluster-tools driver unit)}"
    [ -n "$CT" ] || echo "note: no cluster-tools checkout, the driver unit is a stand-in"
    return 0
}

unit_has() { grep -qx -- "$2" "$1"; }

check_units() { # dir
    local dir=$1 ok=1 u name line sys=$1/host-0/etc/systemd/system
    u=$sys/aeronmd@.service
    local drvdir; drvdir=$(sed -n 's/^Environment=AERON_DIR=//p' "$u")
    for u in "$sys"/oe-worker*@.service; do
        name=$(basename "$u")
        for line in Restart=on-failure RestartPreventExitStatus=78 RestartSec=2s RestartSteps=6 \
            RestartMaxDelaySec=60s StartLimitIntervalSec=0 User=openexchange; do
            unit_has "$u" "$line" || { fail units "$name lacks $line"; ok=0; }
        done
        case "$name" in
            *-embedded@.service)
                if grep -q aeronmd@ "$u"; then fail units "$name (embedded) depends on a driver unit"; ok=0; fi
                ;;
            *)
                unit_has "$u" 'BindsTo=aeronmd@%i.service' || { fail units "$name has no BindsTo=aeronmd@%i.service"; ok=0; }
                unit_has "$u" 'After=aeronmd@%i.service' || { fail units "$name is not ordered After=aeronmd@%i.service"; ok=0; }
                sed -n 's/^WantedBy=//p' "$u" | tr ' ' '\n' | grep -qx 'aeronmd@%i.service' ||
                    { fail units "$name is not WantedBy=aeronmd@%i.service (a restarted driver would not bring it back)"; ok=0; }
                [ "$(sed -n 's/^Environment=AERON_DIR=//p' "$u")" = "$drvdir" ] ||
                    { fail units "$name AERON_DIR default is not its driver's ($drvdir)"; ok=0; }
                ;;
        esac
    done
    [ $ok = 1 ] && pass units "process restart policy, driver binding and AERON_DIR agreement"
    return 0
}

check_analyze() { # dir
    if ! command -v systemd-analyze >/dev/null 2>&1; then
        echo "skip analyze: systemd-analyze not installed"
        return 0
    fi
    local dir=$1 ok=1 h w unit out
    for h in "$dir"/host-*; do
        if [ -d /usr/lib/systemd/system ] && [ ! -d "$h/usr/lib/systemd/system" ]; then
            mkdir -p "$h/usr/lib/systemd"
            cp -r /usr/lib/systemd/system "$h/usr/lib/systemd/"
        fi
        for w in $WORKERS; do
            for unit in "oe-worker@$w.service" "oe-worker-embedded@$w.service"; do
                out=$(cd "$h/etc/systemd/system" && systemd-analyze verify --man=no --root="$h" "$unit" 2>&1) || true
                if [ -n "$out" ]; then fail analyze "$(basename "$h")/$unit: $out"; ok=0; fi
            done
        done
    done
    [ $ok = 1 ] && pass analyze "systemd-analyze verify clean on every instance of every host"
    return 0
}

shellcheck_files() { # shell file...
    local shell=$1 out f mounts=() paths=()
    shift
    if command -v shellcheck >/dev/null 2>&1; then
        out=$(shellcheck -s "$shell" "$@" 2>&1) || { fail launcher "shellcheck: $out"; return 1; }
    elif command -v docker >/dev/null 2>&1 && docker image inspect "$SHELLCHECK_IMAGE" >/dev/null 2>&1; then
        for f in "$@"; do mounts+=(-v "$f:/w/$(basename "$f"):ro"); paths+=("/w/$(basename "$f")"); done
        out=$(docker run --rm "${mounts[@]}" "$SHELLCHECK_IMAGE" -s "$shell" "${paths[@]}" 2>&1) ||
            { fail launcher "shellcheck: $out"; return 1; }
    else
        echo "skip shellcheck: neither shellcheck nor the $SHELLCHECK_IMAGE image is available"
    fi
    return 0
}

check_launcher() { # bin-dir
    local bin=$1 ok=1 tmp rc out f
    for f in "$bin"/*; do sh -n "$f" || { fail launcher "sh -n rejects $f"; ok=0; }; done
    shellcheck_files sh "$bin"/* || ok=0
    shellcheck_files bash "$HERE/render-verify.sh" "$HERE/oci/smoke.sh" || ok=0
    tmp=$(mktemp -d)
    for f in $WORKERS; do
        # shellcheck disable=SC2016 # the stand-in expands its own environment
        printf '#!/bin/sh\nprintf "%%s\\n" "$0" "$AERON_DIR" "$GATEWAY_DRIVER_MODE" "$STREAM_DRIVER_MODE" "$MD_DRIVER_MODE" "$HISTORY_PROJECTOR_DRIVER_MODE" "$@" > "%s/args"\n' "$tmp" > "$tmp/$f"
        chmod +x "$tmp/$f"
    done
    run() { # expected-rc launcher VAR=value... -- args...
        local want=$1 launcher=$2 vars=()
        shift 2
        while [ "$1" != -- ]; do vars+=("$1"); shift; done
        shift
        rc=0
        out=$(env -i PATH=/usr/bin:/bin GW_HOME="$tmp" "${vars[@]}" sh "$bin/$launcher" "$@" 2>&1) || rc=$?
        [ "$rc" = "$want" ]
    }
    run 78 worker-launch GATEWAY_DRIVER_MODE=embedded AERON_DIR=/x -- external oe-gateway ||
        { fail launcher "a contradicting GATEWAY_DRIVER_MODE was not refused with 78 (rc $rc)"; ok=0; }
    run 78 worker-launch HISTORY_PROJECTOR_DRIVER_MODE=external -- embedded oe-projector ||
        { fail launcher "a contradicting HISTORY_PROJECTOR_DRIVER_MODE was not refused with 78 (rc $rc)"; ok=0; }
    run 78 worker-launch -- external oe-stream || { fail launcher "external without AERON_DIR was not refused (rc $rc)"; ok=0; }
    run 78 worker-launch -- embedded ct-bridge || { fail launcher "an unknown process was not refused (rc $rc)"; ok=0; }
    run 78 worker-launch CPU_SET=x -- embedded oe-marketdata || { fail launcher "a malformed CPU_SET was not refused (rc $rc)"; ok=0; }
    if run 0 worker-launch AERON_DIR=/dev/shm/g -- external oe-gateway; then
        [ "$(tr '\n' ' ' < "$tmp/args")" = "$tmp/oe-gateway /dev/shm/g external    " ] ||
            { fail launcher "unexpected oe-gateway start: $(tr '\n' ' ' < "$tmp/args")"; ok=0; }
    else
        fail launcher "a valid oe-gateway start did not exec (rc $rc): $out"; ok=0
    fi
    if run 0 worker-launch -- embedded oe-marketdata; then
        [ "$(tr '\n' ' ' < "$tmp/args")" = "$tmp/oe-marketdata    embedded  " ] ||
            { fail launcher "unexpected oe-marketdata start: $(tr '\n' ' ' < "$tmp/args")"; ok=0; }
    else
        fail launcher "a valid oe-marketdata start did not exec (rc $rc): $out"; ok=0
    fi
    rm -rf "$tmp"
    [ $ok = 1 ] && pass launcher "syntax, lint, refusals (78) and commands of worker-launch"
    return 0
}

render "$OUT/hosts"
echo "rendered: $OUT/hosts"
check_identity "$OUT/hosts"
check_units "$OUT/hosts"
check_analyze "$OUT/hosts"
check_launcher "$HERE/bin"

if [ $SELF_TEST = 1 ]; then
    expect_fail() { # name expected-text command...
        local name=$1 text=$2 out before=$FAILS; shift 2
        out=$("$@" 2>&1) || true
        FAILS=$before
        if grep -q -- "$text" <<<"$out"; then
            pass "self-test" "$name fails with '$text'"
        else
            fail "self-test" "$name did not fail with '$text'; got: $out"
        fi
    }
    neg=$OUT/neg
    render "$neg"
    printf '# host edit\n' >> "$neg/host-2/etc/systemd/system/oe-worker@.service"
    expect_fail "a unit edited on one host" "oe-worker@.service differs between host-0 and host-2" check_identity "$neg"
    render "$neg"
    sed -i '/^RestartPreventExitStatus=78$/d' "$neg/host-0/etc/systemd/system/oe-worker-embedded@.service"
    expect_fail "a process unit without RestartPreventExitStatus=78" "oe-worker-embedded@.service lacks RestartPreventExitStatus=78" check_units "$neg"
    render "$neg"
    sed -i '/^BindsTo=/d' "$neg/host-0/etc/systemd/system/oe-worker@.service"
    expect_fail "an external process not bound to its driver" "oe-worker@.service has no BindsTo" check_units "$neg"
    if command -v systemd-analyze >/dev/null 2>&1; then
        render "$neg"
        rm "$neg/host-1/opt/openexchange/gateway/bin/worker-launch"
        expect_fail "a host without the launcher" "worker-launch is not executable" check_analyze "$neg"
        if [ -n "$CT" ]; then
            render "$neg"
            rm "$neg/host-1/etc/systemd/system/aeronmd@.service"
            expect_fail "a host without the driver unit" "Unit aeronmd@oe-gateway.service not found" check_analyze "$neg"
        fi
    fi
    negbin=$OUT/neg-bin
    mkdir -p "$negbin"
    cp "$HERE"/bin/* "$negbin/"
    # shellcheck disable=SC2016 # a literal pattern
    sed -i 's/refuse "\$var=\$current/: "$var=$current/' "$negbin/worker-launch"
    expect_fail "a launcher that accepts a contradicting driver mode" "contradicting GATEWAY_DRIVER_MODE was not refused" check_launcher "$negbin"
fi

if [ $FAILS -gt 0 ]; then
    echo "render-verify: $FAILS check(s) failed"
    exit 1
fi
echo "render-verify: all checks passed"
