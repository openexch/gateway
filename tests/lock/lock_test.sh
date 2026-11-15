#!/bin/bash
# Lock verification: scripts/fetch-cluster-tools.sh accepts only the tarball the lock pins (sha256, top directory,
# the commit in the URL, the same money schema as the tree's own lock), and the configure-time package check refuses
# a package built from another commit or carrying another money schema. Offline: fake trees, no network.
#
# usage: lock_test.sh <repo-root> <cmake> <work-dir>
set -uo pipefail
[ $# -eq 3 ] || { echo "usage: $0 <repo-root> <cmake> <work-dir>" >&2; exit 64; }
ROOT=$1
CMAKE=$2
WORK=$3
FETCH="$ROOT/scripts/fetch-cluster-tools.sh"
rm -rf "$WORK"
mkdir -p "$WORK"
FAILURES=0
pass() { echo "PASS $1"; }
fail() { echo "FAIL $1: $2"; FAILURES=$((FAILURES + 1)); }

REF=1111111111111111111111111111111111111111
MONEY_REF=2222222222222222222222222222222222222222
MONEY_SHA=3333333333333333333333333333333333333333333333333333333333333333

# fake_tarball <name> <top-dir> <money-sha-in-tree>: a cluster-tools tree with only its lock
fake_tarball() {
    local dir="$WORK/src-$1"
    mkdir -p "$dir/$2"
    cat > "$dir/$2/dependencies.lock" <<JSON
{"money_schema_repo": "openexch/core", "money_schema_ref": "$MONEY_REF", "money_schema_sha256": "$3",
 "money_schema_id": 2}
JSON
    tar -czf "$WORK/$1.tar.gz" -C "$dir" "$2"
    sha256sum "$WORK/$1.tar.gz" | cut -d' ' -f1
}

# write_lock <file> <ref> <url> <tarball-sha>
write_lock() {
    cat > "$1" <<JSON
{
  "cluster_tools_repo": "openexch/cluster-tools",
  "cluster_tools_ref": "$2",
  "cluster_tools_tarball_url": "$3",
  "cluster_tools_tarball_sha256": "$4",
  "money_schema_repo": "openexch/core",
  "money_schema_ref": "$MONEY_REF",
  "money_schema_path": "core-common/src/main/resources/sbe/money-schema.xml",
  "money_schema_sha256": "$MONEY_SHA",
  "money_schema_id": 2,
  "money_schema_version": 8
}
JSON
}

# run_fetch <case> <lock> <tarball> : offline, the tarball pre-placed in a fresh cache
run_fetch() {
    local cache="$WORK/cache-$1"
    mkdir -p "$cache"
    cp "$3" "$cache/cluster-tools-$REF.tar.gz"
    GW_LOCK="$2" FETCH_OFFLINE=1 "$FETCH" "$cache" --verify-only > "$WORK/$1.out" 2> "$WORK/$1.err"
}

URL="https://codeload.github.com/openexch/cluster-tools/tar.gz/$REF"
GOOD_SHA=$(fake_tarball good "cluster-tools-$REF" "$MONEY_SHA")

# 1. the pinned tarball is accepted and extracted
write_lock "$WORK/good.lock" "$REF" "$URL" "$GOOD_SHA"
if run_fetch good "$WORK/good.lock" "$WORK/good.tar.gz" && grep -qx -- "-DCT_BUILD_SOURCE_SHA=$REF" "$WORK/good.out" \
    && [ -f "$WORK/cache-good/cluster-tools-$REF/dependencies.lock" ]; then
    pass lock.accepts_the_pinned_tarball
else
    fail lock.accepts_the_pinned_tarball "$(cat "$WORK/good.err")"
fi

# 2. a wrong sha256 is refused, both values named, nothing extracted
WRONG_SHA=$(printf '%s' "$GOOD_SHA" | tr '0-9a-f' '1-9a-f0')
write_lock "$WORK/wrong.lock" "$REF" "$URL" "$WRONG_SHA"
if run_fetch wrong "$WORK/wrong.lock" "$WORK/good.tar.gz"; then
    fail lock.refuses_a_wrong_sha256 "accepted a tarball whose sha256 the lock does not pin"
elif grep -q "sha256 mismatch" "$WORK/wrong.err" && grep -q "$WRONG_SHA" "$WORK/wrong.err" && grep -q "$GOOD_SHA" "$WORK/wrong.err" \
    && [ ! -e "$WORK/cache-wrong/cluster-tools-$REF" ]; then
    pass lock.refuses_a_wrong_sha256
else
    fail lock.refuses_a_wrong_sha256 "refused without the expected text or left a tree: $(cat "$WORK/wrong.err")"
fi

# 3. the right sha256 of a tarball whose top directory is another commit's is refused
OTHER_SHA=$(fake_tarball other "cluster-tools-9999999999999999999999999999999999999999" "$MONEY_SHA")
write_lock "$WORK/other.lock" "$REF" "$URL" "$OTHER_SHA"
if run_fetch other "$WORK/other.lock" "$WORK/other.tar.gz"; then
    fail lock.refuses_another_top_directory "accepted"
elif grep -q "expected exactly cluster-tools-$REF" "$WORK/other.err"; then
    pass lock.refuses_another_top_directory
else
    fail lock.refuses_another_top_directory "$(cat "$WORK/other.err")"
fi

# 4. a tree that pins another money schema is refused
DRIFT_SHA=$(fake_tarball drift "cluster-tools-$REF" "4444444444444444444444444444444444444444444444444444444444444444")
write_lock "$WORK/drift.lock" "$REF" "$URL" "$DRIFT_SHA"
if run_fetch drift "$WORK/drift.lock" "$WORK/drift.tar.gz"; then
    fail lock.refuses_a_money_schema_drift "accepted"
elif grep -q "pins the money schema" "$WORK/drift.err"; then
    pass lock.refuses_a_money_schema_drift
else
    fail lock.refuses_a_money_schema_drift "$(cat "$WORK/drift.err")"
fi

# 5. a URL that names another commit, and a ref that is not a full sha, are refused before any download
write_lock "$WORK/url.lock" "$REF" "https://codeload.github.com/openexch/cluster-tools/tar.gz/main" "$GOOD_SHA"
if run_fetch url "$WORK/url.lock" "$WORK/good.tar.gz"; then
    fail lock.refuses_a_url_without_the_ref "accepted"
elif grep -q "does not name cluster_tools_ref" "$WORK/url.err"; then
    pass lock.refuses_a_url_without_the_ref
else
    fail lock.refuses_a_url_without_the_ref "$(cat "$WORK/url.err")"
fi
write_lock "$WORK/short.lock" "f0e93f3" "$URL" "$GOOD_SHA"
if GW_LOCK="$WORK/short.lock" FETCH_OFFLINE=1 "$FETCH" "$WORK/cache-short" --verify-only > /dev/null 2> "$WORK/short.err"; then
    fail lock.refuses_a_short_ref "accepted"
elif grep -q "40-hex commit sha" "$WORK/short.err"; then
    pass lock.refuses_a_short_ref
else
    fail lock.refuses_a_short_ref "$(cat "$WORK/short.err")"
fi

# 6. the repository's own lock is well formed (full shas, the URL names the ref)
if python3 -I - "$ROOT/dependencies.lock" <<'PY'
import json, re, sys
lock = json.load(open(sys.argv[1]))
assert re.fullmatch(r"[0-9a-f]{40}", lock["cluster_tools_ref"]), "cluster_tools_ref"
assert re.fullmatch(r"[0-9a-f]{64}", lock["cluster_tools_tarball_sha256"]), "cluster_tools_tarball_sha256"
assert lock["cluster_tools_ref"] in lock["cluster_tools_tarball_url"], "url names the ref"
assert re.fullmatch(r"[0-9a-f]{40}", lock["money_schema_ref"]), "money_schema_ref"
assert re.fullmatch(r"[0-9a-f]{64}", lock["money_schema_sha256"]), "money_schema_sha256"
assert isinstance(lock["money_schema_version"], int) and isinstance(lock["money_schema_id"], int)
PY
then
    pass lock.repository_lock_is_well_formed
else
    fail lock.repository_lock_is_well_formed "see above"
fi

# 7. the configure-time package check: the pinned package passes; another commit, another schema sha256, id or
# version fails, each by name
LOCK="$ROOT/dependencies.lock"
lockv() { python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$LOCK" "$1"; }
PIN=$(lockv cluster_tools_ref)
PMS=$(lockv money_schema_sha256)
PMV=$(lockv money_schema_version)
PMI=$(lockv money_schema_id)
# pin_check <sha> <money-sha256> <money-version> [<money-id>]  (the id defaults to the lock's)
pin_check() { "$CMAKE" -DLOCK="$LOCK" -DPKG_SHA="$1" -DPKG_MONEY_SHA="$2" -DPKG_MONEY_VERSION="$3" \
    -DPKG_MONEY_ID="${4:-$PMI}" -P "$ROOT/tests/lock/pin_check.cmake" > "$WORK/pin.out" 2>&1; }
# pin_check_headers <include-dir>: the pinned package values, the money schema id read from codec headers
pin_check_headers() { "$CMAKE" -DLOCK="$LOCK" -DPKG_SHA="$PIN" -DPKG_MONEY_SHA="$PMS" -DPKG_MONEY_VERSION="$PMV" \
    -DPKG_MONEY_INCLUDE="$1" -P "$ROOT/tests/lock/pin_check.cmake" > "$WORK/pin.out" 2>&1; }
# fake_codec <include-dir> <name> <schema-id>: a generated money codec header carrying that schema id
fake_codec() {
    mkdir -p "$1/cluster_tools/sbe/money/com_example_generated"
    printf '    static constexpr std::uint16_t SBE_SCHEMA_ID = static_cast<std::uint16_t>(%s);\n' "$3" \
        > "$1/cluster_tools/sbe/money/com_example_generated/$2.h"
}
if pin_check "$PIN" "$PMS" "$PMV"; then pass package.accepts_the_pinned_package
else fail package.accepts_the_pinned_package "$(cat "$WORK/pin.out")"; fi
if pin_check 0000000000000000000000000000000000000000 "$PMS" "$PMV"; then
    fail package.refuses_another_commit "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "dependencies.lock pins $PIN"; then
    pass package.refuses_another_commit
else fail package.refuses_another_commit "$(cat "$WORK/pin.out")"; fi
if pin_check "$PIN" "$MONEY_SHA" "$PMV"; then
    fail package.refuses_another_money_schema "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "carries money schema sha256"; then
    pass package.refuses_another_money_schema
else fail package.refuses_another_money_schema "$(cat "$WORK/pin.out")"; fi
if pin_check "$PIN" "$PMS" 6; then
    fail package.refuses_another_money_version "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "carries money schema version"; then
    pass package.refuses_another_money_version
else fail package.refuses_another_money_version "$(cat "$WORK/pin.out")"; fi
OTHER_ID=$((PMI + 1))
if pin_check "$PIN" "$PMS" "$PMV" "$OTHER_ID"; then
    fail package.refuses_another_money_schema_id "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "carries money schema id '$OTHER_ID'; dependencies.lock pins $PMI"; then
    pass package.refuses_another_money_schema_id
else fail package.refuses_another_money_schema_id "$(cat "$WORK/pin.out")"; fi

# 8. the money schema id read from the package's codec headers: the lock's id passes; another id, codecs of two ids,
# and no codec at all are refused by name
fake_codec "$WORK/inc-good" A "$PMI"; fake_codec "$WORK/inc-good" B "$PMI"
if pin_check_headers "$WORK/inc-good"; then pass package.reads_the_money_schema_id_from_the_codecs
else fail package.reads_the_money_schema_id_from_the_codecs "$(cat "$WORK/pin.out")"; fi
fake_codec "$WORK/inc-other" A "$OTHER_ID"
if pin_check_headers "$WORK/inc-other"; then
    fail package.refuses_codecs_of_another_money_schema_id "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "carries money schema id '$OTHER_ID'"; then
    pass package.refuses_codecs_of_another_money_schema_id
else fail package.refuses_codecs_of_another_money_schema_id "$(cat "$WORK/pin.out")"; fi
fake_codec "$WORK/inc-mixed" A "$PMI"; fake_codec "$WORK/inc-mixed" B "$OTHER_ID"
if pin_check_headers "$WORK/inc-mixed"; then
    fail package.refuses_codecs_of_several_money_schema_ids "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "money codecs of several schema ids"; then
    pass package.refuses_codecs_of_several_money_schema_ids
else fail package.refuses_codecs_of_several_money_schema_ids "$(cat "$WORK/pin.out")"; fi
mkdir -p "$WORK/inc-empty"
if pin_check_headers "$WORK/inc-empty"; then
    fail package.refuses_a_package_without_money_codecs "accepted"
elif tr '\n' ' ' < "$WORK/pin.out" | tr -s ' ' | grep -q "carries no money codec"; then
    pass package.refuses_a_package_without_money_codecs
else fail package.refuses_a_package_without_money_codecs "$(cat "$WORK/pin.out")"; fi

if [ "$FAILURES" -ne 0 ]; then
    echo "lock_test: $FAILURES failure(s)"
    exit 1
fi
echo "lock_test: PASS"
