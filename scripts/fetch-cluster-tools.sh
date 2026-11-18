#!/bin/bash
# Fetches and VERIFIES the cluster-tools source tree this repository pins in dependencies.lock, and the inputs
# its build needs. Nothing is trusted by name, branch or version string:
#
#   1. the source tarball of cluster_tools_ref is downloaded (or taken from the cache) and its sha256 must equal
#      cluster_tools_tarball_sha256; the URL must name the same commit; the tarball must hold exactly one top
#      directory, cluster-tools-<ref>. A mismatch refuses, names both values and leaves nothing extracted;
#   2. the money schema pin of the extracted tree's own lock must equal this lock's (one schema for the client
#      library and for this repository);
#   3. (unless --verify-only) the tree's own scripts/fetch-deps.sh fetches and verifies Aeron, sbe-tool, agrona and
#      the core feed harness jar against the tree's lock (the archive follow library's build, WITH_BRIDGE, refuses to
#      configure without the harness the tree pins), and the three wire schemas are downloaded at their pinned commits
#      and verified: the money schema against this lock, the match and ME journal schemas against the tree's lock.
#
# Usage: scripts/fetch-cluster-tools.sh <cache-dir> [--verify-only]
# Prints the cmake arguments for the cluster-tools build on stdout, one per line:
#   -DCLUSTER_TOOLS_SOURCE_DIR=<cache>/cluster-tools-<ref>   (not a cluster-tools option: build-cluster-tools.sh reads it)
#   -DCT_BUILD_SOURCE_SHA=<ref> -DAERON_TARBALL=... -DSBE_CLASSPATH=... -DMONEY_SCHEMA=... -DMATCH_SCHEMA=...
#   -DME_JOURNAL_SCHEMA=... -DCT_FEED_HARNESS_JAR=...
#
# Env: GW_LOCK         the lock to read (default: dependencies.lock next to this script's parent)
#      FETCH_OFFLINE=1 never download: verify what is in the cache only
#      CT_ARTIFACT_BASE_URL  where the feed harness jar is downloaded from (default: the releases of the core repository
#                      this lock names as money_schema_repo, https://github.com/<money_schema_repo>)
#      CT_FEED_HARNESS_JAR   a local harness jar, accepted only when its sha256 is the one the tree's lock pins
set -euo pipefail

usage() { echo "usage: $0 <cache-dir> [--verify-only]" >&2; exit 64; }
[ $# -ge 1 ] && [ $# -le 2 ] || usage
VERIFY_ONLY=0
if [ $# -eq 2 ]; then
    [ "$2" = "--verify-only" ] || usage
    VERIFY_ONLY=1
fi

HERE=$(cd "$(dirname "$0")/.." && pwd)
LOCK=${GW_LOCK:-$HERE/dependencies.lock}
[ -f "$LOCK" ] || { echo "FATAL: no lock at $LOCK" >&2; exit 1; }
CACHE=$(mkdir -p "$1" && cd "$1" && pwd)

lockOf() { # file key
    python3 -I -c 'import json,sys; v=json.load(open(sys.argv[1]))[sys.argv[2]]; print(v)' "$1" "$2"
}
lock() { lockOf "$LOCK" "$1"; }

fetch_verified() { # url expected-sha256 destination
    local url=$1 want=$2 dest=$3
    if [ ! -f "$dest" ]; then
        if [ "${FETCH_OFFLINE:-0}" = "1" ]; then
            echo "FATAL: $dest missing and FETCH_OFFLINE=1" >&2
            exit 1
        fi
        curl -sfL --retry 3 "$url" -o "$dest.tmp"
        mv "$dest.tmp" "$dest"
    fi
    local got
    got=$(sha256sum "$dest" | cut -d' ' -f1)
    if [ "$got" != "$want" ]; then
        echo "FATAL: sha256 mismatch for $dest ($url)" >&2
        echo "  the lock pins $want" >&2
        echo "  the file is   $got" >&2
        exit 1
    fi
    echo "verified $(basename "$dest") $got" >&2
}

REF=$(lock cluster_tools_ref)
TARBALL_SHA=$(lock cluster_tools_tarball_sha256)
TARBALL_URL=$(lock cluster_tools_tarball_url)
MONEY_REF=$(lock money_schema_ref)
MONEY_SHA=$(lock money_schema_sha256)

[[ "$REF" =~ ^[0-9a-f]{40}$ ]] || { echo "FATAL: cluster_tools_ref must be a 40-hex commit sha, got '$REF'" >&2; exit 1; }
[[ "$TARBALL_SHA" =~ ^[0-9a-f]{64}$ ]] || { echo "FATAL: cluster_tools_tarball_sha256 must be 64 hex, got '$TARBALL_SHA'" >&2; exit 1; }
[[ "$MONEY_REF" =~ ^[0-9a-f]{40}$ ]] || { echo "FATAL: money_schema_ref must be a 40-hex commit sha, got '$MONEY_REF'" >&2; exit 1; }
[[ "$MONEY_SHA" =~ ^[0-9a-f]{64}$ ]] || { echo "FATAL: money_schema_sha256 must be 64 hex, got '$MONEY_SHA'" >&2; exit 1; }
case "$TARBALL_URL" in
    *"$REF"*) ;;
    *) echo "FATAL: cluster_tools_tarball_url does not name cluster_tools_ref $REF: $TARBALL_URL" >&2; exit 1 ;;
esac

# 1. the tarball, by sha256, then its layout
TARBALL="$CACHE/cluster-tools-$REF.tar.gz"
fetch_verified "$TARBALL_URL" "$TARBALL_SHA" "$TARBALL"
SRC="$CACHE/cluster-tools-$REF"
STAGE="$CACHE/.extract-$REF"
rm -rf "$STAGE" "$SRC"
mkdir -p "$STAGE"
tar -xzf "$TARBALL" -C "$STAGE" --no-same-owner
TOP=$(find "$STAGE" -mindepth 1 -maxdepth 1 -printf '%f\n')
if [ "$TOP" != "cluster-tools-$REF" ]; then
    rm -rf "$STAGE"
    echo "FATAL: the tarball's top directory is '$TOP', expected exactly cluster-tools-$REF" >&2
    exit 1
fi
mv "$STAGE/$TOP" "$SRC"
rmdir "$STAGE"

# 2. one money schema for the client library and for this repository
CT_LOCK="$SRC/dependencies.lock"
CT_MONEY_REF=$(lockOf "$CT_LOCK" money_schema_ref)
CT_MONEY_SHA=$(lockOf "$CT_LOCK" money_schema_sha256)
if [ "$CT_MONEY_REF" != "$MONEY_REF" ] || [ "$CT_MONEY_SHA" != "$MONEY_SHA" ]; then
    echo "FATAL: cluster-tools $REF pins the money schema at $CT_MONEY_REF ($CT_MONEY_SHA)," >&2
    echo "  this lock pins $MONEY_REF ($MONEY_SHA): re-pin with scripts/repin-cluster-tools.sh" >&2
    exit 1
fi
if [ "$(lockOf "$CT_LOCK" money_schema_id)" != "$(lock money_schema_id)" ]; then
    echo "FATAL: money_schema_id differs between this lock and cluster-tools $REF" >&2
    exit 1
fi
echo "verified cluster-tools $REF (money schema $MONEY_REF $MONEY_SHA)" >&2

echo "-DCLUSTER_TOOLS_SOURCE_DIR=$SRC"
echo "-DCT_BUILD_SOURCE_SHA=$REF"
if [ "$VERIFY_ONLY" = "1" ]; then
    exit 0
fi

# 3. the tree's own third-party inputs (its script verifies them against its lock), then the wire schemas
CT_ARTIFACT_BASE_URL=${CT_ARTIFACT_BASE_URL:-https://github.com/$(lock money_schema_repo)} \
    bash "$SRC/scripts/fetch-deps.sh" "$CACHE/ct-deps" --harness

raw() { # repo ref path
    echo "https://raw.githubusercontent.com/$1/$2/$3"
}
SCHEMAS="$CACHE/schemas-$REF"
mkdir -p "$SCHEMAS"
fetch_verified "$(raw "$(lock money_schema_repo)" "$MONEY_REF" "$(lock money_schema_path)")" "$MONEY_SHA" \
    "$SCHEMAS/money-schema-$MONEY_SHA.xml"
MATCH_SHA=$(lockOf "$CT_LOCK" match_schema_sha256)
fetch_verified "$(raw "$(lockOf "$CT_LOCK" match_schema_repo)" "$(lockOf "$CT_LOCK" match_schema_ref)" \
    "$(lockOf "$CT_LOCK" match_schema_path)")" "$MATCH_SHA" "$SCHEMAS/order-schema-$MATCH_SHA.xml"
JOURNAL_SHA=$(lockOf "$CT_LOCK" me_journal_schema_sha256)
fetch_verified "$(raw "$(lockOf "$CT_LOCK" me_journal_schema_repo)" "$(lockOf "$CT_LOCK" me_journal_schema_ref)" \
    "$(lockOf "$CT_LOCK" me_journal_schema_path)")" "$JOURNAL_SHA" "$SCHEMAS/me-journal-schema-$JOURNAL_SHA.xml"
echo "-DMONEY_SCHEMA=$SCHEMAS/money-schema-$MONEY_SHA.xml"
echo "-DMATCH_SCHEMA=$SCHEMAS/order-schema-$MATCH_SHA.xml"
echo "-DME_JOURNAL_SCHEMA=$SCHEMAS/me-journal-schema-$JOURNAL_SHA.xml"
