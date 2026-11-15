#!/bin/bash
# Re-pins cluster-tools: one command, and the only file it changes is dependencies.lock.
#
#   scripts/repin-cluster-tools.sh <40-hex cluster-tools commit sha>
#
# Downloads the source tarball of that commit, computes its sha256, reads the money schema pin from the tree's own
# dependencies.lock, downloads that schema at its commit, checks its sha256 against the tree's lock and reads its
# version, then rewrites the cluster_tools_* and money_schema_* entries of this repository's lock. Nothing is taken
# from a branch tip: the argument is a full commit sha. Run scripts/fetch-cluster-tools.sh afterwards (CI does) to
# verify the new pin end to end; review the diff it prints before committing.
set -euo pipefail

[ $# -eq 1 ] || { echo "usage: $0 <40-hex cluster-tools commit sha>" >&2; exit 64; }
REF=$1
[[ "$REF" =~ ^[0-9a-f]{40}$ ]] || { echo "FATAL: give the full 40-hex commit sha, got '$REF'" >&2; exit 64; }

HERE=$(cd "$(dirname "$0")/.." && pwd)
LOCK=${GW_LOCK:-$HERE/dependencies.lock}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

REPO=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["cluster_tools_repo"])' "$LOCK")
URL="https://codeload.github.com/$REPO/tar.gz/$REF"
curl -sfL --retry 3 "$URL" -o "$WORK/ct.tar.gz"
TARBALL_SHA=$(sha256sum "$WORK/ct.tar.gz" | cut -d' ' -f1)
mkdir "$WORK/x"
tar -xzf "$WORK/ct.tar.gz" -C "$WORK/x" --no-same-owner "cluster-tools-$REF/dependencies.lock"
CT_LOCK="$WORK/x/cluster-tools-$REF/dependencies.lock"

read_ct() { python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$CT_LOCK" "$1"; }
MONEY_REPO=$(read_ct money_schema_repo)
MONEY_REF=$(read_ct money_schema_ref)
MONEY_PATH=$(read_ct money_schema_path)
MONEY_SHA=$(read_ct money_schema_sha256)
MONEY_ID=$(read_ct money_schema_id)
curl -sfL --retry 3 "https://raw.githubusercontent.com/$MONEY_REPO/$MONEY_REF/$MONEY_PATH" -o "$WORK/money.xml"
GOT=$(sha256sum "$WORK/money.xml" | cut -d' ' -f1)
[ "$GOT" = "$MONEY_SHA" ] || { echo "FATAL: money schema at $MONEY_REF is $GOT, cluster-tools $REF pins $MONEY_SHA" >&2; exit 1; }
MONEY_VERSION=$(python3 -I -c '
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
print(root.attrib["version"])' "$WORK/money.xml")

cp "$LOCK" "$WORK/lock.before"
python3 -I - "$LOCK" "$REF" "$URL" "$TARBALL_SHA" "$MONEY_REPO" "$MONEY_REF" "$MONEY_PATH" "$MONEY_SHA" "$MONEY_ID" \
    "$MONEY_VERSION" <<'PY'
import json, sys
path, ref, url, tsha, mrepo, mref, mpath, msha, mid, mver = sys.argv[1:]
lock = json.load(open(path))
lock.update({
    "cluster_tools_ref": ref,
    "cluster_tools_tarball_url": url,
    "cluster_tools_tarball_sha256": tsha,
    "money_schema_repo": mrepo,
    "money_schema_ref": mref,
    "money_schema_path": mpath,
    "money_schema_sha256": msha,
    "money_schema_id": int(mid),
    "money_schema_version": int(mver),
})
with open(path, "w") as f:
    json.dump(lock, f, indent=2)
    f.write("\n")
PY
diff -u "$WORK/lock.before" "$LOCK" || true
echo "re-pinned cluster-tools to $REF (tarball $TARBALL_SHA, money schema v$MONEY_VERSION at $MONEY_REF)" >&2
