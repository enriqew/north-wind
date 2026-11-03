#!/usr/bin/env bash
# Bundle what the live machine needs and upload it: the committed code, the built job, and the masks.
#
#   scripts/aws/package.sh <bucket> <masks-window>
#
# A live run has no window of its own to build masks from, so ports and coverage come from an
# earlier window, outages start empty, and the coast is the whole coast (`northwind.shore --all`).
# Packs HEAD, not the working tree: commit first.
set -euo pipefail
export MSYS_NO_PATHCONV=1

bucket=$1 masks_from=$2
cd "$(dirname "$0")/../.."
[ -z "$(git status --porcelain)" ] || { echo "uncommitted changes: commit before packaging"; exit 1; }
[ -f flink/target/north-wind.jar ] || { echo "build the job first"; exit 1; }
data=$(sed -n 's/^NW_DATA_DIR=//p' .env)

live="$data/live-masks"
mkdir -p "$live"
cp "$data/windows/$masks_from/masks/ports.csv" "$data/windows/$masks_from/masks/coverage.csv" "$live/"
echo "from_ms,to_ms" > "$live/outages.csv"
[ -f "$live/shore.csv" ] && [ -f "$live/farms.csv" ] || \
    docker compose run --rm --no-deps -T tools python -m northwind.shore --all --out /data/live-masks

# A mixed-style path (drive letter, forward slashes) works for git, tar and the AWS CLI alike on Windows, and is a no-op elsewhere.
work=$(mktemp -d)
work=$(cygpath -m "$work" 2>/dev/null || echo "$work")
git archive --format=tar --output="$work/north-wind.tar" HEAD
tar --force-local -rf "$work/north-wind.tar" flink/target/north-wind.jar
tar --force-local -rf "$work/north-wind.tar" -C "$data" live-masks
gzip "$work/north-wind.tar"
aws s3 cp "$(cygpath -w "$work/north-wind.tar.gz" 2>/dev/null || echo "$work/north-wind.tar.gz")" \
    "s3://$bucket/package/north-wind.tar.gz" --only-show-errors
echo "uploaded $(git rev-parse --short HEAD) with masks from $masks_from: $(du -h "$work/north-wind.tar.gz" | cut -f1)"
rm -rf "$work"
