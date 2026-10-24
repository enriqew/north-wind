#!/usr/bin/env bash
# Start a live run on this machine: record the feed and run the job on it as it arrives.
#
#   scripts/aws/live-start.sh <name> <window-start> <window-end>
#
# The ingest and the recorder start now; the job reads ais.raw from its first offset and keeps only
# what arrives inside [window-start, window-end), so starting early costs nothing. Masks come from
# $NW_DATA_DIR/live-masks (ports and coverage from an earlier window, the whole coast, the farms),
# because a live run has no window of its own to build them from yet.
set -euo pipefail
export MSYS_NO_PATHCONV=1

name=$1 start=$2 end=$3
cd "$(dirname "$0")/../.."

docker compose up -d kafka topics ingest recorder
docker compose --profile flink up -d jobmanager taskmanager-1 taskmanager-2

slots() { curl -s localhost:8081/taskmanagers | grep -o '"slotsNumber"' | wc -l; }
until [ "$(slots)" -ge 2 ]; do sleep 2; done

docker compose exec -T jobmanager flink run -d /opt/flink/usrlib/north-wind.jar \
    --live --window "$name" --topic ais.raw --start "$start" --end "$end" \
    --masks /data/live-masks --lateness-bound-s 120 --out "/data/runs/$name-live"

docker compose --profile tools run -d --no-deps --name north-wind-sampler tools \
    python -m northwind.sampler --out "/data/runs/$name-live/metrics.jsonl"

echo "live run $name started at $(date -u +%FT%TZ) for [$start, $end)"
