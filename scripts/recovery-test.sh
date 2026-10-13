#!/usr/bin/env bash
# Kill a TaskManager in the middle of a run and check the run still matches a clean one.
#
#   scripts/recovery-test.sh <window> <clean-run> <new-run> [bound-s]
#
# The clean run must already exist (same window, same bound). The new run publishes its detections
# to Kafka exactly once; after it finishes, its files are compared with the clean run's and the
# committed topic with its own files. Writes /data/runs/<new-run>/recovery.json via the compare.
set -euo pipefail
export MSYS_NO_PATHCONV=1

window=$1 clean=$2 run=$3 bound=${4:-120}
topic="events.$run"
flink=http://localhost:8081
cd "$(dirname "$0")/.."
data=$(sed -n 's/^NW_DATA_DIR=//p' .env 2>/dev/null || true)
data=${data:-./data}

docker compose run --rm --no-deps tools python -m northwind.compare --runs "$clean" --topic "$topic" --create-topic

jid=$(docker compose exec -T jobmanager flink run -d /opt/flink/usrlib/north-wind.jar \
    --topic "replay.$window" --masks "/data/windows/$window/masks" --out "/data/runs/$run" \
    --lateness-bound-s "$bound" --checkpoint-interval-s 1 --events-topic "$topic" \
    | sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p')
echo "job $jid"

completed() { curl -s "$flink/jobs/$jid/checkpoints" | python -c 'import sys,json; print(json.load(sys.stdin)["counts"]["completed"])'; }
state() { curl -s "$flink/jobs/$jid" | python -c 'import sys,json; print(json.load(sys.stdin)["state"])'; }

until [ "$(completed)" -ge 2 ]; do
    [ "$(state)" = FINISHED ] && { echo "job finished before the kill: use a longer window"; exit 1; }
    sleep 0.2
done
echo "$(completed) checkpoints completed; killing taskmanager-2 at $(date -u +%H:%M:%SZ)"
docker compose kill taskmanager-2 >/dev/null
sleep 15
docker compose --profile flink up -d taskmanager-2 >/dev/null

until [[ "$(state)" =~ ^(FINISHED|FAILED|CANCELED)$ ]]; do sleep 2; done
final=$(state)
restarts=$(curl -s "$flink/jobs/$jid/exceptions" | python -c 'import sys,json; print(len(json.load(sys.stdin).get("exceptionHistory",{}).get("entries",[])))')
echo "job $final after $restarts failure(s)"
[ "$final" = FINISHED ] || exit 1

docker compose run --rm --no-deps tools python -m northwind.compare --runs "$clean" "$run" \
    | tee "$data/runs/$run/recovery-vs-clean.json"
docker compose run --rm --no-deps tools python -m northwind.compare --runs "$run" --topic "$topic" \
    | tee "$data/runs/$run/recovery-vs-kafka.json"
