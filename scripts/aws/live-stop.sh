#!/usr/bin/env bash
# End a live run and keep everything: drain the job, close the recording, copy both to S3.
#
#   scripts/aws/live-stop.sh <name>
#
# The job is stopped with --drain, which ends it the way a bounded replay ends, so its output can be
# compared with the replay of the same window. The ingest stops before the recorder, and the
# recording is checked to end at the topic's last offset before anything is copied. Needs
# NW_BUCKET (the results bucket) in the environment.
set -euo pipefail
export MSYS_NO_PATHCONV=1

name=$1
cd "$(dirname "$0")/../.."
data=$(sed -n 's/^NW_DATA_DIR=//p' .env)
out="$data/runs/$name-live"

job=$(curl -s localhost:8081/jobs | python3 -c 'import sys,json; print(next(j["id"] for j in json.load(sys.stdin)["jobs"] if j["status"]=="RUNNING"))')
docker compose exec -T jobmanager flink stop --drain --savepointPath "file:///checkpoints/sp-$name" "$job"
docker stop north-wind-sampler >/dev/null

docker compose stop ingest
end=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ais.raw | cut -d: -f3)
sleep 30
docker compose stop -t 120 recorder
last=$(find "$data/recordings/live/raw" -name 'part-*.parquet' | sed -E 's/.*part-[0-9]+-0*([0-9]+)\.parquet/\1/' | sort -n | tail -1)
echo "ais.raw ends at offset $((end - 1)); the recording ends at $last"
[ "$last" -eq "$((end - 1))" ] || { echo "the recording is short of the topic: not stopping Kafka"; exit 1; }

# Every profile, or Flink's logs are left out (the dry run lost them this way).
docker compose --profile flink --profile tools logs --no-color > "$out/compose.log" 2>&1
docker logs north-wind-sampler > "$out/sampler.log" 2>&1 || true
aws s3 sync "$data/recordings/live" "s3://$NW_BUCKET/recordings/live" --only-show-errors
aws s3 sync "$out" "s3://$NW_BUCKET/runs/$name-live" --only-show-errors
docker compose --profile flink stop
docker compose stop kafka
echo "live run $name stopped at $(date -u +%FT%TZ), copied to s3://$NW_BUCKET"
