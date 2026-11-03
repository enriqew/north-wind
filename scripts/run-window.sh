#!/usr/bin/env bash
# Everything for one window, from the recording to the checks.
#
#   scripts/run-window.sh <window> <start> <end> [bound-s]
#
#   replay   the window into replay.<window>, with its manifest
#   masks    port areas, coverage and outages from the window
#   shore    the coast near where vessels sat still (needs the Dybdedata download, see shore.py)
#   installations  offshore installations from Sodir's register (see installations.py)
#   profile  rate, mix, duplicates and lateness
#   run      the job, then the conservation check
#   rerun    the job again, compared with the first run (determinism)
#   recovery a run with a TaskManager killed, compared with the first run and with its topic
#
# Stops at the first failure. Runs are named <window>-b<bound>, -rerun and -killed.
set -euo pipefail
export MSYS_NO_PATHCONV=1

window=$1 start=$2 end=$3 bound=${4:-120}
run="$window-b$bound"
cd "$(dirname "$0")/.."

tools() { docker compose run --rm --no-deps tools python -m "$@"; }
job() {
    docker compose exec -T jobmanager flink run /opt/flink/usrlib/north-wind.jar \
        --window "$window" --lateness-bound-s "$bound" --out "/data/runs/$1" \
        | grep -E "has finished|Job Runtime"
}

echo "== replay $window [$start, $end)"
tools northwind.replay --name "$window" --start "$start" --end "$end"
echo "== masks"
tools northwind.masks --name "$window"
echo "== shore"
tools northwind.shore --name "$window"
echo "== installations"
tools northwind.installations --name "$window"
echo "== profile"
tools northwind.profile --name "$window" > /dev/null
echo "== run $run"
job "$run"
tools northwind.check --run "$run" --window "$window" > /dev/null
echo "conservation holds"
echo "== rerun"
job "$run-rerun"
tools northwind.compare --runs "$run" "$run-rerun" --out "runs/$run/rerun.json" > /dev/null
echo "rerun identical"
echo "== recovery"
scripts/recovery-test.sh "$window" "$run" "$run-killed" "$bound" > /dev/null
echo "recovery identical, topic exactly once"
