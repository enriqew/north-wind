"""Samples a running job's health, for the record of a live run.

Every `--every-s` seconds, one JSON line: wall-clock time, the job's state, the latest completed
checkpoint (state size and how long it took), how far the Kafka source is behind the topic in
records, and how long ago the message it last emitted arrived. Nothing here feeds the job or the
checks; it is the measurement a live run is reported with.

    python -m northwind.sampler --out /data/runs/d1-live/metrics.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from pathlib import Path

import requests

log = logging.getLogger("northwind.sampler")

FLINK = os.environ.get("NW_FLINK", "http://jobmanager:8081")

_stop = False


def _on_signal(*_: object) -> None:
    global _stop
    _stop = True


def running_job() -> str | None:
    jobs = requests.get(f"{FLINK}/jobs", timeout=10).json()["jobs"]
    ids = [j["id"] for j in jobs if j["status"] == "RUNNING"]
    return ids[0] if ids else None


def source_metrics(job: str) -> dict:
    """Lag in records and in time (now minus the arrival of the last record emitted) at the source."""
    vertices = requests.get(f"{FLINK}/jobs/{job}", timeout=10).json()["vertices"]
    source = next(v["id"] for v in vertices if v["name"].startswith("Source"))
    available = requests.get(f"{FLINK}/jobs/{job}/vertices/{source}/metrics", timeout=10).json()
    wanted = {m["id"] for m in available
              if m["id"].endswith(("records-lag-max", "currentEmitEventTimeLag", "Source__replay.numRecordsOut"))}
    if not wanted:
        return {}
    values = requests.get(f"{FLINK}/jobs/{job}/vertices/{source}/metrics",
                          params={"get": ",".join(sorted(wanted))}, timeout=10).json()
    out = {}
    for m in values:
        key = m["id"].rsplit(".", 1)[-1]
        out[{"records-lag-max": "lag_records", "currentEmitEventTimeLag": "lag_ms",
             "numRecordsOut": "records_read"}[key]] = float(m["value"])
    return out


def sample() -> dict:
    row: dict = {"at_ms": int(time.time() * 1000)}
    job = running_job()
    row["job"] = job
    if job is None:
        return row
    cp = requests.get(f"{FLINK}/jobs/{job}/checkpoints", timeout=10).json()
    latest = (cp.get("latest") or {}).get("completed")
    row["checkpoints_completed"] = cp["counts"]["completed"]
    row["checkpoints_failed"] = cp["counts"]["failed"]
    if latest:
        row["state_bytes"] = latest["state_size"]
        row["checkpoint_ms"] = latest["end_to_end_duration"]
    row.update(source_metrics(job))
    return row


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--every-s", type=int, default=300)
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    while not _stop:
        try:
            row = sample()
        except requests.RequestException as e:  # Flink restarting: record the miss, keep going
            row = {"at_ms": int(time.time() * 1000), "error": type(e).__name__}
        with out.open("a") as f:
            f.write(json.dumps(row) + "\n")
        for _ in range(args.every_s):
            if _stop:
                break
            time.sleep(1)


if __name__ == "__main__":
    main()
