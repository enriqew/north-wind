import json
from collections import Counter

import pytest

from northwind import export_live
from northwind.export_live import Refused


def line(**fields):
    base = {"pattern": "rendezvous", "mmsi": 1, "startMs": 0, "endMs": 60_000, "lat": 69.6, "lon": 18.9}
    return json.dumps(base | fields, separators=(",", ":"))


def test_quantile_is_nearest_rank():
    values = list(range(1, 21))
    assert export_live.quantile(values, 0.5) == 10
    assert export_live.quantile(values, 0.95) == 19
    assert export_live.quantile([7], 0.95) == 7


def test_health_summarises_the_sampler():
    samples = [
        {"at_ms": 0, "checkpoints_completed": 0, "checkpoints_failed": 0},
        {"at_ms": 300_000, "checkpoints_completed": 9, "checkpoints_failed": 1, "state_bytes": 10, "checkpoint_ms": 60},
        {"at_ms": 600_000, "checkpoints_completed": 19, "checkpoints_failed": 1, "state_bytes": 30,
         "checkpoint_ms": 40, "lag_records": 0.0, "records_read": 100.0, "lag_ms": 90.0},
        {"at_ms": 900_000, "checkpoints_completed": 29, "checkpoints_failed": 1, "state_bytes": 20,
         "checkpoint_ms": 50, "lag_records": 0.0, "records_read": 250.0, "lag_ms": 120.0},
    ]
    h = export_live.health(samples)
    assert h["samples"] == 4 and h["every_s"] == 300
    assert h["records_read"] == 250 and h["lag_records_max"] == 0
    assert h["lag_ms"] == {"p50": 90.0, "p95": 120.0, "max": 120.0}
    assert h["state_bytes"] == {"p50": 20, "max": 30}
    assert h["checkpoints"]["completed"] == 29 and h["checkpoints"]["failed"] == 1
    assert h["checkpoints"]["failed_at_ms"] == [300_000]
    assert h["series"]["rows"][0] == [0, None, None]


def test_health_refuses_a_job_that_never_read():
    with pytest.raises(Refused):
        export_live.health([{"at_ms": 0, "checkpoints_completed": 0, "checkpoints_failed": 0}])


LOG = [
    "ingest-1  | 2026-10-27 23:00:53,278 INFO session 1455112c open ",
    "kafka-1   | [2026-10-27 23:00:54,000] INFO something else",
    "ingest-1  | 2026-10-27 23:58:52,381 INFO session 1455112c received 600000",
    "ingest-1  | 2026-10-27 23:58:52,381 INFO session 1455112c close {'reason': 'token_refresh', 'received': 687930}",
    "ingest-1  | 2026-10-27 23:58:52,642 INFO session 2d96f79b open ",
    "ingest-1  | 2026-10-28 17:56:05,788 INFO session 2d96f79b close {'reason': 'error: ChunkedEncodingError: Response ended prematurely', 'received': 464943}",
    "ingest-1  | 2026-10-28 17:56:06,947 INFO session 69d73fab open ",
    "ingest-1  | 2026-10-29 01:00:01,000 INFO session 69d73fab close {'reason': 'stopped', 'received': 5}",
    "jobmanager-1     | 2026-10-27 14:48:49,044 INFO  org.apache.flink.runtime.entrypoint.ClusterEntrypoint        [] -  Arch: aarch64",
    "taskmanager-1-1  | 2026-10-27 14:48:49,302 INFO  org.apache.flink.runtime.taskexecutor.TaskManagerRunner      [] -  Arch: aarch64",
]


def test_feed_reads_why_each_session_closed_and_the_gap_after():
    f = export_live.feed(LOG)
    assert f["sessions"] == 3 and f["token_refreshes"] == 1
    assert f["token_refresh_gap_s_max"] == pytest.approx(0.261)
    [error] = f["errors"]
    assert error["reason"] == "ChunkedEncodingError: Response ended prematurely"
    assert error["back_after_s"] == pytest.approx(1.159)
    assert error["at_ms"] == export_live.log_ms("2026-10-28 17:56:05,788")


def test_feed_refuses_a_session_that_never_reopened():
    with pytest.raises(Refused):
        export_live.feed(LOG[:6])


def test_cpu_is_the_one_flink_reported():
    assert export_live.cpu(LOG) == "aarch64"
    with pytest.raises(Refused):
        export_live.cpu(LOG + ["taskmanager-2-1  | 2026 INFO x [] -  Arch: amd64"])


def test_differences_pair_events_that_differ_in_the_last_digit_of_a_distance():
    same = line(mmsi=9, minDistanceM=12.5)
    live = Counter([same, line(minDistanceM=0.38111385966453193)])
    replay = Counter([same, line(minDistanceM=0.3811138596645318)])
    d = export_live.differences(live, replay)
    assert d["events"] == 1 and d["by_field"] == {"minDistanceM": 1}
    assert d["max_ulps"] == 2
    assert d["largest"]["first"] == "0.38111385966453193" and d["largest"]["second"] == "0.3811138596645318"
    assert "mmsi" not in json.dumps(d)


def test_differences_refuse_an_event_that_differs_in_anything_else():
    with pytest.raises(Refused, match="more than a distance"):
        export_live.differences(Counter([line(endMs=1)]), Counter([line(endMs=2)]))


def test_differences_refuse_a_distance_that_differs_by_more_than_rounding():
    with pytest.raises(Refused, match="more than rounding"):
        export_live.differences(Counter([line(distanceKm=1.0)]), Counter([line(distanceKm=1.0001)]))


def test_identical_runs_have_no_differences():
    runs = Counter([line()])
    assert export_live.differences(runs, runs)["events"] == 0
