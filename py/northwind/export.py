"""Export one checked run as the static JSON the portfolio page reads. See docs/EXPORT.md.

    python -m northwind.export --window w1 --run w1-b120

Writes /data/exports/<run>/ only when every check holds, and never writes an MMSI, a name or an
unrounded position.
"""

from __future__ import annotations

import argparse
import json
import shutil
from collections import Counter, defaultdict
from pathlib import Path

from northwind.check import read_jsonl
from northwind.recording import DATA, Manifest, connect, raw_glob

SCHEMA_VERSION = 1
POSITION_DECIMALS = 2

ATTRIBUTION = [
    "Contains data under the Norwegian licence for Open Government Data (NLOD) distributed by the "
    "Norwegian Coastal Administration.",
    "Data delivered by BarentsWatch.",
]

# Mirrors the constants in the Flink job (GapLogic, StopLogic, RendezvousLogic, TrackFilter).
THRESHOLDS = {
    "ais_gap": {"min_silence_min": 60, "underway_kn": 1.0},
    "port_call": {"stop_kn": 0.5, "dwell_min": 30},
    "loitering": {"max_kn": 1.5, "min_duration_min": 120, "max_silence_min": 60, "hold_radius_m": 1000, "hold_share": 0.9},
    "rendezvous": {"radius_m": 500, "max_kn": 1.0, "min_duration_min": 30, "separation_min": 15},
    "approached": {"moving_kn": 1.0, "within_min": 60},
    "track_filter": {"max_kn": 60, "min_jump_m": 2000},
}

LATENESS_EDGES_S = [0, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200]


class Refused(Exception):
    pass


def ship_category(code: int | None) -> str:
    """AIS ship type code to a category coarse enough to say nothing about a single vessel."""
    if code is None or code == 0:
        return "unknown"
    if code == 30:
        return "fishing"
    if code in (31, 32, 33, 34, 35, 50, 51, 52, 53, 54, 55, 58):
        return "service"
    if code in (36, 37):
        return "leisure"
    if 40 <= code <= 49:
        return "high_speed"
    if 60 <= code <= 69:
        return "passenger"
    if 70 <= code <= 79:
        return "cargo"
    if 80 <= code <= 89:
        return "tanker"
    return "other"


def read_json(path: Path) -> dict:
    if not path.exists():
        raise Refused(f"missing {path.relative_to(DATA)}")
    return json.loads(path.read_text())


def checks(window: Manifest, run: str) -> dict:
    runs = DATA / "runs"
    summary = read_json(runs / run / "summary.json")
    rerun = read_json(runs / run / "rerun.json")
    vs_clean = read_json(runs / f"{run}-killed" / "recovery-vs-clean.json")
    vs_kafka = read_json(runs / f"{run}-killed" / "recovery-vs-kafka.json")

    if summary["window"]["sha256"] != window.sha256:
        raise Refused("the run's summary names another window")
    if rerun["runs"] != [run, f"{run}-rerun"] or vs_clean["runs"] != [run, f"{run}-killed"]:
        raise Refused("a comparison report is about other runs")
    if not summary["conservation"]["holds"]:
        raise Refused("conservation does not hold: " + "; ".join(summary["conservation"]["failures"]))
    if not rerun["identical"]:
        raise Refused("a rerun of the window gave different output")
    if not vs_clean["identical"]:
        raise Refused("the run with a TaskManager killed differs from the clean run")
    kafka = vs_kafka.get("kafka", {})
    if not vs_kafka["identical"] or not kafka.get("identical_to_files") or kafka.get("duplicates_in_topic"):
        raise Refused("the committed topic is not exactly the run's detections")
    return {
        "conservation": summary["conservation"],
        "determinism": {"identical": True, "runs": rerun["runs"]},
        "recovery": {"identical": True, "runs": vs_clean["runs"]},
        "exactly_once": {
            "committed_records": kafka["committed_records"],
            "duplicates_in_topic": kafka["duplicates_in_topic"],
        },
    }


def anonymise(events: list[dict]) -> list[dict]:
    """Replace MMSIs with ids local to this export, in order of first detection."""
    order = sorted(events, key=lambda e: (e["startMs"], e["pattern"], e["mmsi"], e.get("otherMmsi") or 0))
    ids: dict[int, str] = {}

    def vid(mmsi: int) -> str:
        if mmsi not in ids:
            ids[mmsi] = f"v{len(ids) + 1}"
        return ids[mmsi]

    out = []
    for e in order:
        row = {
            "pattern": e["pattern"],
            "vessel": vid(e["mmsi"]),
            "category": ship_category(e.get("shipType")),
            "start_ms": e["startMs"],
            "end_ms": e["endMs"],
            "lat": round(e["lat"], POSITION_DECIMALS),
            "lon": round(e["lon"], POSITION_DECIMALS),
            "truncated": e.get("truncated", False),
        }
        if e.get("endLat") is not None:
            row["end_lat"] = round(e["endLat"], POSITION_DECIMALS)
            row["end_lon"] = round(e["endLon"], POSITION_DECIMALS)
        if e.get("otherMmsi") is not None:
            row["other_vessel"] = vid(e["otherMmsi"])
            row["other_category"] = ship_category(e.get("otherShipType"))
        for src, dst, digits in [
            ("distanceKm", "distance_km", 1), ("impliedKnots", "implied_kn", 1),
            ("minDistanceM", "min_distance_m", 0), ("maxDriftKm", "max_drift_km", 1),
        ]:
            if e.get(src) is not None:
                row[dst] = round(e[src], digits)
        for key in ("underway", "coveredStart", "coveredEnd", "outageOverlap", "approached", "heldPosition", "portId", "navStatusMode"):
            if e.get(key) is not None:
                row[key] = e[key]
        out.append(row)
    return out


def funnels(events: list[dict]) -> dict:
    by: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        by[e["pattern"]].append(e)
    out: dict[str, list[dict]] = {}
    gaps = by.get("ais_gap", [])
    steps = [("silent over an hour, then back", lambda e: True),
             ("underway when it went quiet", lambda e: e.get("underway")),
             ("both ends in continuous coverage", lambda e: e.get("coveredStart") and e.get("coveredEnd")),
             ("no ingest outage during it", lambda e: not e.get("outageOverlap"))]
    out["ais_gap"] = funnel(gaps, steps)
    out["rendezvous"] = funnel(by.get("rendezvous", []), [
        ("together 30 min or more", lambda e: True),
        ("did not both hold position (not moored or at anchor together)", lambda e: e.get("heldPosition") is False),
        ("a vessel arrived underway", lambda e: e.get("approached")),
        ("ended inside the window", lambda e: not e.get("truncated")),
    ])
    out["loitering"] = funnel(by.get("loitering", []), [
        ("slow outside port 2 h or more", lambda e: True),
        ("did not hold position (not moored or at anchor)", lambda e: e.get("heldPosition") is False),
        ("arrived underway", lambda e: e.get("approached")),
        ("ended inside the window", lambda e: not e.get("truncated")),
    ])
    out["port_call"] = funnel(by.get("port_call", []), [
        ("stopped in a port area 30 min or more", lambda e: True),
        ("left inside the window", lambda e: not e.get("truncated")),
    ])
    return out


def funnel(rows: list[dict], steps) -> list[dict]:
    out = []
    for label, keep in steps:
        rows = [r for r in rows if keep(r)]
        out.append({"step": label, "events": len(rows)})
    return out


def lateness(window: Manifest) -> dict:
    profile = read_json(DATA / "windows" / window.name / "profile.json")
    edges = ", ".join(str(e) for e in LATENESS_EDGES_S)
    rows = connect().execute(
        f"""
        SELECT stream, bin, count(*) FROM (
            SELECT json_extract_string(raw, '$.stream') AS stream,
                   list_position([{edges}]::INT[],
                       list_max(list_filter([{edges}]::INT[], x -> x <= greatest(0, (arrival_ms - epoch_ms(
                           TRY_CAST(json_extract_string(raw, '$.msgtime') AS TIMESTAMPTZ))) / 1000)))) AS bin
            FROM read_parquet('{raw_glob()}')
            WHERE arrival_ms >= {window.start_ms} AND arrival_ms < {window.end_ms}
        ) WHERE bin IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2
        """
    ).fetchall()
    histogram: dict[str, list[int]] = defaultdict(lambda: [0] * len(LATENESS_EDGES_S))
    for stream, b, n in rows:
        histogram[stream][b - 1] = n
    return {
        "bin_lower_edges_s": LATENESS_EDGES_S,
        "histogram": dict(histogram),
        "quantiles_s": profile["lateness_s"],
        "behind_max_event_time_s": profile["behind_max_event_time_s"],
    }


def traffic(window: Manifest) -> dict:
    rows = connect().execute(
        f"""
        SELECT floor(json_extract(raw, '$.latitude')::DOUBLE / 0.1)::INT AS r,
               floor(json_extract(raw, '$.longitude')::DOUBLE / 0.2)::INT AS c,
               ((arrival_ms - {window.start_ms}) // 3600000)::INT AS hour,
               count(*) AS n
        FROM read_parquet('{raw_glob()}')
        WHERE arrival_ms >= {window.start_ms} AND arrival_ms < {window.end_ms}
          AND json_extract_string(raw, '$.type') = 'Position'
          AND json_extract(raw, '$.latitude')::DOUBLE BETWEEN -90 AND 90
          AND json_extract(raw, '$.longitude')::DOUBLE BETWEEN -180 AND 180
        GROUP BY 1, 2, 3 ORDER BY 3, 1, 2
        """
    ).fetchall()
    return {"cell_deg": [0.1, 0.2], "cells": [[r, c, h, n] for r, c, h, n in rows],
            "columns": ["row", "col", "hour", "position_reports"]}


def write(out: Path, name: str, body: dict) -> None:
    (out / name).write_text(json.dumps({"schema_version": SCHEMA_VERSION, **body}, separators=(",", ":")) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--bound-s", type=int, required=True, help="the run's watermark bound, for the manifest")
    args = ap.parse_args()

    window = Manifest.read(DATA / "windows" / args.window / "manifest.json")
    try:
        verified = checks(window, args.run)
    except Refused as why:
        raise SystemExit(f"export refused: {why}") from None

    raw_events = list(read_jsonl(DATA / "runs" / args.run / "events"))
    events = anonymise(raw_events)
    conservation = verified["conservation"]

    final = DATA / "exports" / args.run
    staging = final.with_name(final.name + ".tmp")
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    write(staging, "manifest.json", {
        "window": {"name": window.name, "start": window.start, "end": window.end,
                   "messages": window.messages, "sha256": window.sha256},
        "run": {"name": args.run, "lateness_bound_s": args.bound_s},
        "thresholds": THRESHOLDS,
        "checks": {k: v for k, v in verified.items() if k != "conservation"} | {
            "conservation": {"holds": True, "input": conservation["input"]}},
        "detections": dict(Counter(e["pattern"] for e in events)),
        "vessels_detected": len({e["vessel"] for e in events} | {e["other_vessel"] for e in events if "other_vessel" in e}),
        "attribution": ATTRIBUTION,
    })
    write(staging, "fates.json", {
        "input": conservation["input"],
        "fates": conservation["fates"],
        "by_stream": conservation["by_stream"],
        "invalid_by_reason": conservation["invalid_by_reason"],
    })
    write(staging, "lateness.json", lateness(window))
    write(staging, "traffic.json", traffic(window))
    write(staging, "events.json", {"events": events})
    write(staging, "funnels.json", {"funnels": funnels(raw_events)})
    shutil.rmtree(final, ignore_errors=True)
    staging.rename(final)
    print(f"exported {len(events)} detections to exports/{args.run}")


if __name__ == "__main__":
    main()
