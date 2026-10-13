"""Check a run: conservation of messages, and a summary of what was detected.

Conservation: every message of the window has exactly one fate. The job counts its input and each
fate; this check fails unless

    input == window messages (from the manifest)
    input == position + static + duplicate + late + invalid

It writes /data/runs/<run>/summary.json and exits non-zero on a failure.

    python -m northwind.check --run day1-b120 --window day1
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from northwind.recording import DATA, Manifest

FATES = ["position", "static", "duplicate", "late", "invalid"]


def read_jsonl(directory: Path):
    files = sorted(p for p in directory.glob("*.jsonl") if not p.name.startswith("."))
    for path in files:
        with path.open() as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


def conservation(audit: list[dict], window_messages: int) -> dict:
    totals: Counter = Counter()
    by_reason: dict[str, Counter] = defaultdict(Counter)
    by_stream: dict[str, Counter] = defaultdict(Counter)
    for row in audit:
        totals[row["category"]] += row["count"]
        if row["reason"]:
            by_reason[row["category"]][row["reason"]] += row["count"]
        by_stream[row["category"]][row["stream"] or "unknown"] += row["count"]
    accounted = sum(totals[f] for f in FATES)
    failures = []
    if totals["input"] != window_messages:
        failures.append(f"job read {totals['input']} messages, the window holds {window_messages}")
    if accounted != totals["input"]:
        failures.append(f"fates sum to {accounted}, input is {totals['input']}")
    return {
        "input": totals["input"],
        "window_messages": window_messages,
        "fates": {f: totals[f] for f in FATES},
        "invalid_by_reason": dict(by_reason["invalid"]),
        "by_stream": {k: dict(v) for k, v in by_stream.items()},
        "holds": not failures,
        "failures": failures,
    }


def detections(events: list[dict]) -> dict:
    out: dict[str, dict] = {}
    by_pattern: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        by_pattern[e["pattern"]].append(e)
    for pattern, rows in sorted(by_pattern.items()):
        durations = sorted((r["endMs"] - r["startMs"]) / 60_000 for r in rows)
        summary = {
            "events": len(rows),
            "vessels": len({r["mmsi"] for r in rows}),
            "truncated": sum(1 for r in rows if r.get("truncated")),
            "duration_min_p50": round(durations[len(durations) // 2], 1),
        }
        if pattern == "ais_gap":
            funnel = Counter()
            for r in rows:
                funnel["all"] += 1
                if r.get("underway"):
                    funnel["underway"] += 1
                    if r.get("coveredStart") and r.get("coveredEnd"):
                        funnel["underway_in_coverage"] += 1
                        if not r.get("outageOverlap"):
                            funnel["underway_in_coverage_no_outage"] += 1
            summary["funnel"] = dict(funnel)
        out[pattern] = summary
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--window", required=True)
    args = ap.parse_args()

    run = DATA / "runs" / args.run
    m = Manifest.read(DATA / "windows" / args.window / "manifest.json")
    audit = list(read_jsonl(run / "audit"))
    events = list(read_jsonl(run / "events"))
    summary = {
        "run": args.run,
        "window": {"name": m.name, "start": m.start, "end": m.end, "sha256": m.sha256},
        "conservation": conservation(audit, m.messages),
        "detections": detections(events),
    }
    (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    if not summary["conservation"]["holds"]:
        raise SystemExit("conservation does not hold: " + "; ".join(summary["conservation"]["failures"]))


if __name__ == "__main__":
    main()
