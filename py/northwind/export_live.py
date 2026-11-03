"""Export a live run, and how its replays compare with it, as live.json. See docs/EXPORT.md.

    python -m northwind.export_live --window d1 --live d1-live --bound-s 120 \\
        --same-cpu d1-math-aarch64-b --other-cpu d1-b120 --strict d1-strict-x86 d1-strict-aarch64

The live run is compared with replays of its own recording: one of the same build on the live run's
CPU, one of the same build on another CPU, and two of a build that computes distances with
StrictMath, one on each CPU. Writes /data/exports/<live>/live.json only when the live run's messages
are conserved, the replay on its CPU is identical to it, the two StrictMath replays are identical,
and every event the other CPU gives differently differs only in a distance, by less than 1e-12.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import re
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from northwind.compare import OUTPUTS, lines
from northwind.recording import DATA, Manifest, connect, raw_glob

SCHEMA_VERSION = 1

# The fields the job computes with sin, cos and asin, so the only ones a CPU is allowed to change.
DISTANCE_FIELDS = ("distanceKm", "impliedKnots", "minDistanceM", "maxDriftKm")
MAX_RELATIVE = 1e-12

_TIME = r"(\d{4}-\d\d-\d\d [0-9]{2}:[0-9]{2}:[0-9]{2},\d{3})"
SESSION = re.compile(r"^ingest\S*\s+\|\s+" + _TIME + r" INFO session (\w+) (open|close)\s*(.*)$")
ARCH = re.compile(r"^(?:jobmanager|taskmanager)\S*\s+\|.* Arch: (\S+)\s*$")


class Refused(Exception):
    pass


def read_json(path: Path) -> dict:
    if not path.exists():
        raise Refused(f"missing {path.name} in {path.parent.name}")
    return json.loads(path.read_text())


def quantile(values: list[float], q: float) -> float:
    """Nearest rank: the smallest value with at least a share q of the values at or below it."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def log_ms(stamp: str) -> int:
    """A Compose log time (the containers run on UTC) as epoch milliseconds."""
    return int(datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc).timestamp() * 1000)


def health(samples: list[dict]) -> dict:
    """What the sampler recorded every few minutes: figures over the whole run, and the series behind them."""
    reading = [s for s in samples if "records_read" in s]
    if not reading:
        raise Refused("the sampler never saw the job reading")
    lag_ms = [s["lag_ms"] for s in reading if "lag_ms" in s]
    state = [s["state_bytes"] for s in samples if "state_bytes" in s]
    took = [s["checkpoint_ms"] for s in samples if "checkpoint_ms" in s]
    failed_at = [b["at_ms"] for a, b in zip(samples, samples[1:])
                 if b.get("checkpoints_failed", 0) > a.get("checkpoints_failed", 0)]
    return {
        "samples": len(samples),
        "every_s": round(quantile([b["at_ms"] - a["at_ms"] for a, b in zip(samples, samples[1:])], 0.5) / 1000),
        "first_at_ms": samples[0]["at_ms"],
        "last_at_ms": samples[-1]["at_ms"],
        "records_read": int(max(s["records_read"] for s in reading)),
        "lag_records_max": int(max(s.get("lag_records", 0) for s in reading)),
        "lag_ms": {"p50": quantile(lag_ms, 0.5), "p95": quantile(lag_ms, 0.95), "max": max(lag_ms)},
        "state_bytes": {"p50": quantile(state, 0.5), "max": max(state)},
        "checkpoints": {
            "completed": samples[-1]["checkpoints_completed"],
            "failed": samples[-1]["checkpoints_failed"],
            "failed_at_ms": failed_at,
            "duration_ms": {"p50": quantile(took, 0.5), "max": max(took)},
        },
        "series": {
            "columns": ["at_ms", "lag_ms", "state_bytes"],
            "rows": [[s["at_ms"], s.get("lag_ms"), s.get("state_bytes")] for s in samples],
        },
    }


def arrivals_per_hour() -> dict:
    """Messages recorded per clock hour of arrival: the load the live job kept up with. Taken from
    the recording rather than the sampler, whose record counter is not refreshed at every sample."""
    rows = connect().execute(
        f"SELECT arrival_ms // 3600000 * 3600000 AS hour, count(*) FROM read_parquet('{raw_glob()}') "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {"columns": ["hour_ms", "messages"], "rows": [[h, n] for h, n in rows]}


def feed(log: list[str]) -> dict:
    """The ingest's sessions with BarentsWatch: why each closed, and how long until the next opened."""
    sessions = []
    for line in log:
        m = SESSION.match(line.rstrip("\n"))
        if m:
            stamp, _, event, rest = m.groups()
            sessions.append((log_ms(stamp), event, ast.literal_eval(rest) if event == "close" else {}))
    if not sessions or sessions[0][1] != "open":
        raise Refused("the ingest log has no session opening")

    refresh_gaps, errors, closes = [], [], Counter()
    for (at, event, fields), nxt in zip(sessions, sessions[1:] + [None]):
        if event != "close":
            continue
        reason = fields["reason"]
        kind = reason.split(":", 1)[0]
        closes[kind] += 1
        gap = (nxt[0] - at) / 1000 if nxt and nxt[1] == "open" else None
        if kind == "token_refresh":
            refresh_gaps.append(gap)
        elif kind == "error":
            errors.append({"at_ms": at, "reason": reason.split(":", 1)[1].strip(), "back_after_s": gap})
        elif kind != "stopped":
            raise Refused(f"a session closed for a reason this export does not know: {reason}")
    if None in refresh_gaps or any(e["back_after_s"] is None for e in errors):
        raise Refused("a session closed and no other opened")
    return {
        "sessions": sum(1 for s in sessions if s[1] == "open"),
        "token_refreshes": closes["token_refresh"],
        "token_refresh_gap_s_max": max(refresh_gaps, default=None),
        "errors": errors,
    }


def cpu(log: list[str]) -> str:
    """The CPU architecture Flink's JobManager and TaskManagers reported at startup."""
    found = {m.group(1) for line in log if (m := ARCH.match(line.rstrip("\n")))}
    if len(found) != 1:
        raise Refused(f"the Flink processes report {len(found)} architectures")
    return found.pop()


def _without_distances(line: str) -> str:
    event = json.loads(line)
    return json.dumps({k: v for k, v in event.items() if k not in DISTANCE_FIELDS}, sort_keys=True)


def _literal(line: str, field: str) -> str:
    """The number exactly as the job wrote it, which is what makes two lines differ."""
    return re.search(rf'"{field}":(-?[0-9][0-9.eE+-]*)', line).group(1)


def differences(first: Counter, second: Counter) -> dict:
    """Pair the events only one of two runs gave, and reduce each pair to the distance that differs."""
    unmatched: dict[str, list[str]] = {}
    for line in (second - first).elements():
        unmatched.setdefault(_without_distances(line), []).append(line)
    pairs = []
    for line in sorted((first - second).elements()):
        partners = unmatched.get(_without_distances(line))
        if not partners:
            raise Refused("an event differs between CPUs in more than a distance")
        pairs.append((line, partners.pop()))
    if any(unmatched.values()):
        raise Refused("an event differs between CPUs in more than a distance")

    by_field: Counter = Counter()
    worst = None
    max_ulps = 0
    for a, b in pairs:
        ea, eb = json.loads(a), json.loads(b)
        changed = [f for f in DISTANCE_FIELDS if ea.get(f) != eb.get(f)]
        if not changed:
            raise Refused("two event lines differ in their text but not in their values")
        for f in changed:
            x, y = ea[f], eb[f]
            relative = abs(x - y) / max(abs(x), abs(y))
            if relative >= MAX_RELATIVE:
                raise Refused(f"{f} differs between CPUs by {relative:.1e}, more than rounding")
            by_field[f] += 1
            max_ulps = max(max_ulps, round(abs(x - y) / math.ulp(max(abs(x), abs(y)))))
            if worst is None or relative > worst["relative"]:
                worst = {"field": f, "first": _literal(a, f), "second": _literal(b, f), "relative": relative}
    return {"events": len(pairs), "by_field": dict(by_field), "max_ulps": max_ulps,
            "max_relative": worst["relative"] if worst else 0.0, "largest": worst}


def compare(first: str, second: str) -> tuple[dict, Counter, Counter]:
    outputs, events = {}, (Counter(), Counter())
    for output in OUTPUTS:
        a, b = lines(first, output), lines(second, output)
        outputs[output] = {"lines": [sum(a.values()), sum(b.values())], "identical": a == b}
        if output == "events":
            events = (a, b)
    return {"runs": [first, second], "outputs": outputs,
            "identical": all(o["identical"] for o in outputs.values())}, *events


def build(window: Manifest, live: str, bound_s: int, same: str, other: str, strict: tuple[str, str]) -> dict:
    run = DATA / "runs" / live
    summary = read_json(run / "summary.json")
    if summary["window"]["sha256"] != window.sha256:
        raise Refused("the live run's summary names another window")
    if not summary["conservation"]["holds"]:
        raise Refused("conservation does not hold for the live run")
    log = (run / "compose.log").read_text(errors="replace").splitlines()
    samples = [json.loads(line) for line in (run / "metrics.jsonl").read_text().splitlines() if line.strip()]

    same_cpu, _, _ = compare(live, same)
    if not same_cpu["identical"]:
        raise Refused("the replay on the live run's CPU differs from the live run")
    strict_pair, _, _ = compare(*strict)
    if not strict_pair["identical"]:
        raise Refused("the StrictMath replays differ between CPUs")
    other_cpu, live_events, other_events = compare(live, other)
    if not (other_cpu["outputs"]["rejected"]["identical"] and other_cpu["outputs"]["audit"]["identical"]):
        raise Refused("the replay on another CPU rejected or counted messages differently")
    other_cpu["differences"] = differences(live_events, other_events)

    return {
        "schema_version": SCHEMA_VERSION,
        "window": {"name": window.name, "start": window.start, "end": window.end,
                   "start_ms": window.start_ms, "end_ms": window.end_ms,
                   "messages": window.messages, "sha256": window.sha256},
        "run": {"name": live, "lateness_bound_s": bound_s, "cpu": cpu(log)},
        "conservation": {"holds": True, "input": summary["conservation"]["input"]},
        "events": sum(live_events.values()),
        "health": health(samples),
        "arrivals": arrivals_per_hour(),
        "feed": feed(log),
        "replays": {"same_cpu": same_cpu, "other_cpu": other_cpu, "strict": strict_pair},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", required=True)
    ap.add_argument("--live", required=True, help="the live run")
    ap.add_argument("--bound-s", type=int, required=True, help="the live run's watermark bound")
    ap.add_argument("--same-cpu", required=True, help="a replay of the same build on the live run's CPU")
    ap.add_argument("--other-cpu", required=True, help="a replay of the same build on another CPU")
    ap.add_argument("--strict", nargs=2, required=True, help="replays of the StrictMath build, one per CPU")
    args = ap.parse_args()

    window = Manifest.read(DATA / "windows" / args.window / "manifest.json")
    try:
        body = build(window, args.live, args.bound_s, args.same_cpu, args.other_cpu, tuple(args.strict))
    except Refused as why:
        raise SystemExit(f"export refused: {why}") from None

    out = DATA / "exports" / args.live
    out.mkdir(parents=True, exist_ok=True)
    staging = out / "live.json.tmp"
    staging.write_text(json.dumps(body, separators=(",", ":")) + "\n")
    staging.replace(out / "live.json")
    print(f"exported exports/{args.live}/live.json")


if __name__ == "__main__":
    main()
