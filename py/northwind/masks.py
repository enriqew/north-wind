"""Masks for a window, derived from the window itself.

- ports.csv: fine cells (0.01 x 0.02 degrees) where at least MIN_VESSELS distinct vessels report
  being moored or at anchor while stopped, grown by one cell, grouped into port areas by
  8-connectivity. No external port registry is used: a port area here is where ships in this
  recording actually tie up or anchor.
- coverage.csv: coarse cells (0.1 x 0.2 degrees) with a terrestrial or offshore position in at
  least COVERAGE_SHARE of the window's hours. Inside them, reception is continuous, so a silence is
  a fact about the vessel rather than about the receivers.
- outages.csv: arrival-time intervals with no message at all for longer than STALL_MS, padded on
  both sides. They cover disconnects and stalls of the ingest alike.

The cell formula, floor(lat / dlat) and floor(lon / dlon), is the one in Geo.java and must match it.

    python -m northwind.masks --name day1
"""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

from northwind.recording import DATA, Manifest, connect, raw_glob

log = logging.getLogger("northwind.masks")

PORT_DLAT, PORT_DLON = 0.01, 0.02
COVERAGE_DLAT, COVERAGE_DLON = 0.1, 0.2
MIN_VESSELS = 3
STOP_KNOTS = 0.5
COVERAGE_SHARE = 0.9
STALL_MS = 60_000
OUTAGE_PAD_MS = 120_000

POSITIONS = """
CREATE TEMP TABLE positions AS
SELECT
    json_extract(raw, '$.mmsi')::BIGINT AS mmsi,
    epoch_ms(json_extract_string(raw, '$.msgtime')::TIMESTAMPTZ) AS t,
    json_extract(raw, '$.latitude')::DOUBLE AS lat,
    json_extract(raw, '$.longitude')::DOUBLE AS lon,
    json_extract(raw, '$.speedOverGround')::DOUBLE AS sog,
    json_extract(raw, '$.navigationalStatus')::INTEGER AS nav,
    json_extract_string(raw, '$.stream') AS stream
FROM read_parquet('{glob}')
WHERE arrival_ms >= {start} AND arrival_ms < {end}
  AND json_extract_string(raw, '$.type') = 'Position'
  AND json_extract(raw, '$.latitude') IS NOT NULL
  AND json_extract(raw, '$.longitude') IS NOT NULL
"""


def components(cells: set[tuple[int, int]]) -> dict[tuple[int, int], int]:
    """8-connected components, numbered from 1 in order of their smallest cell, so ids are stable."""
    ids: dict[tuple[int, int], int] = {}
    next_id = 1
    for cell in sorted(cells):
        if cell in ids:
            continue
        stack = [cell]
        ids[cell] = next_id
        while stack:
            r, c = stack.pop()
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    n = (r + dr, c + dc)
                    if n in cells and n not in ids:
                        ids[n] = next_id
                        stack.append(n)
        next_id += 1
    return ids


def grow(cells: set[tuple[int, int]]) -> set[tuple[int, int]]:
    return {(r + dr, c + dc) for r, c in cells for dr in (-1, 0, 1) for dc in (-1, 0, 1)}


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for a, b in sorted(intervals):
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    return merged


def write_csv(path: Path, header: list[str], rows) -> int:
    n = 0
    with path.open("w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        for row in rows:
            w.writerow(row)
            n += 1
    return n


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    args = ap.parse_args()

    window = DATA / "windows" / args.name
    m = Manifest.read(window / "manifest.json")
    out = window / "masks"
    out.mkdir(parents=True, exist_ok=True)
    con = connect()
    con.execute(POSITIONS.format(glob=raw_glob(), start=m.start_ms, end=m.end_ms))

    stopped = con.execute(
        f"""
        SELECT floor(lat / {PORT_DLAT})::BIGINT AS r, floor(lon / {PORT_DLON})::BIGINT AS c
        FROM positions
        WHERE nav IN (1, 5) AND sog IS NOT NULL AND sog < {STOP_KNOTS}
        GROUP BY 1, 2 HAVING count(DISTINCT mmsi) >= {MIN_VESSELS}
        """
    ).fetchall()
    port_cells = grow({(int(r), int(c)) for r, c in stopped})
    port_ids = components(port_cells)
    n_ports = write_csv(out / "ports.csv", ["row", "col", "port_id"],
                        ((r, c, port_ids[(r, c)]) for r, c in sorted(port_cells)))

    hours = max(1, round((m.end_ms - m.start_ms) / 3_600_000))
    covered = con.execute(
        f"""
        SELECT floor(lat / {COVERAGE_DLAT})::BIGINT, floor(lon / {COVERAGE_DLON})::BIGINT
        FROM positions
        WHERE stream IN ('terra', 'offshore') AND t >= {m.start_ms} AND t < {m.end_ms}
        GROUP BY 1, 2 HAVING count(DISTINCT t // 3600000) >= {COVERAGE_SHARE * hours}
        """
    ).fetchall()
    n_cov = write_csv(out / "coverage.csv", ["row", "col"], sorted((int(r), int(c)) for r, c in covered))

    stalls = con.execute(
        f"""
        SELECT prev, arrival_ms FROM (
            SELECT arrival_ms, lag(arrival_ms) OVER (ORDER BY "offset") AS prev
            FROM read_parquet('{raw_glob()}')
            WHERE arrival_ms >= {m.start_ms} AND arrival_ms < {m.end_ms}
        ) WHERE arrival_ms - prev > {STALL_MS}
        """
    ).fetchall()
    outages = merge_intervals([(a - OUTAGE_PAD_MS, b + OUTAGE_PAD_MS) for a, b in stalls])
    n_out = write_csv(out / "outages.csv", ["from_ms", "to_ms"], outages)

    log.info("%d port cells in %d port areas, %d covered cells (of %d hours), %d outages",
             n_ports, len(set(port_ids.values())), n_cov, hours, n_out)


if __name__ == "__main__":
    main()
