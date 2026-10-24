"""The coast near where vessels in a window sat still, and the fish farms, for telling moored from
anchored and either from loitering.

A vessel that holds position may be tied to a quay or at anchor, and the AIS alone does not say
which: about half the vessels that hold position send no heading, so swinging at anchor cannot be
seen, and a ship at anchor in calm weather does not swing. Where it holds position does say: a quay
is at the shore, an anchorage is not.

The coast comes from Kartverket's "Sjokart - Dybdedata" (CC BY 4.0), downloaded once from Geonorge
as the national FGDB in EPSG:4258. Two kinds of line are kept:

- built (b): quays, piers, jetties, breakwaters, floating dock edges, sheet piling, dry dock edges,
  ramps and slipways;
- natural (n): the coastline (kystkontur).

shore.csv holds those lines as segments, only in the port-grid cells (0.01 x 0.02 degrees) where a
vessel in the window reported under SLOW_KNOTS, grown by GROW cells. The job looks up the cells
around a position, so every segment within a cell's width of a slow position is there. Lines are
simplified first by SIMPLIFY_DEG, a few metres, far below the distances the job tells apart.

farms.csv holds the outline of every licensed aquaculture site from the Directorate of Fisheries'
"Akvakultur - lokaliteter" (NLOD), downloaded once from Geonorge as the national FGDB in EPSG:25833,
one row per vertex. A well boat or feed carrier holding position at a salmon farm, alone or beside
another, is at work, and away from the shore it otherwise looks like a ship at anchor. Only the
outlines are read: the register also names owners and contact people, and none of that is kept.

A live run has no window to cut the coast to yet, so `--all` writes the whole coast instead, into
any directory: built coast at the same few metres, the natural coastline simplified by
SIMPLIFY_ALL_DEG (about 10 m), which keeps the file near the size of a window's.

    python -m northwind.shore --name w4h
    python -m northwind.shore --all --out /data/live-masks
"""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

import numpy as np
import pyogrio
import shapely
from pyproj import Transformer

from northwind.masks import PORT_DLAT, PORT_DLON
from northwind.recording import DATA, Manifest, connect, raw_glob

log = logging.getLogger("northwind.shore")

COAST = DATA / "external" / "dybdedata" / "Basisdata_0000_Norge_4258_Dybdedata_FGDB.gdb"
BUILT = ["kaibrygge", "pirkant", "utstikker", "molo", "flytedokkkant", "spuntvegger", "torrdokkkant",
         "rampe", "slipp"]
NATURAL = ["kystkontur"]
FARMS = DATA / "external" / "akvakultur" / "KystOgFiskeri_0000_Norge_25833_AkvakulturLokaliteter_FGDB.gdb"
SLOW_KNOTS = 1.5  # loitering's speed limit; rendezvous uses 1.0
GROW = 2
SIMPLIFY_DEG = 0.00003  # about 3 m north-south, less east-west
SIMPLIFY_ALL_DEG = 0.0001  # about 11 m north-south, for the natural coast under --all


def slow_cells(name: str) -> set[tuple[int, int]]:
    m = Manifest.read(DATA / "windows" / name / "manifest.json")
    rows = connect().execute(
        f"""
        SELECT DISTINCT floor(lat / {PORT_DLAT})::BIGINT, floor(lon / {PORT_DLON})::BIGINT FROM (
            SELECT json_extract(raw, '$.latitude')::DOUBLE AS lat,
                   json_extract(raw, '$.longitude')::DOUBLE AS lon,
                   json_extract(raw, '$.speedOverGround')::DOUBLE AS sog
            FROM read_parquet('{raw_glob()}')
            WHERE arrival_ms >= {m.start_ms} AND arrival_ms < {m.end_ms}
              AND json_extract_string(raw, '$.type') = 'Position'
        ) WHERE sog < {SLOW_KNOTS} AND lat IS NOT NULL AND lon IS NOT NULL
        """
    ).fetchall()
    return {(int(r), int(c)) for r, c in rows}


def grow(cells: set[tuple[int, int]], n: int) -> set[tuple[int, int]]:
    return {(r + dr, c + dc) for r, c in cells for dr in range(-n, n + 1) for dc in range(-n, n + 1)}


def segments(layer: str, bbox: tuple[float, float, float, float] | None, tolerance: float = SIMPLIFY_DEG) -> np.ndarray:
    """Every segment of a line layer inside the box (or all of it), as rows of lat1, lon1, lat2, lon2."""
    wkb = pyogrio.raw.read(str(COAST), layer=layer, read_geometry=True, columns=[], bbox=bbox)[2]
    lines = shapely.get_parts(shapely.simplify(shapely.from_wkb(wkb), tolerance))
    out = []
    for line in lines:
        xy = shapely.get_coordinates(line)
        if len(xy) >= 2:
            out.append(np.column_stack([xy[:-1, 1], xy[:-1, 0], xy[1:, 1], xy[1:, 0]]))
    return np.concatenate(out) if out else np.empty((0, 4))


def keep(seg: np.ndarray, cells: set[tuple[int, int]]) -> np.ndarray:
    """Segments touching any of the cells, judged by the cells their bounding box covers."""
    r0 = np.floor(np.minimum(seg[:, 0], seg[:, 2]) / PORT_DLAT).astype(np.int64)
    r1 = np.floor(np.maximum(seg[:, 0], seg[:, 2]) / PORT_DLAT).astype(np.int64)
    c0 = np.floor(np.minimum(seg[:, 1], seg[:, 3]) / PORT_DLON).astype(np.int64)
    c1 = np.floor(np.maximum(seg[:, 1], seg[:, 3]) / PORT_DLON).astype(np.int64)
    keys = np.array(sorted(r * 1_000_000 + c for r, c in cells), dtype=np.int64)
    # Most segments lie in one cell: test those all at once, and walk the cells of the rest.
    mask = (r0 == r1) & (c0 == c1) & np.isin(r0 * 1_000_000 + c0, keys)
    for i in np.flatnonzero((r0 != r1) | (c0 != c1)):
        mask[i] = any((r, c) in cells for r in range(r0[i], r1[i] + 1) for c in range(c0[i], c1[i] + 1))
    return seg[mask]


def write_farms(path) -> int:
    """Every licensed site's outline in EPSG:4258, vertices in order, one site after another."""
    wkb = pyogrio.raw.read(str(FARMS), layer="akvakulturflate", read_geometry=True, columns=[])[2]
    rings = [shapely.get_exterior_ring(p) for p in shapely.get_parts(shapely.from_wkb(wkb))]
    to_deg = Transformer.from_crs(25833, 4258, always_xy=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["farm", "lat", "lon"])
        for i, ring in enumerate(rings):
            xy = shapely.get_coordinates(ring)
            lon, lat = to_deg.transform(xy[:, 0], xy[:, 1])
            for a, b in zip(lat, lon):
                w.writerow([i, f"{a:.7f}", f"{b:.7f}"])
    return len(rings)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", help="the window whose slow cells the coast is cut to")
    ap.add_argument("--all", action="store_true", help="the whole coast, for a live run")
    ap.add_argument("--out", help="the masks directory to write to (default: the window's)")
    args = ap.parse_args()
    if args.all == bool(args.name):
        ap.error("give either --name or --all")

    if args.all:
        cells, bbox = None, None
        out = Path(args.out) / "shore.csv"
    else:
        cells = grow(slow_cells(args.name), GROW)
        rows = [r for r, _ in cells]
        cols = [c for _, c in cells]
        bbox = (min(cols) * PORT_DLON, min(rows) * PORT_DLAT, (max(cols) + 1) * PORT_DLON, (max(rows) + 1) * PORT_DLAT)
        out = Path(args.out) / "shore.csv" if args.out else DATA / "windows" / args.name / "masks" / "shore.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    with out.open("w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["kind", "lat1", "lon1", "lat2", "lon2"])
        for kind, layers in (("b", BUILT), ("n", NATURAL)):
            for layer in layers:
                tolerance = SIMPLIFY_ALL_DEG if args.all and kind == "n" else SIMPLIFY_DEG
                seg = segments(layer, bbox, tolerance)
                if cells is not None:
                    seg = keep(seg, cells)
                # Sorted, so the file is the same whatever order the driver returns features in.
                seg = seg[np.lexsort(seg.T[::-1])]
                for s in seg:
                    w.writerow([kind] + [f"{v:.7f}" for v in s])
                counts[layer] = len(seg)
    log.info("%s, segments: %s", "whole coast" if cells is None else f"{len(cells)} slow cells (grown)", counts)
    log.info("%d fish farms", write_farms(out.with_name("farms.csv")))


if __name__ == "__main__":
    main()
