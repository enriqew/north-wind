"""Offshore installations from the Norwegian Offshore Directorate's facility register, for saying what
an episode that survives every filter lies beside.

The register (Sodir factpages, NLOD) is downloaded once as CSV into external/sodir/facility_fixed.csv:

    https://factpages.sodir.no/public?/Factpages/external/tableview/facility_fixed&rs:Command=Render
        &rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=en&rs:Format=CSV&Top100=false

Kept: installations at the surface and in service, at sea. That is platforms, production and storage
ships, spars and the wind turbines that power fields; subsea templates are under water, and onshore
plants and landfalls are on land. The register gives positions in degrees, minutes and seconds in
ED50, converted here to WGS84. The moveable facilities (rigs and vessels) are left out because the
register gives them no position.

    python -m northwind.installations --name oct28
"""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

from pyproj import Transformer

from northwind.recording import DATA

log = logging.getLogger("northwind.installations")

SOURCE = DATA / "external" / "sodir" / "facility_fixed.csv"
PHASE = "IN SERVICE"
ON_LAND = {"ONSHORE FACILITY", "LANDFALL"}


def degrees(deg: str, minutes: str, seconds: str, hemisphere: str) -> float:
    value = int(deg) + int(minutes) / 60 + float(seconds) / 3600
    return -value if hemisphere in ("S", "W") else value


def read(path: Path) -> list[dict]:
    """The register's surface installations in service at sea, with WGS84 positions."""
    ed50 = Transformer.from_crs(4230, 4326, always_xy=True)
    out, skipped = [], 0
    with path.open(encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r["fclSurface"] != "Y" or r["fclPhase"] != PHASE or r["fclKind"] in ON_LAND:
                continue
            if not r["fclNsDeg"] or r["fclGeodeticDatum"] not in ("ED50", "WGS84"):
                skipped += 1
                continue
            lat = degrees(r["fclNsDeg"], r["fclNsMin"], r["fclNsSec"], r["fclNsCode"])
            lon = degrees(r["fclEwDeg"], r["fclEwMin"], r["fclEwSec"], r["fclEwCode"])
            if r["fclGeodeticDatum"] == "ED50":
                lon, lat = ed50.transform(lon, lat)
            out.append({"name": r["fclName"], "kind": r["fclKind"], "lat": round(lat, 6), "lon": round(lon, 6)})
    if skipped:
        log.info("%d installations without a usable position left out", skipped)
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, help="the window whose masks directory gets installations.csv")
    args = ap.parse_args()

    rows = read(SOURCE)
    out = DATA / "windows" / args.name / "masks" / "installations.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["name", "kind", "lat", "lon"], lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    log.info("%d offshore installations", len(rows))


if __name__ == "__main__":
    main()
