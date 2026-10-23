import json

import pytest

from northwind import export
from northwind.recording import Manifest


def event(pattern, mmsi, start, other=None, **extra):
    e = {"pattern": pattern, "mmsi": mmsi, "startMs": start, "endMs": start + 60_000,
         "lat": 69.654321, "lon": 18.987654, "shipType": 70, **extra}
    if other is not None:
        e["otherMmsi"] = other
        e["otherShipType"] = 30
    return e


def test_ship_categories():
    assert export.ship_category(None) == "unknown"
    assert export.ship_category(30) == "fishing"
    assert export.ship_category(52) == "service"
    assert export.ship_category(65) == "passenger"
    assert export.ship_category(79) == "cargo"
    assert export.ship_category(84) == "tanker"
    assert export.ship_category(99) == "other"


def test_anonymise_drops_mmsi_rounds_positions_and_links_vessels():
    rows = export.anonymise([
        event("rendezvous", 257000002, 2000, other=257000001),
        event("port_call", 257000001, 1000),
    ])
    text = json.dumps(rows)
    assert "2570000" not in text and "mmsi" not in text.lower()
    assert rows[0]["vessel"] == "v1" and rows[0]["pattern"] == "port_call"
    assert rows[1]["vessel"] == "v2" and rows[1]["other_vessel"] == "v1"
    assert rows[0]["lat"] == 69.65 and rows[0]["lon"] == 18.99
    assert rows[1]["other_category"] == "fishing"


def test_anonymise_keeps_the_shore_band_and_rounds_its_distance():
    row = export.anonymise([event("loitering", 1, 0, heldPosition=True, shore="near", shoreM=137.4)])[0]
    assert row["shore"] == "near" and row["shore_m"] == 140


def test_gap_funnel_counts_each_step():
    gaps = [
        event("ais_gap", 1, 0, underway=True, coveredStart=True, coveredEnd=True, outageOverlap=False),
        event("ais_gap", 2, 0, underway=True, coveredStart=True, coveredEnd=True, outageOverlap=True),
        event("ais_gap", 3, 0, underway=True, coveredStart=False, coveredEnd=True, outageOverlap=False),
        event("ais_gap", 4, 0, underway=False, coveredStart=True, coveredEnd=True, outageOverlap=False),
    ]
    assert [s["events"] for s in export.funnels(gaps)["ais_gap"]] == [4, 3, 2, 1]


def test_loitering_funnel_drops_vessels_that_held_position_at_the_shore_first():
    rows = [
        event("loitering", 1, 0, heldPosition=True, shore="quay", atFarm=False, approached=True, truncated=False),
        event("loitering", 2, 0, heldPosition=False, atFarm=False, approached=True, truncated=False),
        event("loitering", 3, 0, heldPosition=False, atFarm=False, approached=False, truncated=False),
        event("loitering", 4, 0, heldPosition=False, atFarm=False, approached=True, truncated=True),
        event("loitering", 5, 0),  # an older job without the flags: not counted as moving
        event("loitering", 6, 0, heldPosition=True, shore="near", atFarm=False, approached=True, truncated=False),
        event("loitering", 7, 0, heldPosition=True, shore="open", atFarm=False, approached=True, truncated=False),
        event("loitering", 8, 0, heldPosition=True, approached=True, truncated=False),  # no shore: not offshore
        event("loitering", 9, 0, heldPosition=True, shore="open", atFarm=True, approached=True, truncated=False),
        event("loitering", 10, 0, heldPosition=False, approached=True, truncated=False),  # no farm flag
    ]
    steps = export.funnels(rows)["loitering"]
    assert [s["events"] for s in steps] == [10, 6, 4, 3, 2]
    assert "hold position" in steps[1]["step"] and "fish farm" in steps[2]["step"]


def test_rendezvous_funnel_drops_pairs_that_held_position_at_the_shore_first():
    rows = [
        event("rendezvous", 1, 0, other=2, heldPosition=True, shore="coast", atFarm=False, approached=True, truncated=False),
        event("rendezvous", 7, 0, other=8, heldPosition=True, shore="open", atFarm=False, approached=True, truncated=True),
        event("rendezvous", 3, 0, other=4, heldPosition=False, atFarm=False, approached=True, truncated=False),
        event("rendezvous", 5, 0, other=6, heldPosition=False, atFarm=False, approached=False, truncated=False),
        event("rendezvous", 9, 0, other=10, heldPosition=False, atFarm=True, approached=True, truncated=False),
    ]
    steps = export.funnels(rows)["rendezvous"]
    assert [s["events"] for s in steps] == [5, 4, 3, 2, 1]
    assert "hold position" in steps[1]["step"] and "fish farm" in steps[2]["step"]


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setattr(export, "DATA", tmp_path)
    window = Manifest("w", "a", "b", 0, 1, 10, 0, 9, "abc")
    runs = tmp_path / "runs"
    (runs / "r").mkdir(parents=True)
    (runs / "r-killed").mkdir()
    files = {
        runs / "r" / "summary.json": {"window": {"sha256": "abc"},
                                      "conservation": {"holds": True, "failures": [], "input": 10}},
        runs / "r" / "rerun.json": {"runs": ["r", "r-rerun"], "identical": True},
        runs / "r-killed" / "recovery-vs-clean.json": {"runs": ["r", "r-killed"], "identical": True},
        runs / "r-killed" / "recovery-vs-kafka.json": {"identical": True, "kafka": {
            "identical_to_files": True, "duplicates_in_topic": 0, "committed_records": 3}},
    }
    for path, body in files.items():
        path.write_text(json.dumps(body))
    return window, files


def test_checks_pass_when_everything_holds(data):
    window, _ = data
    assert export.checks(window, "r")["exactly_once"]["committed_records"] == 3


@pytest.mark.parametrize("name, patch, reason", [
    ("summary.json", {"window": {"sha256": "other"}}, "another window"),
    ("rerun.json", {"identical": False}, "rerun"),
    ("recovery-vs-clean.json", {"identical": False}, "TaskManager killed"),
    ("recovery-vs-kafka.json", {"kafka": {"identical_to_files": True, "duplicates_in_topic": 1}}, "committed topic"),
])
def test_checks_refuse(data, name, patch, reason):
    window, files = data
    path = next(p for p in files if p.name == name)
    path.write_text(json.dumps({**files[path], **patch}))
    with pytest.raises(export.Refused, match=reason):
        export.checks(window, "r")


def test_checks_refuse_a_missing_report(data):
    window, files = data
    next(p for p in files if p.name == "rerun.json").unlink()
    with pytest.raises(export.Refused, match="missing"):
        export.checks(window, "r")
