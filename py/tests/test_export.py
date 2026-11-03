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


S = 257_000_000  # a ship station's MMSI: Norway's MID and six digits


def test_ship_mmsi_is_nine_digits_from_a_maritime_identification_digit():
    assert export.ship_mmsi(257123456) and export.ship_mmsi(538001234)
    assert not export.ship_mmsi(109123456)  # a net buoy
    assert not export.ship_mmsi(1234567)  # seven digits
    assert not export.ship_mmsi(992576001)  # an aid to navigation
    assert not export.ship_mmsi(None)


def test_anonymise_keeps_whether_the_mmsi_is_a_ship_s():
    rows = export.anonymise([event("rendezvous", S + 1, 0, other=109123456)])
    assert rows[0]["ship_station"] is True and rows[0]["other_ship_station"] is False


def test_anonymise_names_the_installation_an_episode_began_beside():
    fpso = {"name": "AN FPSO", "kind": "FPSO", "lat": 69.66, "lon": 18.99}
    far = {"name": "FAR", "kind": "JACKET 4 LEGS", "lat": 60.0, "lon": 2.0}
    near, alone = export.anonymise([event("loitering", S + 1, 0), event("loitering", S + 2, 1, lat=69.9)], [far, fpso])
    assert near["installation"] == "AN FPSO" and near["installation_kind"] == "FPSO"
    assert 500 < near["installation_m"] < 1000 and near["installation_m"] % 10 == 0
    assert "installation" not in alone
    port = export.anonymise([event("port_call", S + 3, 0)], [fpso])[0]
    assert "installation" not in port


def test_loitering_funnel_drops_vessels_that_held_position_at_the_shore_first():
    rows = [
        event("loitering", S + 1, 0, heldPosition=True, shore="quay", atFarm=False, approached=True, truncated=False),
        event("loitering", S + 2, 0, heldPosition=False, atFarm=False, approached=True, truncated=False),
        event("loitering", S + 3, 0, heldPosition=False, atFarm=False, approached=False, truncated=False),
        event("loitering", S + 4, 0, heldPosition=False, atFarm=False, approached=True, truncated=True),
        event("loitering", S + 5, 0),  # an older job without the flags: not counted as moving
        event("loitering", S + 6, 0, heldPosition=True, shore="near", atFarm=False, approached=True, truncated=False),
        event("loitering", S + 7, 0, heldPosition=True, shore="open", atFarm=False, approached=True, truncated=False),
        event("loitering", S + 8, 0, heldPosition=True, approached=True, truncated=False),  # no shore: not offshore
        event("loitering", S + 9, 0, heldPosition=True, shore="open", atFarm=True, approached=True, truncated=False),
        event("loitering", S + 10, 0, heldPosition=False, approached=True, truncated=False),  # no farm flag
        event("loitering", 109123456, 0, heldPosition=False, atFarm=False, approached=True, truncated=False),  # a buoy
        event("loitering", S + 12, 0, shipType=None, heldPosition=False, atFarm=False, approached=True, truncated=False),
    ]
    steps = export.funnels(rows)["loitering"]
    assert [s["events"] for s in steps] == [12, 11, 10, 6, 4, 3, 2]
    assert "MMSI" in steps[1]["step"] and "type" in steps[2]["step"]
    assert "hold position" in steps[3]["step"] and "fish farm" in steps[4]["step"]


def test_rendezvous_funnel_drops_pairs_that_held_position_at_the_shore_first():
    rows = [
        event("rendezvous", S + 1, 0, other=S + 2, heldPosition=True, shore="coast", atFarm=False, approached=True, truncated=False),
        event("rendezvous", S + 7, 0, other=S + 8, heldPosition=True, shore="open", atFarm=False, approached=True, truncated=True),
        event("rendezvous", S + 3, 0, other=S + 4, heldPosition=False, atFarm=False, approached=True, truncated=False),
        event("rendezvous", S + 5, 0, other=S + 6, heldPosition=False, atFarm=False, approached=False, truncated=False),
        event("rendezvous", S + 9, 0, other=S + 10, heldPosition=False, atFarm=True, approached=True, truncated=False),
        event("rendezvous", S + 11, 0, other=109123456, heldPosition=False, atFarm=False, approached=True, truncated=False),
    ]
    steps = export.funnels(rows)["rendezvous"]
    assert [s["events"] for s in steps] == [6, 5, 5, 4, 3, 2, 1]
    assert "hold position" in steps[3]["step"] and "fish farm" in steps[4]["step"]


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
