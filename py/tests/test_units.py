import math

from northwind.barentswatch import sse_payloads
from northwind.check import conservation
from northwind.masks import PORT_DLAT, PORT_DLON, COVERAGE_DLAT, COVERAGE_DLON, components, grow, merge_intervals
from northwind.record import PART, next_offset


def test_sse_payloads_skip_comments_and_blank_lines():
    lines = [b": start", b"", b'data: {"a":1}', b"data:", b'data:{"b":2}']
    assert list(sse_payloads(iter(lines))) == [b'{"a":1}', b'{"b":2}']


def test_next_offset_resumes_after_the_highest_file(tmp_path):
    day = tmp_path / "date=2026-10-13"
    day.mkdir()
    (day / "part-000000000000-000000000099.parquet").touch()
    (day / "part-000000000100-000000000250.parquet").touch()
    (day / "part-000000000251-000000000300.parquet.tmp").touch()  # never renamed: not committed
    assert next_offset(tmp_path) == 251
    assert next_offset(tmp_path / "empty") == 0
    assert PART.search("part-000000000001-000000000002.parquet")


def test_cells_match_the_flink_job():
    # Same expectations as GeoTest.cellsMatchThePythonMaskBuilder.
    assert (math.floor(69.655 / PORT_DLAT), math.floor(18.965 / PORT_DLON)) == (6965, 948)
    assert (math.floor(69.655 / COVERAGE_DLAT), math.floor(18.965 / COVERAGE_DLON)) == (696, 94)


def test_port_areas_are_8_connected_and_numbered_stably():
    cells = {(0, 0), (1, 1), (5, 5)}
    ids = components(cells)
    assert ids[(0, 0)] == ids[(1, 1)] == 1
    assert ids[(5, 5)] == 2
    assert len(grow({(0, 0)})) == 9


def test_outages_merge_when_they_overlap():
    assert merge_intervals([(10, 20), (0, 5), (15, 30)]) == [(0, 5), (10, 30)]


def test_conservation_fails_when_a_message_is_unaccounted():
    audit = [
        {"category": "input", "reason": "", "stream": "terra", "count": 10},
        {"category": "position", "reason": "", "stream": "terra", "count": 7},
        {"category": "duplicate", "reason": "", "stream": "terra", "count": 1},
        {"category": "invalid", "reason": "null_island", "stream": "terra", "count": 1},
    ]
    result = conservation(audit, 10)
    assert not result["holds"]
    audit.append({"category": "late", "reason": "", "stream": "satellite", "count": 1})
    assert conservation(audit, 10)["holds"]
    assert not conservation(audit, 11)["holds"]
