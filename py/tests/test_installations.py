import pytest

from northwind import installations

HEADER = ("fclName,fclPhase,fclSurface,fclKind,fclGeodeticDatum,"
          "fclNsDeg,fclNsMin,fclNsSec,fclNsCode,fclEwDeg,fclEwMin,fclEwSec,fclEwCode\n")


def test_degrees_from_degrees_minutes_seconds():
    assert installations.degrees("72", "30", "36", "N") == pytest.approx(72.51)
    assert installations.degrees("2", "15", "0", "W") == pytest.approx(-2.25)


def test_read_keeps_surface_installations_in_service_at_sea(tmp_path):
    path = tmp_path / "facility_fixed.csv"
    path.write_text(HEADER + "\n".join([
        "A FPSO,IN SERVICE,Y,FPSO,ED50,72,30,0,N,20,20,0,E",
        "A TEMPLATE,IN SERVICE,N,SINGLE WELL TEMPLATE,ED50,72,30,0,N,20,20,0,E",
        "A PLANT,IN SERVICE,Y,ONSHORE FACILITY,ED50,70,40,0,N,23,40,0,E",
        "A JACKET,REMOVED,Y,JACKET 4 LEGS,ED50,60,0,0,N,2,0,0,E",
        "NO POSITION,IN SERVICE,Y,JACKET 4 LEGS,,,,,,,,,",
    ]) + "\n", encoding="utf-8")
    [kept] = installations.read(path)
    assert kept["name"] == "A FPSO" and kept["kind"] == "FPSO"
    # ED50 to WGS84 moves a point in the Barents Sea by tens of metres, mostly west.
    assert kept["lat"] == pytest.approx(72.5, abs=0.001)
    assert 0.0005 < (20 + 1 / 3) - kept["lon"] < 0.004
