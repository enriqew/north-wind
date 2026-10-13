"""Selecting a window of the recording, and naming it.

A window is a half-open interval of arrival time, [start, end), over the raw recording. Everything
derived from a window (the replay topic, the masks, the profile, a run) carries its manifest: the
interval, the message count, and a sha256 over the messages in arrival order. Two things built from
the same manifest were built from the same input.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DATA = Path(os.environ.get("NW_DATA", "/data"))
RECORDINGS = DATA / "recordings" / "live"


def parse_time(value: str) -> int:
    """ISO-8601 UTC (`2026-10-13T10:00:00Z`) to epoch milliseconds."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Manifest:
    name: str
    start: str
    end: str
    start_ms: int
    end_ms: int
    messages: int
    first_offset: int
    last_offset: int
    sha256: str

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2) + "\n")

    @staticmethod
    def read(path: Path) -> "Manifest":
        return Manifest(**json.loads(path.read_text()))


def raw_glob(kind: str = "raw") -> str:
    return str(RECORDINGS / kind / "*" / "part-*.parquet")


def window_sql(start_ms: int, end_ms: int, kind: str = "raw") -> str:
    """The window's rows in arrival order. Offsets are the arrival order; ties cannot occur."""
    return (
        f"SELECT \"offset\", arrival_ms, raw FROM read_parquet('{raw_glob(kind)}') "
        f"WHERE arrival_ms >= {start_ms} AND arrival_ms < {end_ms} ORDER BY \"offset\""
    )


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '4GB'")
    return con


def iter_window(con: duckdb.DuckDBPyConnection, start_ms: int, end_ms: int, batch: int = 200_000):
    """Yield (offset, arrival_ms, raw) in arrival order, in batches to bound memory."""
    reader = con.execute(window_sql(start_ms, end_ms)).fetch_record_batch(batch)
    for rb in reader:
        offsets = rb.column(0).to_pylist()
        arrivals = rb.column(1).to_pylist()
        raws = rb.column(2).to_pylist()
        yield from zip(offsets, arrivals, raws)


class Hasher:
    """Accumulates the manifest fields over rows seen in arrival order."""

    def __init__(self) -> None:
        self.digest = hashlib.sha256()
        self.count = 0
        self.first: int | None = None
        self.last: int | None = None
        self.previous: int | None = None

    def add(self, offset: int, raw: str) -> None:
        if self.previous is not None and offset != self.previous + 1:
            raise ValueError(f"recording has a hole: offset {offset} after {self.previous}")
        self.digest.update(raw.encode("utf-8"))
        self.digest.update(b"\n")
        self.count += 1
        if self.first is None:
            self.first = offset
        self.last = offset
        self.previous = offset

    def manifest(self, name: str, start_ms: int, end_ms: int) -> Manifest:
        return Manifest(
            name=name,
            start=iso(start_ms),
            end=iso(end_ms),
            start_ms=start_ms,
            end_ms=end_ms,
            messages=self.count,
            first_offset=self.first if self.first is not None else -1,
            last_offset=self.last if self.last is not None else -1,
            sha256=self.digest.hexdigest(),
        )
