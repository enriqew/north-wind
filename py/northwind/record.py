"""Kafka into Parquet: the recording that every later run replays.

Each file holds a contiguous range of offsets from one single-partition topic, and its name says
which: `part-<first>-<last>.parquet`. The file names are the recorder's only commit log. On
restart it reads the highest `last` on disk and resumes at the next offset, so a crash between
writing a file and anything else can neither lose a range nor write it twice. A file is written to
a temporary name and renamed into place, so a half-written file never carries a valid name.

Columns: `offset`, `arrival_ms` (the Kafka timestamp, set by the ingest on receipt) and `raw`
(the message exactly as BarentsWatch sent it).

    python -m northwind.record
"""

from __future__ import annotations

import logging
import os
import re
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, TopicPartition

log = logging.getLogger("northwind.record")

BOOTSTRAP = os.environ.get("NW_KAFKA", "kafka:9092")
ROOT = Path(os.environ.get("NW_RECORDINGS", "/data/recordings/live"))
TOPICS = {
    os.environ.get("NW_RAW_TOPIC", "ais.raw"): "raw",
    os.environ.get("NW_SESSIONS_TOPIC", "ais.sessions"): "sessions",
}
FLUSH_RECORDS = int(os.environ.get("NW_FLUSH_RECORDS", "250000"))
FLUSH_SECONDS = float(os.environ.get("NW_FLUSH_SECONDS", "300"))

SCHEMA = pa.schema(
    [
        ("offset", pa.int64()),
        ("arrival_ms", pa.int64()),
        ("raw", pa.string()),
    ]
)
PART = re.compile(r"part-(\d+)-(\d+)\.parquet$")

_stop = False


def _on_signal(*_: object) -> None:
    global _stop
    _stop = True


def next_offset(directory: Path) -> int:
    """The first offset not yet on disk for one topic's directory."""
    last = -1
    for path in directory.rglob("part-*.parquet"):
        match = PART.search(path.name)
        if match:
            last = max(last, int(match.group(2)))
    return last + 1


def file_path(directory: Path, first: int, last: int, arrival_ms: int) -> Path:
    day = datetime.fromtimestamp(arrival_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return directory / f"date={day}" / f"part-{first:012d}-{last:012d}.parquet"


def write_batch(directory: Path, rows: list[tuple[int, int, str]]) -> Path:
    offsets, arrivals, raws = zip(*rows)
    table = pa.Table.from_arrays(
        [pa.array(offsets, pa.int64()), pa.array(arrivals, pa.int64()), pa.array(raws, pa.string())],
        schema=SCHEMA,
    )
    target = file_path(directory, offsets[0], offsets[-1], arrivals[0])
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd", compression_level=9)
    os.replace(tmp, target)
    return target


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "northwind-recorder",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    assignment = []
    buffers: dict[str, list[tuple[int, int, str]]] = {}
    for topic, name in TOPICS.items():
        start = next_offset(ROOT / name)
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=30)
        if start > high or start < low:
            # The files say the topic should hold offset `start`; it does not, so the topic was
            # recreated or truncated. Resuming would either wait forever or skip messages.
            raise SystemExit(f"{topic}: next offset on disk is {start}, topic holds {low}..{high}")
        assignment.append(TopicPartition(topic, 0, start))
        buffers[topic] = []
        log.info("%s resumes at offset %d", topic, start)
    consumer.assign(assignment)

    last_flush = time.monotonic()
    while not _stop:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consumer error: %s", msg.error())
            else:
                buf = buffers[msg.topic()]
                expected = buf[-1][0] + 1 if buf else None
                if expected is not None and msg.offset() != expected:
                    # Offsets on a compacted or transactional topic can skip; these are neither,
                    # so a skip means something unexpected and the file must not span it.
                    raise RuntimeError(f"{msg.topic()}: offset {msg.offset()} after {expected - 1}")
                buf.append((msg.offset(), msg.timestamp()[1], msg.value().decode("utf-8")))

        due = time.monotonic() - last_flush >= FLUSH_SECONDS
        for topic, buf in buffers.items():
            if buf and (due or len(buf) >= FLUSH_RECORDS):
                path = write_batch(ROOT / TOPICS[topic], buf)
                log.info("wrote %d records to %s", len(buf), path.relative_to(ROOT))
                buf.clear()
        if due:
            last_flush = time.monotonic()

    for topic, buf in buffers.items():
        if buf:
            write_batch(ROOT / TOPICS[topic], buf)
    consumer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    run()


if __name__ == "__main__":
    main()
