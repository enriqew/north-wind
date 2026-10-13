"""Replay a window of the recording into its own Kafka topic, in arrival order.

The topic is `replay.<name>`, one partition, each record stamped with its original arrival time.
The window's manifest is written next to it under /data/windows/<name>/manifest.json; a topic that
already exists is refused rather than appended to, since a replay is only worth anything if it
holds exactly the window.

    python -m northwind.replay --name day1 --start 2026-10-13T10:00:00Z --end 2026-10-14T09:00:00Z
"""

from __future__ import annotations

import argparse
import logging
import os

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from northwind.recording import DATA, Hasher, connect, iso, iter_window, parse_time, raw_glob

log = logging.getLogger("northwind.replay")
BOOTSTRAP = os.environ.get("NW_KAFKA", "kafka:9092")


def topic_name(name: str) -> str:
    return f"replay.{name}"


def create_topic(admin: AdminClient, topic: str) -> None:
    if topic in admin.list_topics(timeout=10).topics:
        raise SystemExit(f"{topic} already exists; delete it or pick another --name")
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1, config={"retention.ms": "-1", "retention.bytes": "-1"})]
    )
    futures[topic].result()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--start", required=True, help="ISO-8601 UTC, inclusive")
    ap.add_argument("--end", required=True, help="ISO-8601 UTC, exclusive")
    args = ap.parse_args()

    start_ms, end_ms = parse_time(args.start), parse_time(args.end)
    con = connect()
    first, last = con.execute(f"SELECT min(arrival_ms), max(arrival_ms) FROM read_parquet('{raw_glob()}')").fetchone()
    # The recorder writes in batches, so the newest minutes are not on disk yet. A window that
    # reaches past the last recorded message would be silently shorter than its manifest says.
    if start_ms < first or end_ms > last:
        raise SystemExit(f"window {iso(start_ms)}..{iso(end_ms)} is not inside the recording "
                         f"{iso(first)}..{iso(last)}")
    topic = topic_name(args.name)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    create_topic(admin, topic)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "compression.type": "zstd",
            "linger.ms": 50,
            "batch.size": 1_000_000,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    hasher = Hasher()
    for offset, arrival_ms, raw in iter_window(con, start_ms, end_ms):
        hasher.add(offset, raw)
        while True:
            try:
                producer.produce(topic, value=raw.encode("utf-8"), timestamp=arrival_ms)
                break
            except BufferError:
                producer.poll(0.5)
        if hasher.count % 1_000_000 == 0:
            producer.poll(0)
            log.info("%d messages", hasher.count)
    remaining = producer.flush(120)
    if remaining:
        raise SystemExit(f"{remaining} messages not delivered")

    manifest = hasher.manifest(args.name, start_ms, end_ms)
    manifest.write(DATA / "windows" / args.name / "manifest.json")
    log.info("replayed %d messages into %s, sha256 %s", manifest.messages, topic, manifest.sha256[:12])


if __name__ == "__main__":
    main()
