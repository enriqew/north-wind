"""Compare runs, and a run with what it published to Kafka.

Two runs over the same window must produce the same detections, the same rejected messages and the
same totals. Order within a file is not part of the contract (several subtasks write in parallel),
so each output is compared as a multiset of lines.

    python -m northwind.compare --runs w1-b120 w1-b120-killed
    python -m northwind.compare --runs w1-b120-killed --topic events.w1-b120-killed

With --topic, the topic is read with isolation.level=read_committed, which is what a downstream
consumer of an exactly-once sink sees, and compared with the run's own events/ files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from northwind.recording import DATA

OUTPUTS = ["events", "rejected", "audit"]
BOOTSTRAP = os.environ.get("NW_KAFKA", "kafka:9092")


def lines(run: str, output: str) -> Counter:
    directory = DATA / "runs" / run / output
    counter: Counter = Counter()
    for path in sorted(directory.glob("*.jsonl")) if directory.exists() else []:
        if path.name.startswith("."):
            continue
        with path.open() as f:
            counter.update(line.rstrip("\n") for line in f if line.strip())
    return counter


def digest(counter: Counter) -> str:
    h = hashlib.sha256()
    for line in sorted(counter.elements()):
        h.update(line.encode())
        h.update(b"\n")
    return h.hexdigest()


def committed(topic: str) -> Counter:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"northwind-compare-{os.getpid()}",
        "isolation.level": "read_committed",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p, 0) for p in meta.partitions]
    consumer.assign(parts)
    remaining = set(meta.partitions)
    counter: Counter = Counter()
    while remaining:
        msg = consumer.poll(10)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                remaining.discard(msg.partition())
                continue
            raise RuntimeError(msg.error())
        counter[msg.value().decode()] += 1
    consumer.close()
    return counter


def create_topic(topic: str, partitions: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if topic in admin.list_topics(timeout=10).topics:
        raise SystemExit(f"{topic} already exists")
    admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic].result()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", nargs="+", required=True)
    ap.add_argument("--topic")
    ap.add_argument("--create-topic", action="store_true", help="only create --topic and exit")
    args = ap.parse_args()

    if args.create_topic:
        create_topic(args.topic)
        print(f"created {args.topic}")
        return

    report: dict = {"runs": args.runs, "outputs": {}}
    ok = True
    for output in OUTPUTS:
        sets = [lines(r, output) for r in args.runs]
        digests = [digest(c) for c in sets]
        same = len(set(digests)) == 1
        ok &= same
        entry = {"lines": [sum(c.values()) for c in sets], "sha256": [d[:16] for d in digests], "identical": same}
        if not same and len(sets) == 2:
            entry["only_in_first"] = sum((sets[0] - sets[1]).values())
            entry["only_in_second"] = sum((sets[1] - sets[0]).values())
        report["outputs"][output] = entry

    if args.topic:
        files = lines(args.runs[-1], "events")
        published = committed(args.topic)
        same = files == published
        ok &= same
        report["kafka"] = {
            "topic": args.topic,
            "committed_records": sum(published.values()),
            "file_records": sum(files.values()),
            "duplicates_in_topic": sum(n - 1 for n in published.values() if n > 1),
            "identical_to_files": same,
        }

    report["identical"] = ok
    print(json.dumps(report, indent=2))
    if not ok:
        raise SystemExit("runs differ")


if __name__ == "__main__":
    main()
