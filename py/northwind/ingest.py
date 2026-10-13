"""Live AIS into Kafka.

Each SSE payload becomes one record on `ais.raw`, keyed by nothing and stamped with the wall-clock
time it arrived as the Kafka record timestamp. The topic has one partition, so the order of the
topic is the order of arrival, which is what makes a recording replayable in the same order.

Every connection is logged to `ais.sessions` as an open and a close with wall-clock times. A vessel
that goes silent while this process was disconnected did not go silent at all, and the gap detector
reads these sessions to tell the two apart.

    python -m northwind.ingest
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import time
import uuid

import requests
from confluent_kafka import Producer

from northwind import barentswatch as bw

log = logging.getLogger("northwind.ingest")

RAW_TOPIC = os.environ.get("NW_RAW_TOPIC", "ais.raw")
SESSIONS_TOPIC = os.environ.get("NW_SESSIONS_TOPIC", "ais.sessions")
BOOTSTRAP = os.environ.get("NW_KAFKA", "kafka:9092")
BACKOFF_MAX_S = 60

_stop = False


def _on_signal(*_: object) -> None:
    global _stop
    _stop = True


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": f"northwind-ingest-{socket.gethostname()}",
            "compression.type": "zstd",
            "linger.ms": 200,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def log_session(producer: Producer, session_id: str, event: str, **fields: object) -> None:
    record = {"session": session_id, "event": event, "at_ms": now_ms(), **fields}
    producer.produce(SESSIONS_TOPIC, value=json.dumps(record).encode(), timestamp=record["at_ms"])
    producer.flush(10)
    log.info("session %s %s %s", session_id[:8], event, fields or "")


def run() -> None:
    client_id, client_secret = bw.credentials_from_env()
    producer = make_producer()
    http = requests.Session()
    token: bw.Token | None = None
    backoff = 1.0

    while not _stop:
        session_id = uuid.uuid4().hex
        received = 0
        reason = "stopped"
        try:
            if token is None or not token.fresh():
                token = bw.fetch_token(client_id, client_secret, http)
            resp = bw.open_stream(token, http)
            log_session(producer, session_id, "open")
            backoff = 1.0
            last_report = time.monotonic()
            for payload in bw.sse_payloads(resp.iter_lines(chunk_size=65536)):
                producer.produce(RAW_TOPIC, value=payload, timestamp=now_ms())
                received += 1
                producer.poll(0)
                if time.monotonic() - last_report > 60:
                    log.info("session %s received %d", session_id[:8], received)
                    last_report = time.monotonic()
                if _stop:
                    break
                # Reconnect with a fresh token before the current one expires, rather than
                # finding out mid-stream whether the server enforces expiry on open connections.
                if not token.fresh():
                    reason = "token_refresh"
                    break
            else:
                reason = "server_closed"
            resp.close()
        except (requests.RequestException, ValueError) as exc:
            reason = f"error: {type(exc).__name__}: {exc}"[:300]
        finally:
            producer.flush(30)
            if received or reason != "stopped":
                log_session(producer, session_id, "close", reason=reason, received=received)

        if not _stop and reason != "token_refresh":
            log.warning("reconnecting in %.0fs after %s", backoff, reason)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_S)

    producer.flush(30)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    run()


if __name__ == "__main__":
    main()
