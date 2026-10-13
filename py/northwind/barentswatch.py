"""BarentsWatch Live AIS: client-credentials token and the raw SSE stream.

The stream is consumed as bytes and passed on untouched. BarentsWatch's terms forbid changing the
data content, and the recording is meant to be the exact input the pipeline replays, so nothing
here parses or rewrites a message. Arrival time travels beside the message, never inside it.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from dataclasses import dataclass

import requests

TOKEN_URL = "https://id.barentswatch.no/connect/token"
STREAM_URL = "https://live.ais.barentswatch.no/v1/sse/ais"

# Positions and static data only. Aids to navigation, safety messages and met-hydro broadcasts
# are not vessels, so they are not requested rather than filtered out downstream.
STREAM_FILTER = {
    "includePosition": True,
    "includeStatic": True,
    "includeAton": False,
    "includeSafetyRelated": False,
    "includeBinaryBroadcastMetHyd": False,
    "downsample": False,
}

# Refresh a little before the token's own expiry so a reconnect never starts with a dead token.
TOKEN_MARGIN_S = 120


@dataclass
class Token:
    value: str
    expires_at: float

    def fresh(self, now: float | None = None) -> bool:
        return (now or time.time()) < self.expires_at - TOKEN_MARGIN_S


def fetch_token(client_id: str, client_secret: str, session: requests.Session | None = None) -> Token:
    http = session or requests
    resp = http.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "ais",
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    return Token(body["access_token"], time.time() + float(body["expires_in"]))


def credentials_from_env() -> tuple[str, str]:
    try:
        return os.environ["BW_CLIENT_ID"], os.environ["BW_CLIENT_SECRET"]
    except KeyError as missing:
        raise SystemExit(f"{missing} is not set; copy .env.example to .env") from None


def sse_payloads(lines: Iterator[bytes]) -> Iterator[bytes]:
    """Yield the payload of each `data:` line. Comments (`: start`) and blank lines are skipped.

    BarentsWatch sends one JSON object per `data:` line, so multi-line SSE events do not occur;
    a line that is not `data:` carries nothing to keep.
    """
    for line in lines:
        if line.startswith(b"data:"):
            payload = line[5:].strip()
            if payload:
                yield payload


def open_stream(token: Token, session: requests.Session, read_timeout_s: float = 60) -> requests.Response:
    resp = session.post(
        STREAM_URL,
        json=STREAM_FILTER,
        headers={"Authorization": f"Bearer {token.value}", "Accept": "text/event-stream"},
        stream=True,
        timeout=(30, read_timeout_s),
    )
    resp.raise_for_status()
    return resp
