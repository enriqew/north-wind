"""Measure a window: message rate, mix, duplication and lateness.

Lateness is arrival time minus the time the message says it was sent. It is what decides the
watermark bound: a bound shorter than a stream's lateness turns that stream into late data.

Duplicates use the same notion as the job: one transmission received more than once, whatever
network received it (see AisRecord.signature in the Flink job).

    python -m northwind.profile --name day1
"""

from __future__ import annotations

import argparse
import json
import logging

from northwind.recording import DATA, Manifest, connect, raw_glob

log = logging.getLogger("northwind.profile")

DECODED = """
CREATE TEMP TABLE m AS
SELECT
    "offset",
    arrival_ms,
    json_extract_string(raw, '$.type') AS type,
    json_extract_string(raw, '$.stream') AS stream,
    json_extract(raw, '$.mmsi')::BIGINT AS mmsi,
    epoch_ms(TRY_CAST(json_extract_string(raw, '$.msgtime') AS TIMESTAMPTZ)) AS msgtime_ms,
    concat_ws('|', json_extract_string(raw, '$.type'), json_extract_string(raw, '$.messageType'),
              json_extract_string(raw, '$.mmsi'), json_extract_string(raw, '$.msgtime'),
              json_extract_string(raw, '$.latitude'), json_extract_string(raw, '$.longitude'),
              json_extract_string(raw, '$.speedOverGround'), json_extract_string(raw, '$.courseOverGround'),
              json_extract_string(raw, '$.trueHeading'), json_extract_string(raw, '$.navigationalStatus'),
              json_extract_string(raw, '$.shipType'), json_extract_string(raw, '$.shipLength')) AS sig
FROM read_parquet('{glob}')
WHERE arrival_ms >= {start} AND arrival_ms < {end}
"""

QUANTILES = [0.5, 0.9, 0.99, 0.999]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    args = ap.parse_args()

    window = DATA / "windows" / args.name
    m = Manifest.read(window / "manifest.json")
    con = connect()
    con.execute(DECODED.format(glob=raw_glob(), start=m.start_ms, end=m.end_ms))

    total = con.execute("SELECT count(*) FROM m").fetchone()[0]
    if total != m.messages:
        raise SystemExit(f"window holds {total} messages, manifest says {m.messages}")
    seconds = (m.end_ms - m.start_ms) / 1000

    mix = con.execute("SELECT type, stream, count(*) FROM m GROUP BY 1, 2 ORDER BY 3 DESC").fetchall()
    vessels = con.execute("SELECT count(DISTINCT mmsi) FROM m").fetchone()[0]
    per_minute = con.execute(
        "SELECT quantile_cont(n, [0.01, 0.5, 0.99]), max(n) FROM "
        "(SELECT arrival_ms // 60000 AS minute, count(*) AS n FROM m GROUP BY 1)"
    ).fetchone()
    duplicates = con.execute("SELECT count(*) - count(DISTINCT sig) FROM m").fetchone()[0]
    dup_by_stream = con.execute(
        "SELECT stream, count(*) FROM (SELECT stream, row_number() OVER (PARTITION BY sig ORDER BY \"offset\") AS k FROM m) "
        "WHERE k > 1 GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    q = ", ".join(str(x) for x in QUANTILES)
    lateness = con.execute(
        f"SELECT stream, count(*), quantile_cont((arrival_ms - msgtime_ms) / 1000.0, [{q}]), "
        "max((arrival_ms - msgtime_ms) / 1000.0), min((arrival_ms - msgtime_ms) / 1000.0) "
        "FROM m WHERE msgtime_ms IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    # Out-of-orderness as the watermark sees it: how far behind the largest event time seen so far
    # a message arrives. This is what a bound must cover, and it can differ from lateness.
    behind = con.execute(
        f"SELECT stream, quantile_cont(b, [{q}]), max(b), avg((b > 120)::INT) FROM ("
        "  SELECT stream, (max(msgtime_ms) OVER (ORDER BY \"offset\" ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
        "                  - msgtime_ms) / 1000.0 AS b"
        "  FROM m WHERE msgtime_ms IS NOT NULL"
        ") WHERE b IS NOT NULL GROUP BY 1 ORDER BY 1"
    ).fetchall()

    profile = {
        "window": {"name": m.name, "start": m.start, "end": m.end, "messages": m.messages, "sha256": m.sha256},
        "rate_per_second": round(total / seconds, 1),
        "per_minute": {"p01": per_minute[0][0], "p50": per_minute[0][1], "p99": per_minute[0][2], "max": per_minute[1]},
        "distinct_mmsi": vessels,
        "mix": [{"type": t, "stream": s, "messages": n} for t, s, n in mix],
        "duplicates": {"messages": duplicates, "share": round(duplicates / total, 5),
                       "by_stream": {s: n for s, n in dup_by_stream}},
        "lateness_s": [
            {"stream": s, "messages": n, **{f"p{str(k)[2:]:0<2}": round(v, 1) for k, v in zip(QUANTILES, qs)},
             "max": round(mx, 1), "min": round(mn, 1)}
            for s, n, qs, mx, mn in lateness
        ],
        "behind_max_event_time_s": [
            {"stream": s, **{f"p{str(k)[2:]:0<2}": round(v, 1) for k, v in zip(QUANTILES, qs)},
             "max": round(mx, 1), "share_over_120s": round(share, 5)}
            for s, qs, mx, share in behind
        ],
    }
    (window / "profile.json").write_text(json.dumps(profile, indent=2) + "\n")
    print(json.dumps(profile, indent=2))


if __name__ == "__main__":
    main()
