# Design

## Pipeline

```
BarentsWatch SSE ─► ingest ─► Kafka ais.raw (1 partition) ─► recorder ─► Parquet (the recording)
                         └──► Kafka ais.sessions (connects and disconnects)

recording window ─► replay ─► Kafka replay.<window> (1 partition, arrival order)
                         ├──► masks: port areas, coverage, ingest outages
                         └──► profile: rate, mix, duplicates, lateness

replay.<window> ─► Flink job ─► events/   detections
                             ├► rejected/ late and invalid messages
                             └► audit/    one total per fate, written at end of input
                                   └──► check: conservation, detection summary
```

Nothing reads the live stream except the ingest. Every analysis runs over a **window**: a half-open
interval of arrival time over the recording, named by a manifest that carries its message count and
a sha256 over its messages in arrival order. The live feed cannot be replayed; a window can, as many
times as needed, and that is what makes a run checkable.

## Ingest and recording

`ingest` reads `POST /v1/sse/ais` with positions and static reports only, and writes each message's
bytes unchanged to `ais.raw`. The arrival time goes in the Kafka record timestamp, not in the
message: BarentsWatch's terms forbid altering the data, and the recording should be exactly what
was received. Each connection writes an open and a close to `ais.sessions`; the token is refreshed
by reconnecting two minutes before it expires.

`recorder` drains both topics into Parquet. Each file is one contiguous range of offsets,
`part-<first>-<last>.parquet`, written under a temporary name and renamed into place. The file names
are the recorder's only commit log: on restart it resumes at the offset after the highest one on
disk, so a crash can neither lose a range nor write one twice. If the topic no longer holds that
offset, it stops rather than guess.

Both topics have one partition, so topic order is arrival order.

### Measured

One recording, 2026-10-13 09:43 to 14:27 UTC (4 h 44 min), read back from the Parquet files:

| | |
|---|---|
| Messages | 4,066,561, 238 per second on average |
| On disk | 113 MB of zstd Parquet, 28 bytes per message, about 575 MB a day |
| Mix | 91.3% terrestrial positions, 4.4% static reports, 3.0% offshore and 1.2% satellite positions |
| Connections | 6 sessions: 4 closed for the hourly token refresh, 1 by the server mid-stream; each reconnect took 1 to 2 s |

The window `w4h` (10:00 to 14:00, 3,429,777 messages, `northwind.profile`):

| | Terrestrial | Offshore | Satellite |
|---|---|---|---|
| Messages | 3,281,108 | 108,023 | 40,646 |
| Lateness p50 | 3.2 s | 1.9 s | 1 h 39 min |
| Lateness p99 | 4.6 s | 2.9 s | n/a, see below |
| Lateness max | 20.9 s | 4.8 s | n/a, see below |
| More than 120 s behind the latest event time | 0 | 0 | 61% |

Per minute the rate ran from 13,585 (p1) to 15,896 (p99) messages, from 4,399 distinct MMSIs. The
job counted 22,816 duplicates (0.7%), nearly all one broadcast heard by two terrestrial stations.

## The job

### Fates

Every message ends in exactly one place, and the job counts them:

| Fate | Decided by | Meaning |
|---|---|---|
| outside_window | decode | Sent before the window opened or after it closed (see below) |
| invalid | decode, track filter | Undecodable, no position, default coordinates, out of range, stamped in the future, or an impossible jump. Kept with its reason |
| late | late split | Event time at or behind the watermark |
| duplicate | dedup | A transmission already seen, possibly through another receiver network |
| static | dedup | A static report: type and length, remembered for the vessel |
| position | track filter | A clean position, in event-time order for its vessel |

The check fails unless `input` equals the window's message count and the fates sum to `input`.

### Event time and lateness

The watermark is bounded out-of-orderness over `msgtime`, but emitted on every event rather than
periodically. Flink's built-in generator emits on a processing-time timer, so which messages count
as late depends on how fast the machine happens to replay them. Emitted per event, message `n` is
judged against the largest event time among messages `0..n-1`, on any machine, at any speed.

The source, the decoder, the watermark assigner and the late split all run at parallelism 1. After
that the work is keyed, and every keyed operator buffers by event time and acts only when the
watermark passes, in a fixed order (offset, or MMSI then offset). Nothing downstream depends on
which subtask delivered first. Two runs over the same window give the same output; this is checked,
not assumed.

Satellite receptions arrive tens of minutes after coastal ones. With a bound of a few minutes they
are late by construction: counted, written to `rejected/`, and left out of the detections. A bound
long enough to admit them delays every detection by as much. The bound is a run parameter and the
run's name records it.

### Event time is bounded by the window too

A window is cut on arrival time, but a message's event time can be far older: a satellite reception
arriving at 09:46 may describe 07:55. With a watermark bound long enough to admit satellite data,
those early positions entered the job, and every vessel whose track began with one sparse
satellite fix and resumed with coastal reception an hour and a half later produced an AIS gap that
never happened. On a 90-minute window a two-hour bound gave 417 gaps and 118 loitering episodes;
with the window also applied to event time, it gives 8 gaps and none, the same as a two-minute
bound. Messages with an event time outside the window are their own fate, `outside_window`,
counted like the rest. The job reads the interval from the window's manifest.

The bound also absorbs a fault in the feed. In the four-hour window 7,471 satellite messages (18%
of the window's satellite traffic, from 1,964 vessels) carry an event time between 00:15 and 01:38
UTC on 2014-01-01: a receiver clock reset to its default, not a time. That is why the satellite
lateness quantiles past p50 read as twelve years. The messages are kept as received, end as
`outside_window`, and never reach a detector.

### Per vessel

`VesselCleaner`, keyed by MMSI:

- **Dedup** on a signature of the decoded fields, excluding the receiver network: one broadcast heard
  by a coastal station and an offshore one is one broadcast. Signatures are kept for ten minutes of
  event time.
- **Static reports** update the vessel's type and length, newest by event time.
- **Ordering**: positions wait in state until the watermark passes them, then leave in event-time
  order.
- **Track filter**: a position that would need more than 60 knots (and more than 2 km) from the last
  accepted one is invalid. A rejected position is kept as a candidate; if the next one agrees with
  it rather than with the last accepted position, the track moves. Otherwise one bad anchor would
  make every later position look impossible.

### Detectors

All on clean, ordered positions.

| Pattern | Rule | Notes |
|---|---|---|
| Port call | Under 0.5 kn inside a port area for 30 min opens a call, dated from when the vessel stopped; the first position outside any port area closes it | Manoeuvring inside the port does not end the call |
| Loitering | Under 1.5 kn outside every port area for at least 2 h, without holding position | Holding position is 90% of positions within 1 km of the start: moored or at anchor. Flagged, not filtered. A silence over 1 h ends the episode. Fishing, towing, dredging, diving, military, pilot, SAR, tug, port tender, anti-pollution, law enforcement and medical vessels are not considered: stopping at sea is their work |
| AIS gap | No position for at least 1 h, then a position again | Every such silence is emitted with: underway when it went quiet (≥ 1 kn), both ends inside continuous coverage, ingest outage overlapping. The funnel is reported, not hidden in a filter. A vessel that never comes back within the window is not a gap: there is no second end |
| Rendezvous | Two vessels within 500 m, both under 1 kn, outside port areas, for at least 30 min | Towing, pilot, SAR, tug, port tender, anti-pollution and law enforcement vessels are not considered: coming alongside is their trade |

Episodes still open when the input ends are emitted with `truncated: true`.

Loitering and rendezvous also carry `approached`: a vessel involved was moving (1 kn or more) within
the hour before the episode began. In a 90-minute window, 180 of 235 rendezvous were already in
place in the first five minutes and 190 were still in place at the end: boats moored side by side
at quays and fish farms the port mask had not caught. 41 were approached and 7 of those were
complete (approach, stay, part). The flag is reported, not filtered on, so the funnel stays visible.
`VesselCleaner` keeps each vessel's last underway time and stamps it on every clean position.

**Rendezvous as a spatial join.** The stream is keyed by grid cell (0.01° × 0.02°). A position goes
to its own cell as a home visit and to every cell within 500 m of it as a visitor, so two vessels in
range always share a cell. A pair is tracked only in the cell where the lower MMSI is at home, which
reports it once instead of once per shared cell. A pair is together as of the older of the two
reports, so a vessel that has left cannot keep a pair alive with its last report from the cell.

### Masks

Built from the window itself (`northwind.masks`), not from an external registry:

- **Port areas**: fine cells where at least three vessels each sit under 0.5 kn for 20 minutes or
  more, grown by one cell and grouped by 8-connectivity. Stopping is measured, not declared: many
  vessels report "under way using engine" while tied up, and a mask built on the declared status
  missed whole fishing harbours.
- **Coverage**: coarse cells (0.1° × 0.2°) with a coastal or offshore position in at least 90% of
  the window's hours.
- **Outages**: arrival-time intervals with no message for over a minute, padded by two minutes.

The grid formula, `floor(lat / dlat)` and `floor(lon / dlon)`, exists in Java and Python and is
pinned by a test on each side.

## The four-hour window

`scripts/run-window.sh w4h 2026-10-13T10:00:00Z 2026-10-13T14:00:00Z 120`, on Docker Compose on one
machine: two TaskManagers with one slot each, parallelism 2, heap state backend. The job read the
window's 3,429,777 messages from Kafka in 17.9 s, and its rerun in 15.0 s. The whole script,
replay to recovery test, took 2 min 22 s.

| Check | Result |
|---|---|
| Conservation | 3,429,777 in; 3,231,990 positions, 149,745 static, 22,816 duplicates, 16,434 outside the window, 8,789 late (all satellite), 3 invalid (implied speed) |
| Determinism | the rerun's events, rejected messages and totals are identical |
| Recovery | a TaskManager killed after the second checkpoint; the restored run's output is identical to the clean run's |
| Exactly once | the killed run's topic, read committed: 2,951 records, the same 2,951 as its files, no duplicates |

| Pattern | Detected | Funnel |
|---|---|---|
| Port call | 1,903 | 226 left inside the window |
| Rendezvous | 291 | 129 approached, 67 of those also parted inside the window |
| AIS gap | 121 | 26 underway when they went quiet, 8 of those covered at both ends, 8 clear of an outage |
| Loitering | 636 | 3 did not hold position, none of those arrived underway |

**Loitering, as first detected, was mostly vessels tied up outside the port mask.** 615 of the 636
episodes stayed within 0.2 km of where they started; 349 are cargo ships and 187 declare themselves
moored. The port mask needs three vessels stopped in the same cell to call it a port, so a quay
with one berth, an industrial jetty or an anchorage used by one ship at a time is not one, and a
ship alongside it for two hours is "slow outside every port area".

The detector now flags `heldPosition`: at least 90% of the episode's positions within 1 km of where
it began. A kilometre is wider than a large ship's swing at anchor and narrower than two hours of
drift at half a knot; counting the share outside it rather than the furthest fix keeps one stray
position from undoing it. The flag is the funnel's first step after detection, not a filter. Rerun
on the same window (same sha256, all four checks again), 633 of the 636 held position and 3 did
not:

| Type | Moved | Over | Where |
|---|---|---|---|
| Other (90) | 2.3 km | 2 h 18 min, 820 positions | North Sea, 56.4°N 3.0°E, in the Ekofisk area |
| Cargo | 1.6 km | 2 h 01 min, 1,116 positions | Off Tromsø, 69.8°N 19.2°E |
| Unknown | 3.5 km | 3 h 56 min, 38 positions, still open at the end | Near Tromsø, 69.6°N 18.0°E |

None of the three arrived underway within the window, so none passes the whole funnel. Three
candidates in four hours is the honest size of loitering on this coast once mooring and anchoring
are taken out. A berth layer (an external quay register) would still be the better port mask; the
flag makes the detection right without one.

## What this does not claim

- An AIS gap is a fact about reception. It says nothing about why.
- Masks come from the same window the detectors run on. That is circular by design: they describe
  where vessels in this recording stop and where reception is continuous, not where ports are.
- No throughput or latency figure is stated anywhere without the run it came from.
