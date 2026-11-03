# Design

## Pipeline

```
BarentsWatch SSE ─► ingest ─► Kafka ais.raw (1 partition) ─► recorder ─► Parquet (the recording)
                         └──► Kafka ais.sessions (connects and disconnects)

recording window ─► replay ─► Kafka replay.<window> (1 partition, arrival order)
                         ├──► masks: port areas, coverage, ingest outages
                         ├──► shore: coast and fish farms near slow vessels (Kartverket, Fiskeridirektoratet)
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
| Loitering | Under 1.5 kn outside every port area for at least 2 h, without holding position at the shore or at a fish farm | Holding position is 90% of positions within 1 km of the start. Where the episode began is measured against the coast and the licensed fish farms. All flagged, not filtered. A silence over 1 h ends the episode. Fishing, towing, dredging, diving, military, pilot, SAR, tug, port tender, anti-pollution, law enforcement and medical vessels are not considered: stopping at sea is their work |
| AIS gap | No position for at least 1 h, then a position again | Every such silence is emitted with: underway when it went quiet (≥ 1 kn), both ends inside continuous coverage, ingest outage overlapping. The funnel is reported, not hidden in a filter. A vessel that never comes back within the window is not a gap: there is no second end |
| Rendezvous | Two vessels within 500 m, both under 1 kn, outside port areas, for at least 30 min, not both holding position at the shore, not at a fish farm | Holding position: both vessels within 1 km of one point since before they came together. Shore and fish farm as for loitering. All flagged, not filtered. Towing, pilot, SAR, tug, port tender, anti-pollution and law enforcement vessels are not considered: coming alongside is their trade |

Episodes still open when the input ends are emitted with `truncated: true`.

Loitering and rendezvous also carry `approached`: a vessel involved was moving (1 kn or more) within
the hour before the episode began. In a 90-minute window, 180 of 235 rendezvous were already in
place in the first five minutes and 190 were still in place at the end: boats moored side by side
at quays and fish farms the port mask had not caught. 41 were approached and 7 of those were
complete (approach, stay, part). The flag is reported, not filtered on, so the funnel stays visible.
`VesselCleaner` keeps each vessel's last underway time and stamps it on every clean position.

**Rendezvous as a spatial join, then per pair.** Finding pairs is keyed by grid cell (0.01° ×
0.02°). A position goes to its own cell as a home visit and to every cell within 500 m of it as a
visitor, so two vessels in range always share a cell. A cell reports a sighting only when the lower
MMSI is at home in it, which reports each pair once instead of once per shared cell. A pair is
together as of the older of the two reports, so a vessel that has left cannot keep a pair alive
with its last report from the cell.

Episodes are kept per pair, in a second operator keyed by the two MMSIs, which buffers sightings
by the event time of the cell batch that produced them and applies them in that order. They used
to be kept in the cell, which cut a pair drifting together into one episode per cell it crossed
(about one per kilometre), each too short or too still to mean anything.

**Holding position.** `VesselCleaner` stamps every clean position with `heldSinceMs`: since when the
vessel has stayed within 1 km of one point. A position outside the radius moves that point only if
the next one is outside too, so one stray fix does not. Loitering uses the share of an episode's
positions within 1 km of its start; rendezvous asks whether both vessels had held position since
before the pair came together, compared with the newer report of the first sighting (a vessel first
heard at the start of the input is held only since its own first report).

**Where it held position.** Holding position says moored or at anchor, not which. A last operator,
at parallelism 1 and without state, stamps every loitering and rendezvous episode with where it
began: `quay` within 50 m of a built coast (quay, pier, jetty, breakwater), `coast` within 50 m of
the natural coastline, `near` within 200 m, `open` beyond; and `atFarm`, within 200 m of a licensed
fish farm. Held at the shore is moored. Held in open water is at anchor or holding station, which
is where a transfer between two ships would happen, so it stays a candidate. The 50 to 200 m band is
left undecided.

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

### Context from outside the recording

Two public layers, downloaded once from Geonorge and cut to the window by `northwind.shore`:

- **Coast**: Kartverket's *Sjøkart - Dybdedata* (CC BY 4.0), the national file in EPSG:4258. Built
  coast (`kaibrygge`, `pirkant`, `utstikker`, `molo`, dock edges, ramps, slipways) and the natural
  coastline (`kystkontur`), simplified by about 3 m and kept as segments only in the port-grid
  cells where a vessel reported under 1.5 kn, grown by two cells. For `w4h`: 2.4 million segments,
  121 thousand of them built, 108 MB, in 51 s. The job indexes them by cell and measures exact
  distances in a local flat projection.
- **Fish farms**: the Directorate of Fisheries' *Akvakultur - lokaliteter* (NLOD), the outlines of
  1,427 licensed sites. The register also names owners and contacts; only the outlines are read.

Two more were looked at and not used. Kystverket's anchorage areas (NLOD) hold no episode of
`w4h`: the port mask already covers the anchorages vessels used. Kartverket's harbour and maritime
infrastructure layers would map berths and pens directly, but they are restricted to Norge digitalt
members.

## The four-hour window

`scripts/run-window.sh w4h 2026-10-13T10:00:00Z 2026-10-13T14:00:00Z 120`, on Docker Compose on one
machine: two TaskManagers with one slot each, parallelism 2, heap state backend. The job read the
window's 3,429,777 messages from Kafka in 17.6 s, and its rerun in 16.5 s, with the shore step below
(loading the coast index is part of that). The whole script, replay to recovery test, took between
2 min 14 s and 2 min 22 s on the earlier runs of this window that were timed.

| Check | Result |
|---|---|
| Conservation | 3,429,777 in; 3,231,990 positions, 149,745 static, 22,816 duplicates, 16,434 outside the window, 8,789 late (all satellite), 3 invalid (implied speed) |
| Determinism | the rerun's events, rejected messages and totals are identical |
| Recovery | a TaskManager killed after the second checkpoint; the restored run's output is identical to the clean run's |
| Exactly once | the killed run's topic, read committed: 2,944 records, the same 2,944 as its files, no duplicates |

| Pattern | Detected | Funnel |
|---|---|---|
| Port call | 1,903 | 226 left inside the window |
| Rendezvous | 284 | 67 did not hold position at the shore; 7 of those not at a fish farm; 4 approached; 3 parted inside the window |
| AIS gap | 121 | 26 underway when they went quiet, 8 of those covered at both ends, 8 clear of an outage |
| Loitering | 636 | 156 did not hold position at the shore; 68 of those not at a fish farm; 7 arrived underway; 3 ended inside the window |

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

**Rendezvous had the same problem.** In the first runs 291 pairs were together 30 minutes or more.
With episodes kept per pair, fragments of drifting pairs join up and there are 284. Of those, 280
held position: boats alongside each other at quays the port mask misses, and ships anchored
together. The 4 that moved stayed 5 to 24 m apart, so alongside, while moving together:

| Types | Together | Closest | Where | Funnel |
|---|---|---|---|---|
| Cargo and cargo | 2 h 54 min, still open at the end | 21 m | 63.0°N 7.7°E | approached |
| Other and other | 1 h 18 min | 24 m | 65.1°N 12.4°E | approached, parted: through every step |
| Dredger and cargo | 2 h 40 min | 10 m | 69.0°N 15.3°E | |
| Cargo and cargo | 2 h 42 min, still open at the end | 5 m | 68.3°N 14.9°E | approached |

**Holding position could not tell a quay from an anchorage.** Two ships at anchor together
offshore, where a ship-to-ship transfer would happen, hold position just like two boats tied up at
a quay. The AIS alone does not separate them: 48% of the held loitering episodes report no
heading, so swinging at anchor cannot be seen, and of the vessels declaring themselves at anchor
about half kept a steady heading for four hours. Where they held position does. Measured against
the coast, the port calls (vessels known to be in port) began 11 m from it at the median and 90%
within 66 m. The held episodes split as:

| Began | Loitering held | Rendezvous held |
|---|---|---|
| Within 50 m of a quay | 278 | 118 |
| Within 50 m of the natural coast | 105 | 34 |
| 50 to 200 m | 97 | 65 |
| Open water, beyond 200 m | 153 | 63 |

Two independent signals agree with the split. Of the 27 held loitering vessels whose heading swung
(spread of 20° or more, positions spread 25 m or more), 23 were in open water; of the 35 declaring
"at anchor", 28 were. Of the 187 declaring "moored", 143 began within 50 m of the shore.

Open water is not empty. 88 of the 153 loitering episodes and 56 of the 63 rendezvous held in open
water began within 200 m of a licensed salmon farm: well boats and feed carriers at the pens, which
AIS mostly lists as cargo. Without that step, 18 loitering episodes and 21 rendezvous went through
the whole funnel, and 15 and 18 of them were at a farm.

Six are left, three loitering and three rendezvous. Checked by hand against the Norwegian Offshore
Directorate's facility register (NLOD), three lie within a kilometre of a producing oil or gas
installation: a tanker 90 m from Njord Bravo, a tanker and a cargo vessel 460 m from the Johan
Castberg FPSO, a vessel declaring restricted manoeuvrability 650 m from the Fulla template. The
other three are not explained by anything this pipeline knows. The facility register is not a step
in the funnel; with a handful of survivors, reading them is cheaper than another layer.

## A day live on AWS

Window `d1`: Wednesday 2026-10-28, 00:00 to 24:00 UTC. The same Compose stack ran on one EC2
`t4g.large` (Graviton, ARM) in `eu-north-1`, recording from 23:00 the day before to 01:00 the day
after. The job ran in live mode on `ais.raw` and kept what arrived inside the window. The machine
is in `infra/run`, and systemd timers start and stop the run (`scripts/aws/`). Metrics were read
from Flink's REST API every five minutes, 312 samples:

| Measured | Over the 26 hours |
|---|---|
| Read from `ais.raw` | 20,866,853 messages, 19,535,335 of them inside the window |
| Behind the feed | 0 records at every sample; arrival to emission at the source 112 ms median, 202 ms p95, 207 ms max |
| State | 22.6 MB median, 24.9 MB max |
| Checkpoints | 3,110 completed, 264 ms median, 580 ms max. Two failed: one triggered before the tasks were running, one declined while the job drained |
| Feed | 26 planned reconnections to refresh the token, and one dropped stream at 17:56:05, back in 1.2 s |

Replayed afterwards on x86 from the recording, with the same masks and lateness bound, the window
gave the same 145,436 rejected messages and the same totals, byte for byte, and the same 9,037
events. Byte for byte, 39 of those events were not the same. Each had one distance
(`maxDriftKm`, `minDistanceM`, `distanceKm` or `impliedKnots`) that differed in its last digit,
0.38111385966453193 against 0.3811138596645318, a relative difference of at most 3.6e-16. Java
allows `Math.sin`, `cos` and `asin` to be off by an ulp, and the JIT uses each CPU's own routines:
the live run was on ARM, the replay on x86. The dry run had not shown it because its half hour had
no events. Distances now go through `StrictMath`, which gives the same bits on every platform.
Replayed with it on x86 and on emulated arm64 (QEMU under Docker), the window gave identical
events, rejected messages and totals, byte for byte. On x86, `Math` and `StrictMath` differ in 40
of the 9,037 events. The emulation was checked against the real machine: the old `Math` build
(`301e2db`) replayed on emulated arm64 reproduced the live run's output byte for byte, so on the
same architecture the live run and its replay are identical.

## The day as the analysis window

The four-hour window was a test. The page reads window `oct28`: the interval of `d1`, Wednesday
2026-10-28 00:00 to 24:00 UTC, the same 19,535,335 messages with the same sha256, replayed with
port, coverage and shore masks built from the day itself (400 port areas). `d1` keeps the masks the
live run used and the runs `live.json` compares, so nothing behind the live comparison moved. All
four checks hold on `oct28-b120`: conservation, a rerun, and a run with a TaskManager killed after
its fifth checkpoint, whose transactional topic held 7,399 records and no duplicate.

**Over a day, much of what loiters is not a ship.** With the four-hour window's funnel, the day left
269 loitering episodes and 59 rendezvous. Most of the loitering ones had no ship type, reported
"under way using engine" while holding position for about three hours, and clustered in the fjords
of Troms and Finnmark. Their MMSIs were not a ship station's: a 109 prefix, unassigned maritime
identification digits, seven digits. Their names, in the static reports and never exported, are
often a name followed by BUOY or BOUY and a number: of the 190 loitering episodes from a non-ship
MMSI on the day, 92 come from a station whose name says buoy or net. They are AIS buoys on fishing
nets, or devices like them. Two steps now open the loitering and rendezvous funnels: a ship station's
MMSI (ITU-R M.585: nine digits from a maritime identification digit of 2 to 7) and a declared ship
type. Of 1,623 loitering episodes, 190 fail the first, 92% of them north of 69°N, and 132 the second.

| Funnel | Loitering | Rendezvous |
|---|---|---|
| Detected | 1,623 | 414 |
| A ship station's MMSI | 1,433 | 377 |
| A declared ship type | 1,301 | 363 |
| Not held at the shore | 341 | 96 |
| Not at a fish farm | 161 | 22 |
| Arrived underway | 101 | 22 |
| Ended inside the window | 66 | 15 |

The 81 survivors are too many to check by hand, as the four-hour window's six were. Instead
`northwind.installations` writes the surface installations in service at sea from the Norwegian
Offshore Directorate's facility register (NLOD) beside the window's masks, ED50 positions converted
to WGS84, and the export names the one within 1 km of where an episode began. 13 of the 81 began
beside one, three of them within 50 m of the Norne production ship.

## What this does not claim

- An AIS gap is a fact about reception. It says nothing about why.
- A station whose MMSI has no ship form, or that declares no type, is taken out as a device. A real
  vessel with a malformed MMSI or no declared type goes out with the buoys.
- Beside an installation is a distance, not a purpose: supply and standby vessels wait there.
- Masks come from the same window the detectors run on. That is circular by design: they describe
  where vessels in this recording stop and where reception is continuous, not where ports are.
- No throughput or latency figure is stated anywhere without the run it came from.
- The live run of `d1` used `Math`, so it replays byte for byte only on ARM; on x86 its events match
  in number, vessels and times, with distances equal to fifteen significant digits. Byte for byte
  on any machine holds for runs made with `StrictMath`.
- The shore band is measured where an episode began, and the 50 to 200 m band is undecided. The
  farm register lists licensed sites, not which ones are stocked or worked that day.
