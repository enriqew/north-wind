# Export contract

`python -m northwind.export --window <w> --run <run>` turns one checked run into the static JSON the
portfolio page reads. It writes to `/data/exports/<run>/`, all or nothing, and refuses to write
anything unless every check below holds.

## Checks

| Check | Source | Fails when |
|---|---|---|
| Conservation | `runs/<run>/summary.json` (from `northwind.check`) | The job's input differs from the window's message count, or the fates do not sum to it |
| Determinism | `runs/<run>/rerun.json` (from `northwind.compare --out`) | A second run over the same window produced different events, rejected messages or totals |
| Recovery | `runs/<run>-killed/recovery-vs-clean.json` | The run with a TaskManager killed differs from the clean one |
| Exactly once | `runs/<run>-killed/recovery-vs-kafka.json` | The committed topic differs from the run's files, or holds a duplicate |
| Same window | the summary and the compare reports | The summary names another window's sha256, or a report compares other runs than this one, its rerun and its killed run |

`scripts/run-window.sh` produces all of them.

## Files

Every file carries `schema_version` (currently `1`). A reader must refuse a version it does not know.

| File | Holds |
|---|---|
| `manifest.json` | The window (interval, message count, sha256), the run and its lateness bound, the thresholds the detectors used, the result of each check, and the attribution lines |
| `fates.json` | Message totals per fate, per receiving network, and invalid messages per reason |
| `lateness.json` | Lateness (arrival minus event time) per network: quantiles and a log-binned histogram, from the window's profile and the recording |
| `traffic.json` | Position reports received per coverage cell (0.1° × 0.2°) per hour of the window, from the recording, for the map |
| `events.json` | Every detection, with the funnel fields (`underway`, `coveredStart`, `coveredEnd`, `outageOverlap`, `approached`, `heldPosition`, `shore`, `atFarm`, `truncated`) and, for loitering and rendezvous, `shore_m`: the distance to the nearest coast, rounded to 10 m |
| `funnels.json` | Per pattern, how many detections pass each successive filter |

## The live run

`python -m northwind.export_live --window <w> --live <run> --bound-s <s> --same-cpu <run>
--other-cpu <run> --strict <run> <run>` writes `/data/exports/<live run>/live.json`: a run made on
the live stream (`scripts/aws/`), set beside replays of its own recording. The page reads it next
to the window export above, which stays the analysis; this file is about the job running live.

| Check | Fails when |
|---|---|
| Conservation | `runs/<live>/summary.json` (from `northwind.check`) names another window, or its fates do not add up |
| Same CPU | The replay of the same build on the live run's CPU differs from the live run in events, rejected messages or totals |
| StrictMath | The two replays of the `StrictMath` build, one per CPU, differ |
| Other CPU | The replay of the same build on another CPU rejects or counts messages differently, or an event it gives differently differs in anything but a distance (`distanceKm`, `impliedKnots`, `minDistanceM`, `maxDriftKm`), or by 1e-12 relative or more |
| Log | The ingest's log has a session that closed for an unknown reason or never reopened, or Flink's processes report more than one CPU architecture |

| Key | Holds |
|---|---|
| `window`, `run` | The window, the live run's lateness bound, and the CPU architecture Flink reported at startup |
| `conservation`, `events` | The live run's input and its number of detections |
| `health` | The sampler's figures over the run (records read, lag in records and in milliseconds, state size, checkpoints) as nearest-rank quantiles, and the samples behind them |
| `arrivals` | Messages recorded per clock hour of arrival, from the recording |
| `feed` | The ingest's sessions: how many, the planned token refreshes and the longest gap one left, and every error with the time it took to reconnect |
| `replays` | The three comparisons, each output as line counts and whether they are identical; for the other CPU, the differing events paired, counted by field, and the largest difference as the job wrote both numbers |

Nothing in it identifies a vessel: the differing events are reduced to a field name and two numbers.

## What is not exported

- **No MMSI, no vessel name, no call sign.** Each vessel gets an id local to the export (`v1`, `v2`,
  ...), assigned in order of its first detection, so detections of one vessel can be linked on the
  page and to nothing outside it.
- **Ship type as a category, not a code**: cargo, tanker, passenger, fishing, service (tug, pilot,
  SAR, port tender, law enforcement), high speed, leisure, other, unknown.
- **Positions rounded to 0.01°** (about a kilometre).
- Nothing from the raw recording: BarentsWatch's terms forbid altering the data, and the export is
  derived aggregates, not redistributed messages.

## Attribution

`manifest.json` carries these lines and the page must show them:

- Contains data under the Norwegian licence for Open Government Data (NLOD) distributed by the
  Norwegian Coastal Administration.
- Data delivered by BarentsWatch.
- Coastline © Kartverket, CC BY 4.0.
- Contains data under the Norwegian licence for Open Government Data (NLOD) distributed by the
  Directorate of Fisheries.
