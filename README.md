# North Wind

Pattern detection over live AIS (the position reports ships broadcast) in Norwegian waters, with
Apache Flink. Port calls, loitering, AIS gaps and rendezvous between vessels, detected in event
time over a feed that is duplicated, out of order and, for satellite receptions, the better part
of an hour late.

The subject is less the patterns than what a real AIS feed does to a streaming job, and how the job
stays correct anyway:

- **Every message has exactly one fate.** Position, static report, duplicate, late or invalid
  (with a reason). The job counts its input and each fate, and a run fails its check unless they
  add up to the recorded window.
- **The same input gives the same output.** Watermarks are emitted per event rather than on a
  processing-time timer, everything that judges lateness runs at parallelism 1, and every keyed
  operator orders its input by event time before acting. Lateness is a property of the recording,
  not of how fast the machine replays it.
- **Lateness is measured, not assumed.** Satellite messages arrive about 40 minutes after they were
  sent. Whether they count is a choice of watermark bound, and the run records which.

## Data

Live AIS from the Norwegian Coastal Administration (Kystverket), open tier, read through the
BarentsWatch Live AIS API. The open tier excludes fishing vessels under 15 m and leisure craft under
45 m.

> Contains data under the Norwegian licence for Open Government Data (NLOD) distributed by the
> Norwegian Coastal Administration. Data delivered by BarentsWatch.

The recording stays local. What leaves this repository is aggregate output: detections without
vessel names or MMSI numbers.

"AIS gap" means a vessel stopped being received. Most gaps are coverage, equipment or congestion,
and a detection here is a pattern in the data, not a claim about what a ship did.

## Running it

Requirements: Docker. Nothing else is installed on the host; the job is built in a Maven container.

```bash
cp .env.example .env          # BarentsWatch client id and secret, and where data lives

# Record
docker compose up -d kafka topics ingest recorder

# Build the job and start Flink
docker compose --profile build run --rm maven package
docker compose --profile flink up -d jobmanager taskmanager-1 taskmanager-2

# Pick a window of the recording, build its masks, measure it
docker compose run --rm --no-deps tools python -m northwind.replay --name w1 \
    --start 2026-10-13T10:00:00Z --end 2026-10-13T16:00:00Z
docker compose run --rm --no-deps tools python -m northwind.masks --name w1
docker compose run --rm --no-deps tools python -m northwind.profile --name w1

# Run the job over it and check the run
docker compose exec jobmanager flink run /opt/flink/usrlib/north-wind.jar \
    --topic replay.w1 --masks /data/windows/w1/masks --out /data/runs/w1-b120 \
    --lateness-bound-s 120
docker compose run --rm --no-deps tools python -m northwind.check --run w1-b120 --window w1
```

On Git Bash for Windows, prefix `docker compose exec` and `run` commands with `MSYS_NO_PATHCONV=1`
so container paths are not rewritten.

Tests: `docker compose --profile build run --rm maven test` and
`docker compose run --rm --no-deps tools python -m pytest -q tests`.

See [docs/DESIGN.md](docs/DESIGN.md) for how it works and why.

## License

MIT for the code. The data is NLOD, see above.
