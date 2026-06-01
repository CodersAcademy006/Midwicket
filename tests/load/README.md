# Midwicket Load Testing

This directory contains the [Locust](https://locust.io) load test suite used to
characterise the Midwicket HTTP API under concurrent traffic.

## Quick Start

Install the load testing extras (Locust is not a runtime dependency):

```bash
pip install locust
# or, if you have a perf extra defined:
pip install -e .[perf]
```

Start the API in another shell, then run the load test:

```bash
# Interactive web UI on http://localhost:8089
locust -f tests/load/locustfile.py --host=http://localhost:8000
```

## Headless Mode

For CI or scripted runs, use headless mode:

```bash
locust -f tests/load/locustfile.py \
    --host=http://localhost:8000 \
    --headless \
    -u 50 \
    -r 5 \
    -t 60s
```

Flags:
- `-u 50` — simulate 50 concurrent users
- `-r 5` — spawn 5 new users per second
- `-t 60s` — run for 60 seconds, then stop

A convenience wrapper is available at `scripts/run_load_test.sh`.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MIDWICKET_API_KEY` | `test-key-for-load-testing` | API key sent with every request |
| `LOAD_TEST_INCLUDE_WRITES` | `false` | Set to `true` to enable the live-ingestion user (writes) |

## Test Shape

Two `HttpUser` classes are defined:

- `MidwicketUser` — exercises read endpoints: `/health`, player search,
  venue list/resolve, matchup analysis, win prediction, `/v1/analyze` SQL,
  `/metrics`. Task weights mirror typical client traffic mix.
- `LiveIngestionUser` — exercises the live ingestion path
  (`/v1/live/register`, `/v1/live/ingest`). Only runs when
  `LOAD_TEST_INCLUDE_WRITES=true`.

## Sample Target SLOs

These are reference numbers, not contract guarantees:

| Endpoint | p95 latency | Error rate |
|---|---|---|
| `/health` | < 50 ms | 0% |
| `/v1/players/search` | < 200 ms | < 0.1% |
| `/v1/venues/resolve` | < 150 ms | < 0.1% |
| `/v1/matchup` | < 400 ms | < 0.5% |
| `/v1/predict/win` | < 250 ms | < 0.5% |
| `/v1/analyze` | < 1000 ms | < 1% |

## Interpreting Output

After a headless run, Locust prints a summary table plus the custom
summary block emitted by `locustfile.py`:

```
Load Test Summary
============================================================
Total requests:   12480
Total failures:   3
Median response:  42ms
95th percentile:  187ms
99th percentile:  412ms
Requests/second:  208.00
============================================================
```

Look for:
- **Median vs p95 gap** — large gaps indicate tail-latency issues.
- **Failure rate** — anything > 1% on read endpoints is a regression.
- **RPS plateau** — if RPS stops climbing as users increase, you've hit a
  throughput ceiling (CPU, DB connections, or thread pool).

For deeper guidance, see `docs/load_testing.md`.
