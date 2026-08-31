# Load Testing Guide

This document describes how Midwicket is load-tested, what the targets are,
and how to run the suite locally or in CI.

## When to Run Load Tests

Run the load testing suite when:

- Changing any code path on the request hot path (search, predict, analyze).
- Bumping a major dependency (FastAPI, DuckDB, PyArrow).
- Changing caching, rate-limiting, or connection pool settings.
- Before tagging a new release candidate.
- After a production incident, to validate the fix under realistic load.

Do **not** run load tests as part of the default PR CI — they take minutes
and consume runner time. Use a dedicated workflow, scheduled or
`workflow_dispatch`.

## Prerequisites

```bash
# Install perf extras (Locust + pytest-benchmark)
pip install -e .[perf]

# Or install ad-hoc
pip install locust pytest-benchmark
```

You also need a running Midwicket API instance to test against. For local
development:

```bash
midwicket serve --port 8000
# or
uvicorn midwicket.serve.app:app --host 0.0.0.0 --port 8000
```

## Running the Suite

### Interactive (Locust web UI)

```bash
locust -f tests/load/locustfile.py --host=http://localhost:8000
```

Open <http://localhost:8089> and configure user count, spawn rate, and
duration interactively.

### Headless (CI-friendly)

```bash
locust -f tests/load/locustfile.py \
    --host=http://localhost:8000 \
    --headless \
    -u 50 -r 5 -t 60s
```

Or use the wrapper script:

```bash
scripts/run_load_test.sh
HOST=http://staging.example.com USERS=200 DURATION=300s scripts/run_load_test.sh
```

The wrapper writes CSV and HTML reports under `reports/load/`.

### Throughput Benchmarks (pytest-benchmark)

For micro-benchmarks that don't require a running server:

```bash
pytest tests/perf/test_throughput.py --benchmark-only
```

Results are printed in a table. Use `--benchmark-save=<name>` and
`--benchmark-compare` to track regressions over time.

### Memory Profiling

```bash
pytest tests/perf/test_memory.py -v
```

These tests assert peak allocations stay within configured budgets. If a
test fails, look for unintended caches, full-DB scans, or large
intermediate DataFrames.

## Sample Baseline Numbers

These are reference numbers from a development laptop (M1, 16 GB) hitting
a locally running API with the bundled IPL dataset. Your hardware will
differ — record your own baselines on a stable host.

### HTTP API (50 concurrent users, 60s)

| Endpoint | RPS | p50 | p95 | p99 | Errors |
|---|---|---|---|---|---|
| `/health` | 110 | 3 ms | 8 ms | 14 ms | 0% |
| `/v1/players/search` | 85 | 22 ms | 78 ms | 145 ms | 0% |
| `/v1/venues/resolve` | 60 | 18 ms | 65 ms | 120 ms | 0% |
| `/v1/matchup` | 35 | 110 ms | 280 ms | 450 ms | 0% |
| `/v1/predict/win` | 40 | 35 ms | 95 ms | 180 ms | 0% |
| `/v1/analyze` (simple) | 25 | 180 ms | 620 ms | 1100 ms | 0% |

### Throughput Benchmarks

| Benchmark | Operations/sec |
|---|---|
| `win_probability` (single) | ~12,000 |
| `win_probability` (100-batch) | ~120 batches/sec |
| Player search (cold) | ~150 |
| Venue resolve | ~5,000 |

### Memory Budgets

| Operation | Peak |
|---|---|
| `MidwicketSession()` init | < 100 MiB |
| 1000 `win_probability` calls | < 50 MiB |
| 50-query batch | < 100 MiB |

## CI Integration

Add a scheduled GitHub Actions workflow that runs the load suite nightly
or weekly against a staging API:

```yaml
name: Load Test
on:
  schedule:
    - cron: "0 3 * * 1"   # Mondays at 03:00 UTC
  workflow_dispatch:

jobs:
  load-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e .[perf]
      - run: scripts/run_load_test.sh
        env:
          HOST: ${{ secrets.STAGING_HOST }}
          MIDWICKET_API_KEY: ${{ secrets.STAGING_API_KEY }}
      - uses: actions/upload-artifact@v4
        with:
          name: load-test-report
          path: reports/load/
```

For throughput regression in PR CI, run only the pytest-benchmark suite
(fast, no server required):

```yaml
- run: pytest tests/perf/test_throughput.py --benchmark-only --benchmark-json=bench.json
- uses: benchmark-action/github-action-benchmark@v1
  with:
    tool: pytest
    output-file-path: bench.json
    fail-on-alert: true
```

## Interpreting Results

### Locust output

- **Median (p50) vs p95** — if the gap is wide, the API has tail-latency
  issues. Look for GC pauses, lock contention, or slow downstream calls.
- **Failure rate** — > 1% on any read endpoint is a regression. Inspect
  the failure detail rows; common causes are timeouts, 429 (rate
  limiting), and 500 (unhandled exceptions).
- **RPS plateau** — if the total RPS stops growing as you add users,
  you've hit a throughput ceiling. Typical bottlenecks: DuckDB
  single-writer lock, FastAPI worker count, or Python's GIL on
  CPU-bound endpoints.

### pytest-benchmark output

- **mean** — primary signal. Compare across runs with
  `pytest --benchmark-compare`.
- **stddev** — high stddev means the benchmark is noisy. Increase
  rounds with `--benchmark-min-rounds=20`.

### Memory test failures

If `test_memory.py` fails, look in this order:
1. Recently added caches without size bounds.
2. Code paths that materialise an entire table to a Python list.
3. Pickle/joblib loading large model files unexpectedly.

## Related Files

- `tests/load/locustfile.py` — the Locust scenarios
- `tests/load/README.md` — quick-start for the Locust suite
- `tests/perf/test_throughput.py` — pytest-benchmark micro-benchmarks
- `tests/perf/test_memory.py` — tracemalloc budget tests
- `scripts/run_load_test.sh` — headless runner wrapper
