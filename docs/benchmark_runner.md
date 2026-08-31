# Benchmark Runner

Midwicket ships a small benchmark framework that evaluates any model
against the four standard cricket prediction problems defined in
[`docs/benchmarks.md`](benchmarks.md):

| Benchmark | Task | Primary metric |
|---|---|---|
| `win_probability` | Binary classification | AUC-ROC |
| `wicket_probability` | Binary classification | AUC-ROC |
| `fantasy_points` | Regression | MAE |
| `score_projection` | Regression | RMSE |

The framework provides:

- A registry of benchmark specs (`midwicket.benchmark.get_benchmark`)
- A runner that loads data, applies a model, and computes metrics
  (`midwicket.benchmark.evaluate_benchmark`)
- A leaderboard that persists results as JSON Lines
  (`midwicket.benchmark.record_result`, `get_leaderboard`)
- A CLI: `scripts/run_benchmark.py`

The runner is deliberately small and dependency-free (pure Python
metrics) so that benchmarks can be reproduced on a fresh checkout.

## Quick Start

### Programmatic

```python
from midwicket.benchmark import evaluate_benchmark, record_result

def my_model(features: dict) -> float:
    # return P(positive) for binary tasks, or scalar value for regression
    return 0.5

result = evaluate_benchmark(
    model=my_model,
    benchmark_name="win_probability",
    data_path="data/benchmarks/win_probability.jsonl",
)
print(result["metrics"])

record_result(
    benchmark="win_probability",
    model="my_xgboost_v1",
    metrics=result["metrics"],
    extra={"notes": "depth=5, n_estimators=200"},
)
```

### CLI

```bash
# List all benchmarks
python scripts/run_benchmark.py --list

# Run a benchmark against the shipped default model
python scripts/run_benchmark.py \
    --benchmark win_probability \
    --data data/benchmarks/win_probability.jsonl

# Load a custom joblib model and record the result
python scripts/run_benchmark.py \
    --benchmark fantasy_points \
    --model /path/to/model.joblib \
    --data data/benchmarks/fantasy_points.jsonl \
    --model-name "xgboost_d5_n200" \
    --record

# Show the leaderboard
python scripts/run_benchmark.py --leaderboard win_probability
```

## Model Interface

The runner accepts three model shapes, in order of preference:

1. Objects with `predict_proba(X)` — used for classifiers; the runner
   takes the probability of the positive class.
2. Objects with `predict(X)` — used for regressors.
3. Plain callables — receive a dict of features and return a scalar.
   This is the simplest contract and the one shown in the examples
   above.

For binary classification, return a probability in `[0, 1]`. For
regression, return a real number in the same units as the label.

## Evaluation Data Format

Each benchmark expects a JSON Lines (`.jsonl`) or CSV file at one of:

1. The `data_path` argument
2. `${MIDWICKET_BENCHMARK_DATA}/{benchmark}.jsonl`
3. `data/benchmarks/{benchmark}.jsonl`
4. `~/.midwicket/benchmarks/{benchmark}.jsonl`

Every row must contain:

- All features listed in the benchmark spec (`get_benchmark(name)["features"]`)
- The label key (`get_benchmark(name)["label"]`)

Example row for `win_probability`:

```json
{
    "runs_scored": 95,
    "wickets_down": 3,
    "overs_done": 12.4,
    "target": 180,
    "venue": "Wankhede",
    "required_run_rate": 11.3,
    "current_run_rate": 7.6,
    "chase_won": 1
}
```

## Output Format

`evaluate_benchmark` returns a dict:

```json
{
    "benchmark": "win_probability",
    "model": "my_module.MyModel",
    "metrics": {
        "auc_roc": 0.871,
        "brier_score": 0.162
    },
    "runtime_seconds": 0.342,
    "timestamp": "2026-06-01T10:30:00+00:00",
    "n_samples": 4821
}
```

## Leaderboard Storage

Results are appended to JSON Lines files under
`data/benchmark_results/{benchmark}.jsonl` (override with
`MIDWICKET_BENCHMARK_RESULTS`). Each line is one result row:

```json
{
    "run_id": "ab12cd34...",
    "benchmark": "win_probability",
    "model": "xgboost_d5_n200",
    "metrics": {"auc_roc": 0.871, "brier_score": 0.162},
    "primary_metric": "auc_roc",
    "primary_value": 0.871,
    "timestamp": "2026-06-01T10:30:00+00:00",
    "extra": {"runtime_seconds": 0.342}
}
```

`get_leaderboard(name)` reads every row and sorts:

- **Higher-is-better** (AUC-ROC, PR-AUC, Spearman rho): descending
- **Lower-is-better** (MAE, RMSE, MAPE, Brier): ascending

## Adding a New Benchmark

1. Add the spec to `_BENCHMARKS` in
   [`midwicket/benchmark/registry.py`](../midwicket/benchmark/registry.py).
   The spec must include `name`, `task`, `label`, `features`, `split`,
   `primary_metric`, `secondary_metrics`, and `baseline`.
2. Add a matching section to [`docs/benchmarks.md`](benchmarks.md) so
   the human-readable docs and the registry stay in sync.
3. If a new metric is required, add it to `_metric` in
   [`midwicket/benchmark/runner.py`](../midwicket/benchmark/runner.py)
   and update the leaderboard's lower-is-better set in
   [`midwicket/benchmark/leaderboard.py`](../midwicket/benchmark/leaderboard.py).
4. Add a unit test in `tests/test_benchmark_runner.py`.

## Reproducibility Bar

A result is **only valid** for the public leaderboard if it satisfies
all four requirements from `docs/benchmarks.md`:

1. Uses only the specified training split.
2. Reports the specified primary metric.
3. Uses no external data sources outside Cricsheet.
4. Posts code that reproduces the result from raw Cricsheet data.

The runner does not enforce these rules — they are honour-system —
but the leaderboard PR template asks submitters to confirm them.

## Related Files

- `midwicket/benchmark/__init__.py` — public API surface
- `midwicket/benchmark/registry.py` — benchmark specs
- `midwicket/benchmark/runner.py` — `evaluate_benchmark` and metrics
- `midwicket/benchmark/leaderboard.py` — record/retrieve results
- `scripts/run_benchmark.py` — CLI
- `tests/test_benchmark_runner.py` — unit tests
- `docs/benchmarks.md` — problem definitions
