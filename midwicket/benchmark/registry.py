"""
Benchmark registry: the canonical list of evaluation problems.

Each benchmark is a frozen spec describing the task, available features,
data split, primary/secondary metrics, and the published baseline. The
specs mirror ``docs/benchmarks.md``. If you change one, change the other.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Dict, List


# Frozen, in-memory specs. Hard-coded by design — these define the
# reproducibility contract and must not drift across runs.
_BENCHMARKS: Dict[str, Dict] = {
    "win_probability": {
        "name": "win_probability",
        "task": "binary_classification",
        "label": "chase_won",
        "features": [
            "runs_scored",
            "wickets_down",
            "overs_done",
            "target",
            "venue",
            "required_run_rate",
            "current_run_rate",
        ],
        "split": {
            "train": "IPL 2008-2021",
            "validation": "IPL 2022",
            "test": "IPL 2023-2026",
        },
        "primary_metric": "auc_roc",
        "secondary_metrics": ["brier_score"],
        "baseline": {
            "model": "midwicket logistic regression",
            "auc_roc": 0.843,
            "brier_score": 0.181,
        },
        "min_samples_per_match": 50,
    },
    "wicket_probability": {
        "name": "wicket_probability",
        "task": "binary_classification",
        "label": "is_wicket",
        "features": [
            "batter_id",
            "bowler_id",
            "over",
            "ball_in_over",
            "batter_balls_faced",
            "bowler_balls_bowled",
            "innings_wickets_down",
            "innings_run_rate",
            "pressure_index",
        ],
        "split": {
            "train": "IPL 2008-2020, all T20IS 2005-2020",
            "validation": "IPL 2021, T20IS 2021",
            "test": "IPL 2022-2026, T20IS 2022-2026",
        },
        "primary_metric": "auc_roc",
        "secondary_metrics": ["pr_auc"],
        "baseline": {
            "model": "logistic regression (over + balls_faced + pressure)",
            "auc_roc": 0.610,
        },
        "class_imbalance": "approximately 1 wicket per 20 deliveries",
    },
    "fantasy_points": {
        "name": "fantasy_points",
        "task": "regression",
        "label": "fantasy_points",
        "features": [
            "player_id",
            "rolling_avg_5",
            "rolling_sr_5",
            "bqr",
            "pressure_index_avg",
            "venue_adj_form",
            "opposition_bqr_avg",
        ],
        "split": {
            "train": "IPL 2012-2021",
            "validation": "IPL 2022",
            "test": "IPL 2023-2026",
        },
        "primary_metric": "mae",
        "secondary_metrics": ["spearman_rho"],
        "baseline": {
            "model": "previous match points",
            "mae": 22.0,
            "spearman_rho": 0.38,
        },
        "min_prior_matches": 5,
    },
    "score_projection": {
        "name": "score_projection",
        "task": "regression",
        "label": "final_innings_total",
        "features": [
            "runs_at_10",
            "wickets_at_10",
            "run_rate_10",
            "venue",
            "batting_team",
            "season",
        ],
        "split": {
            "train": "IPL 2008-2020",
            "validation": "IPL 2021",
            "test": "IPL 2022-2026",
        },
        "primary_metric": "rmse",
        "secondary_metrics": ["mape"],
        "baseline": {
            "model": "naive: runs_at_10 * 2 + 15",
            "rmse": 21.0,
            "mape": 0.11,
        },
        "predict_at_ball": 60,
    },
}


def list_benchmarks() -> List[str]:
    """Return the names of all registered benchmarks."""
    return sorted(_BENCHMARKS.keys())


def get_benchmark(name: str) -> Dict:
    """Return the spec for a benchmark by name.

    Returns a deep copy so callers cannot mutate the registry.

    Raises:
        KeyError: if ``name`` is not a registered benchmark.
    """
    if name not in _BENCHMARKS:
        raise KeyError(
            f"Unknown benchmark {name!r}. "
            f"Known benchmarks: {list_benchmarks()}"
        )
    return deepcopy(_BENCHMARKS[name])


__all__ = ["list_benchmarks", "get_benchmark"]
