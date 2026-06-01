"""
Benchmark leaderboard storage.

Results are appended to a JSON Lines ledger at
``data/benchmark_results/{benchmark}.jsonl`` (override via the
``MIDWICKET_BENCHMARK_RESULTS`` env var). The format is intentionally
append-only so historical runs are never silently overwritten.
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .registry import get_benchmark


_lock = threading.Lock()


def _results_dir() -> Path:
    override = os.environ.get("MIDWICKET_BENCHMARK_RESULTS")
    if override:
        return Path(override)
    return Path("data") / "benchmark_results"


def _ledger_path(benchmark: str) -> Path:
    safe = Path(benchmark).name
    if safe != benchmark:
        raise ValueError(f"invalid benchmark name: {benchmark!r}")
    return _results_dir() / f"{safe}.jsonl"


def record_result(
    benchmark: str,
    model: str,
    metrics: Dict[str, float],
    run_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Append a result row to the leaderboard.

    Args:
        benchmark: Benchmark name (must exist in registry).
        model: Human-readable model identifier.
        metrics: Dict of metric name -> value.
        run_id: Optional client-supplied id; one is generated if absent.
        extra: Optional dict merged into the row (e.g. code link, notes).

    Returns:
        The row that was written.
    """
    spec = get_benchmark(benchmark)  # raises KeyError if unknown
    primary = spec["primary_metric"]
    if primary not in metrics:
        raise ValueError(
            f"metrics dict is missing the primary metric {primary!r} "
            f"for benchmark {benchmark!r}"
        )

    row: Dict[str, Any] = {
        "run_id": run_id or uuid.uuid4().hex,
        "benchmark": benchmark,
        "model": model,
        "metrics": dict(metrics),
        "primary_metric": primary,
        "primary_value": float(metrics[primary]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        row["extra"] = dict(extra)

    path = _ledger_path(benchmark)
    with _lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
    return row


def get_leaderboard(benchmark: str) -> List[Dict[str, Any]]:
    """Return all recorded results for a benchmark, sorted by primary metric.

    For metrics where lower-is-better (mae, rmse, mape, brier_score), the
    sort is ascending. For others (auc_roc, pr_auc, spearman_rho), it is
    descending.
    """
    spec = get_benchmark(benchmark)
    primary = spec["primary_metric"]
    lower_is_better = primary in {"mae", "rmse", "mape", "brier_score"}

    path = _ledger_path(benchmark)
    if not path.exists():
        return []

    rows: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    rows.sort(
        key=lambda r: r.get("primary_value", float("inf") if lower_is_better else float("-inf")),
        reverse=not lower_is_better,
    )
    return rows


__all__ = ["record_result", "get_leaderboard"]
