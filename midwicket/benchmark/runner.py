"""
Benchmark runner.

The ``evaluate_benchmark`` entrypoint takes a model (or a callable that
behaves like one) plus a benchmark name and returns a structured metrics
dict. The runner is intentionally minimal: it loads the spec, builds an
evaluation harness from the spec, calls the model, and computes the
declared metrics.

Data loading is delegated to ``data_path`` if provided; otherwise the
runner tries common Midwicket dataset locations. If neither yields a
usable dataset, :class:`BenchmarkDataNotFound` is raised with guidance.
"""
from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .registry import get_benchmark


class BenchmarkError(Exception):
    """Base class for benchmark runner errors."""


class BenchmarkDataNotFound(BenchmarkError):
    """Raised when no benchmark evaluation data can be located."""


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def evaluate_benchmark(
    model: Any,
    benchmark_name: str,
    data_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Run a benchmark against a model and return metrics.

    Args:
        model: Any object exposing one of ``predict``, ``predict_proba``,
            or being directly callable. If it is callable, the runner
            passes a feature dict and expects a scalar prediction.
        benchmark_name: One of the names from
            :func:`midwicket.benchmark.registry.list_benchmarks`.
        data_path: Optional path to a JSONL/CSV evaluation file. If not
            provided, the runner searches default locations.

    Returns:
        Dict with keys ``benchmark``, ``model``, ``metrics``,
        ``runtime_seconds``, ``timestamp``.

    Raises:
        KeyError: if ``benchmark_name`` is unknown.
        BenchmarkDataNotFound: if no usable evaluation data is available.
        BenchmarkError: on evaluation failure.
    """
    spec = get_benchmark(benchmark_name)

    start = time.perf_counter()
    data = _load_data(spec, data_path)
    predict_fn = _resolve_predict_fn(model)
    model_label = _label_for_model(model)

    y_true: List[float] = []
    y_pred: List[float] = []
    for row in data:
        features, label = _split_row(row, spec)
        prediction = predict_fn(features)
        try:
            pred_value = float(prediction)
        except (TypeError, ValueError) as exc:
            raise BenchmarkError(
                f"model produced non-numeric prediction {prediction!r}: {exc}"
            ) from exc
        y_true.append(float(label))
        y_pred.append(pred_value)

    if not y_true:
        raise BenchmarkError(
            f"No evaluation rows found for benchmark {benchmark_name!r}"
        )

    metrics = _compute_metrics(spec, y_true, y_pred)
    runtime = time.perf_counter() - start

    return {
        "benchmark": benchmark_name,
        "model": model_label,
        "metrics": metrics,
        "runtime_seconds": runtime,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "n_samples": len(y_true),
    }


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #


def _load_data(spec: Dict[str, Any], data_path: Optional[str]) -> List[Dict[str, Any]]:
    candidates: List[Path] = []
    if data_path:
        candidates.append(Path(data_path))
    else:
        name = spec["name"]
        default_locations = [
            Path("data") / "benchmarks" / f"{name}.jsonl",
            Path("data") / "benchmarks" / f"{name}.csv",
            Path.home() / ".midwicket" / "benchmarks" / f"{name}.jsonl",
        ]
        env_dir = os.environ.get("MIDWICKET_BENCHMARK_DATA")
        if env_dir:
            default_locations.insert(0, Path(env_dir) / f"{name}.jsonl")
            default_locations.insert(1, Path(env_dir) / f"{name}.csv")
        candidates.extend(default_locations)

    for path in candidates:
        if path.exists() and path.is_file():
            return _read_rows(path)

    searched = "\n  ".join(str(p) for p in candidates)
    raise BenchmarkDataNotFound(
        f"No evaluation data found for benchmark {spec['name']!r}.\n"
        f"Searched:\n  {searched}\n"
        f"Provide a JSONL/CSV file via data_path= or set MIDWICKET_BENCHMARK_DATA."
    )


def _read_rows(path: Path) -> List[Dict[str, Any]]:
    import json
    import csv

    rows: List[Dict[str, Any]] = []
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        with open(path, encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise BenchmarkError(
                        f"{path}: invalid JSON on line {line_no}: {exc}"
                    ) from exc
    elif suffix == ".csv":
        with open(path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append({k: _coerce(v) for k, v in row.items()})
    else:
        raise BenchmarkError(
            f"Unsupported benchmark data format: {path.suffix}. "
            f"Use .jsonl or .csv."
        )
    return rows


def _coerce(value: str) -> Any:
    """Best-effort string coercion for CSV inputs."""
    if value is None or value == "":
        return None
    lowered = value.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


# --------------------------------------------------------------------------- #
# Row splitting / model adapter
# --------------------------------------------------------------------------- #


def _split_row(row: Dict[str, Any], spec: Dict[str, Any]) -> Tuple[Dict[str, Any], Any]:
    """Split a row into (features, label) using the benchmark spec."""
    label_key = spec["label"]
    if label_key not in row:
        raise BenchmarkError(
            f"row missing label key {label_key!r}; row keys={list(row.keys())}"
        )
    label = row[label_key]
    features = {k: v for k, v in row.items() if k != label_key}
    return features, label


def _resolve_predict_fn(model: Any) -> Callable[[Dict[str, Any]], Any]:
    """Wrap a model object in a uniform callable interface."""
    if model is None:
        raise BenchmarkError("model is None")

    if hasattr(model, "predict_proba"):
        def _call(features: Dict[str, Any]) -> Any:
            probs = model.predict_proba([list(features.values())])
            # binary classifier -> probability of positive class
            arr = list(probs[0]) if hasattr(probs, "__iter__") else [probs]
            return arr[-1] if len(arr) > 1 else arr[0]
        return _call

    if hasattr(model, "predict"):
        def _call(features: Dict[str, Any]) -> Any:
            out = model.predict([list(features.values())])
            return out[0] if hasattr(out, "__iter__") else out
        return _call

    if callable(model):
        return model  # type: ignore[return-value]

    raise BenchmarkError(
        f"model {model!r} is not callable and has no predict/predict_proba method"
    )


def _label_for_model(model: Any) -> str:
    if hasattr(model, "__class__"):
        cls = model.__class__
        return f"{cls.__module__}.{cls.__name__}"
    return repr(model)


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #


def _compute_metrics(
    spec: Dict[str, Any],
    y_true: Sequence[float],
    y_pred: Sequence[float],
) -> Dict[str, float]:
    primary = spec["primary_metric"]
    secondaries = spec.get("secondary_metrics", [])
    metrics: Dict[str, float] = {}
    metrics[primary] = _metric(primary, y_true, y_pred)
    for sec in secondaries:
        try:
            metrics[sec] = _metric(sec, y_true, y_pred)
        except BenchmarkError:
            # Secondary metric not computable on this data — skip silently.
            continue
    return metrics


def _metric(name: str, y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    if name == "auc_roc":
        return _auc_roc(y_true, y_pred)
    if name == "brier_score":
        return _brier(y_true, y_pred)
    if name == "pr_auc":
        return _pr_auc(y_true, y_pred)
    if name == "mae":
        return _mae(y_true, y_pred)
    if name == "rmse":
        return _rmse(y_true, y_pred)
    if name == "mape":
        return _mape(y_true, y_pred)
    if name == "spearman_rho":
        return _spearman(y_true, y_pred)
    raise BenchmarkError(f"unsupported metric: {name!r}")


def _mae(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    return sum(abs(a - b) for a, b in zip(y_true, y_pred)) / len(y_true)


def _rmse(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(y_true, y_pred)) / len(y_true))


def _mape(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    total = 0.0
    count = 0
    for actual, predicted in zip(y_true, y_pred):
        if actual == 0:
            continue
        total += abs(actual - predicted) / abs(actual)
        count += 1
    if count == 0:
        raise BenchmarkError("MAPE undefined: all actuals are zero")
    return total / count


def _brier(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    return sum((a - b) ** 2 for a, b in zip(y_true, y_pred)) / len(y_true)


def _auc_roc(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    # Mann-Whitney U formulation. Robust for small datasets, no scipy dep.
    positives = [p for t, p in zip(y_true, y_pred) if t >= 0.5]
    negatives = [p for t, p in zip(y_true, y_pred) if t < 0.5]
    if not positives or not negatives:
        raise BenchmarkError("AUC undefined: only one class present in y_true")
    rank_sum = 0.0
    combined = sorted(
        [(p, 1) for p in positives] + [(p, 0) for p in negatives],
        key=lambda x: x[0],
    )
    # average ranks for ties
    i = 0
    n = len(combined)
    while i < n:
        j = i
        while j + 1 < n and combined[j + 1][0] == combined[i][0]:
            j += 1
        avg_rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            if combined[k][1] == 1:
                rank_sum += avg_rank
        i = j + 1
    n_pos = len(positives)
    n_neg = len(negatives)
    u = rank_sum - n_pos * (n_pos + 1) / 2
    return u / (n_pos * n_neg)


def _pr_auc(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    # Simple PR-AUC via trapezoidal rule over thresholds.
    pairs = sorted(
        zip(y_pred, y_true), key=lambda x: x[0], reverse=True
    )
    total_pos = sum(1 for _, t in pairs if t >= 0.5)
    if total_pos == 0:
        raise BenchmarkError("PR-AUC undefined: no positive samples")
    tp = 0
    fp = 0
    prev_recall = 0.0
    auc = 0.0
    prev_precision = 1.0
    for _, t in pairs:
        if t >= 0.5:
            tp += 1
        else:
            fp += 1
        precision = tp / (tp + fp)
        recall = tp / total_pos
        auc += (recall - prev_recall) * (precision + prev_precision) / 2
        prev_recall = recall
        prev_precision = precision
    return auc


def _spearman(y_true: Sequence[float], y_pred: Sequence[float]) -> float:
    n = len(y_true)
    if n < 2:
        raise BenchmarkError("Spearman undefined: fewer than 2 samples")
    rank_true = _rank(y_true)
    rank_pred = _rank(y_pred)
    mean_t = sum(rank_true) / n
    mean_p = sum(rank_pred) / n
    num = sum((rt - mean_t) * (rp - mean_p) for rt, rp in zip(rank_true, rank_pred))
    den_t = math.sqrt(sum((rt - mean_t) ** 2 for rt in rank_true))
    den_p = math.sqrt(sum((rp - mean_p) ** 2 for rp in rank_pred))
    if den_t == 0 or den_p == 0:
        return 0.0
    return num / (den_t * den_p)


def _rank(values: Sequence[float]) -> List[float]:
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg
        i = j + 1
    return ranks


__all__ = [
    "evaluate_benchmark",
    "BenchmarkError",
    "BenchmarkDataNotFound",
]
