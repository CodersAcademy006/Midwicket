#!/usr/bin/env python3
"""
CLI entrypoint for running a Midwicket benchmark against a model.

Examples:

    # Use the default shipped win probability model
    python scripts/run_benchmark.py --benchmark win_probability \\
        --data data/benchmarks/win_probability.jsonl

    # Use a joblib-pickled sklearn model
    python scripts/run_benchmark.py --benchmark fantasy_points \\
        --model /path/to/model.joblib \\
        --data data/benchmarks/fantasy_points.jsonl

    # List benchmarks
    python scripts/run_benchmark.py --list

    # Show a leaderboard
    python scripts/run_benchmark.py --leaderboard win_probability
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

# Ensure the project root is on sys.path so this script works when invoked
# from a checkout without installation.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from midwicket.benchmark import (  # noqa: E402
    BenchmarkDataNotFound,
    evaluate_benchmark,
    get_benchmark,
    get_leaderboard,
    list_benchmarks,
    record_result,
)


def _load_model(path: Optional[str]) -> Any:
    """Load a model from a joblib file, or return the shipped default."""
    if path is None:
        from midwicket.compute.winprob import win_probability

        def default_model(features: dict) -> float:
            return float(
                win_probability(
                    target=int(features.get("target", 180)),
                    current_runs=int(features.get("runs_scored", 0)),
                    wickets_down=int(features.get("wickets_down", 0)),
                    overs_done=float(features.get("overs_done", 0.0)),
                    venue=features.get("venue"),
                )["win_prob"]
            )

        return default_model

    try:
        import joblib  # type: ignore[import]
    except ImportError as exc:
        raise SystemExit(
            "joblib is required to load model files. Install with: pip install joblib"
        ) from exc

    if not Path(path).exists():
        raise SystemExit(f"model file not found: {path}")
    return joblib.load(path)


def _print_table(rows: list, headers: list) -> None:
    widths = [
        max(len(str(h)), max((len(str(r.get(h, ""))) for r in rows), default=0))
        for h in headers
    ]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print(fmt.format(*("-" * w for w in widths)))
    for r in rows:
        print(fmt.format(*[str(r.get(h, ""))[:widths[i]] for i, h in enumerate(headers)]))


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a Midwicket benchmark and optionally record the result."
    )
    parser.add_argument("--benchmark", help="benchmark name (see --list)")
    parser.add_argument(
        "--model",
        help="path to a joblib-pickled model (omit to use the shipped default)",
    )
    parser.add_argument("--data", help="path to JSONL/CSV evaluation data")
    parser.add_argument(
        "--record",
        action="store_true",
        help="append the result to the leaderboard ledger",
    )
    parser.add_argument(
        "--model-name",
        help="human-readable model name to record (defaults to class name)",
    )
    parser.add_argument(
        "--list", action="store_true", help="list available benchmarks and exit"
    )
    parser.add_argument(
        "--leaderboard",
        metavar="BENCHMARK",
        help="print the leaderboard for the given benchmark and exit",
    )
    parser.add_argument(
        "--json", action="store_true", help="emit machine-readable JSON output"
    )
    args = parser.parse_args(argv)

    if args.list:
        for name in list_benchmarks():
            spec = get_benchmark(name)
            print(
                f"{name:20s}  task={spec['task']:25s}  "
                f"primary_metric={spec['primary_metric']}"
            )
        return 0

    if args.leaderboard:
        try:
            lb = get_leaderboard(args.leaderboard)
        except KeyError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps(lb, indent=2))
            return 0
        rows = [
            {
                "rank": i + 1,
                "model": r["model"],
                "primary_metric": r["primary_metric"],
                "primary_value": f"{r['primary_value']:.4f}",
                "timestamp": r["timestamp"],
            }
            for i, r in enumerate(lb)
        ]
        if not rows:
            print(f"(no results recorded for {args.leaderboard})")
            return 0
        _print_table(
            rows, ["rank", "model", "primary_metric", "primary_value", "timestamp"]
        )
        return 0

    if not args.benchmark:
        parser.error("--benchmark is required (or use --list)")

    try:
        model = _load_model(args.model)
        result = evaluate_benchmark(
            model=model,
            benchmark_name=args.benchmark,
            data_path=args.data,
        )
    except BenchmarkDataNotFound as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except KeyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    model_name = args.model_name or result["model"]
    if args.record:
        recorded = record_result(
            benchmark=args.benchmark,
            model=model_name,
            metrics=result["metrics"],
            extra={"runtime_seconds": result["runtime_seconds"]},
        )
        result["run_id"] = recorded["run_id"]

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    print("=" * 60)
    print(f"Benchmark:    {result['benchmark']}")
    print(f"Model:        {model_name}")
    print(f"Samples:      {result['n_samples']}")
    print(f"Runtime:      {result['runtime_seconds']:.3f}s")
    print(f"Timestamp:    {result['timestamp']}")
    print("-" * 60)
    print("Metrics:")
    for metric, value in result["metrics"].items():
        print(f"  {metric:20s}  {value:.4f}")
    if args.record:
        print(f"\nRecorded as run_id={result['run_id']}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
