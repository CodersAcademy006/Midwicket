"""
Midwicket benchmark runner.

Provides a reproducible evaluation framework for the four cricket
prediction benchmarks defined in ``docs/benchmarks.md``:

* win_probability
* wicket_probability
* fantasy_points
* score_projection

See :mod:`midwicket.benchmark.runner` for the main entrypoint
:func:`evaluate_benchmark`, :mod:`midwicket.benchmark.registry` for
benchmark specs, and :mod:`midwicket.benchmark.leaderboard` for
recording and listing results.
"""

from .registry import list_benchmarks, get_benchmark
from .runner import evaluate_benchmark, BenchmarkDataNotFound, BenchmarkError
from .leaderboard import record_result, get_leaderboard

__all__ = [
    "list_benchmarks",
    "get_benchmark",
    "evaluate_benchmark",
    "record_result",
    "get_leaderboard",
    "BenchmarkDataNotFound",
    "BenchmarkError",
]
