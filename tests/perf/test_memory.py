"""
Memory profiling tests for Midwicket using tracemalloc.

These tests assert that critical paths stay within reasonable memory
bounds. They are not micro-benchmarks; they catch egregious regressions
such as accidental full-DB loads or unbounded caches.

Run with:
    pytest tests/perf/test_memory.py -v
"""
from __future__ import annotations

import gc
import tempfile
from pathlib import Path

import pytest

# tracemalloc is stdlib, so this should never skip — but use the same
# pattern as the throughput suite for consistency.
pytest.importorskip("tracemalloc")

import tracemalloc  # noqa: E402

# Memory budgets (bytes). Generous on purpose — these are upper bounds
# meant to catch regressions, not enforce tight allocation discipline.
SESSION_INIT_BUDGET = 100 * 1024 * 1024          # 100 MiB
WINPROB_BATCH_BUDGET = 50 * 1024 * 1024          # 50 MiB
QUERY_BATCH_BUDGET = 100 * 1024 * 1024           # 100 MiB


def _measure_peak(callable_fn) -> int:
    """Run callable_fn under tracemalloc and return peak bytes allocated."""
    gc.collect()
    tracemalloc.start()
    try:
        callable_fn()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak


# --------------------------------------------------------------------------- #
# Session init memory
# --------------------------------------------------------------------------- #


def test_session_init_memory_bound() -> None:
    """MidwicketSession init should not balloon memory."""
    from midwicket.api.session import MidwicketSession

    def _init() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            raw_dir = data_dir / "raw" / "ipl"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "dummy_match.json").write_text("{}", encoding="utf-8")
            with MidwicketSession(
                data_dir=str(data_dir), skip_registry_build=True
            ):
                pass

    peak = _measure_peak(_init)
    assert peak < SESSION_INIT_BUDGET, (
        f"Session init peak {peak:,} bytes exceeded budget "
        f"{SESSION_INIT_BUDGET:,} bytes"
    )


# --------------------------------------------------------------------------- #
# Win probability batch memory
# --------------------------------------------------------------------------- #


def test_winprob_batch_memory_bound() -> None:
    """A batch of 1000 win_probability calls must stay under budget."""
    from midwicket.compute.winprob import win_probability

    def _batch() -> None:
        for i in range(1000):
            win_probability(
                target=180,
                current_runs=95 + (i % 50),
                wickets_down=3,
                overs_done=12.0 + (i % 6) * 0.1,
            )

    peak = _measure_peak(_batch)
    assert peak < WINPROB_BATCH_BUDGET, (
        f"win_probability batch peak {peak:,} bytes exceeded budget "
        f"{WINPROB_BATCH_BUDGET:,} bytes"
    )


# --------------------------------------------------------------------------- #
# Query batch memory
# --------------------------------------------------------------------------- #


def test_query_batch_memory_bound() -> None:
    """Repeated session queries should not leak memory."""
    from midwicket.api.session import MidwicketSession

    def _queries() -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            raw_dir = data_dir / "raw" / "ipl"
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "dummy_match.json").write_text("{}", encoding="utf-8")
            with MidwicketSession(
                data_dir=str(data_dir), skip_registry_build=True
            ) as session:
                for _ in range(50):
                    try:
                        session.get_player_stats("V Kohli")
                    except Exception:
                        pass

    peak = _measure_peak(_queries)
    assert peak < QUERY_BATCH_BUDGET, (
        f"Query batch peak {peak:,} bytes exceeded budget "
        f"{QUERY_BATCH_BUDGET:,} bytes"
    )
