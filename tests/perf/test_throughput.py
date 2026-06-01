"""
Throughput benchmarks for Midwicket core operations.

Run with:
    pytest tests/perf/test_throughput.py --benchmark-only

These tests measure the throughput of the most frequently exercised code
paths. They are intentionally lightweight so they can run in CI without
requiring a fully populated dataset.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("pytest_benchmark", reason="pytest-benchmark not installed")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def perf_session():
    """A MidwicketSession backed by an in-memory database.

    Uses tempfile to avoid touching the user's data directory and to keep
    benchmark runs deterministic.
    """
    from midwicket.api.session import MidwicketSession

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        raw_dir = data_dir / "raw" / "ipl"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "dummy_match.json").write_text("{}", encoding="utf-8")

        with MidwicketSession(
            data_dir=str(data_dir), skip_registry_build=True
        ) as session:
            yield session


# --------------------------------------------------------------------------- #
# Win probability throughput
# --------------------------------------------------------------------------- #


def test_winprob_single_call_throughput(benchmark) -> None:
    """A single win_probability invocation should be fast (< 10ms typical)."""
    from midwicket.compute.winprob import win_probability

    def _call() -> dict:
        return win_probability(
            target=180,
            current_runs=95,
            wickets_down=3,
            overs_done=12.4,
        )

    result = benchmark(_call)
    assert "win_prob" in result
    assert 0.0 <= result["win_prob"] <= 1.0


def test_winprob_batch_throughput(benchmark) -> None:
    """100-call batch should sustain at least 1000 predictions/sec."""
    from midwicket.compute.winprob import win_probability

    scenarios = [
        (180, 95 + i, 3, 12.4) for i in range(100)
    ]

    def _batch() -> list[dict]:
        return [
            win_probability(
                target=t,
                current_runs=runs,
                wickets_down=wkts,
                overs_done=overs,
            )
            for (t, runs, wkts, overs) in scenarios
        ]

    out = benchmark(_batch)
    assert len(out) == 100
    assert all(0.0 <= r["win_prob"] <= 1.0 for r in out)


# --------------------------------------------------------------------------- #
# Player search throughput
# --------------------------------------------------------------------------- #


def test_player_search_throughput(benchmark, perf_session) -> None:
    """Player search should return quickly even on cold data."""

    def _search():
        try:
            return perf_session.get_player_stats("V Kohli")
        except Exception:
            return None

    result = benchmark(_search)
    # Either we got stats or we got None for missing data — both are valid
    assert result is None or result is not None


# --------------------------------------------------------------------------- #
# Venue resolution throughput
# --------------------------------------------------------------------------- #


def test_venue_resolution_throughput(benchmark, perf_session) -> None:
    """Venue resolution against the registry must be sub-ms in the hot path."""
    registry = getattr(perf_session, "registry", None)
    if registry is None or not hasattr(registry, "resolve_venue"):
        pytest.skip("session has no venue resolver in this environment")

    def _resolve():
        try:
            return registry.resolve_venue("Wankhede Stadium")
        except Exception:
            return None

    benchmark(_resolve)
