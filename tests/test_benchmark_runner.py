"""
Tests for midwicket.benchmark — runner, registry, and leaderboard.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from midwicket.benchmark import (
    BenchmarkDataNotFound,
    evaluate_benchmark,
    get_benchmark,
    get_leaderboard,
    list_benchmarks,
    record_result,
)


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


class TestRegistry:
    def test_list_benchmarks_returns_expected_names(self) -> None:
        names = list_benchmarks()
        assert set(names) == {
            "win_probability",
            "wicket_probability",
            "fantasy_points",
            "score_projection",
        }

    def test_get_benchmark_returns_spec(self) -> None:
        spec = get_benchmark("win_probability")
        assert spec["name"] == "win_probability"
        assert spec["primary_metric"] == "auc_roc"
        assert spec["label"] == "chase_won"
        assert "runs_scored" in spec["features"]
        assert "split" in spec and "train" in spec["split"]

    def test_get_benchmark_unknown_raises(self) -> None:
        with pytest.raises(KeyError):
            get_benchmark("not_a_real_benchmark")

    def test_get_benchmark_returns_copy(self) -> None:
        spec1 = get_benchmark("score_projection")
        spec1["primary_metric"] = "MUTATED"
        spec2 = get_benchmark("score_projection")
        assert spec2["primary_metric"] == "rmse"


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #


def _write_jsonl(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


class TestRunner:
    def test_evaluate_classifier_returns_metrics(self, tmp_path: Path) -> None:
        # Fake win-probability rows. Label is `chase_won` (0/1).
        rows = []
        for i in range(20):
            won = 1 if i % 2 == 0 else 0
            rows.append({
                "runs_scored": 80 + i,
                "wickets_down": 3,
                "overs_done": 12.0,
                "target": 180,
                "venue": "Wankhede",
                "required_run_rate": 9.0,
                "current_run_rate": 7.5,
                "chase_won": won,
            })
        data_file = tmp_path / "wp.jsonl"
        _write_jsonl(data_file, rows)

        # Model: predict probability proportional to wickets_down (terrible but valid).
        def model(features):
            return 1.0 - features["wickets_down"] / 10.0

        result = evaluate_benchmark(
            model=model,
            benchmark_name="win_probability",
            data_path=str(data_file),
        )

        assert result["benchmark"] == "win_probability"
        assert result["n_samples"] == 20
        assert "auc_roc" in result["metrics"]
        assert "brier_score" in result["metrics"]
        assert result["runtime_seconds"] >= 0.0
        assert "timestamp" in result

    def test_evaluate_regressor_returns_mae(self, tmp_path: Path) -> None:
        rows = [
            {"player_id": "p1", "rolling_avg_5": 35.0, "rolling_sr_5": 130.0,
             "bqr": 1.0, "pressure_index_avg": 0.5, "venue_adj_form": 1.1,
             "opposition_bqr_avg": 0.9, "fantasy_points": 40.0},
            {"player_id": "p2", "rolling_avg_5": 25.0, "rolling_sr_5": 110.0,
             "bqr": 0.9, "pressure_index_avg": 0.4, "venue_adj_form": 0.95,
             "opposition_bqr_avg": 0.8, "fantasy_points": 28.0},
        ]
        data_file = tmp_path / "fp.jsonl"
        _write_jsonl(data_file, rows)

        # Perfect-ish model
        def model(features):
            return features["rolling_avg_5"] + 5.0

        result = evaluate_benchmark(
            model=model,
            benchmark_name="fantasy_points",
            data_path=str(data_file),
        )
        assert "mae" in result["metrics"]
        assert result["metrics"]["mae"] >= 0.0

    def test_missing_data_raises(self) -> None:
        with pytest.raises(BenchmarkDataNotFound):
            evaluate_benchmark(
                model=lambda f: 0.5,
                benchmark_name="win_probability",
                data_path="/nonexistent/path/to/data.jsonl",
            )

    def test_unknown_benchmark_raises(self) -> None:
        with pytest.raises(KeyError):
            evaluate_benchmark(
                model=lambda f: 0.5,
                benchmark_name="not_real",
            )


# --------------------------------------------------------------------------- #
# Leaderboard
# --------------------------------------------------------------------------- #


class TestLeaderboard:
    def test_record_and_get_round_trip(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("MIDWICKET_BENCHMARK_RESULTS", str(tmp_path))

        row = record_result(
            benchmark="win_probability",
            model="test_xgb_v1",
            metrics={"auc_roc": 0.87, "brier_score": 0.16},
        )
        assert row["run_id"]
        assert row["primary_metric"] == "auc_roc"
        assert row["primary_value"] == pytest.approx(0.87)

        lb = get_leaderboard("win_probability")
        assert len(lb) == 1
        assert lb[0]["model"] == "test_xgb_v1"

    def test_leaderboard_sorts_higher_is_better(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setenv("MIDWICKET_BENCHMARK_RESULTS", str(tmp_path))
        record_result("win_probability", "low", {"auc_roc": 0.7})
        record_result("win_probability", "high", {"auc_roc": 0.9})
        record_result("win_probability", "mid", {"auc_roc": 0.8})
        lb = get_leaderboard("win_probability")
        assert [r["model"] for r in lb] == ["high", "mid", "low"]

    def test_leaderboard_sorts_lower_is_better(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setenv("MIDWICKET_BENCHMARK_RESULTS", str(tmp_path))
        record_result("score_projection", "high_rmse", {"rmse": 30.0})
        record_result("score_projection", "low_rmse", {"rmse": 15.0})
        record_result("score_projection", "mid_rmse", {"rmse": 22.0})
        lb = get_leaderboard("score_projection")
        assert [r["model"] for r in lb] == ["low_rmse", "mid_rmse", "high_rmse"]

    def test_record_missing_primary_metric(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setenv("MIDWICKET_BENCHMARK_RESULTS", str(tmp_path))
        with pytest.raises(ValueError):
            record_result(
                benchmark="win_probability",
                model="incomplete",
                metrics={"brier_score": 0.2},
            )

    def test_get_leaderboard_empty_returns_empty_list(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        monkeypatch.setenv("MIDWICKET_BENCHMARK_RESULTS", str(tmp_path))
        assert get_leaderboard("win_probability") == []
