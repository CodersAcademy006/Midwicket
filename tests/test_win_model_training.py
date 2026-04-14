"""Tests for trainable win model and deployment loading paths."""

from importlib import reload
import pickle

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from pypitch.models.win_predictor import WinPredictor
from pypitch.models.train import WinProbabilityTrainer


def _synthetic_match_df(matches: int = 8, balls_per_match: int = 24) -> pd.DataFrame:
    rows = []
    for mid in range(matches):
        target = 150 + (mid % 25)
        chase_won = (mid % 2 == 0)
        final_runs = target + 8 if chase_won else target - 12
        for i in range(balls_per_match):
            over = i // 6
            ball = (i % 6) + 1
            progress = (i + 1) / balls_per_match
            runs_total = max(0, int(progress * final_runs))
            wickets_fallen = min(9, i // 8)
            rows.append(
                {
                    "match_id": f"m{mid}",
                    "inning": 2,
                    "over": over,
                    "ball": ball,
                    "runs_total": runs_total,
                    "wickets_fallen": wickets_fallen,
                    "target": target,
                    "venue": "Wankhede",
                }
            )
    return pd.DataFrame(rows)


def test_create_trained_model_from_dataframe():
    df = _synthetic_match_df(matches=10, balls_per_match=24)

    model = WinPredictor.create_trained_model(df)

    assert isinstance(model, WinPredictor)
    assert model.training_metadata is not None
    metrics = model.training_metadata["metrics"]
    assert metrics["training_samples"] > 0
    assert 0.0 <= metrics["test_accuracy"] <= 1.0
    assert metrics["split_strategy"] == "grouped_by_match"
    assert metrics["training_matches"] is not None


def test_create_trained_model_from_list_rows():
    rows = _synthetic_match_df(matches=6, balls_per_match=24).to_dict("records")

    model = WinPredictor.create_trained_model(rows)

    assert isinstance(model, WinPredictor)
    prob, conf = model.predict(target=170, current_runs=90, wickets_down=3, overs_done=11.0)
    assert 0.0 <= prob <= 1.0
    assert 0.0 <= conf <= 1.0


def test_compute_winprob_loads_model_from_path(monkeypatch, tmp_path):
    custom = WinPredictor(
        custom_coefs={
            "intercept": 2.0,
            "runs_remaining": 0.0,
            "balls_remaining": 0.0,
            "wickets_remaining": 0.0,
            "run_rate_required": 0.0,
            "run_rate_current": 0.0,
            "wickets_pressure": 0.0,
            "momentum_factor": 0.0,
            "target_size_factor": 0.0,
        }
    )
    artifact = tmp_path / "win_model.pkl"
    with artifact.open("wb") as f:
        pickle.dump(custom, f)

    monkeypatch.setenv("PYPITCH_WIN_MODEL_MODE", "path")
    monkeypatch.setenv("PYPITCH_WIN_MODEL_PATH", str(artifact))

    import pypitch.config as config_mod
    import pypitch.compute.winprob as winprob_mod

    reload(config_mod)
    reload(winprob_mod)

    result = winprob_mod.win_probability(
        target=150,
        current_runs=50,
        wickets_down=2,
        overs_done=10.0,
    )

    # sigmoid(2.0) ~= 0.8808
    assert abs(result["win_prob"] - 0.8808) < 0.02


def test_venue_normalization_aliases():
    model = WinPredictor()

    p1, _ = model.predict(target=180, current_runs=80, wickets_down=2, overs_done=10.0, venue="Brabourne Stadium")
    p2, _ = model.predict(target=180, current_runs=80, wickets_down=2, overs_done=10.0, venue="brabourne")

    assert abs(p1 - p2) < 1e-9


def test_create_win_predictor_uses_external_scaler_metadata():
    trainer = WinProbabilityTrainer()

    # Keep trainer.scaler unfitted; metadata must come from explicit scaler arg.
    x = pd.DataFrame(
        {
            "runs_remaining": [10, 30, 60, 80],
            "balls_remaining": [30, 40, 50, 60],
            "wickets_remaining": [7, 6, 5, 4],
            "run_rate_required": [2.0, 4.5, 7.2, 8.0],
            "run_rate_current": [9.0, 8.0, 7.0, 6.0],
            "wickets_pressure": [0, 0, 1, 1],
            "momentum_factor": [3.0, 2.0, 1.0, 0.0],
            "target_size_factor": [0.7, 0.75, 0.8, 0.85],
            "venue_adjustment": [0.1, 0.1, 0.0, 0.0],
        }
    )
    y = pd.Series([1, 1, 0, 0])

    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)

    model = LogisticRegression(max_iter=2000, random_state=42)
    model.fit(x_scaled, y)

    predictor = trainer.create_win_predictor(model, {"test_auc": 0.8}, scaler=scaler)
    assert predictor.training_metadata is not None
    assert predictor.training_metadata["scaler_mean"] is not None
    assert predictor.training_metadata["scaler_scale"] is not None
