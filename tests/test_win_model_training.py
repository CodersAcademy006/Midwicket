"""Tests for trainable win model and deployment loading paths."""

from importlib import reload
import hashlib
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


def _synthetic_ball_events_df(matches: int = 8, balls_per_match: int = 24) -> pd.DataFrame:
    rows = []
    for mid in range(matches):
        target = 151 + (mid % 10)
        final_runs = target + 6 if mid % 2 == 0 else target - 8
        for i in range(balls_per_match * 2):
            inning = 1 if i < balls_per_match else 2
            ball_index = i % balls_per_match
            over = ball_index // 6
            ball = (ball_index % 6) + 1
            runs_batter = 1 if (i + mid) % 4 == 0 else 0
            runs_extras = 1 if (i + mid) % 11 == 0 else 0
            is_wicket = bool((i + mid) % 17 == 0)
            rows.append(
                {
                    "match_id": f"m{mid}",
                    "inning": inning,
                    "over": over,
                    "ball": ball,
                    "runs_batter": runs_batter,
                    "runs_extras": runs_extras,
                    "is_wicket": is_wicket,
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

    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()

    monkeypatch.setenv("PYPITCH_WIN_MODEL_MODE", "path")
    monkeypatch.setenv("PYPITCH_WIN_MODEL_PATH", str(artifact))
    monkeypatch.setenv("PYPITCH_WIN_MODEL_SHA256", digest)

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


def test_load_default_uses_bundled_model_metadata():
    model = WinPredictor.load_default()
    assert isinstance(model, WinPredictor)
    assert model.training_metadata is not None
    assert model.training_metadata.get("source") == "bundled"
    assert len(model.training_metadata.get("scaler_mean", [])) > 0
    assert abs(model.coefs.get("intercept", 0.0) - (-0.9245411089399495)) < 1e-9


def test_winprob_module_initializes_bundled_default_model(monkeypatch):
    monkeypatch.delenv("PYPITCH_WIN_MODEL_MODE", raising=False)
    monkeypatch.delenv("PYPITCH_WIN_MODEL_PATH", raising=False)
    monkeypatch.delenv("PYPITCH_WIN_MODEL_SHA256", raising=False)

    import pypitch.config as config_mod
    import pypitch.compute.winprob as winprob_mod

    reload(config_mod)
    reload(winprob_mod)

    with winprob_mod._model_lock:
        model = winprob_mod._default_model

    assert isinstance(model, WinPredictor)
    assert model.training_metadata is not None
    assert model.training_metadata.get("source") == "bundled"


def test_venue_normalization_aliases():
    model = WinPredictor()

    p1, _ = model.predict(target=180, current_runs=80, wickets_down=2, overs_done=10.0, venue="Brabourne Stadium")
    p2, _ = model.predict(target=180, current_runs=80, wickets_down=2, overs_done=10.0, venue="brabourne")

    assert abs(p1 - p2) < 1e-9


def test_prepare_training_dataset_from_ball_events():
    trainer = WinProbabilityTrainer()
    ball_events = _synthetic_ball_events_df(matches=8, balls_per_match=24)

    features, target, groups = trainer.prepare_training_dataset(ball_events)

    assert len(features) == len(target) == len(groups)
    assert len(features) > 0
    assert set(groups.unique()) == {f"m{i}" for i in range(8)}
    assert "pressure_index" in features.columns
    assert "overs_remaining" in features.columns


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
