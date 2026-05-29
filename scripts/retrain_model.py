"""
scripts/retrain_model.py

Full retrain pipeline:
  1. Download the Cricsheet IPL ball-by-ball dataset (~50 MB)
  2. Parse all JSON match files into a flat ball-events DataFrame
  3. Engineer features (same function used at inference — no train/serve drift)
  4. Train logistic regression with grouped-by-match train/test split
  5. Evaluate and print metrics
  6. Write new coefficients to midwicket/models/data/win_model_default.json

Usage:
    cd /path/to/Midwicket
    .venv/bin/python scripts/retrain_model.py

The script overwrites win_model_default.json in-place. Commit the result
to lock the new model into the repo.
"""

import json
import logging
import sys
import time
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

# ── project root on sys.path ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from midwicket.data.loader import DataLoader
from midwicket.models.train import WinProbabilityTrainer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("retrain")

MODEL_JSON = ROOT / "midwicket" / "models" / "data" / "win_model_default.json"
DATA_DIR   = Path.home() / ".midwicket_data"


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Download data
# ─────────────────────────────────────────────────────────────────────────────

def ensure_data() -> Path:
    loader = DataLoader(str(DATA_DIR))
    raw_dir = loader.raw_dir
    json_files = list(raw_dir.glob("*.json"))

    if json_files:
        log.info("Data already present: %d match files in %s", len(json_files), raw_dir)
    else:
        log.info("Downloading Cricsheet IPL dataset (~50 MB)...")
        t0 = time.time()
        loader.download()
        elapsed = time.time() - t0
        json_files = list(raw_dir.glob("*.json"))
        log.info("Downloaded and extracted in %.1fs — %d match files", elapsed, len(json_files))

    return raw_dir


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Parse JSON files into a flat ball-events DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def parse_cricsheet(raw_dir: Path) -> pd.DataFrame:
    log.info("Parsing match JSON files...")
    rows = []
    skipped = 0

    for path in sorted(raw_dir.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                match = json.load(f)
        except (json.JSONDecodeError, OSError):
            skipped += 1
            continue

        info    = match.get("info", {})
        innings = match.get("innings", [])
        match_id = path.stem

        # Venue
        venue = info.get("venue", "")

        # Derive match result — who won and did they bat first?
        outcome  = info.get("outcome", {})
        winner   = outcome.get("winner", "")
        teams    = info.get("teams", [])

        # batting_team for each inning
        for inning_idx, inning in enumerate(innings[:2], start=1):
            batting_team = inning.get("team", "")
            overs_data   = inning.get("overs", [])
            cumulative_runs   = 0
            cumulative_wickets = 0

            for over_data in overs_data:
                over_num     = over_data.get("over", 0)
                deliveries   = over_data.get("deliveries", [])

                for ball_idx, delivery in enumerate(deliveries):
                    runs_obj    = delivery.get("runs", {})
                    runs_batter = runs_obj.get("batter", 0)
                    runs_extras = runs_obj.get("extras", 0)
                    total_runs  = runs_obj.get("total", 0)

                    wickets = delivery.get("wickets", [])
                    is_wicket = len(wickets) > 0

                    cumulative_runs    += total_runs
                    cumulative_wickets += int(is_wicket)

                    rows.append({
                        "match_id":        match_id,
                        "inning":          inning_idx,
                        "over":            over_num,
                        "ball":            ball_idx,
                        "batting_team":    batting_team,
                        "venue":           venue,
                        "runs_batter":     runs_batter,
                        "runs_extras":     runs_extras,
                        "runs_total":      cumulative_runs,
                        "is_wicket":       is_wicket,
                        "wickets_fallen":  cumulative_wickets,
                        "winner":          winner,
                    })

    log.info("Parsed %d deliveries from %d matches (%d skipped)",
             len(rows), len(list(raw_dir.glob("*.json"))) - skipped, skipped)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Build training dataset (derive target, filter to 2nd innings)
# ─────────────────────────────────────────────────────────────────────────────

def build_training_df(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Building training dataset...")

    # Derive target for each match = 1st innings total + 1
    first_innings_totals = (
        df[df["inning"] == 1]
        .groupby("match_id")["runs_total"]
        .max()
        .rename("target")
        .reset_index()
    )
    first_innings_totals["target"] = first_innings_totals["target"] + 1

    df = df.merge(first_innings_totals, on="match_id", how="left")

    # Keep only 2nd innings
    df2 = df[df["inning"] == 2].copy()
    df2 = df2.dropna(subset=["target"])

    # Label: did the batting team (chasing) win?
    df2["won"] = (df2["batting_team"] == df2["winner"]).astype(int)

    log.info("Training set: %d deliveries across %d unique matches",
             len(df2), df2["match_id"].nunique())
    log.info("Win rate in training set: %.1f%%", df2["won"].mean() * 100)

    return df2


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Train
# ─────────────────────────────────────────────────────────────────────────────

def train(df2: pd.DataFrame):
    log.info("Engineering features and training model...")
    trainer = WinProbabilityTrainer()

    # prepare_training_data expects: match_id, inning, over, ball,
    #                                runs_total, wickets_fallen, target, venue
    subset = df2[["match_id", "inning", "over", "ball",
                  "runs_total", "wickets_fallen", "target", "venue"]].copy()

    features, target = trainer.prepare_training_data(subset)

    match_ids = df2["match_id"].iloc[:len(features)].tolist()

    log.info("Feature matrix: %d rows x %d cols", *features.shape)

    t0 = time.time()
    model, metrics = trainer.train_model(features, target, match_ids=match_ids)
    elapsed = time.time() - t0

    log.info("Training complete in %.1fs", elapsed)
    log.info("  Test AUC      : %.4f", metrics["test_auc"])
    log.info("  Test accuracy : %.1f%%", metrics["test_accuracy"] * 100)
    log.info("  Test log-loss : %.4f", metrics["test_log_loss"])
    log.info("  Train samples : %d", metrics["training_samples"])
    log.info("  Test samples  : %d", metrics["test_samples"])
    if metrics.get("training_matches"):
        log.info("  Train matches : %d", metrics["training_matches"])

    predictor = trainer.create_win_predictor(model, metrics)
    return predictor, metrics


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Write new model JSON
# ─────────────────────────────────────────────────────────────────────────────

def write_model(predictor, metrics):
    log.info("Writing new model to %s ...", MODEL_JSON)

    scaler_mean  = predictor.training_metadata.get("scaler_mean", [])
    scaler_scale = predictor.training_metadata.get("scaler_scale", [])

    payload = {
        "coefs": predictor.coefs,
        "venue_adjustments": predictor.venue_adjustments,
        "training_metadata": {
            "source":           "retrained",
            "trained_at":       datetime.now().isoformat(),
            "metrics": {
                "test_auc":          metrics["test_auc"],
                "test_accuracy":     metrics["test_accuracy"],
                "test_log_loss":     metrics["test_log_loss"],
                "training_samples":  metrics["training_samples"],
                "test_samples":      metrics["test_samples"],
                "training_matches":  metrics.get("training_matches"),
            },
            "scaler_mean":  scaler_mean,
            "scaler_scale": scaler_scale,
        },
    }

    MODEL_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    log.info("Model written successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Quick sanity check
# ─────────────────────────────────────────────────────────────────────────────

def sanity_check():
    # Reload from disk to confirm JSON round-trips correctly
    from midwicket.models.win_predictor import WinPredictor
    model = WinPredictor.load_default()

    cases = [
        # (target, score, wkts, overs, description)
        (170, 0,   0,  0.0,  "Start of chase, 0/0"),
        (170, 85,  2, 10.0,  "Halfway, comfortable"),
        (170, 120, 5, 15.0,  "Pressure — 50 off 30"),
        (170, 160, 3, 18.0,  "Nearly there, 10 off 12"),
        (170, 155, 8, 19.0,  "Crisis — 15 off 6, 2 wkts"),
    ]

    print("\nSanity check — loaded model from disk:")
    print(f"  {'Scenario':<38} {'Win Prob':>9}  {'Confidence':>11}")
    print("  " + "-" * 62)
    for target, score, wkts, overs, desc in cases:
        prob, conf = model.predict(target, score, wkts, overs)
        print(f"  {desc:<38} {prob:>8.1%}  {conf:>10.1%}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  Midwicket — Win Probability Model Retrain")
    print("=" * 65)

    raw_dir   = ensure_data()
    df        = parse_cricsheet(raw_dir)
    df2       = build_training_df(df)
    predictor, metrics = train(df2)
    write_model(predictor, metrics)
    sanity_check()

    print("=" * 65)
    print("  Done. Commit midwicket/models/data/win_model_default.json")
    print("  to lock the retrained model into the repository.")
    print("=" * 65)
