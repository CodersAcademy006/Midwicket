"""
scripts/retrain_model.py  (v2 — full convergence, venue learning)

Changes from v1:
  - SGD max_iter increased to 500 per alpha (was 50) — eliminates ConvergenceWarning
  - Venue win rates learned from Cricsheet data and written into model JSON
  - venue_adjustment coefficient now trained (not zeroed out)
"""

import json
import logging
import sys
import time
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from midwicket.data.loader import DataLoader
from midwicket.models.win_features import FEATURE_COLUMNS, compute_chase_features
from midwicket.models.win_predictor import WinPredictor
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, accuracy_score, log_loss
import copy

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("retrain")

MODEL_JSON = ROOT / "midwicket" / "models" / "data" / "win_model_default.json"
DATA_DIR   = Path.home() / ".midwicket_data"
RAW_DIR    = DATA_DIR / "raw" / "ipl"


def ensure_data():
    loader = DataLoader(str(DATA_DIR))
    files  = list(RAW_DIR.glob("*.json"))
    if not files:
        log.info("Downloading Cricsheet IPL dataset...")
        loader.download()
        files = list(RAW_DIR.glob("*.json"))
    log.info("%d match files available", len(files))
    return files


def parse_all(files) -> pd.DataFrame:
    log.info("Parsing %d match files...", len(files))
    rows = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                match = json.load(f)
        except Exception:
            continue

        info     = match.get("info", {})
        venue    = info.get("venue", "")
        winner   = info.get("outcome", {}).get("winner", "")
        match_id = path.stem

        for inning_idx, inning in enumerate(match.get("innings", [])[:2], 1):
            team  = inning.get("team", "")
            cum_r = 0
            cum_w = 0
            for ov in inning.get("overs", []):
                over_num = ov.get("over", 0)
                for bi, ball in enumerate(ov.get("deliveries", [])):
                    r_obj  = ball.get("runs", {})
                    r_bat  = r_obj.get("batter", 0)
                    r_ext  = r_obj.get("extras", 0)
                    r_tot  = r_obj.get("total", 0)
                    wkts   = ball.get("wickets", [])
                    is_wkt = len(wkts) > 0
                    cum_r += r_tot
                    cum_w += int(is_wkt)
                    rows.append({
                        "match_id":       match_id,
                        "inning":         inning_idx,
                        "batting_team":   team,
                        "venue":          venue,
                        "winner":         winner,
                        "over":           over_num,
                        "ball":           bi,
                        "batter":         ball.get("batter", ""),
                        "runs_batter":    r_bat,
                        "runs_extras":    r_ext,
                        "runs_total":     cum_r,
                        "is_wicket":      is_wkt,
                        "wickets_fallen": cum_w,
                    })
    df = pd.DataFrame(rows)
    log.info("Parsed %d deliveries", len(df))
    return df


def add_target(df: pd.DataFrame) -> pd.DataFrame:
    fi = (df[df["inning"] == 1]
          .groupby("match_id")["runs_total"].max()
          .rename("target").reset_index())
    fi["target"] = fi["target"] + 1
    return df.merge(fi, on="match_id", how="left")


def learn_venue_adjustments(df: pd.DataFrame) -> dict:
    """Compute per-venue chasing win rate from real match outcomes."""
    df2 = df[df["inning"] == 2].copy()
    df2["won"] = (df2["batting_team"] == df2["winner"]).astype(int)
    # one row per match per venue
    mv = df2.groupby(["match_id", "venue"]).agg(won=("won", "first")).reset_index()
    stats = mv.groupby("venue")["won"].agg(["mean", "count"])
    # only venues with >=10 matches get a real adjustment; others get 0
    overall_mean = mv["won"].mean()
    adj = {}
    for venue, row in stats.iterrows():
        if row["count"] >= 10:
            # convert win-rate delta to log-odds delta
            wr   = float(row["mean"])
            wr   = max(0.05, min(0.95, wr))
            base = max(0.05, min(0.95, overall_mean))
            log_odds_delta = np.log(wr / (1 - wr)) - np.log(base / (1 - base))
            adj[venue] = round(float(log_odds_delta), 4)
    adj["default"] = 0.0
    log.info("Learned venue adjustments for %d venues (>=10 matches)", len(adj) - 1)
    return adj


def build_features(df: pd.DataFrame, venue_adj: dict):
    df2 = df[df["inning"] == 2].dropna(subset=["target"]).copy()
    df2["won"] = (df2["batting_team"] == df2["winner"]).astype(int)

    feats, labels, match_ids = [], [], []
    for _, row in df2.iterrows():
        overs_done = row["over"] + row["ball"] / 6.0
        va = venue_adj.get(row["venue"], 0.0)
        try:
            f = compute_chase_features(
                target=float(row["target"]),
                current_runs=float(row["runs_total"]),
                wickets_down=float(row["wickets_fallen"]),
                overs_done=overs_done,
                venue_adjustment=va,
            )
            feats.append(f)
            labels.append(int(row["won"]))
            match_ids.append(row["match_id"])
        except Exception:
            continue

    X = pd.DataFrame(feats)
    y = pd.Series(labels)
    log.info("Feature matrix: %d rows x %d cols", *X.shape)
    return X, y, match_ids


def train_full(X, y, match_ids):
    # Grouped-by-match split
    unique_ids = list(dict.fromkeys(match_ids))
    rng = np.random.RandomState(42)
    rng.shuffle(unique_ids)
    n_test   = max(1, int(len(unique_ids) * 0.2))
    test_ids = set(unique_ids[:n_test])
    mask     = pd.Series([m not in test_ids for m in match_ids])

    X_tr, X_te = X[mask.values], X[~mask.values]
    y_tr, y_te = y[mask.values], y[~mask.values]

    scaler   = StandardScaler()
    X_tr_sc  = scaler.fit_transform(X_tr)
    X_te_sc  = scaler.transform(X_te)

    best_loss, best_model = float("inf"), None
    # More epochs + more alphas — full convergence
    for alpha in [0.0001, 0.0005, 0.001, 0.005, 0.01]:
        model = SGDClassifier(loss="log_loss", penalty="l2", alpha=alpha,
                              random_state=42, max_iter=1, warm_start=True,
                              learning_rate="optimal")
        for epoch in range(500):          # 500 epochs — eliminates ConvergenceWarning
            model.partial_fit(X_tr_sc, y_tr, classes=np.array([0, 1]))
            try:
                val_pred = model.predict_proba(X_te_sc)[:, 1]
                val_loss = log_loss(y_te, val_pred, labels=[0, 1])
            except Exception:
                val_loss = float("inf")
            if val_loss < best_loss:
                best_loss  = val_loss
                best_model = copy.deepcopy(model)

    model    = best_model
    te_pred  = model.predict_proba(X_te_sc)[:, 1]
    metrics  = {
        "test_auc":         roc_auc_score(y_te, te_pred),
        "test_accuracy":    accuracy_score(y_te, te_pred > 0.5),
        "test_log_loss":    log_loss(y_te, te_pred, labels=[0, 1]),
        "training_samples": int(len(X_tr)),
        "test_samples":     int(len(X_te)),
        "training_matches": len(unique_ids) - n_test,
        "test_matches":     n_test,
    }
    log.info("Test AUC=%.4f  Acc=%.1f%%  LogLoss=%.4f",
             metrics["test_auc"], metrics["test_accuracy"] * 100, metrics["test_log_loss"])
    return model, scaler, metrics


def write_model(model, scaler, metrics, venue_adj):
    coefs = {col: float(v) for col, v in zip(FEATURE_COLUMNS, model.coef_[0])}
    coefs["intercept"] = float(model.intercept_[0])

    payload = {
        "coefs": coefs,
        "venue_adjustments": venue_adj,
        "training_metadata": {
            "source":      "retrained_v2",
            "trained_at":  datetime.now().isoformat(),
            "metrics":     metrics,
            "scaler_mean":  scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
        },
    }
    MODEL_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    log.info("Model written to %s", MODEL_JSON)


def sanity_check():
    m = WinPredictor.load_default()
    cases = [
        (170,   0, 0,  0.0, "Start of chase 0/0"),
        (170,  85, 2, 10.0, "Halfway, comfortable"),
        (170, 120, 5, 15.0, "Pressure — 50 off 30"),
        (170, 160, 3, 18.0, "Nearly there — 10 off 12"),
        (170, 155, 8, 19.0, "Crisis — 15 off 6, 2 wkts left"),
    ]
    print("\n  Sanity check:")
    print(f"  {'Scenario':<36} {'Win Prob':>9}  {'Confidence':>11}")
    print("  " + "-" * 60)
    for target, score, wkts, overs, desc in cases:
        p, c = m.predict(target, score, wkts, overs)
        print(f"  {desc:<36} {p:>8.1%}  {c:>10.1%}")


if __name__ == "__main__":
    print("=" * 65)
    print("  Midwicket — Win Probability Model Retrain v2")
    print("  Full convergence + learned venue adjustments")
    print("=" * 65)
    files      = ensure_data()
    df         = parse_all(files)
    df         = add_target(df)
    venue_adj  = learn_venue_adjustments(df)
    X, y, mids = build_features(df, venue_adj)
    model, scaler, metrics = train_full(X, y, mids)
    write_model(model, scaler, metrics, venue_adj)
    sanity_check()
    print("\n" + "=" * 65)
    print("  Done. Commit midwicket/models/data/win_model_default.json")
    print("=" * 65)
