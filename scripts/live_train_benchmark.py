"""Live end-to-end win model training + benchmark selection.

This script will:
1. Download/update raw IPL data from Cricsheet.
2. Canonicalize + ingest deliveries into DuckDB.
3. Train multiple logistic configurations (C, class_weight, random_state).
4. Select best model by highest test AUC then lowest Brier score.
5. Register best model in ModelRegistry and print deploy env vars.

Usage:
    e:/Srijan/PyPitch/.venv/Scripts/python.exe scripts/live_train_benchmark.py --data-dir ./data
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score

from pypitch.api.session import PyPitchSession
from pypitch.core.canonicalize import canonicalize_match
from pypitch.models.registry import get_model_registry
from pypitch.models.train import WinProbabilityTrainer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live benchmark training for win probability model")
    parser.add_argument("--data-dir", default="./data", help="Data directory for DuckDB and raw files")
    parser.add_argument("--force-download", action="store_true", help="Force redownload raw dataset")
    parser.add_argument("--max-matches", type=int, default=0, help="Optional cap for number of matches (0 = all)")
    return parser.parse_args()


def ingest_all_matches(session: PyPitchSession, max_matches: int = 0) -> dict[str, int]:
    # Reset training table to avoid duplicate rows across repeated runs.
    session.engine.execute_sql("DROP TABLE IF EXISTS ball_events", read_only=False)

    files = sorted(session.loader.raw_dir.glob("*.json"))
    if max_matches and max_matches > 0:
        files = files[:max_matches]

    ok = 0
    fail = 0
    start = time.time()
    for idx, file_path in enumerate(files, start=1):
        match_id = file_path.stem
        try:
            raw = session.loader.get_match(match_id)
            table = canonicalize_match(raw, session.registry, match_id=match_id)
            session.engine.ingest_events(table, snapshot_tag=f"match_{match_id}", append=True)
            ok += 1
        except Exception:
            fail += 1

        if idx % 100 == 0:
            print(f"Ingested {idx}/{len(files)} matches (ok={ok}, fail={fail})")

    elapsed = time.time() - start
    print(f"Ingestion complete: ok={ok}, fail={fail}, elapsed={elapsed:.1f}s")
    return {"ok": ok, "fail": fail, "total": len(files), "elapsed_s": round(elapsed, 2)}


def load_training_events(session: PyPitchSession) -> pd.DataFrame:
    query = """
        SELECT
            match_id,
            inning,
            over,
            ball,
            runs_batter,
            runs_extras,
            is_wicket,
            venue_id
        FROM ball_events
        ORDER BY match_id, inning, over, ball
    """
    return session.engine.execute_sql(query).to_pandas()


def evaluate_config(
    features: pd.DataFrame,
    target: pd.Series,
    groups: pd.Series,
    *,
    c_value: float,
    class_weight: str | None,
    random_state: int,
) -> tuple[dict, LogisticRegression, StandardScaler]:
    grouped = pd.DataFrame({"group": groups.values, "target": target.values})
    group_targets = grouped.groupby("group")["target"].max().astype(int)

    group_values = group_targets.index.to_numpy(dtype=object)
    group_labels = group_targets.to_numpy(dtype=int)

    train_groups, test_groups = train_test_split(
        group_values,
        test_size=0.2,
        random_state=random_state,
        stratify=group_labels,
    )

    train_mask = groups.isin(train_groups)
    test_mask = groups.isin(test_groups)

    x_train = features.loc[train_mask].reset_index(drop=True)
    y_train = target.loc[train_mask].reset_index(drop=True)
    g_train = groups.loc[train_mask].reset_index(drop=True)
    x_test = features.loc[test_mask].reset_index(drop=True)
    y_test = target.loc[test_mask].reset_index(drop=True)

    scaler = StandardScaler()
    x_train_scaled = scaler.fit_transform(x_train)
    x_test_scaled = scaler.transform(x_test)

    model = LogisticRegression(
        random_state=random_state,
        max_iter=3000,
        class_weight=class_weight,
        C=c_value,
    )
    model.fit(x_train_scaled, y_train)

    train_prob = model.predict_proba(x_train_scaled)[:, 1]
    test_prob = model.predict_proba(x_test_scaled)[:, 1]

    # Group-aware CV
    min_group_class = int(pd.DataFrame({"g": g_train, "y": y_train}).groupby("g")["y"].max().value_counts().min())
    n_splits = max(2, min(5, min_group_class))
    cv = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=random_state)
    cv_pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "lr",
                LogisticRegression(
                    random_state=random_state,
                    max_iter=3000,
                    class_weight=class_weight,
                    C=c_value,
                ),
            ),
        ]
    )
    cv_auc = cross_val_score(cv_pipeline, x_train, y_train, cv=cv, groups=g_train, scoring="roc_auc").tolist()

    metrics = {
        "C": c_value,
        "class_weight": class_weight,
        "random_state": random_state,
        "train_accuracy": float(accuracy_score(y_train, train_prob > 0.5)),
        "test_accuracy": float(accuracy_score(y_test, test_prob > 0.5)),
        "train_log_loss": float(log_loss(y_train, train_prob)),
        "test_log_loss": float(log_loss(y_test, test_prob)),
        "train_auc": float(roc_auc_score(y_train, train_prob)),
        "test_auc": float(roc_auc_score(y_test, test_prob)),
        "train_brier": float(brier_score_loss(y_train, train_prob)),
        "test_brier": float(brier_score_loss(y_test, test_prob)),
        "cv_auc_mean": float(np.mean(cv_auc)),
        "cv_auc_std": float(np.std(cv_auc)),
        "training_samples": int(len(x_train)),
        "test_samples": int(len(x_test)),
        "training_matches": int(pd.Series(g_train).nunique()),
        "test_matches": int(pd.Series(groups.loc[test_mask]).nunique()),
        "split_strategy": "grouped_by_match",
    }

    return metrics, model, scaler


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    print("Initializing PyPitch session...")
    session = PyPitchSession(data_dir=str(data_dir))

    try:
        print("Downloading dataset...")
        session.download_data(force=args.force_download)

        ingest_stats = ingest_all_matches(session, max_matches=args.max_matches)

        print("Loading training events from DuckDB...")
        events = load_training_events(session)
        print(f"Training rows: {len(events):,}")

        trainer = WinProbabilityTrainer()
        features, target, groups = trainer.prepare_training_dataset(events)

        print("Running benchmark grid (retraining multiple configs)...")
        c_values = [0.1, 0.5, 1.0, 2.0, 5.0]
        class_weights = ["balanced", None]
        seeds = [42, 73, 101]

        runs: list[tuple[dict, LogisticRegression, StandardScaler]] = []
        for c in c_values:
            for cw in class_weights:
                for seed in seeds:
                    metrics, model, scaler = evaluate_config(
                        features,
                        target,
                        groups,
                        c_value=c,
                        class_weight=cw,
                        random_state=seed,
                    )
                    runs.append((metrics, model, scaler))
                    print(
                        f"C={c:>4} cw={str(cw):>8} seed={seed} "
                        f"AUC={metrics['test_auc']:.4f} Brier={metrics['test_brier']:.4f}"
                    )

        runs.sort(key=lambda x: (-x[0]["test_auc"], x[0]["test_brier"], x[0]["test_log_loss"]))
        best_metrics, best_model, best_scaler = runs[0]

        predictor = trainer.create_win_predictor(best_model, best_metrics, scaler=best_scaler)
        registry = get_model_registry()
        version = registry.register_model(
            name="win_predictor",
            model=predictor,
            metadata={
                "type": "win_probability",
                "training_date": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "best_metrics": best_metrics,
                "benchmark_runs": [m for m, _, _ in runs],
                "ingest_stats": ingest_stats,
                "data_samples": len(features),
            },
        )

        report = {
            "model_version": version,
            "best_metrics": best_metrics,
            "top_5": [m for m, _, _ in runs[:5]],
            "ingest_stats": ingest_stats,
        }
        report_path = data_dir / "win_model_benchmark_report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print("\n=== BEST MODEL ===")
        print(json.dumps(best_metrics, indent=2))
        print(f"\nModel registered: {version}")
        print(f"Report written: {report_path}")
        print("\nDeploy with:")
        print("  PYPITCH_WIN_MODEL_MODE=registry")
        print(f"  PYPITCH_WIN_MODEL_VERSION={version}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
