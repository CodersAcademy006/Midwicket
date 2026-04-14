"""Train and register a production win-probability model from DuckDB events.

Usage:
    e:/Srijan/PyPitch/.venv/Scripts/python.exe scripts/train_win_model.py --db-path ./data/pypitch.duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pypitch.models.train import WinProbabilityTrainer
from pypitch.storage.engine import QueryEngine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and register a win-probability model")
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to pypitch DuckDB file containing ball_events",
    )
    parser.add_argument(
        "--model-name",
        default="win_predictor",
        help="Registry model name",
    )
    return parser.parse_args()


def load_training_events(engine: QueryEngine):
    if not engine.table_exists("ball_events"):
        raise RuntimeError("ball_events table not found. Ingest matches before training.")

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
    return engine.execute_sql(query).to_pandas()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)

    if not db_path.exists():
        print(f"Error: DB path does not exist: {db_path}")
        return 1

    engine = QueryEngine(str(db_path))
    try:
        events = load_training_events(engine)
        trainer = WinProbabilityTrainer()
        version = trainer.train_and_register(events, model_name=args.model_name)
        print(f"Model trained and registered: {version}")
        print("Deploy with:")
        print("  PYPITCH_WIN_MODEL_MODE=registry")
        print(f"  PYPITCH_WIN_MODEL_VERSION={version}")
        return 0
    finally:
        engine.close()


if __name__ == "__main__":
    sys.exit(main())
