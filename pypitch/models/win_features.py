"""Shared feature engineering for win probability models."""

from __future__ import annotations

from typing import Dict


FEATURE_COLUMNS = [
    "runs_remaining",
    "balls_remaining",
    "wickets_remaining",
    "overs_remaining",
    "run_rate_required",
    "run_rate_current",
    "wickets_pressure",
    "momentum_factor",
    "target_size_factor",
    "venue_adjustment",
    "rr_gap",
    "required_boundary_rate",
    "runs_per_wicket_remaining",
    "wickets_per_over_remaining",
    "wickets_in_hand_ratio",
    "runs_per_ball_required",
    "target_runs_per_ball",
    "pressure_index",
    "chase_progress",
    "death_overs",
]


def compute_chase_features(
    *,
    target: float,
    current_runs: float,
    wickets_down: float,
    overs_done: float,
    venue_adjustment: float,
) -> Dict[str, float]:
    """Build model features from a current chase state.

    This function is shared across training and inference to avoid
    train/serve feature drift.
    """
    runs_remaining = max(0.0, float(target) - float(current_runs))
    balls_bowled = int(max(0.0, float(overs_done) * 6.0))
    balls_remaining = max(1.0, 120.0 - float(balls_bowled))
    wickets_remaining = max(0.0, 10.0 - float(wickets_down))
    overs_remaining = max(balls_remaining / 6.0, 1.0 / 6.0)

    run_rate_required = runs_remaining / overs_remaining
    run_rate_current = float(current_runs) / float(overs_done) if overs_done > 0 else 0.0

    wickets_pressure = 1.0 if wickets_down >= 3 and overs_done < 10 else 0.0
    momentum_factor = max(0.0, run_rate_current - 6.0)
    target_size_factor = min(float(target) / 200.0, 1.0)

    rr_gap = run_rate_required - run_rate_current
    required_boundary_rate = (runs_remaining / 4.0) / balls_remaining
    runs_per_wicket_remaining = runs_remaining / max(1.0, wickets_remaining)
    wickets_per_over_remaining = wickets_remaining / overs_remaining
    wickets_in_hand_ratio = wickets_remaining / 10.0
    runs_per_ball_required = runs_remaining / balls_remaining
    target_runs_per_ball = float(target) / 120.0
    chase_progress = min(max(float(overs_done) / 20.0, 0.0), 1.0)
    death_overs = 1.0 if overs_done >= 15.0 else 0.0
    pressure_index = rr_gap * (1.0 + wickets_pressure + death_overs)

    return {
        "runs_remaining": runs_remaining,
        "balls_remaining": balls_remaining,
        "wickets_remaining": wickets_remaining,
        "overs_remaining": overs_remaining,
        "run_rate_required": run_rate_required,
        "run_rate_current": run_rate_current,
        "wickets_pressure": wickets_pressure,
        "momentum_factor": momentum_factor,
        "target_size_factor": target_size_factor,
        "venue_adjustment": float(venue_adjustment),
        "rr_gap": rr_gap,
        "required_boundary_rate": required_boundary_rate,
        "runs_per_wicket_remaining": runs_per_wicket_remaining,
        "wickets_per_over_remaining": wickets_per_over_remaining,
        "wickets_in_hand_ratio": wickets_in_hand_ratio,
        "runs_per_ball_required": runs_per_ball_required,
        "target_runs_per_ball": target_runs_per_ball,
        "pressure_index": pressure_index,
        "chase_progress": chase_progress,
        "death_overs": death_overs,
    }
