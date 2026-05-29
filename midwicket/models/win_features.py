"""Shared feature engineering for win probability models."""

from __future__ import annotations

from typing import Dict, Optional

# Constants used during model training
PAR_RUN_RATE = 6.0
RUNS_PER_BOUNDARY = 4.0
_T20_PAR_TOTAL = 200.0
_T20_BALLS = 120.0


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


# A boundary is worth 4 runs in every format — universal, not a T20 constant.
RUNS_PER_BOUNDARY = 4.0
# Par scoring rate (runs/over). A *rate* is format-intensive: it does not scale
# with innings length, so it is shared across formats rather than hardcoded per-T20.
PAR_RUN_RATE = 6.0
# Par chase total is *extensive* (it scales with innings length). It is anchored
# to a 120-ball (T20) innings and rescaled by balls_per_innings below, so the
# feature vector stays internally consistent for non-T20 formats (MW-041).
_T20_PAR_TOTAL = 200.0
_T20_BALLS = 120.0


def compute_chase_features(
    *,
    target: float,
    current_runs: float,
    wickets_down: float,
    overs_done: float,
    venue_adjustment: float,
    balls_per_innings: float = 120.0,
    balls_bowled: Optional[int] = None,
) -> Dict[str, float]:
    """Build model features from a current chase state.

    This function is shared across training and inference to avoid
    train/serve feature drift.
    """
    runs_remaining = max(0.0, float(target) - float(current_runs))
    
    if balls_bowled is not None:
        actual_balls_bowled = max(0, int(balls_bowled))
    else:
        # Smart parse for overs_done to handle both decimal overs (e.g., 10.833 -> 65) 
        # and cricket notation (e.g., 10.5 -> 65).
        import math
        overs = math.floor(float(overs_done))
        fraction = float(overs_done) - overs
        rounded_frac = round(fraction, 2)
        
        if rounded_frac in [0.1, 0.2, 0.3, 0.4, 0.5]:
            # Cricket notation (e.g. 10.5 means 10 overs and 5 balls)
            actual_balls_bowled = int(overs * 6 + round(rounded_frac * 10))
        else:
            # Standard decimal overs
            actual_balls_bowled = int(round(float(overs_done) * 6.0))
            
    balls_remaining = max(1.0, float(balls_per_innings) - float(actual_balls_bowled))
    wickets_remaining = max(0.0, 10.0 - float(wickets_down))
    overs_remaining = max(balls_remaining / 6.0, 1.0 / 6.0)

    # Convert actual_balls_bowled to canonical decimal overs completed
    overs_completed = float(actual_balls_bowled) / 6.0

    run_rate_required = runs_remaining / overs_remaining
    run_rate_current = float(current_runs) / overs_completed if overs_completed > 0.0 else 0.0

    wickets_pressure = 1.0 if wickets_down >= 3 and overs_completed < 10.0 else 0.0
    # Par total scales with format (extensive); par run rate and runs-per-boundary
    # do not (intensive / universal). For T20 (120 balls) par_total == 200.0, so
    # this is identical to the old hardcoded behavior (MW-041).
    par_total = (float(balls_per_innings) / _T20_BALLS) * _T20_PAR_TOTAL

    momentum_factor = max(0.0, run_rate_current - PAR_RUN_RATE)
    target_size_factor = min(float(target) / par_total, 1.0) if par_total > 0 else 0.0

    rr_gap = run_rate_required - run_rate_current
    required_boundary_rate = (runs_remaining / RUNS_PER_BOUNDARY) / balls_remaining
    runs_per_wicket_remaining = runs_remaining / max(1.0, wickets_remaining)
    wickets_per_over_remaining = wickets_remaining / overs_remaining
    wickets_in_hand_ratio = wickets_remaining / 10.0
    runs_per_ball_required = runs_remaining / balls_remaining
    target_runs_per_ball = float(target) / float(balls_per_innings)
    chase_progress = min(max(overs_completed / (float(balls_per_innings) / 6.0), 0.0), 1.0)
    death_overs = 1.0 if overs_completed >= (float(balls_per_innings) / 6.0) * 0.75 else 0.0
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
