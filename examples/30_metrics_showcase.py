"""
30_metrics_showcase.py — Pure Compute Metrics (no database required)

All functions in pypitch.compute.metrics operate on PyArrow arrays.
This script builds synthetic data and exercises every metric family:
  - Batting  (strike rate, impact score)
  - Bowling  (economy, pressure index)
  - Partnership (run rate, contribution)
  - Team    (win rate, run rate)

Usage:
    python examples/30_metrics_showcase.py
"""

import pyarrow as pa
import pyarrow.compute as pc

# Batting
from pypitch.compute.metrics.batting import (
    calculate_strike_rate,
    calculate_impact_score,
)

# Bowling
from pypitch.compute.metrics.bowling import (
    calculate_economy,
    calculate_pressure_index,
)

# Partnership
from pypitch.compute.metrics.partnership import (
    calculate_partnership_run_rate,
    calculate_partnership_contribution,
    calculate_partnership_runs,
)

# Team
from pypitch.compute.metrics.team import (
    calculate_team_win_rate,
    calculate_team_run_rate,
    calculate_average_first_innings_score,
)


def section(title: str) -> None:
    print(f"\n{'=' * 55}")
    print(f"  {title}")
    print("=" * 55)


def main() -> None:
    print("PyPitch Metrics Showcase — synthetic data demo")

    # ------------------------------------------------------------------
    # Batting
    # ------------------------------------------------------------------
    section("BATTING METRICS")

    players = ["Kohli", "Rohit", "Dhoni", "Buttler", "Warner"]
    runs   = pa.array([68, 45, 30, 82, 55], type=pa.int64())
    balls  = pa.array([42, 38, 28, 49, 33], type=pa.int64())
    phase  = pa.array(["Powerplay", "Middle", "Death", "Powerplay", "Death"])

    sr = calculate_strike_rate(runs, balls)
    impact = calculate_impact_score(runs, balls, phase)

    print(f"\n{'Player':<12} {'Runs':>6} {'Balls':>6} {'SR':>8} {'Impact':>9}")
    print("-" * 46)
    for i, name in enumerate(players):
        print(
            f"{name:<12} {runs[i].as_py():>6} {balls[i].as_py():>6}"
            f" {sr[i].as_py():>8.1f} {impact[i].as_py():>9.1f}"
        )

    # ------------------------------------------------------------------
    # Bowling
    # ------------------------------------------------------------------
    section("BOWLING METRICS")

    bowlers      = ["Bumrah", "Shami", "Chahal", "Rashid", "Kuldeep"]
    runs_c       = pa.array([24, 38, 32, 22, 28], type=pa.int64())
    legal_balls  = pa.array([24, 24, 24, 24, 24], type=pa.int64())
    dot_balls    = pa.array([10, 6, 8, 12, 9], type=pa.int64())

    econ    = calculate_economy(runs_c, legal_balls)
    pressure = calculate_pressure_index(dot_balls, legal_balls)

    print(f"\n{'Bowler':<12} {'Runs':>6} {'Economy':>9} {'PressureIdx':>13}")
    print("-" * 44)
    for i, name in enumerate(bowlers):
        print(
            f"{name:<12} {runs_c[i].as_py():>6}"
            f" {econ[i].as_py():>9.2f} {pressure[i].as_py():>13.1f}"
        )

    # ------------------------------------------------------------------
    # Partnership
    # ------------------------------------------------------------------
    section("PARTNERSHIP METRICS")

    p_runs1  = pa.array([45, 30, 20], type=pa.int64())
    p_runs2  = pa.array([35, 50, 10], type=pa.int64())
    p_balls  = pa.array([48, 42, 18], type=pa.int64())

    total_p  = calculate_partnership_runs(p_runs1, p_runs2)
    prr      = calculate_partnership_run_rate(total_p, p_balls)
    contrib  = calculate_partnership_contribution(p_runs1, total_p)

    print(f"\n{'Partnership':>12} {'Total':>7} {'RR/Over':>9} {'P1 Contrib %':>14}")
    print("-" * 46)
    for i in range(len(p_runs1)):
        label = f"P{i+1} ({p_runs1[i].as_py()}+{p_runs2[i].as_py()})"
        print(
            f"{label:>12} {total_p[i].as_py():>7}"
            f" {prr[i].as_py():>9.2f} {contrib[i].as_py():>14.1f}"
        )

    # ------------------------------------------------------------------
    # Team
    # ------------------------------------------------------------------
    section("TEAM METRICS")

    teams    = ["MI", "CSK", "RCB", "KKR", "DC"]
    wins     = pa.array([9, 10, 7, 8, 6], type=pa.int64())
    matches  = pa.array([14, 14, 14, 14, 14], type=pa.int64())
    tot_runs = pa.array([2450, 2380, 2210, 2300, 2150], type=pa.int64())
    overs    = pa.array([195.0, 200.0, 198.0, 202.0, 193.0], type=pa.float64())
    fi_runs  = pa.array([4900, 4760, 4420, 4600, 4300], type=pa.int64())
    fi_count = pa.array([14, 14, 14, 14, 14], type=pa.int64())

    win_rate = calculate_team_win_rate(wins, matches)
    rr       = calculate_team_run_rate(tot_runs, overs)
    avg_fi   = calculate_average_first_innings_score(fi_runs, fi_count)

    print(f"\n{'Team':<8} {'Win%':>7} {'RR':>7} {'Avg1stInns':>12}")
    print("-" * 38)
    for i, name in enumerate(teams):
        print(
            f"{name:<8} {win_rate[i].as_py():>7.1f}"
            f" {rr[i].as_py():>7.2f} {avg_fi[i].as_py():>12.1f}"
        )

    print("\nAll metrics computed in-memory — no database session required.")


if __name__ == "__main__":
    main()
