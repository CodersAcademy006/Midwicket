"""Midwicket compute metrics - batting / bowling / partnership / team, all on PyArrow."""

import pyarrow as pa
from midwicket.compute.metrics.batting     import calculate_strike_rate, calculate_impact_score
from midwicket.compute.metrics.bowling     import calculate_economy, calculate_pressure_index
from midwicket.compute.metrics.partnership import (
    calculate_partnership_run_rate, calculate_partnership_contribution, calculate_partnership_runs)
from midwicket.compute.metrics.team        import (
    calculate_team_win_rate, calculate_team_run_rate, calculate_average_first_innings_score)

# Batting
runs, balls = pa.array([68, 45, 30, 82, 55]), pa.array([42, 38, 28, 49, 33])
phase = pa.array(["Powerplay", "Middle", "Death", "Powerplay", "Death"])
print("SR     :", calculate_strike_rate(runs, balls).to_pylist())
print("Impact :", calculate_impact_score(runs, balls, phase).to_pylist())

# Bowling
r_c, lb, db = pa.array([24, 38, 32, 22, 28]), pa.array([24] * 5), pa.array([10, 6, 8, 12, 9])
print("Econ   :", calculate_economy(r_c, lb).to_pylist())
print("PI     :", calculate_pressure_index(db, lb).to_pylist())

# Partnership
p1, p2, pb = pa.array([45, 30, 20]), pa.array([35, 50, 10]), pa.array([48, 42, 18])
total = calculate_partnership_runs(p1, p2)
print("P-runs :", total.to_pylist())
print("P-RR   :", calculate_partnership_run_rate(total, pb).to_pylist())
print("P1 %   :", calculate_partnership_contribution(p1, total).to_pylist())

# Team
wins, m = pa.array([9, 10, 7, 8, 6]), pa.array([14] * 5)
print("Win %  :", calculate_team_win_rate(wins, m).to_pylist())
print("RR     :", calculate_team_run_rate(
    pa.array([2450, 2380, 2210, 2300, 2150]),
    pa.array([195.0, 200.0, 198.0, 202.0, 193.0])).to_pylist())
print("Avg1st :", calculate_average_first_innings_score(
    pa.array([4900, 4760, 4420, 4600, 4300]), m).to_pylist())
