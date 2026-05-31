"""
Study 16: Powerplay Scoring — Batting First vs Chasing
=======================================================

QUESTION
--------
Do chasing teams score faster in the powerplay than teams setting a
target, and has this advantage grown over time?

METHODOLOGY
-----------
Dataset: IPL.
Compute mean powerplay run rate per innings (1st vs 2nd) per season.
Δ = 2nd innings PP RR − 1st innings PP RR. Track trend of Δ over seasons.

EXPECTED RESULTS
----------------
Chasing teams score 0.2–0.5 runs/over faster in the powerplay on average.
Effect grows post-2018, reflecting more aggressive chasing templates.

LIMITATIONS
-----------
- Second innings powerplay strategy depends on target size.
  Chasing 180 is different from chasing 140 — both are pooled here.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    SELECT
        YEAR(date)  AS season,
        inning,
        ROUND(SUM(runs_total) * 6.0 / NULLIF(COUNT(*), 0), 3) AS pp_run_rate
    FROM ball_events
    WHERE over < 6
    GROUP BY season, inning
    ORDER BY season, inning
""").to_pandas()

pivot = result.pivot(index="season", columns="inning", values="pp_run_rate")
pivot.columns = ["pp_rr_1st", "pp_rr_2nd"]
pivot["chase_advantage"] = (pivot["pp_rr_2nd"] - pivot["pp_rr_1st"]).round(3)
print(pivot.reset_index().to_string(index=False))

# Chart: line plot of chase_advantage by season.
# Save to: research/charts/16_powerplay_chase_advantage.png
