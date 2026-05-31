"""
Study 13: Altitude Effect on Scoring
=====================================

QUESTION
--------
Do high-altitude venues in the IPL and T20 Internationals show elevated
scoring rates compared to sea-level venues?

METHODOLOGY
-----------
Dataset: IPL + T20IS.
Manually classify venues by approximate altitude:
  High  (>1000m): HPCA Dharamsala, Centurion SuperSport Park
  Medium (500-1000m): Wanderers, Newlands, Kingsmead
  Low   (<500m): Wankhede, Eden Gardens, Chepauk, MCG
Compute mean run rate per group. Two-sample t-test: high vs low.

EXPECTED RESULTS
----------------
High-altitude venues show marginally elevated scoring (thinner air,
ball travels further). Effect is small (~0.2 runs/over) compared to
pitch type effects (~1.0 runs/over).

LIMITATIONS
-----------
- Sample sizes for Dharamsala are small (<30 matches).
- Pitch type and outfield pace confound altitude effect.
- Altitude classification is manual and approximate.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

# Altitude proxy: venues known to be high/low
altitude_map = {
    "HPCA Stadium, Dharamsala": "high",
    "SuperSport Park, Centurion": "high",
    "New Wanderers Stadium, Johannesburg": "medium",
    "Newlands Cricket Ground, Cape Town": "medium",
    "Wankhede Stadium, Mumbai": "low",
    "Eden Gardens, Kolkata": "low",
    "MA Chidambaram Stadium, Chepauk": "low",
}

result = session.engine.execute_sql("""
    SELECT
        venue,
        COUNT(DISTINCT match_id)                     AS matches,
        ROUND(AVG(runs_total) * 6.0, 2)              AS approx_run_rate
    FROM ball_events
    GROUP BY venue
    HAVING matches >= 10
    ORDER BY approx_run_rate DESC
""").to_pandas()

result["altitude_group"] = result["venue"].map(altitude_map).fillna("unknown")

grouped = (
    result[result["altitude_group"] != "unknown"]
    .groupby("altitude_group")
    .agg(venues=("venue", "count"), mean_rr=("approx_run_rate", "mean"))
    .round(3)
)
print("\nRun rates by altitude group:\n")
print(grouped)
print("\nFull venue data:\n")
print(result.to_string(index=False))

# Chart: box plot of run rates by altitude group.
# Save to: research/charts/13_altitude_effect.png
