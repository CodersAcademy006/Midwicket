"""
Study 02: Powerplay Boundary Surge
===================================

QUESTION
--------
When did powerplay boundary rates (fours + sixes per ball) structurally
shift upward in IPL cricket, and by how much?

METHODOLOGY
-----------
Dataset: IPL (2008–2026).
Unit: powerplay boundary rate = (fours + sixes) / balls faced, per season.
Change-point detection: compute season-over-season delta; flag seasons where
delta > 1.5 standard deviations above the rolling mean.

EXPECTED RESULTS
----------------
A structural break around 2016–2018 coinciding with the proliferation of
specialist power hitters (Russell, Pandya) and shortened boundary ropes
at several venues.

LIMITATIONS
-----------
- Boundary counts in Cricsheet do not flag whether a boundary was a
  top edge, mishit, or genuine shot. Boundary rate captures volume, not quality.
- Venue composition changes confound year-on-year comparisons.

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
        YEAR(date)                                                  AS season,
        COUNT(*)                                                    AS pp_balls,
        SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)           AS fours,
        SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)           AS sixes,
        ROUND(
            (SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END))
            * 100.0 / COUNT(*), 2
        )                                                           AS boundary_pct
    FROM ball_events
    WHERE over < 6
    GROUP BY season
    ORDER BY season
""").to_pandas()

try:
    import numpy as np
    bp = result["boundary_pct"].values
    delta = np.diff(bp)
    mu, sd = delta.mean(), delta.std()
    breaks = result["season"].values[1:][delta > mu + 1.5 * sd]
    print(f"\nStructural break seasons (delta > mu+1.5sd): {breaks}\n")
except ImportError:
    pass

print(result.to_string(index=False))

# Chart: bar chart of boundary_pct by season.
# Expected: visible step-change around 2016–2018.
# Save to: research/charts/02_powerplay_boundary_surge.png
