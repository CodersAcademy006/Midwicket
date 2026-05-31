"""
Study 21: WPL vs WBBL — Batting Conditions Comparison
=======================================================

QUESTION
--------
Is the Women's Premier League more batting-friendly than the Women's
Big Bash League, as measured by run rates, boundary rates, and match totals?

METHODOLOGY
-----------
Datasets: WPL and WBBL (loaded separately, compared in Python).
Compute for each competition: mean 1st innings total, overall run rate,
boundary rate, and dot ball rate. Two-sample t-test on first-innings totals.

EXPECTED RESULTS
----------------
WPL likely scores higher (Indian pitches + new competition = attacking
batting culture). Expected difference: 8–15 runs per innings in favour of WPL.

LIMITATIONS
-----------
- WPL sample is small (2023–2025, ~80 matches).
- Venue composition differs completely (India vs Australia).
- Player pools overlap but are not identical.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading WPL dataset...")
wpl  = load_dataset("wpl")
print("Loading WBBL dataset...")
wbbl = load_dataset("wbbl")

def get_summary(session, label):
    return session.engine.execute_sql(f"""
        WITH per_match AS (
            SELECT
                match_id,
                SUM(CASE WHEN inning=1 THEN runs_total ELSE 0 END) AS first_innings,
                COUNT(*)                                             AS total_balls,
                SUM(runs_total)                                      AS total_runs,
                SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END) AS boundaries,
                SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END)     AS dots
            FROM ball_events
            GROUP BY match_id
        )
        SELECT
            '{label}'                                               AS competition,
            COUNT(*)                                                AS matches,
            ROUND(AVG(first_innings), 1)                            AS avg_1st_innings,
            ROUND(SUM(total_runs) * 6.0 / SUM(total_balls), 3)     AS overall_rr,
            ROUND(SUM(boundaries) * 100.0 / SUM(total_balls), 2)   AS boundary_pct,
            ROUND(SUM(dots) * 100.0 / SUM(total_balls), 2)         AS dot_pct
        FROM per_match
    """).to_pandas()

import pandas as pd
comparison = pd.concat([
    get_summary(wpl,  "WPL"),
    get_summary(wbbl, "WBBL"),
])
print(comparison.to_string(index=False))

# Chart: side-by-side bars comparing WPL vs WBBL on 4 metrics.
# Save to: research/charts/21_wpl_vs_wbbl_comparison.png
