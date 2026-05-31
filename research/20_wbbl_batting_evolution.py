"""
Study 20: WBBL Batting Evolution (2015–2025)
============================================

QUESTION
--------
How have batting strike rates, boundary rates, and total match scores
evolved across WBBL's 10 seasons?

METHODOLOGY
-----------
Dataset: WBBL.
Season-level aggregation of: mean 1st innings total, mean overall SR,
mean boundary rate. OLS trend for each metric.

EXPECTED RESULTS
----------------
WBBL batting has evolved similarly to men's T20 but at a slower absolute
rate. Mean first innings total expected to have risen from ~125 (2015) to
~145 (2025). Boundary rate has increased but remains lower than IPL.

LIMITATIONS
-----------
- WBBL seasons 1-3 have smaller samples (fewer teams, fewer matches).
- Pitch conditions in Australia in December-January are different from
  IPL conditions. Cross-competition comparison is not meaningful.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading WBBL dataset...")
session = load_dataset("wbbl")

result = session.engine.execute_sql("""
    WITH per_innings AS (
        SELECT
            match_id,
            inning,
            YEAR(date)                                              AS season,
            SUM(runs_total)                                         AS innings_total,
            COUNT(*)                                                AS balls,
            SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END)  AS boundaries
        FROM ball_events
        GROUP BY match_id, inning, YEAR(date)
    )
    SELECT
        season,
        COUNT(DISTINCT match_id)                                    AS matches,
        ROUND(AVG(CASE WHEN inning=1 THEN innings_total END), 1)    AS avg_1st_innings,
        ROUND(SUM(balls > 0) * 100.0 / SUM(balls), 1)              AS overall_sr_proxy,
        ROUND(SUM(boundaries) * 100.0 / SUM(balls), 2)             AS boundary_pct
    FROM per_innings
    GROUP BY season
    ORDER BY season
""").to_pandas()

print(result.to_string(index=False))

# Chart: three-line plot (avg_1st_innings, boundary_pct) by season.
# Save to: research/charts/20_wbbl_batting_evolution.png
