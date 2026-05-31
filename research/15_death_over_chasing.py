"""
Study 15: Batting Strategy When Required Rate Exceeds 12
=========================================================

QUESTION
--------
When a chasing team needs more than 12 runs per over in overs 16–20,
does boundary rate increase, dot rate decrease, and wicket rate increase
compared to required rates below 10?

METHODOLOGY
-----------
Dataset: IPL, second innings only.
Compute required run rate at the start of each over = (runs_needed / overs_remaining).
Classify each delivery into: pressure (RRR > 12), normal (RRR 8–10), comfortable (RRR < 8).
Compare boundary%, dot%, dismissal% across these three bands.

EXPECTED RESULTS
----------------
Under high pressure (RRR > 12): boundary rate ~24%, dot rate ~22%, dismissal rate ~3.1%.
Under comfortable conditions (RRR < 8): boundary rate ~14%, dot rate ~38%, dismissal rate ~1.6%.

LIMITATIONS
-----------
- Required run rate computation needs cumulative in-match score tracking.
  The SQL below uses a simple proxy: over number and inning score.
- No control for batter quality in each pressure band.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH chase_balls AS (
        SELECT
            match_id, over, ball, runs_batter, runs_total, is_wicket,
            SUM(runs_total) OVER (
                PARTITION BY match_id ORDER BY over, ball
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )                                          AS cumulative_runs,
            -- approximate target from the max 2nd innings score observed
            MAX(SUM(runs_total)) OVER (PARTITION BY match_id) AS inferred_target
        FROM ball_events
        WHERE inning = 2
    ),
    with_rrr AS (
        SELECT *,
            CASE
                WHEN (20 - over) > 0
                THEN (inferred_target - COALESCE(cumulative_runs, 0)) * 1.0 / (20 - over)
                ELSE 99
            END AS rrr
        FROM chase_balls
    ),
    banded AS (
        SELECT *,
            CASE
                WHEN rrr > 12 THEN 'high_pressure'
                WHEN rrr BETWEEN 8 AND 12 THEN 'normal'
                ELSE 'comfortable'
            END AS pressure_band
        FROM with_rrr
        WHERE over >= 15
    )
    SELECT
        pressure_band,
        COUNT(*)                                                  AS balls,
        ROUND(SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS boundary_pct,
        ROUND(SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)      AS dot_pct,
        ROUND(SUM(is_wicket) * 100.0 / COUNT(*), 2)                                        AS dismissal_pct
    FROM banded
    GROUP BY pressure_band
    ORDER BY pressure_band
""").to_pandas()

print(result.to_string(index=False))

# Chart: grouped bar chart — boundary%, dot%, dismissal% per pressure band.
# Save to: research/charts/15_death_over_chasing.png
