"""
Study 19: 20th Over Dismissal Rate
=====================================

QUESTION
--------
Are batters more likely to be dismissed in the 20th over (over 19)
than in overs 16–18, controlling for scoring rate?

METHODOLOGY
-----------
Dataset: IPL.
Compare dismissal rate (dismissals / balls) per over for overs 15–19.
Also compare across innings (1st vs 2nd) and by match state
(first innings defending vs second innings chasing).

EXPECTED RESULTS
----------------
Over 19 does NOT show higher dismissal rate than overs 16–18.
Batters accept more risk in the 20th over but bowlers also bowl
more defensive deliveries. Net effect is neutral or slightly lower
dismissal rate in over 19.

LIMITATIONS
-----------
- Some matches end before over 19. These are excluded by the GROUP BY filter.
- Scoring rate and dismissal rate are correlated (more attacking = more risk).

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
        over,
        inning,
        COUNT(*)                                             AS balls,
        SUM(is_wicket)                                       AS dismissals,
        ROUND(SUM(is_wicket) * 100.0 / COUNT(*), 2)         AS dismissal_rate,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1)       AS batter_sr
    FROM ball_events
    WHERE over BETWEEN 15 AND 19
    GROUP BY over, inning
    ORDER BY inning, over
""").to_pandas()

print(result.to_string(index=False))

# Chart: line plot of dismissal_rate by over (15-19), split by inning.
# Save to: research/charts/19_final_over_dismissal_rate.png
