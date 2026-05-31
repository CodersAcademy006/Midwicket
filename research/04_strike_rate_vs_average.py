"""
Study 04: Strike Rate vs Average Trade-off Across T20 Eras
===========================================================

QUESTION
--------
Has the trade-off between strike rate and batting average changed across
the three eras of T20 cricket (2008–2013, 2014–2019, 2020–2026)?

METHODOLOGY
-----------
Dataset: IPL.
Segment batters into three eras by their predominant season of activity.
Compute the Pearson correlation between career SR and career average
within each era. A weakening negative correlation suggests that modern
batters can maintain high SR without sacrificing average.

EXPECTED RESULTS
----------------
Early era (2008–2013): moderate negative correlation (r ≈ -0.25).
Recent era (2020–2026): near-zero or positive correlation (r ≈ 0.05).
Interpretation: pitch conditions, fielding restrictions, and bat
technology have decoupled SR from average in modern T20.

LIMITATIONS
-----------
- Small sample per era for batters with 200+ balls in that era only.
- Correlation does not imply causation.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH era_stats AS (
        SELECT
            batter_id,
            CASE
                WHEN YEAR(date) BETWEEN 2008 AND 2013 THEN 'early'
                WHEN YEAR(date) BETWEEN 2014 AND 2019 THEN 'mid'
                ELSE 'modern'
            END                                    AS era,
            SUM(runs_batter)                       AS runs,
            COUNT(*)                               AS balls,
            SUM(is_wicket)                         AS dismissals
        FROM ball_events
        GROUP BY batter_id, era
        HAVING balls >= 200 AND dismissals >= 5
    )
    SELECT
        era,
        COUNT(DISTINCT batter_id)                              AS batters,
        ROUND(AVG(runs * 100.0 / balls), 2)                    AS mean_sr,
        ROUND(AVG(runs * 1.0 / dismissals), 2)                 AS mean_avg,
        ROUND(CORR(runs * 100.0 / balls, runs * 1.0 / dismissals), 3) AS sr_avg_corr
    FROM era_stats
    GROUP BY era
    ORDER BY era
""").to_pandas()

print(result.to_string(index=False))

# Chart: scatter plot of SR vs Avg, colour-coded by era, with per-era regression lines.
# Save to: research/charts/04_strike_rate_vs_average.png
