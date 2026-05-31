"""
Study 05: Era-Adjusted T20 Batting Greatness
=============================================

QUESTION
--------
Who are the greatest T20 batters in IPL history after controlling for
the era in which they played?

METHODOLOGY
-----------
Era adjustment: for each season, compute the mean and standard deviation
of batter strike rates (batters with 100+ balls that season). Express
each batter's season SR as a z-score relative to contemporaries.
Career era-adjusted SR = mean of season-level z-scores, weighted by
balls faced per season. Minimum 1,000 career balls.

EXPECTED RESULTS
----------------
Batters who dominated in the low-scoring early era (2008–2013) should
score higher than their raw SR suggests. Players who were merely
average in 2024 should score lower than their raw SR suggests.

LIMITATIONS
-----------
- Z-score assumes normal distribution of SR each season, which is
  left-skewed (few very high SR outliers pull the distribution).
- Cannot adjust for opposition quality within a season.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH season_stats AS (
        SELECT
            batter_id,
            YEAR(date)                                 AS season,
            SUM(runs_batter)                           AS runs,
            COUNT(*)                                   AS balls,
            SUM(runs_batter) * 100.0 / COUNT(*)        AS sr
        FROM ball_events
        GROUP BY batter_id, season
        HAVING balls >= 100
    ),
    season_norms AS (
        SELECT
            season,
            AVG(sr)    AS season_mean_sr,
            STDDEV(sr) AS season_sd_sr
        FROM season_stats
        GROUP BY season
    ),
    z_scores AS (
        SELECT
            s.batter_id,
            s.season,
            s.balls,
            (s.sr - n.season_mean_sr) / NULLIF(n.season_sd_sr, 0) AS z_sr
        FROM season_stats s
        JOIN season_norms n ON s.season = n.season
    )
    SELECT
        batter_id,
        COUNT(DISTINCT season)                         AS seasons,
        SUM(balls)                                     AS career_balls,
        ROUND(
            SUM(z_sr * balls) / NULLIF(SUM(balls), 0), 3
        )                                              AS era_adj_sr_zscore
    FROM z_scores
    GROUP BY batter_id
    HAVING career_balls >= 1000
    ORDER BY era_adj_sr_zscore DESC
    LIMIT 20
""").to_pandas()

print("\nTop 20 era-adjusted T20 batters (IPL):\n")
print(result.to_string(index=False))

# Chart: horizontal bar chart of era_adj_sr_zscore for top 20.
# Save to: research/charts/05_era_adjusted_greatness.png
