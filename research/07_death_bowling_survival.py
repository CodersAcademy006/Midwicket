"""
Study 07: Death Bowling Survival — Who Maintained Sub-8 Economy?
=================================================================

QUESTION
--------
Which IPL bowlers maintained a sub-8.0 economy rate in overs 16–20
across three or more consecutive seasons, and what is their statistical
profile?

METHODOLOGY
-----------
Dataset: IPL.
Filter: over >= 15, minimum 60 death balls per season.
Find bowlers who appear in 3+ seasons with economy < 8.0 in those seasons.
Report their death-over economy, dot rate, wickets, and phase concentration.

EXPECTED RESULTS
----------------
A small set (<12) bowlers: Bumrah, Malinga, Narine, Boult. Their dot-ball
percentages will be 5–10 points above the league average in death overs.

LIMITATIONS
-----------
- "Economy" counts all extras. Bowlers with many wides inflate economy.
- Three consecutive seasons is a strict filter; some greats had gaps.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH death_per_season AS (
        SELECT
            bowler_id,
            YEAR(date)                                              AS season,
            SUM(runs_total)                                         AS runs,
            COUNT(*)                                                AS balls,
            SUM(is_wicket)                                          AS wickets,
            SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END)        AS dots,
            ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2)             AS economy
        FROM ball_events
        WHERE over >= 15
        GROUP BY bowler_id, season
        HAVING balls >= 60 AND economy < 8.0
    ),
    consecutive AS (
        SELECT
            bowler_id,
            COUNT(DISTINCT season) AS qualifying_seasons,
            MIN(season)            AS first_season,
            MAX(season)            AS last_season,
            ROUND(AVG(economy), 2) AS avg_death_economy,
            SUM(wickets)           AS total_wickets,
            ROUND(SUM(dots) * 100.0 / SUM(balls), 1) AS dot_pct
        FROM death_per_season
        GROUP BY bowler_id
        HAVING qualifying_seasons >= 3
    )
    SELECT * FROM consecutive
    ORDER BY avg_death_economy
""").to_pandas()

print(f"\n{len(result)} bowlers maintained sub-8 economy in death overs across 3+ seasons:\n")
print(result.to_string(index=False))

# Chart: scatter of avg_death_economy vs dot_pct, bubble sized by total_wickets.
# Save to: research/charts/07_death_bowling_survival.png
