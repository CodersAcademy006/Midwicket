"""
Study 14: Chase Success Rate by Target Band
============================================

QUESTION
--------
What target makes a T20 chase statistically impossible (< 10% success),
and does this threshold shift between pre-2016 and post-2020 IPL?

METHODOLOGY
-----------
Dataset: IPL.
Bin first-innings totals into 10-run bands (130–140, 140–150, ..., 220–230).
For each band, compute the fraction of matches won by the chasing team.
Logistic regression: P(chase_win) ~ target_band.

EXPECTED RESULTS
----------------
Modern (post-2020) IPL: the "impossible" threshold has risen from ~185 to ~195.
Pre-2016: threshold was closer to ~175. This reflects improved batting depth
and better powerplay exploitation.

LIMITATIONS
-----------
- Small samples in the 200+ target bands (< 30 matches each).
- DLS-affected matches distort raw target comparisons.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH innings1 AS (
        SELECT
            match_id,
            SUM(runs_total)                             AS first_innings_total,
            MIN(batting_team)                           AS batting_team_1
        FROM ball_events
        WHERE inning = 1
        GROUP BY match_id
    ),
    match_result AS (
        SELECT match_id, MAX(winner) AS winner
        FROM ball_events
        WHERE winner IS NOT NULL
        GROUP BY match_id
    ),
    joined AS (
        SELECT
            i.match_id,
            i.first_innings_total,
            i.batting_team_1,
            r.winner,
            CASE WHEN r.winner != i.batting_team_1 THEN 1 ELSE 0 END AS chase_win
        FROM innings1 i
        JOIN match_result r ON i.match_id = r.match_id
    )
    SELECT
        FLOOR(first_innings_total / 10) * 10 AS target_band,
        COUNT(*)                              AS matches,
        SUM(chase_win)                        AS chases_won,
        ROUND(SUM(chase_win) * 100.0 / COUNT(*), 1) AS chase_win_pct
    FROM joined
    WHERE first_innings_total >= 120
    GROUP BY target_band
    HAVING matches >= 5
    ORDER BY target_band
""").to_pandas()

print(result.to_string(index=False))
impossible = result[result["chase_win_pct"] < 10]["target_band"]
if not impossible.empty:
    print(f"\nChase becomes < 10% probable above target: {impossible.min()}")

# Chart: line plot of chase_win_pct vs target_band.
# Save to: research/charts/14_chase_success_by_target.png
