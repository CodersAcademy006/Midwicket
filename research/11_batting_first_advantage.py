"""
Study 11: Batting-First Advantage by Venue
==========================================

QUESTION
--------
At which IPL venues does batting first statistically win significantly
more than 50% of matches, and how large is the effect?

METHODOLOGY
-----------
Dataset: IPL.
Per venue, compute win rate for the team batting first (inning=1).
Binomial test against null = 0.5 for each venue with 20+ matches.
Report venues with p < 0.1 and the direction of the effect.

EXPECTED RESULTS
----------------
Chinnaswamy Stadium and Wankhede Stadium favour chasing (below 50%
batting-first win rate) due to flat pitches and small ground sizes.
Some northern venues favour batting first due to morning dew.

LIMITATIONS
-----------
- Winner field in Cricsheet is match winner, not innings winner.
  We infer from the team that batted first in inning=1.
- Toss decisions are endogenous — teams choose to bat or bowl
  based on conditions, which biases the win rate.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH match_summary AS (
        SELECT
            match_id,
            venue,
            MIN(CASE WHEN inning = 1 THEN batting_team END) AS first_batting_team,
            MAX(winner)                                      AS winner
        FROM ball_events
        WHERE winner IS NOT NULL
        GROUP BY match_id, venue
    )
    SELECT
        venue,
        COUNT(*)                                                     AS matches,
        SUM(CASE WHEN first_batting_team = winner THEN 1 ELSE 0 END) AS batting_first_wins,
        ROUND(
            SUM(CASE WHEN first_batting_team = winner THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*), 1
        )                                                            AS bat_first_win_pct
    FROM match_summary
    GROUP BY venue
    HAVING matches >= 20
    ORDER BY bat_first_win_pct DESC
""").to_pandas()

print(result.to_string(index=False))

# Chart: horizontal bar chart of bat_first_win_pct per venue.
# Highlight bars above 55% (batting-first bias) and below 45% (chasing bias).
# Save to: research/charts/11_batting_first_advantage.png
