"""
Study 10: Bowling Diversity Index and Match Wins
=================================================

QUESTION
--------
Do T20 teams that use a more diverse mix of bowling types (spin, pace,
medium) win significantly more matches than teams that concentrate
bowling in fewer types?

METHODOLOGY
-----------
Dataset: IPL.
Per match, compute the Shannon entropy of bowling-type distribution:
H = -sum(p_i * log(p_i)) where p_i is the fraction of balls bowled by
type i. High H = diverse attack. Match result from ball_events winner.
OLS regression: win ~ bowling_diversity_H.

EXPECTED RESULTS
----------------
Weak positive association between diversity and winning (OR ~1.1 per
unit of entropy). Effect is stronger in high-scoring matches where
a specialist exploited early can be countered by a type change.

LIMITATIONS
-----------
- Bowling type in Cricsheet is not always populated.
- Diversity is partly forced by regulations (over limits), not strategy.
- Does not control for team quality or opponent batting strength.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset
import math

print("Loading IPL dataset...")
session = load_dataset("ipl")

# Get bowling type distribution per team per match
result = session.engine.execute_sql("""
    SELECT
        match_id,
        bowling_team,
        bowling_kind,
        COUNT(*) AS balls
    FROM ball_events
    WHERE bowling_kind IS NOT NULL
    GROUP BY match_id, bowling_team, bowling_kind
""").to_pandas()

if result.empty:
    print("bowling_kind not populated in this dataset snapshot — study cannot run.")
else:
    def shannon_entropy(group):
        totals = group["balls"].sum()
        p = group["balls"] / totals
        return -(p * p.apply(math.log)).sum()

    entropy = (
        result
        .groupby(["match_id", "bowling_team"])
        .apply(shannon_entropy)
        .reset_index(name="diversity_H")
    )
    print(f"\nDiversity index computed for {len(entropy)} team-match observations.\n")
    print(entropy.describe())

# Chart: box plot of diversity_H split by match outcome (win/loss).
# Save to: research/charts/10_bowling_diversity_index.png
