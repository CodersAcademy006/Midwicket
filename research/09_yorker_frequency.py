"""
Study 09: Yorker Frequency in Death Overs
==========================================

QUESTION
--------
Have bowlers increased yorker frequency in death overs as scoring rates
inflated, and does higher yorker frequency correlate with lower economy?

METHODOLOGY
-----------
Dataset: IPL.
Proxy for yorkers: deliveries where runs_batter = 0 AND extras = 0
in over >= 15 (not a perfect proxy but the best available without
ball-tracking data). This captures dot balls that were not wides,
no-balls, or boundary misses — the population where yorkers concentrate.
Compare proxy-yorker rate per season vs death economy.

EXPECTED RESULTS
----------------
Proxy-yorker rate rises after 2016 (era of Bumrah, Boult). Correlation
with economy expected to be negative (more yorkers → lower economy).

LIMITATIONS
-----------
- The proxy is noisy. It includes short-pitched dots and good-length dots.
- Requires ball-tracking data for confirmation, which is not in Cricsheet.

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
        YEAR(date)  AS season,
        COUNT(*)    AS death_balls,
        SUM(CASE WHEN runs_batter = 0 AND extras = 0 THEN 1 ELSE 0 END)   AS proxy_yorkers,
        ROUND(
            SUM(CASE WHEN runs_batter = 0 AND extras = 0 THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*), 2
        )           AS proxy_yorker_pct,
        ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2)                        AS death_economy
    FROM ball_events
    WHERE over >= 15
    GROUP BY season
    ORDER BY season
""").to_pandas()

try:
    corr = result[["proxy_yorker_pct", "death_economy"]].corr().iloc[0, 1]
    print(f"\nCorrelation(proxy_yorker_pct, death_economy): {corr:.3f}\n")
except Exception:
    pass

print(result.to_string(index=False))

# Chart: scatter of proxy_yorker_pct vs death_economy by season.
# Save to: research/charts/09_yorker_frequency.png
