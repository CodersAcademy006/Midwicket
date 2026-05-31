"""
Study 25: Dot Ball Percentages Across T20, ODI, and Test
=========================================================

QUESTION
--------
How do dot ball percentages (balls where no runs are scored off the bat)
compare across T20, ODI, and Test cricket, and has the T20 dot rate declined
as batters have become more aggressive?

METHODOLOGY
-----------
Datasets: T20IS, ODIS, Tests.
Compute per-format, per-season dot ball percentage (runs_batter = 0 AND
no extras). Track trend for T20I format specifically.

EXPECTED RESULTS
----------------
Tests: ~55% dot balls (batters defend frequently).
ODIs: ~42% dot balls (moderate aggression).
T20Is: ~35% dot balls and falling (aggressive batting templates).

LIMITATIONS
-----------
- "Dot ball" here means no runs off the bat. Wides and no-balls with
  no runs are included in the denominator, slightly inflating dot%.
- Format composition differs across nations: India plays many ODIs,
  affecting the ODI pool.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset
import pandas as pd

print("Loading T20IS, ODIS, and Tests datasets...")
t20  = load_dataset("t20is")
odi  = load_dataset("odis")
test = load_dataset("tests")

def dot_pct_by_season(session, label):
    return session.engine.execute_sql(f"""
        SELECT
            '{label}'   AS format,
            YEAR(date)  AS season,
            COUNT(*)    AS balls,
            ROUND(
                SUM(CASE WHEN runs_batter = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2)          AS dot_pct
        FROM ball_events
        GROUP BY season
        HAVING balls >= 3000
        ORDER BY season
    """).to_pandas()

combined = pd.concat([
    dot_pct_by_season(t20,  "T20I"),
    dot_pct_by_season(odi,  "ODI"),
    dot_pct_by_season(test, "Test"),
])

summary = combined.groupby("format")["dot_pct"].agg(["mean", "min", "max"]).round(2)
print("\nFormat-level summary:\n")
print(summary)
print("\nSeason-level data:\n")
print(combined.to_string(index=False))

# Chart: three-line plot of dot_pct by season for T20I, ODI, and Test.
# Save to: research/charts/25_format_comparison_dot_balls.png
