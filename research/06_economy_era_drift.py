"""
Study 06: Bowling Economy Era Drift
====================================

QUESTION
--------
How much harder has it become to bowl economically in IPL T20 cricket
since 2008, and does the difficulty compound in death overs?

METHODOLOGY
-----------
Dataset: IPL (2008–2026).
Compute season-level mean economy (runs per over) split by phase:
powerplay (0-5), middle (6-14), death (15-19).
OLS trend for each phase. Death economy trend compared with powerplay.

EXPECTED RESULTS
----------------
All three phases show upward economy drift. Death overs trend steeper
(+0.2/season) than powerplay (+0.1/season) because death batting
strategy has evolved faster than bowling.

LIMITATIONS
-----------
- Economy per over includes wides and no-balls. True bowling economy
  is slightly lower.
- No control for ball quality, pitch type, or outfield speed.

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
        YEAR(date)                                              AS season,
        ROUND(
            SUM(CASE WHEN over < 6 THEN runs_total ELSE 0 END)
            * 6.0 / NULLIF(SUM(CASE WHEN over < 6 THEN 1 ELSE 0 END), 0),
        2) AS pp_economy,
        ROUND(
            SUM(CASE WHEN over BETWEEN 6 AND 14 THEN runs_total ELSE 0 END)
            * 6.0 / NULLIF(SUM(CASE WHEN over BETWEEN 6 AND 14 THEN 1 ELSE 0 END), 0),
        2) AS mid_economy,
        ROUND(
            SUM(CASE WHEN over >= 15 THEN runs_total ELSE 0 END)
            * 6.0 / NULLIF(SUM(CASE WHEN over >= 15 THEN 1 ELSE 0 END), 0),
        2) AS death_economy
    FROM ball_events
    GROUP BY season
    ORDER BY season
""").to_pandas()

print(result.to_string(index=False))

# Chart: three-line plot (pp / mid / death economy) by season.
# Save to: research/charts/06_economy_era_drift.png
