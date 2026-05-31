"""
Study 23: Boundary Rates — T20I vs ODI
========================================

QUESTION
--------
How different are boundary rates (boundaries per ball) between T20
Internationals and ODIs, and how has the gap evolved since 2015?

METHODOLOGY
-----------
Datasets: T20IS and ODIS.
Per season, compute boundary rate = (fours + sixes) / total balls.
Report absolute and relative gap between formats.

EXPECTED RESULTS
----------------
T20I boundary rate is roughly 2x ODI boundary rate overall.
The gap has narrowed slightly since 2019 as ODI batting has become more
aggressive (Powerplay expansion, batting-friendly conditions).

LIMITATIONS
-----------
- T20I and ODI play different numbers of overs, so raw boundary counts
  are not comparable. Rate per ball controls for this.
- Player pool overlap is high for major nations — same batters appear
  in both datasets, driving correlation.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset
import pandas as pd

print("Loading T20IS and ODIS datasets...")
t20 = load_dataset("t20is")
odi = load_dataset("odis")

def boundary_rate_by_season(session, label):
    return session.engine.execute_sql(f"""
        SELECT
            '{label}'   AS format,
            YEAR(date)  AS season,
            COUNT(*)    AS balls,
            SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END) AS boundaries,
            ROUND(
                SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2)          AS boundary_pct
        FROM ball_events
        GROUP BY season
        HAVING balls >= 5000
        ORDER BY season
    """).to_pandas()

combined = pd.concat([
    boundary_rate_by_season(t20, "T20I"),
    boundary_rate_by_season(odi, "ODI"),
])
print(combined.to_string(index=False))

# Chart: dual-line plot of boundary_pct by season for T20I and ODI.
# Save to: research/charts/23_t20_vs_odi_boundary_rate.png
