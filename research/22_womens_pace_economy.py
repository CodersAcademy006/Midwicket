"""
Study 22: Pace Bowling Economy — Women's vs Men's T20
======================================================

QUESTION
--------
How does pace bowling economy in women's T20 (WBBL) compare to men's T20
(IPL), and is the gap narrowing over time?

METHODOLOGY
-----------
Datasets: WBBL and IPL.
Filter deliveries where bowling_kind LIKE '%pace%'. Compute season-level
economy per competition. Track absolute economy difference per season.

EXPECTED RESULTS
----------------
Women's T20 pace economy is lower (~6.2) than men's T20 (~8.4) overall.
However, WBBL pace economy has risen faster (+0.18/season) than IPL
(+0.12/season), suggesting a faster rate of batting improvement in WBBL.

LIMITATIONS
-----------
- bowling_kind field is sparsely populated in Cricsheet. Study may run on
  a subset of deliveries.
- Different pitches, bat sizes, and rule interpretations between competitions.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset
import pandas as pd

print("Loading IPL and WBBL datasets...")
ipl  = load_dataset("ipl")
wbbl = load_dataset("wbbl")

def pace_economy_by_season(session, label):
    return session.engine.execute_sql(f"""
        SELECT
            '{label}'   AS competition,
            YEAR(date)  AS season,
            ROUND(SUM(runs_total) * 6.0 / NULLIF(COUNT(*), 0), 3) AS pace_economy,
            COUNT(*)    AS balls
        FROM ball_events
        WHERE bowling_kind LIKE '%pace%' OR bowling_kind LIKE '%fast%'
        GROUP BY season
        HAVING balls >= 300
        ORDER BY season
    """).to_pandas()

combined = pd.concat([
    pace_economy_by_season(ipl,  "IPL"),
    pace_economy_by_season(wbbl, "WBBL"),
])
print(combined.to_string(index=False))

# Chart: dual-line plot of pace_economy by season for IPL and WBBL.
# Save to: research/charts/22_womens_pace_economy.png
