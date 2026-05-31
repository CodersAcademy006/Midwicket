"""
Study 12: Dew Factor Proxy — Second-Innings Scoring Surplus
============================================================

QUESTION
--------
Can a systematic second-innings scoring surplus at specific venues
be used as a proxy for dew conditions in T20 cricket?

METHODOLOGY
-----------
Dataset: IPL.
Compute run rate difference: (2nd innings RR) - (1st innings RR) per match per venue.
A persistent positive surplus at a venue suggests conditions become easier
in the second innings, consistent with dew. Compare across day (14:00–18:00 IST)
vs evening (18:00–22:00 IST) matches where Cricsheet provides time data.

EXPECTED RESULTS
----------------
Evening matches at flat-pitch venues (Wankhede, Chepauk) show the highest
second-innings surplus, consistent with dew reducing grip for bowlers.

LIMITATIONS
-----------
- Cricsheet match time data is sparse. Many entries have no time.
- Second-innings surplus is also explained by target-driven acceleration
  strategy, not purely dew.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

result = session.engine.execute_sql("""
    WITH innings_rr AS (
        SELECT
            match_id,
            venue,
            inning,
            SUM(runs_total)                              AS runs,
            COUNT(*)                                     AS balls,
            SUM(runs_total) * 6.0 / NULLIF(COUNT(*), 0) AS run_rate
        FROM ball_events
        GROUP BY match_id, venue, inning
    ),
    match_delta AS (
        SELECT
            i1.match_id,
            i1.venue,
            i2.run_rate - i1.run_rate AS second_innings_surplus
        FROM innings_rr i1
        JOIN innings_rr i2
          ON i1.match_id = i2.match_id
          AND i1.inning = 1
          AND i2.inning = 2
    )
    SELECT
        venue,
        COUNT(*)                                                AS matches,
        ROUND(AVG(second_innings_surplus), 3)                   AS avg_surplus,
        ROUND(STDDEV(second_innings_surplus), 3)                AS sd_surplus
    FROM match_delta
    GROUP BY venue
    HAVING matches >= 15
    ORDER BY avg_surplus DESC
""").to_pandas()

print(result.to_string(index=False))

# Chart: sorted bar chart of avg_surplus by venue with error bars.
# Save to: research/charts/12_dew_factor_proxy.png
