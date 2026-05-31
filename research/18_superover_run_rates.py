"""
Study 18: Super Over Run Rates
================================

QUESTION
--------
What is the average runs scored in a T20 Super Over, and how does the
Super Over batting strike rate compare to the 20th-over batting strike rate?

METHODOLOGY
-----------
Dataset: T20IS (has more Super Overs due to tied knockout matches).
Cricsheet marks Super Overs in the match JSON. The Super Over is over
number 0 in innings 3/4 (or marked super_over=true in some schemas).
Compute mean Super Over runs and compare to over 19 of the main innings.

EXPECTED RESULTS
----------------
Mean Super Over score: ~12–14 runs. SR in Super Over: ~180–220%.
Substantially higher than over 19 SR (~145%) due to no-holds-barred batting.

LIMITATIONS
-----------
- Super Over sample is small (<100 events in Cricsheet).
- Some Super Over JSONs use non-standard schemas.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading T20IS dataset...")
session = load_dataset("t20is")

# Super Overs are innings 3 and 4 (or marked via over=0 in inning 3/4)
result = session.engine.execute_sql("""
    SELECT
        'super_over'        AS context,
        COUNT(DISTINCT match_id)                            AS matches,
        SUM(runs_batter)                                    AS total_runs,
        COUNT(*)                                            AS balls,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1)      AS sr
    FROM ball_events
    WHERE inning >= 3

    UNION ALL

    SELECT
        'over_19'           AS context,
        COUNT(DISTINCT match_id),
        SUM(runs_batter),
        COUNT(*),
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 1)
    FROM ball_events
    WHERE inning <= 2 AND over = 19
""").to_pandas()

print(result.to_string(index=False))

# Chart: side-by-side bar: SR in over_19 vs super_over.
# Save to: research/charts/18_superover_run_rates.png
