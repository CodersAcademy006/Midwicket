"""
Study 24: Test Batting Stamina — Scoring Rate Across an Innings
===============================================================

QUESTION
--------
How does the scoring rate (runs per ball) evolve across a Test innings
as the pitch deteriorates, and do batters accelerate or decelerate in
the final 30 overs?

METHODOLOGY
-----------
Dataset: Tests.
Compute mean scoring rate per 10-over band across the first innings
only (to avoid target effects in the 2nd innings).
Bands: 0-9, 10-19, 20-29, 30-39, 40-49, 50-59, 60-69, 70-79, 80+.

EXPECTED RESULTS
----------------
Scoring rate declines progressively over 80 overs as pitch deteriorates.
Slight uptick in the final 10 overs of a declared innings (tail-enders
slog). Middle-overs (30-60) show the most stable scoring rates.

LIMITATIONS
-----------
- Test scoring rate is highly venue-dependent (WACA vs Old Trafford).
- Not controlling for pitch type, day of match, or weather.
- Declared innings and follow-ons distort the over distribution.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading Tests dataset...")
session = load_dataset("tests")

result = session.engine.execute_sql("""
    SELECT
        FLOOR(over / 10) * 10  AS over_band_start,
        COUNT(*)               AS balls,
        ROUND(SUM(runs_batter) * 100.0 / COUNT(*), 2) AS batter_sr,
        ROUND(SUM(runs_total)  * 6.0    / COUNT(*), 3) AS overall_rr,
        SUM(is_wicket)         AS wickets
    FROM ball_events
    WHERE inning = 1
    GROUP BY over_band_start
    HAVING balls >= 5000
    ORDER BY over_band_start
""").to_pandas()

print(result.to_string(index=False))

# Chart: line plot of batter_sr by over_band.
# Save to: research/charts/24_test_batting_stamina.png
