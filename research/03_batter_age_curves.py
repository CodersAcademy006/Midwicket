"""
Study 03: Empirical T20 Batter Age Curves
==========================================

QUESTION
--------
At what age do T20 batters reach peak strike rate and batting average,
and how quickly do they decline afterward?

METHODOLOGY
-----------
Dataset: IPL (2008–2026). Only batters with 500+ career balls included.
Proxy for age: Cricsheet does not store dates of birth. We approximate
age as (match_year - debut_year + 20), treating debut year as a proxy
for career stage. This is a relative-age curve, not absolute age.
Peak detection: fit a quadratic polynomial to season-SR and season-Avg;
peak is the vertex of the downward parabola.

EXPECTED RESULTS
----------------
Peak SR at relative career year 6–9 (mid-career). Batting average peaks
slightly later (year 8–11) due to experience-driven shot selection.

LIMITATIONS
-----------
- Age is approximated from debut year, not actual birthdate.
- Selection bias: batters who decline are dropped before peak is visible.
- Mixing domestic and international players from different physiology pools.

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

print("Loading IPL dataset...")
session = load_dataset("ipl")

# Relative career year: years since debut in the dataset
result = session.engine.execute_sql("""
    WITH debuts AS (
        SELECT batter_id, MIN(YEAR(date)) AS debut_year
        FROM ball_events
        GROUP BY batter_id
    ),
    per_season AS (
        SELECT
            b.batter_id,
            YEAR(b.date)                                AS season,
            YEAR(b.date) - d.debut_year                 AS career_year,
            SUM(b.runs_batter)                          AS runs,
            COUNT(*)                                    AS balls,
            SUM(b.is_wicket)                            AS dismissals
        FROM ball_events b
        JOIN debuts d ON b.batter_id = d.batter_id
        GROUP BY b.batter_id, season, career_year
        HAVING balls >= 50
    )
    SELECT
        career_year,
        COUNT(DISTINCT batter_id)               AS player_seasons,
        ROUND(AVG(runs * 100.0 / balls), 2)     AS mean_sr,
        ROUND(
            SUM(runs) * 1.0 / NULLIF(SUM(dismissals), 0), 2
        )                                       AS mean_avg
    FROM per_season
    GROUP BY career_year
    HAVING player_seasons >= 10
    ORDER BY career_year
""").to_pandas()

try:
    import numpy as np
    cy  = result["career_year"].values.astype(float)
    sr  = result["mean_sr"].values
    avg = result["mean_avg"].values

    coeffs_sr  = np.polyfit(cy, sr,  2)
    coeffs_avg = np.polyfit(cy, avg, 2)

    peak_sr  = -coeffs_sr[1]  / (2 * coeffs_sr[0])
    peak_avg = -coeffs_avg[1] / (2 * coeffs_avg[0])
    print(f"\nPeak strike rate at relative career year: {peak_sr:.1f}")
    print(f"Peak average    at relative career year: {peak_avg:.1f}\n")
except ImportError:
    pass

print(result.to_string(index=False))

# Chart: dual-axis line plot — SR and Avg vs career year with quadratic fits.
# Save to: research/charts/03_batter_age_curves.png
