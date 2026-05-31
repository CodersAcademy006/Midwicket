"""
Study 01: T20 Run Rate Inflation (2008–2026)
============================================

QUESTION
--------
How much has the average T20 run rate per over risen across the full
Cricsheet IPL corpus from 2008 to 2026?

METHODOLOGY
-----------
Dataset: IPL (2008–2026), ~1,100 matches.
Unit of analysis: per-match, per-innings average run rate (total runs / overs).
Statistical approach: OLS linear trend on season-level mean run rate.
The run rate per over is computed per innings (not per match) to avoid
averaging across unequal innings lengths (rain, early wins).

EXPECTED RESULTS
----------------
A monotone upward trend of roughly +0.15 runs/over/season. Total lift
from 2008 to 2026 expected to be 2.0–3.0 runs per over.

LIMITATIONS
-----------
- IPL only. Different leagues may inflate at different rates.
- Incomplete seasons (COVID 2020, rain-affected years) create noise.
- Does not separate batting improvement from bowling deterioration.
- Venue composition changes each season (new grounds added).

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset

# ── 1. Load data ──────────────────────────────────────────────────────────────
print("Loading IPL dataset...")
session = load_dataset("ipl")

# ── 2. Query: season-level average run rate ───────────────────────────────────
result = session.engine.execute_sql("""
    WITH per_innings AS (
        SELECT
            match_id,
            inning,
            YEAR(date)          AS season,
            SUM(runs_total)     AS innings_runs,
            MAX(over) + 1       AS overs_bowled
        FROM ball_events
        GROUP BY match_id, inning, YEAR(date)
        HAVING overs_bowled >= 10          -- exclude rain-shortened < 10 over innings
    )
    SELECT
        season,
        COUNT(*)                                           AS innings_count,
        ROUND(AVG(innings_runs * 6.0 / overs_bowled), 2)  AS avg_run_rate
    FROM per_innings
    GROUP BY season
    ORDER BY season
""").to_pandas()

# ── 3. Compute trend ──────────────────────────────────────────────────────────
try:
    import numpy as np
    seasons = result["season"].values
    rr      = result["avg_run_rate"].values
    slope, intercept = np.polyfit(seasons - seasons[0], rr, 1)
    total_lift = slope * (seasons[-1] - seasons[0])
    print(f"\nLinear trend: +{slope:.3f} runs/over/season")
    print(f"Total lift {seasons[0]}–{seasons[-1]}: +{total_lift:.2f} runs/over\n")
except ImportError:
    print("numpy not available — skipping trend computation\n")

# ── 4. Output ─────────────────────────────────────────────────────────────────
print(result.to_string(index=False))

# ── 5. Chart note ─────────────────────────────────────────────────────────────
# Chart: line plot of avg_run_rate by season with OLS trend line.
# Expected shape: monotone increase with steepening from 2018 onward.
# Save to: research/charts/01_run_rate_inflation.png
