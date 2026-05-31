"""
Study 08: Spin vs Pace Effectiveness Over Time
===============================================

QUESTION
--------
Has spin bowling become more or less effective relative to pace in IPL
T20 cricket since 2015?

METHODOLOGY
-----------
Dataset: IPL.
Separate deliveries by bowling type using Cricsheet's `bowling_kind` field.
Compute economy and dot rate per season for spin and pace.
The effectiveness ratio = spin_economy / pace_economy; ratio < 1 means
spin is cheaper than pace, ratio > 1 means pace is cheaper.

EXPECTED RESULTS
----------------
Spin effectiveness ratio has worsened (ratio risen) since 2020 due to
improved sweep/slog-sweep techniques and shorter boundary ropes.

LIMITATIONS
-----------
- Cricsheet does not always populate bowling_kind; missing values excluded.
- "Spin" includes finger spin and wrist spin. They have different effectiveness profiles.

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
        ROUND(
            SUM(CASE WHEN bowling_kind LIKE '%spin%' THEN runs_total ELSE 0 END) * 6.0
            / NULLIF(SUM(CASE WHEN bowling_kind LIKE '%spin%' THEN 1 ELSE 0 END), 0),
        2) AS spin_economy,
        ROUND(
            SUM(CASE WHEN bowling_kind LIKE '%pace%' OR bowling_kind LIKE '%fast%' THEN runs_total ELSE 0 END) * 6.0
            / NULLIF(SUM(CASE WHEN bowling_kind LIKE '%pace%' OR bowling_kind LIKE '%fast%' THEN 1 ELSE 0 END), 0),
        2) AS pace_economy
    FROM ball_events
    WHERE bowling_kind IS NOT NULL
    GROUP BY season
    ORDER BY season
""").to_pandas()

result["effectiveness_ratio"] = (result["spin_economy"] / result["pace_economy"]).round(3)
print(result.to_string(index=False))

# Chart: dual-axis line (spin_economy, pace_economy) and ratio.
# Save to: research/charts/08_spin_vs_pace_over_time.png
