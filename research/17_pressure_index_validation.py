"""
Study 17: Pressure Index Validation
=====================================

QUESTION
--------
Does a higher Midwicket pressure index correlate with reduced batter
output (lower strike rate and higher dismissal rate) at the delivery level?

METHODOLOGY
-----------
Dataset: IPL. Uses Midwicket's built-in `pressure_index` feature.
Bin deliveries into quintiles by pressure index value.
For each quintile, compute mean batter SR and dismissal rate.
Spearman rank correlation between pressure quintile and batter output.

EXPECTED RESULTS
----------------
Strong negative correlation between pressure index and batter SR (ρ < -0.8).
Positive correlation between pressure index and dismissal rate.
This would validate the pressure index as a meaningful cricket metric.

LIMITATIONS
-----------
- Pressure index uses in-match wickets and over fraction, not external pressure
  indicators (crowd noise, match stakes). It is a mechanical proxy.
- Correlation at the delivery level is subject to Simpson's paradox
  (different batters at different career stages face different pressure).

DATA CUTOFF
-----------
May 2026 Cricsheet snapshot.
"""

import midwicket as md
from midwicket.datasets import load_dataset
from midwicket.features import load_features

print("Loading IPL dataset...")
session = load_dataset("ipl")

print("Computing pressure index features...")
pressure_df = load_features("pressure_index", session)

if pressure_df.empty:
    print("Pressure index not available in this snapshot.")
else:
    pressure_df["pressure_quintile"] = (
        pressure_df["pressure_index"]
        .rank(pct=True)
        .mul(5)
        .apply(lambda x: min(int(x), 4) + 1)
    )

    result = (
        pressure_df
        .groupby("pressure_quintile")
        .agg(
            balls=("runs_batter", "count"),
            mean_sr=("runs_batter", lambda x: x.mean() * 100),
            dismissal_rate=("is_wicket", "mean"),
        )
        .round(3)
    )
    print(result)

    try:
        from scipy.stats import spearmanr
        rho_sr,  _ = spearmanr(pressure_df["pressure_index"], pressure_df["runs_batter"])
        rho_wkt, _ = spearmanr(pressure_df["pressure_index"], pressure_df["is_wicket"])
        print(f"\nSpearman ρ (pressure vs SR):          {rho_sr:.3f}")
        print(f"Spearman ρ (pressure vs dismissal):   {rho_wkt:.3f}")
    except ImportError:
        print("scipy not available — skipping Spearman test")

# Chart: line plot of mean_sr and dismissal_rate across pressure quintiles.
# Save to: research/charts/17_pressure_index_validation.png
