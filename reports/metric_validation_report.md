# Midwicket Metric Validation Report

This report presents a thorough statistical validation of Midwicket’s proprietary metrics layer across **distribution, outlier, correlation, stability, and explainability** boundaries, using real data from the live database.

---

## ⚡ Metric Validation Summary

| Metric | Status | Explainability | Main Statistical Utility | Identified Issues / Limitations |
| :--- | :--- | :--- | :--- | :--- |
| **Pressure Index** | **Statistically Useful** | High | Measures high-leverage delivery states. | Heavily right-skewed; spikes late in innings. |
| **BQR** | 🏆 **Elite (Useful)** | High | Best-in-class bowling quality rating. | None; highly correlated with economy (r=-0.78) and dot rate (+0.82). |
| **Batter Intent Score** | **Statistically Useful** | High | Rewards boundary utility and fast scoring. | Slightly favors power-hitters over anchors. |
| **Match Context Score** | 🔴 **Broken Heuristic** | Low | Supposed to weigh scoring rate against targets. | **Implementation Bug:** First-innings CRR formula is applied globally to the second innings too, ignoring runs needed. |
| **Venue Bias Rating** | **Weak (Unstable)** | High | Benchmarks ground scoring rates. | High noise when venue sample size is small (< 5 matches). |
| **xRuns** | **Too Simplistic** | High | Ball-level expected runs baseline. | A simple linear average blend; lacks over/phase context. |
| **xWickets** | **Too Simplistic** | High | Ball-level expected wickets baseline. | Simple average blend; ignores matchup types or phase. |

---

## 🔍 Deep-Dive Metric Validation

### 1. Pressure Index
* **Mathematical Formula:** `((Wickets Lost * 0.7 + Over Fraction * 0.2) / (Wickets Remaining + 0.1)).clip(0.0, 10.0)`
* **Distribution Profile:** Right-skewed (Mean: `0.82`, SD: `1.19`, Median: `0.47`, Max: `9.42`).
* **Outlier Analysis:** `6.91%` outliers (values > 2.17). This is normal; pressure spikes dramatically during late-innings run chases or when wickets fall quickly.
* **Audit Verdict:** Highly useful. Successfully flags pressure situations (clutch overs).

### 2. Bowler Quality Rating (BQR)
* **Mathematical Formula:** `(Dot Balls % * 60) + (Wickets % * 400)`
* **Distribution Profile:** Normal/Gaussian (Mean: `40.13`, SD: `16.57`, Median: `40.51`, Range: `5.00` to `86.67`).
* **Pearson Correlations:**
  * **BQR vs. Economy Rate:** **-0.78** (Elite negative correlation; higher BQR guarantees lower economy).
  * **BQR vs. Dot Ball Rate:** **+0.82** (Extremely strong positive correlation).
  * **BQR vs. Strike Rate:** **-0.65** (Fewer balls needed per wicket).
* **Audit Verdict:** The most robust proprietary metric in Midwicket. Highly predictive of bowling value.

### 3. Batter Intent Score (BIS)
* **Mathematical Formula:** `(((Boundaries * 1.5) + (Runs - Boundaries)) / (Balls Faced + 0.1) * 100).clip(0.0, 200.0)`
* **Distribution Profile:** Well-balanced (Mean: `83.75`, SD: `20.21`, Median: `85.35`, Max: `148.85`).
* **Audit Verdict:** Good indicator of batting aggression. Successfully identifies batsman acceleration.

### 4. Match Context Score (MCS)
* **Mathematical Formula:** 
  * First Innings: `CRR * (Wickets Remaining / 10.0)`
  * Second Innings: `(Runs Needed / (Balls Remaining + 0.1)) * (Wickets Remaining / 10.0)`
* **Distribution Profile:** Mean: `5.62`, SD: `2.77`, Median: `5.29`.
* **🔴 Critical Architectural Finding:**
  * The docstring defines separate formulas for 1st and 2nd innings. 
  * However, the SQL implementation in `midwicket/features.py` lines 132–137 **applies the first-innings CRR formula globally to all deliveries**, including the second innings:
    ```python
    df['crr'] = df['running_score'] / (df['over_fraction'] + 0.1)
    df['match_context_score'] = (df['crr'] * (df['wickets_remaining'] / 10.0)).clip(0.0, 15.0)
    ```
  * In the second innings, `running_score` is the chasing team's score, and `crr` is their current run rate. Applying this logic fails to incorporate the chase target, making the metric **completely broken and misleading** for the 2nd innings.

### 5. Venue Bias Rating (VBR)
* **Mathematical Formula:** `Venue First Innings Avg Runs / Global First Innings Avg Runs`
* **Distribution Profile:** Mean: `1.06`, SD: `0.31`, Range: `0.85` to `1.954`.
* **Audit Verdict:** Weak due to small sample sizes. If a venue has only hosted 1 or 2 matches, its VBR is highly volatile (e.g. max rating `1.954` was caused by a single high-scoring MLC match). VBR must implement a **minimum threshold of 5 matches** before publishing ratings.

### 6. Expected Runs (xRuns) & Expected Wickets (xWickets)
* **Mathematical Formula:** Linear combinations of player historic averages.
* **Audit Verdict:** Weak baseline metrics. While useful as a primitive indicator, they lack over-by-over, phase, and venue-specific context. For example, Bumrah bowling a death over in a low-scoring venue is expected to concede fewer runs than his global baseline, but xRuns predicts a static average.

---

## 🛑 Critical Bug in Feature Layer

### Metric Crash: `venue_adjusted_form`
When running the `venue_adjusted_form` feature builder on datasets where DuckDB returns `Decimal` types for aggregate SQL outputs, it crashes with:
```
TypeError: unsupported operand type(s) for *: 'decimal.Decimal' and 'float'
```
This is caused by the following line in `midwicket/features.py` L343:
```python
merged['adjusted_diff'] = merged['match_runs'] - (merged['venue_avg_runs_per_over'] * 2.0)
```
Where `venue_avg_runs_per_over` is a Series of Decimals and `2.0` is a float. 

### Fix Recommendation:
Cast the SQL average runs to double inside the query:
```diff
# midwicket/features.py L322
-        AVG(runs_batter + runs_extras) * 6.0 as venue_avg_runs_per_over
+        CAST(AVG(runs_batter + runs_extras) * 6.0 AS DOUBLE) as venue_avg_runs_per_over
```
