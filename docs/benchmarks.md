# Benchmark Registry

Midwicket exposes four prediction problems that emerge naturally from
ball-by-ball data. Each benchmark defines a reproducible evaluation
standard. Models are not implemented here. These are problem definitions
and baselines only.

A result is considered **valid** only if it satisfies all four requirements:
1. Uses only the specified training split.
2. Reports the specified primary metric.
3. Uses no external data sources (no Cricinfo, no proprietary feeds).
4. Posts code that reproduces the result from raw Cricsheet data.

---

## Benchmark 1 — Win Probability

**Task:** Given the current state of a T20 innings, predict the probability
that the chasing team wins the match.

### Formulation

Binary classification problem. Label: `chase_won` (1 if chasing team wins).

**Features available at prediction time:**
| Feature | Description |
|---|---|
| `runs_scored` | Runs scored so far in the 2nd innings |
| `wickets_down` | Wickets lost so far in the 2nd innings |
| `overs_done` | Overs completed (float, e.g. 12.3) |
| `target` | 1st innings total + 1 |
| `venue` | Ground name (categorical) |
| `required_run_rate` | (target - runs_scored) / overs_remaining * 6 |
| `current_run_rate` | runs_scored / overs_done * 6 |

**No features from the future are permitted.** Do not use final match state.

### Dataset

| Split | Definition |
|---|---|
| Train | All IPL matches, 2008–2021 |
| Validation | IPL 2022 |
| Test | IPL 2023–2026 |

Minimum 50 deliveries per match-state sample to include a data point.

### Evaluation Metric

**Primary:** AUC-ROC on test set.
**Secondary:** Brier score (lower is better), calibration plot.

### Baseline

Midwicket ships a logistic regression baseline trained on IPL data.
Reported test AUC: **0.843**. Brier score: **0.181**.

Any submitted model must beat this baseline on the same test split to
be listed in the leaderboard.

### Minimum Reproducibility Bar

```python
from midwicket.datasets import load_dataset
from midwicket.compute.winprob import win_probability

session = load_dataset("ipl")
# Evaluate your model against this session
# Report AUC on matches from 2023 onward only
```

---

## Benchmark 2 — Wicket Probability

**Task:** Given batter, bowler, over, and match state, predict the probability
that the current delivery results in a dismissal.

### Formulation

Binary classification. Label: `is_wicket`.

**Features available at prediction time:**
| Feature | Description |
|---|---|
| `batter_id` | Batter identifier |
| `bowler_id` | Bowler identifier |
| `over` | Current over number (0-indexed) |
| `ball_in_over` | Ball within the over (0-5) |
| `batter_balls_faced` | Balls faced so far this innings (time on crease) |
| `bowler_balls_bowled` | Balls bowled so far this spell |
| `innings_wickets_down` | Wickets already fallen |
| `innings_run_rate` | Current innings run rate |
| `pressure_index` | Midwicket pressure index at this delivery |

**No inter-delivery features from the same delivery are permitted**
(e.g. you cannot use `runs_scored_on_this_ball` as a feature).

### Dataset

| Split | Definition |
|---|---|
| Train | IPL 2008–2020, all T20IS 2005–2020 |
| Validation | IPL 2021, T20IS 2021 |
| Test | IPL 2022–2026, T20IS 2022–2026 |

Class imbalance: approximately 1 wicket per 20 deliveries. Stratified
sampling or weighted loss is appropriate.

### Evaluation Metric

**Primary:** AUC-ROC.
**Secondary:** Precision-Recall AUC (more informative under class imbalance).

### Baseline

Unconditional: predict `wicket_probability = 0.05` for every delivery.
AUC: 0.500. This is the floor that any model must beat.

A logistic regression on over + batter_balls_faced + pressure_index
achieves AUC ~0.61. This is the recommended first-beat threshold.

---

## Benchmark 3 — Fantasy Points

**Task:** Given pre-match features for a player in an upcoming match,
predict their total fantasy points for that match.

### Formulation

Regression problem. Label: `fantasy_points` (computed by Midwicket's
fantasy scoring engine for the completed match).

**Features available before the match:**
| Feature | Description |
|---|---|
| `player_id` | Player identifier |
| `rolling_avg_5` | Rolling 5-match batting average |
| `rolling_sr_5` | Rolling 5-match batting strike rate |
| `bqr` | Bowler quality rating (if bowler) |
| `pressure_index_avg` | Mean pressure index over last 5 matches |
| `venue_adj_form` | Venue-adjusted batting form score |
| `opposition_bqr_avg` | Mean BQR of likely opposition bowlers |

**No features from the match itself are permitted** (no runs scored,
no wickets taken in the match being predicted).

### Dataset

| Split | Definition |
|---|---|
| Train | IPL 2012–2021 (feature window requires 5 previous matches) |
| Validation | IPL 2022 |
| Test | IPL 2023–2026 |

Include only players who appeared in at least 5 prior matches.

### Evaluation Metric

**Primary:** Mean Absolute Error (MAE) in fantasy points.
**Secondary:** Rank correlation (Spearman ρ) between predicted and actual
top-10 scorers per match.

### Baseline

Predict previous-match fantasy points as the forecast.
MAE baseline: approximately 22 points. Rank correlation baseline: ~0.38.

---

## Benchmark 4 — Score Projection

**Task:** After the first 10 overs of a T20 innings, project the final
first-innings total.

### Formulation

Regression problem. Label: `final_innings_total`.

**Features available at the 10-over mark:**
| Feature | Description |
|---|---|
| `runs_at_10` | Runs scored after 10 overs |
| `wickets_at_10` | Wickets fallen after 10 overs |
| `run_rate_10` | Run rate at 10 overs |
| `venue` | Ground name |
| `batting_team` | Batting team (categorical) |
| `season` | Season year |

**Predictions use only features available at the 10-over mark.** No
delivery-level data from overs 11-20 is permitted.

### Dataset

| Split | Definition |
|---|---|
| Train | IPL 2008–2020 |
| Validation | IPL 2021 |
| Test | IPL 2022–2026 |

Include only completed innings (no DLS adjustments).

### Evaluation Metric

**Primary:** Root Mean Squared Error (RMSE) in runs.
**Secondary:** Mean Absolute Percentage Error (MAPE).

### Baseline

Naive projection: `predicted_total = runs_at_10 * 2 + 15`.
RMSE baseline: approximately 21 runs. MAPE: approximately 11%.

---

## Leaderboard Format

To submit a result, open a PR adding one row to `docs/benchmark_results.md`
(not yet created) with:

| Benchmark | Model | Dataset | Metric | Score | Code |
|---|---|---|---|---|---|
| win_probability | XGBoost (depth=5) | IPL 2023–2026 | AUC | 0.871 | [link] |

The PR must include a script that reproduces the result from scratch
using only Midwicket's public API and Cricsheet data.

---

## Benchmark FAQ

**Can I use additional external data?**
No. The benchmarks use Cricsheet exclusively. External feeds are not permitted
because they cannot be reproduced by other researchers.

**Can I use data from multiple competitions?**
Only if the benchmark definition specifies it (Benchmark 2 allows T20IS).
Using IPL data in the IPL-only benchmarks is permitted (train split only).

**How are tied results ranked?**
By secondary metric. If still tied, by model simplicity (fewer features wins).

**What counts as a "model"?**
Any function that takes the specified features and returns a prediction.
A lookup table is a valid model. A neural network is a valid model.
A hard-coded constant is the baseline model.
