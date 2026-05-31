# Midwicket Research Studies

25 reproducible, evidence-based studies generated from Cricsheet data
using Midwicket's analytics engine.

Each study is standalone: one Python file that downloads its own data,
runs the analysis, and writes results. All studies use only public
Cricsheet data — no proprietary feeds, no manual annotations.

---

## Studies by Category

### Batting Evolution (5 studies)

| File | Question |
|---|---|
| [01_t20_run_rate_inflation.py](01_t20_run_rate_inflation.py) | How much has the average T20 scoring rate risen since 2008? |
| [02_powerplay_boundary_surge.py](02_powerplay_boundary_surge.py) | When did powerplay boundary rates structurally shift upward? |
| [03_batter_age_curves.py](03_batter_age_curves.py) | What is the empirical peak age for T20 batters? |
| [04_strike_rate_vs_average.py](04_strike_rate_vs_average.py) | Has the SR/Avg trade-off changed across T20 eras? |
| [05_era_adjusted_greatness.py](05_era_adjusted_greatness.py) | Who are the greatest T20 batters after era adjustment? |

### Bowling Evolution (5 studies)

| File | Question |
|---|---|
| [06_economy_era_drift.py](06_economy_era_drift.py) | How much harder has it become to bowl economically since 2008? |
| [07_death_bowling_survival.py](07_death_bowling_survival.py) | Which bowlers maintained sub-8 economy across 3+ IPL seasons? |
| [08_spin_vs_pace_over_time.py](08_spin_vs_pace_over_time.py) | Has spin become more or less effective in T20 cricket since 2015? |
| [09_yorker_frequency.py](09_yorker_frequency.py) | Have yorker rates increased in death overs as runs inflated? |
| [10_bowling_diversity_index.py](10_bowling_diversity_index.py) | Do teams that use more bowling types win more? |

### Venue Effects (3 studies)

| File | Question |
|---|---|
| [11_batting_first_advantage.py](11_batting_first_advantage.py) | At which venues does batting first statistically win more? |
| [12_dew_factor_proxy.py](12_dew_factor_proxy.py) | Can second-innings scoring surplus proxy for dew conditions? |
| [13_altitude_effect.py](13_altitude_effect.py) | Do high-altitude venues (Dharamsala, Centurion) show elevated scoring? |

### Chase Dynamics (3 studies)

| File | Question |
|---|---|
| [14_chase_success_by_target.py](14_chase_success_by_target.py) | What target makes a T20 chase statistically impossible? |
| [15_death_over_chasing.py](15_death_over_chasing.py) | How do required run rates above 12 affect batting strategy? |
| [16_powerplay_chase_advantage.py](16_powerplay_chase_advantage.py) | Do chasing teams score faster in the powerplay? |

### Pressure Situations (3 studies)

| File | Question |
|---|---|
| [17_pressure_index_validation.py](17_pressure_index_validation.py) | Does a higher pressure index correlate with lower batter output? |
| [18_superover_run_rates.py](18_superover_run_rates.py) | What is the average runs scored in a T20 Super Over? |
| [19_final_over_dismissal_rate.py](19_final_over_dismissal_rate.py) | Are batters more likely to be dismissed in the 20th over? |

### Women's Cricket (3 studies)

| File | Question |
|---|---|
| [20_wbbl_batting_evolution.py](20_wbbl_batting_evolution.py) | How has WBBL batting evolved across its 10 seasons? |
| [21_wpl_vs_wbbl_comparison.py](21_wpl_vs_wbbl_comparison.py) | Is WPL more batting-friendly than WBBL? |
| [22_womens_pace_economy.py](22_womens_pace_economy.py) | How does pace bowling economy in women's T20 compare to men's? |

### Format Comparisons (3 studies)

| File | Question |
|---|---|
| [23_t20_vs_odi_boundary_rate.py](23_t20_vs_odi_boundary_rate.py) | How different are boundary rates between T20I and ODI cricket? |
| [24_test_batting_stamina.py](24_test_batting_stamina.py) | How does scoring rate decay across a Test innings? |
| [25_format_comparison_dot_balls.py](25_format_comparison_dot_balls.py) | How do dot ball percentages compare across T20, ODI, and Test? |

---

## Reproducibility

All studies download data from Cricsheet on first run. Re-runs use
the local cache. To force a fresh download:

```python
session = load_dataset("ipl", force=True)
```

Results are deterministic within a Cricsheet snapshot. As Cricsheet
adds new matches, figures will update slightly. Each study notes the
approximate data cutoff it was validated against.

---

## Adding a Study

1. Copy `_template.py` into a new numbered file.
2. Follow the five-section structure: Question, Methodology, Results, Chart note, Limitations.
3. Ensure the script runs end-to-end with `python research/<filename>.py`.
4. Open a pull request against `main`.
