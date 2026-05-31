# Onboarding Audit — First-Time User Experience

**Method:** Four simulated user personas. Each attempts Midwicket from a cold start (fresh environment, only the README and PyPI page visible).  
**Date:** 2026-05-31  
**Version audited:** v1.1.0  

Issues are ranked **Critical / Major / Minor**. Critical = blocks all progress. Major = causes significant confusion or wasted time. Minor = friction, polish issue.

---

## Persona 1 — Data Scientist

**Profile:** Python fluent, pandas daily user, knows DuckDB, unfamiliar with cricket data formats.  
**Goal:** Load IPL data and reproduce a custom batter leaderboard.

### Audit trace

**Step 1: Install** — `pip install midwicket` ✓ No issues.

**Step 2: Quick start from README**

Tries the win probability snippet:
```python
import midwicket.express as px
result = px.predict_win(venue="Wankhede Stadium", target=180, ...)
```
Works. Confidence is good. But the response dict contains `win_prob` and `confidence` — **the data scientist wants to know what scale `confidence` is on and how it's calculated.** There's no docstring surfaced in the README.

> **Minor M-01:** `predict_win` response fields are not documented inline in the README or quick start. The data scientist has to hunt through `express.py`.

**Step 3: Load dataset**

```python
from midwicket.datasets import load_dataset
session = load_dataset("ipl")
```
The download starts but there's **no progress bar visible** in some environments (Jupyter without tqdm configured). The download appears frozen.

> **Major M-02:** Download progress feedback is inconsistent. `tqdm` is used internally but may not render in all environments. A simple `print()` fallback would prevent "is this frozen?" confusion.

**Step 4: Query ball_events**

The data scientist tries:
```python
df = session.engine.execute_sql("SELECT * FROM ball_events LIMIT 5")
```
Returns an Arrow table. They then call `.to_pandas()` — works. But they notice `to_pydict()` exists too and try that — works. They wish they could just get a pandas DataFrame directly without `.to_pandas()`.

> **Minor M-03:** The query engine returns PyArrow; the `.to_pandas()` call is one extra step that pandas users don't expect. Consider accepting a `pandas=True` flag or returning a DataFrame from a convenience method.

**Step 5: Schema discovery**

The data scientist wants to know what columns are in `ball_events`. They try:
```python
session.engine.execute_sql("DESCRIBE ball_events")
```
Works, but **returns an Arrow table with cryptic column names** (`column_name`, `column_type`, `null`, etc.). They have to `.to_pandas()` and then remember the column names.

```python
# They wish this existed:
session.schema()          # returns list of (column, type) pairs
# or
session.columns           # property
```

> **Minor M-04:** No `session.schema()` or `session.columns` convenience accessor. Users have to run `DESCRIBE ball_events` and parse the Arrow result.

**Step 6: Feature store discovery**

The data scientist finds `from midwicket.features import ...` but **doesn't know what functions are available without reading the source file**. There's no `help(midwicket.features)` output that lists them.

> **Major M-05:** The feature store has no top-level `list_features()` function or `__all__` that surfaces available builders. Users can't discover what's available without reading source code.

**Overall friction:** 2 majors, 3 minors. Would succeed but with 15–20 minutes of confusion.

---

## Persona 2 — Cricket Analyst

**Profile:** Cricket domain expert, comfortable with Excel and basic Python, no DuckDB experience. Goal: generate a bowler comparison for a commentary piece.

### Audit trace

**Step 1: README landing**

Immediately asks: *"where do I get the data?"* The README says "Midwicket connects to Cricsheet automatically" — but this is in the "Loading Datasets" section which is below the fold. The hero section mentions "20,888+ Matches" but doesn't say where they come from.

> **Major C-01:** The hero section doesn't explain the data source. A cricket analyst unfamiliar with Cricsheet will wonder "is this proprietary data? Do I need a license? Is it accurate?"

**Step 2: Quick start**

The analyst tries the win probability snippet, gets the output. But then asks: *"How accurate is this? What's the error rate?"* The model's AUC (0.843) is mentioned in the README but only in a brief parenthetical. There's no context for what AUC means for a non-ML user.

> **Minor C-02:** Technical metrics (AUC 0.843) are stated without a layperson explanation. A cricket analyst doesn't know if 0.843 is good or bad. "This predicts the correct winner in ~84% of situations" would be clearer.

**Step 3: Dataset loading**

The analyst reads `load_dataset("ipl")` and wonders: *"What's the difference between 'ipl' and 'all_t20'? The table doesn't explain coverage dates or whether women's cricket is included."*

> **Major C-03:** The dataset table in the README shows `est_matches` but not `date range` or `gender coverage`. The cricket analyst doesn't know if WBBL is in `all_t20` or separate.

**Step 4: Bowling query**

The analyst is told to run SQL but doesn't know what `extras_type`, `wicket_type`, or `is_wicket` mean semantically. They want to count "real wickets" but don't know to exclude `RUN_OUT` when attributing to bowlers.

> **Critical C-04:** There is no data dictionary. The ball_events schema has 21 columns with cricket-specific semantics (`extras_type`, `wicket_type`, `phase`, `runs_batter` vs `runs_extras`) that are non-obvious. A first-time user will count wickets wrong, miscalculate economy rates, or misinterpret `extras_type`.

**Step 5: Scouting report**

Tries:
```python
import midwicket as md
session = md.init("./data")
report = md.scouting_report("Jasprit Bumrah")
print(report)
```
Gets a dictionary. Prints it — a dense nested dict. Not human-readable.

> **Major C-05:** The scouting report returns a raw dict. There's no `report.summary()`, `report.to_markdown()`, or pretty-print helper. A cricket analyst expects human-readable output, not a Python dict.

**Overall friction:** 1 critical, 3 majors, 1 minor. Would likely give up at the data dictionary step.

---

## Persona 3 — Fantasy Cricket Developer

**Profile:** Backend developer, Python + pandas, wants to build a daily fantasy lineup optimizer. Goal: generate BQR, pressure index, and expected runs for the current IPL season in one pipeline.

### Audit trace

**Step 1: Install and quick start** — no issues.

**Step 2: Load 2026 season data**

```python
session = load_dataset("ipl")
```
The developer wants only 2026 data. Tries:
```python
df = session.engine.execute_sql("SELECT * FROM ball_events WHERE date >= '2026-01-01'").to_pandas()
```
Works. But the developer then asks: *"How do I load ONLY a specific season without downloading all of IPL?"* There's no `load_dataset("ipl", season=2026)` option.

> **Major F-01:** No season filter in `load_dataset()`. Downloading 1,239 matches to use 70 feels wasteful. The developer wants `load_dataset("ipl", start_year=2024)`.

**Step 3: Feature pipeline**

```python
from midwicket.features import build_bowler_quality_rating, build_pressure_index

bqr = build_bowler_quality_rating(session, start_date="2026-01-01")
pi  = build_pressure_index(session, start_date="2026-01-01")
```
Both work. But the developer then tries to join them:
```python
combined = bqr.merge(pi, on="bowler_id")
```
Fails — `pi` doesn't have a `bowler_id` column, it has `bowler_id` per-delivery. The grain is different. The developer doesn't know they need to aggregate `pi` first.

> **Major F-02:** Features have different grains (per-delivery vs per-player) with no documentation. Joining `pressure_index` (delivery-level) with `bowler_quality_rating` (player-level) requires an intermediate aggregation step that is not documented.

**Step 4: Player ID resolution**

The developer gets `bowler_id` integers in the output and wants to map them to names. Tries:
```python
registry = session.registry
registry.search_players("Bumrah")
```
Works — returns `[{"id": 45, "name": "jj bumrah"}]`. But notices the name is **lowercase** — the registry stores normalized names. The original casing (`JJ Bumrah`) is in `ball_events.bowler`.

> **Minor F-03:** Registry returns lowercase normalized names; `ball_events` stores original Cricsheet casing. This discrepancy surprises developers who expect consistent casing.

**Step 5: Export for production**

The developer wants to export the feature DataFrame to Parquet for their optimizer:
```python
bqr.to_parquet("features_2026.parquet")
```
Works — it's a standard pandas DataFrame. No issue here.

**Step 6: Scheduling / refresh**

The developer asks: *"How do I refresh this daily? Is there a `load_dataset("ipl", force=True)` option?"*  
Yes — `load_dataset("ipl", force=True)` exists but is **not documented in the README or getting_started guide**.

> **Minor F-04:** `force=True` parameter for dataset refresh is not surfaced in primary documentation. Fantasy developers building daily pipelines will miss it.

**Overall friction:** 2 majors, 2 minors. Would succeed but the grain mismatch (M-02) could cause silent errors in production.

---

## Persona 4 — Sports Researcher

**Profile:** Academic, familiar with R and Stata, migrating to Python. Goal: reproduce the "IPL scoring trends over 18 years" analysis for a paper.

### Audit trace

**Step 1: Discoverability**

Finds Midwicket via a cricket analytics blog post. Visits GitHub. Reads the README. **The current README leads with "The Problem" and "Architecture"** — two things a researcher doesn't care about immediately. The data is mentioned third.

> **Critical R-01 (pre-overhaul):** The previous README buried the data and insights behind architecture explanations. A researcher closing the tab in 10 seconds is a real risk. *(This is addressed in the v1.1.0 README overhaul.)*

**Step 2: Citation and data provenance**

After loading the dataset, the researcher asks: *"How do I cite the data source? Is Cricsheet citable? What's the data cut-off date for the most recent match?"*

There's no `session.data_info()` or `session.last_updated()` method. The researcher has to run:
```python
session.engine.execute_sql("SELECT MIN(date), MAX(date) FROM ball_events").to_pandas()
```

> **Major R-01:** No `session.metadata()` or `session.data_info()` convenience method. Researchers need provenance information (source, date range, match count) for methodology sections.

**Step 3: Reproducibility**

The researcher asks: *"If I run this analysis in 6 months after more 2026 matches are added, will I get the same results?"*  
Answer is: no, unless they snapshot. But **there's no guidance on pinning a dataset to a specific date or using `force=False` to avoid re-downloading.**

> **Major R-02:** No documented workflow for reproducible analyses. Researchers need a way to say "use data as of 2026-04-01 only" globally, not just per-feature-call.

**Step 4: Statistical validation**

The researcher wants to check sample sizes before drawing conclusions. They query:
```python
session.engine.execute_sql("""
    SELECT EXTRACT(YEAR FROM date) AS season, COUNT(DISTINCT match_id)
    FROM ball_events GROUP BY season
""").to_pandas()
```
Works perfectly. Gets the season-by-season match count. No issues.

**Step 5: Export for R**

```python
df = session.engine.execute_sql("SELECT * FROM ball_events").to_pandas()
df.to_csv("ipl_deliveries.csv")
```
Works but produces a 294,757-row CSV. The researcher then wants to filter first — and discovers the SQL API easily. No significant friction here.

**Overall friction:** 2 majors, 0 minors for the researcher (in v1.1.0 with the new README). The reproducibility gap is the biggest risk.

---

## Consolidated Issue Tracker

### Critical Issues

| ID | Issue | Persona | Impact |
|----|-------|---------|--------|
| **C-04** | No data dictionary for `ball_events` schema | Cricket Analyst | Users miscalculate wickets, economy, extras — silently wrong results |
| **R-01** | README buried insights behind architecture *(addressed in v1.1.0)* | All | First-time visitor leaves before seeing value |

### Major Issues

| ID | Issue | Persona | Recommended Fix |
|----|-------|---------|-----------------|
| **M-02** | Download progress inconsistent across environments | Data Scientist | Add `print()` fallback to `_download_file()` when tqdm not rendering |
| **M-05** | No `list_features()` or feature discovery | Data Scientist | Add `midwicket.features.list_features()` → returns dict of name→description |
| **C-01** | Hero section doesn't explain data source | Cricket Analyst | Add "Data from Cricsheet.org — open, freely licensed" to hero |
| **C-03** | Dataset table missing date range and gender coverage | Cricket Analyst | Add `date_range` and `gender` columns to the datasets table |
| **C-05** | Scouting report returns raw dict, not human-readable | Cricket Analyst | Add `format_report(report)` helper or `report.to_markdown()` |
| **F-01** | No season filter in `load_dataset()` | Fantasy Developer | Add `start_year` / `end_year` kwargs to `load_dataset()` |
| **F-02** | Feature grains undocumented (per-delivery vs per-player) | Fantasy Developer | Document grain in each feature's docstring; add to gallery |
| **R-01** | No `session.metadata()` / data provenance method | Sports Researcher | Add `session.info()` → returns match count, date range, source |
| **R-02** | No reproducibility workflow documented | Sports Researcher | Add "Pinning data for reproducible analysis" section to getting_started |

### Minor Issues

| ID | Issue | Persona | Recommended Fix |
|----|-------|---------|-----------------|
| **M-01** | `predict_win` response fields undocumented in README | Data Scientist | Add inline comment showing response shape in quick start |
| **M-03** | Query engine returns Arrow, not pandas | Data Scientist | Consider `session.query(sql, pandas=True)` convenience wrapper |
| **M-04** | No `session.columns` schema accessor | Data Scientist | Add `session.schema()` → returns column list |
| **C-02** | AUC 0.843 stated without layperson context | Cricket Analyst | Change to "correct winner predicted in ~84% of completed matches" |
| **F-03** | Registry returns lowercase names; ball_events has original casing | Fantasy Developer | Normalise casing or document the difference explicitly |
| **F-04** | `force=True` for dataset refresh not documented | Fantasy Developer | Add to getting_started and datasets table |

---

## Priority Order for v1.2.0

**Do these first (highest adoption impact):**

1. **Add `ball_events` data dictionary** to docs/ — one page, plain English definitions of all 21 columns with valid values. Blocks cricket analysts completely without it.
2. **Add `list_features()`** — three lines of code, massive discoverability improvement.
3. **Add `session.info()`** — provenance for researchers, debugging for developers.
4. **Update dataset table** with date ranges and gender coverage.
5. **Document feature grains** — which features are per-delivery, which are per-player.

**Do these second:**

6. `format_report()` helper for scouting output.
7. `start_year`/`end_year` kwargs on `load_dataset()`.
8. Download progress fallback for non-tqdm environments.
9. Reproducibility section in getting_started.

**Polish last:**

10. `session.columns` accessor.
11. `pandas=True` on execute_sql.
12. Registry casing normalization.
13. `force=True` documentation.

---

*Audit methodology: simulated from cold start with only README.md and PyPI page. No access to source code, issues, or Slack. Represents realistic GitHub visitor experience.*
