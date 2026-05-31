# Midwicket Adoption Readiness Report

This report evaluates Midwicket from the perspectives of five target user personas, identifying friction points, discoverability gaps, onboarding failures, and documentation gaps.

---

## 👥 Persona Audits & Friction Points

### 1. The Data Scientist
* **Objective:** Extract clean, model-ready feature vectors to train machine learning models (e.g. predicting match outcomes, player fantasy yields).
* **Friction Points:**
  * **Global-Only Context:** Calling `load_features("bowler_quality_rating")` computes a global BQR across all historical matches. There is no parameter to slice features by date range, competition, or season (e.g. "BQR in the last 12 months" vs. "BQR all-time"). This causes severe **data leakage** when training predictive models on historical matches.
  * **Pydantic/Arrow Overhead:** Interacting with Pydantic schemas adds type conversion overhead when converting bulk database outputs to pandas dataframes.
* **Onboarding Verdict:** **Moderate Friction.** Useful for baseline feature loading, but lacks temporal-slicing controls critical for professional machine learning.

### 2. The Cricket Analyst
* **Objective:** Run quick query statistics, compile leaderboards, and write match reports.
* **Friction Points:**
  * **0-Indexed Over Numbers:** Midwicket stores over numbers as **0-indexed** (`0` represents the first over, `19` represents the 20th over). Cricket analysts communicate strictly using **1-indexed** overs (e.g., "1st over", "20th over"). This causes massive cognitive load and high risk of off-by-one errors when writing queries.
  * **Lack of High-Level Name Resolver:** There is no simple client method to lookup a player by name (e.g., `session.get_player("Virat Kohli")`). Analysts are forced to write raw SQL joins against the `aliases` and `entities` tables in the registry.
* **Onboarding Verdict:** **High Friction.** Requires strong SQL skills and adjustment to 0-indexed overs.

### 3. The Fantasy Developer
* **Objective:** Build a real-time web application (Flask/Django/FastAPI) to display player ratings, consistency indicators, and live predictions.
* **Friction Points:**
  * **DuckDB Multi-Connection Locking:** DuckDB has a strict single-writer lock model. If the developer runs a web application with multiple worker threads, concurrent writes or active locks will immediately throw `IO Error: Could not set lock on file` and crash the web server.
  * **Lack of Live/Stream Ingestion API:** The engine is built for offline batch ingestion of zip files. There is no simple API to ingest a single live delivery JSON in real-time.
* **Onboarding Verdict:** **Severe Friction.** Impedes production deployment in standard multi-threaded web app architectures.

### 4. The Sports Researcher
* **Objective:** Analyze long-term historical trends (e.g. how scoring rates in Test cricket have evolved over 100 years).
* **Friction Points:**
  * **Missing Test Match Corpus:** Due to the signed 8-bit integer limit on the `over` column, **989 Test and first-class matches were completely excluded** from ingestion. A researcher using the live database would find zero historical records for Pakistan vs. West Indies 2017, Australia vs. Pakistan 2016, and hundreds of other core Test series.
* **Onboarding Verdict:** **Ingestion Blocker.** Completely skews long-format historical research studies.

### 5. The Academy Coach / Scouting Analyst
* **Objective:** Create detailed visual scouting profiles and match performance summaries for players.
* **Friction Points:**
  * **No Spatial Coordinates:** Wagon wheels and spatial pitch maps (X/Y coordinates of ball bounce and shot direction) are completely missing from the schema, preventing standard coach-level visual charting.
  * **High Setup Complexity:** Requires setting up a Python virtual environment and manually executing DuckDB queries, rather than using a simple CLI or Web Dashboard.
* **Onboarding Verdict:** **High Friction.** Unusable for non-technical coaching staff without custom web wrappers.

---

## 🛑 Summary of Onboarding & Adoption Gaps

1. **Cognitive Mismatch (0-Indexed Overs):** Over numbers should be normalized to standard 1-indexed cricket representation during canonicalization, or clearly highlighted in the introductory documentation.
2. **DuckDB Lock Constraints:** Implement a robust SQLite fallback or clear documentation on setting up read-only connections (`read_only=True`) inside multi-threaded application environments.
3. **Temporal Slicing in Feature Store:** Upgraded features must accept `start_date` and `end_date` parameters to prevent data leakage and support rolling time window calculations.
