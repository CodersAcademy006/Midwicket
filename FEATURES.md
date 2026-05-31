# Midwicket Feature Roadmap

## Overview

This document tracks planned features for Midwicket beyond the v1.0.0 stable release. Features are
grouped into four areas: full Cricsheet format support, automatic data sync, automatic model
retraining, and advanced ML/DL models. Each entry includes a unique ID (FT-NNN), motivation,
required changes, dependencies, and estimated scope.

---

## Status

Current version: **1.0.0** (stable, IPL T20 only, logistic regression win probability)  
Target version for this roadmap: **1.1.0** (multi-format), **1.2.0** (auto-sync + auto-retrain),
**1.3.0** (advanced ML/DL models)

---

## Area 1 — Full Cricsheet Format Support

Cricsheet publishes 21,877+ ball-by-ball matches across Tests, ODIs, T20Is, and 50+ domestic
competitions. The library currently ingests IPL T20 data only. This area expands coverage to every
format Cricsheet provides.

---

### FT-001 — Competition catalogue and multi-format download

**Target release**: 1.1.0  
**Priority**: High  

**Motivation**  
`DataLoader` and `config.py` hardcode a single URL (`ipl_json.zip`) and a single directory
(`raw/ipl/`). There is no way for a user to download Tests, ODIs, BBL, PSL, or any other
competition without manually overriding `CRICSHEET_URL`.

**What needs to change**

1. Add a `CRICSHEET_CATALOGUE` dict to `config.py` mapping short keys to Cricsheet ZIP URLs:

   ```
   "all"       → https://cricsheet.org/downloads/all_json.zip
   "tests"     → https://cricsheet.org/downloads/tests_json.zip
   "odis"      → https://cricsheet.org/downloads/odis_json.zip
   "t20s"      → https://cricsheet.org/downloads/t20s_json.zip
   "ipl"       → https://cricsheet.org/downloads/ipl_json.zip
   "bbl"       → https://cricsheet.org/downloads/bbl_json.zip
   "psl"       → https://cricsheet.org/downloads/psl_json.zip
   "t20_blast" → https://cricsheet.org/downloads/t20_blast_json.zip
   "cpl"       → https://cricsheet.org/downloads/cpl_json.zip
   "bpl"       → https://cricsheet.org/downloads/bpl_json.zip
   "the_hundred" → https://cricsheet.org/downloads/the_hundred_json.zip
   "wbbl"      → https://cricsheet.org/downloads/wbbl_json.zip
   ... (full 50+ entry catalogue)
   ```

2. Change `DataLoader.__init__` to accept `competitions: List[str]` instead of a single
   `data_dir`. Each competition downloads into its own subdirectory: `raw/<competition>/`.

3. Change `express.download_data()` signature:
   ```python
   def download_data(competitions: List[str] = ["ipl"], data_dir: Optional[str] = None) -> None
   ```

4. `iter_matches()` iterates all subdirectories under `raw/`.

**Files changed**: `config.py`, `data/loader.py`, `express.py`  
**New files**: none  
**Estimated scope**: 8-12 hours

---

### FT-002 — Schema v2: match_type, gender, competition columns

**Target release**: 1.1.0  
**Priority**: High  

**Motivation**  
The `ball_events` Arrow/DuckDB schema has no `match_type`, `gender`, or `competition` columns.
Without them there is no way to filter analytics to "Tests only", "women's matches", or "BBL vs
IPL". Every analytics query implicitly mixes all ingested data.

**What needs to change**

1. Add three columns to `BALL_EVENT_SCHEMA` in `schema/v1.py` (promote to `schema/v2.py`):

   | Column | Type | Example values |
   |---|---|---|
   | `match_type` | `pa.string()` | `T20`, `ODI`, `Test`, `IT20`, `ODM`, `MDM` |
   | `gender` | `pa.string()` | `male`, `female` |
   | `competition` | `pa.string()` | `ipl`, `bbl`, `tests`, `odis` |

2. Upgrade `over` from `pa.int8()` to `pa.int16()`. `int8` has a maximum of 127; Test matches
   can produce over counts beyond this in unusual follow-on scenarios.

3. Add `storage/migrations.py` with a `migrate_v1_to_v2(db_path)` function that issues
   `ALTER TABLE ball_events ADD COLUMN ...` for the three new columns with NULL defaults. This
   allows existing `.duckdb` files to be upgraded in-place without a full re-ingest.

4. Bump `SCHEMA_META["version"]` to `"2.0.0"`.

5. `canonicalize_match()` reads `match_type`, `gender`, and `competition` from the JSON `info`
   section and writes them into every delivery row.

**Files changed**: `schema/v1.py` (or new `schema/v2.py`), `core/canonicalize.py`  
**New files**: `storage/migrations.py`  
**Estimated scope**: 6-8 hours

---

### FT-003 — Format-aware phase logic

**Target release**: 1.1.0  
**Priority**: Medium  

**Motivation**  
`_determine_phase(over_num)` in `core/canonicalize.py` is hardcoded to T20 phase boundaries
(0-5 Powerplay, 6-14 Middle, 15+ Death). Applied to an ODI, over 7 is labelled "Middle" when
it is still in the ODI Powerplay. Applied to a Test, every over past 15 is labelled "Death"
which is meaningless.

**What needs to change**

1. Change signature to `_determine_phase(over_num: int, match_type: str) -> str`.

2. Phase boundaries per format:

   | Format | Powerplay | Middle | Death |
   |---|---|---|---|
   | T20 / IT20 | 0-5 | 6-14 | 15+ |
   | ODI / ODM | 0-9 | 10-39 | 40+ |
   | Test / MDM | N/A | N/A | N/A (returns "N/A") |

3. Pass `match_type` through from `canonicalize_match()` to `_determine_phase()`.

4. Analytics functions that group by `phase` already handle arbitrary string values; the
   "N/A" label for Tests will appear as its own group without breaking any queries.

**Files changed**: `core/canonicalize.py`  
**New files**: none  
**Estimated scope**: 2-3 hours

---

### FT-004 — match_type filter parameter on analytics functions

**Target release**: 1.1.0  
**Priority**: Medium  

**Motivation**  
Once multi-format data is ingested, `career_batting("V Kohli")` will aggregate across Tests,
ODIs, and T20s in a single result. That is rarely what a caller wants. Every player analytics
function needs an optional `match_type` filter.

**What needs to change**

1. Add `match_type: Optional[str] = None` parameter to all 28 functions in
   `api/player_analytics.py`. When not None, append `AND match_type = ?` to the WHERE clause.

2. Fix `death_over_specialist()` specifically: the over range 16-19 is T20-only. Change to
   derive the death over range from `match_type`:

   ```
   T20 / IT20  → overs 16-19
   ODI / ODM   → overs 41-49
   Test / MDM  → not applicable, raise ValueError with clear message
   ```

3. Add `match_type` filter to `batting_leaderboard()` and `bowling_leaderboard()` for the same
   reason — a leaderboard mixing Test and T20 averages is meaningless.

**Files changed**: `api/player_analytics.py`  
**New files**: none  
**Estimated scope**: 6-10 hours

---

### FT-005 — ODI win probability model

**Target release**: 1.1.0  
**Priority**: Medium  

**Motivation**  
`win_probability()` raises a warning for any `balls_per_innings != 120` and produces unreliable
results for ODI data. With ODI ball-by-ball data now in the database, an ODI-specific logistic
regression model can be trained using the same pipeline as the existing T20 model.

**What needs to change**

1. Add `match_format: str = "T20"` parameter to `win_probability()`. Dispatch to the correct
   model based on format.

2. Train an ODI model using the same `WinProbabilityTrainer` pipeline, with `balls_per_innings=300`
   and an expanded feature set that accounts for the longer format (e.g. wickets remaining at
   over 25 carries less pressure than at over 45).

3. Store the ODI model artifacts alongside the T20 model in `~/.midwicket_data/models/`.

4. For `match_format="Test"`, raise `NotImplementedError` with a clear message. Test win
   probability requires session-level features (pitch wear, day/night, follow-on state) that
   Cricsheet does not provide.

**Files changed**: `compute/winprob.py`, `models/win_predictor.py`  
**New files**: `models/data/odi_model.json` (trained artifact)  
**Estimated scope**: 15-20 hours (feature engineering + training + validation)

---

## Area 2 — Automatic Data Sync

New matches are added to Cricsheet continuously. The library currently requires a manual
`download_data()` call. This area adds the infrastructure to fetch new matches automatically
without any user action.

---

### FT-006 — Incremental match ingestion (diff-based)

**Target release**: 1.2.0  
**Priority**: High  

**Motivation**  
`DataLoader.download()` currently re-downloads the full ZIP and re-ingests all matches every
time it is called. For a 21,877-match all-formats dataset this means downloading ~500MB and
re-processing all data on every sync. An incremental path is needed: only ingest match IDs not
already present in `ball_events`.

**What needs to change**

1. After downloading and extracting a new ZIP, collect the set of JSON filenames (= match IDs)
   from the extracted directory.

2. Query `SELECT DISTINCT match_id FROM ball_events` from the existing database.

3. Compute the difference: `new_ids = extracted_ids - already_ingested_ids`.

4. Run `canonicalize_match()` and `ingest_events()` only for the delta set.

5. Log the count: `"Sync complete: N new matches ingested, M matches already present, skipped"`.

**Files changed**: `data/loader.py`, `api/session.py`  
**New files**: none  
**Estimated scope**: 6-8 hours

---

### FT-007 — ETag-based change detection (avoid unnecessary downloads)

**Target release**: 1.2.0  
**Priority**: High  

**Motivation**  
Even with incremental ingestion (FT-006), the library still downloads the full ZIP on every
sync call to discover new match IDs. A HEAD request costs ~1KB. An `ETag` or `Last-Modified`
check tells the library whether the ZIP has changed at all before committing to a download.

**What needs to change**

1. Before downloading each competition ZIP, issue an HTTP HEAD request to the URL.

2. Store the `ETag` and `Last-Modified` response headers in a small JSON file at
   `~/.midwicket_data/sync_state.json`:

   ```json
   {
     "ipl": { "etag": "\"abc123\"", "last_modified": "Fri, 30 May 2026 10:00:00 GMT", "last_synced": "2026-05-30T10:05:00Z" },
     "bbl": { ... }
   }
   ```

3. On the next sync call, send `If-None-Match: <stored ETag>` (or `If-Modified-Since`) in the
   GET request. A `304 Not Modified` response means the ZIP has not changed; skip the download
   entirely.

4. Update the stored ETag/Last-Modified after a successful download and ingest.

**Files changed**: `data/loader.py`  
**New files**: none (state file at `~/.midwicket_data/sync_state.json` is runtime data, not code)  
**Estimated scope**: 4-6 hours

---

### FT-008 — `midwicket sync` CLI command

**Target release**: 1.2.0  
**Priority**: High  

**Motivation**  
Users and system administrators need a single command to trigger the full sync pipeline
(ETag check → conditional download → incremental ingest → retrain trigger). This command
should be runnable manually or wired into a system cron job.

**What needs to change**

1. Add a `sync` entry point to `pyproject.toml`:

   ```toml
   [project.scripts]
   midwicket = "midwicket._cli:main"
   ```

2. Implement `midwicket/_cli.py` with a `sync` subcommand:

   ```
   midwicket sync [--competitions ipl bbl tests odis] [--data-dir PATH] [--force]
   ```

   - `--competitions` defaults to whatever was last downloaded (reads `sync_state.json`)
   - `--force` bypasses ETag check and re-downloads unconditionally
   - exits with code 0 on success, 1 on network failure, 2 on ingest failure

3. The `sync` command runs: ETag check (FT-007) → conditional download → incremental ingest
   (FT-006) → retrain trigger (FT-010).

4. Structured log output to stdout so cron/systemd can capture it:

   ```
   [2026-05-30 10:05:02] sync: checking ipl ... ZIP unchanged (ETag match), skipped
   [2026-05-30 10:05:03] sync: checking bbl ... ZIP changed, downloading (14.2 MB)
   [2026-05-30 10:05:18] sync: bbl: 12 new matches ingested, 650 already present
   [2026-05-30 10:05:19] sync: retrain triggered (14 new T20 matches since last train)
   ```

**Files changed**: `pyproject.toml`  
**New files**: `midwicket/_cli.py`  
**Estimated scope**: 8-10 hours

---

### FT-009 — Built-in background sync scheduler

**Target release**: 1.2.0  
**Priority**: Medium  

**Motivation**  
`midwicket sync` (FT-008) requires the user to set up their own cron job. For users running
`midwicket serve` as a persistent server, a built-in scheduler is more convenient: the server
keeps the database current automatically without any OS-level cron configuration.

**What needs to change**

1. Add `APScheduler` as an optional dependency under a new `sync` extra:

   ```toml
   [project.optional-dependencies]
   sync = ["apscheduler>=3.10.0"]
   ```

2. When `midwicket serve --sync-interval 6h` is passed, start an `APScheduler`
   `BackgroundScheduler` that fires the sync pipeline on the given interval.

3. Expose sync status via a `/sync/status` endpoint:

   ```json
   {
     "last_synced": "2026-05-30T10:05:19Z",
     "next_sync": "2026-05-30T16:05:19Z",
     "competitions": ["ipl", "bbl"],
     "last_new_matches": 12
   }
   ```

4. If `APScheduler` is not installed and `--sync-interval` is passed, raise a clear error:
   `"sync extra required: pip install midwicket[sync]"`.

**Files changed**: `serve/api.py`, `pyproject.toml`  
**New files**: `serve/scheduler.py`  
**Estimated scope**: 8-12 hours

---

## Area 3 — Automatic Model Retraining

When new matches are ingested, the win probability model should retrain itself to incorporate
the new data. Retraining should be automatic, safe (cannot produce a worse model than what
it replaces), and auditable.

---

### FT-010 — Post-sync retrain trigger

**Target release**: 1.2.0  
**Priority**: High  

**Motivation**  
The sync pipeline (FT-006 through FT-008) ingests new matches but does not update the model.
The model ages relative to the data. A threshold-based retrain trigger fires retraining when
enough new matches have accumulated to make retraining worthwhile.

**What needs to change**

1. Add `MIDWICKET_RETRAIN_THRESHOLD` env var (default: `50`). After each sync, if
   `new_matches_ingested >= threshold`, trigger retraining.

2. Track `matches_since_last_retrain` in `sync_state.json` alongside the ETag data. Reset to
   zero after each retrain.

3. Retrain is triggered per format: if 50 new T20 matches arrived, retrain the T20 model.
   If 50 new ODI matches arrived, retrain the ODI model. Tests do not have a model and are
   skipped silently.

4. Retraining runs in a background thread so it does not block the sync pipeline or the
   serving layer.

**Files changed**: `data/loader.py`, `midwicket/_cli.py`  
**New files**: none  
**Estimated scope**: 4-6 hours

---

### FT-011 — AUC safety gate on retrained model

**Target release**: 1.2.0  
**Priority**: High  

**Motivation**  
Automatic retraining on new data can produce a worse model if the new data is skewed (e.g., a
batch of rain-affected matches, or a tournament with unusual scores). The new model must only
replace the current model if it is at least as good.

**What needs to change**

1. After training, evaluate the new model on a held-out validation set (a random 20% split
   of all available data, fixed seed for reproducibility).

2. Compare new AUC against current model AUC:

   ```
   if new_auc >= (current_auc - MIDWICKET_RETRAIN_AUC_TOLERANCE):
       replace model, log "model updated: AUC {old} -> {new}"
   else:
       discard new model, log warning "retrain degraded model (AUC {new} < {old} - tolerance); keeping previous"
   ```

3. Add `MIDWICKET_RETRAIN_AUC_TOLERANCE` env var (default: `0.005`).

4. Keep the previous model artifacts in `~/.midwicket_data/models/` under a
   `<format>_model_prev.json` name so a manual rollback is always possible.

**Files changed**: `models/win_predictor.py`  
**New files**: none  
**Estimated scope**: 4-6 hours

---

### FT-012 — Retrain audit log and model lineage

**Target release**: 1.2.0  
**Priority**: Medium  

**Motivation**  
When a model silently updates itself, there is no record of when it changed, what data it was
trained on, or what its performance was. For production use and debugging, every retrain event
must be logged with full provenance.

**What needs to change**

1. After each retrain attempt (success or failure), append a structured record to
   `~/.midwicket_data/models/retrain_log.jsonl`:

   ```json
   {
     "timestamp": "2026-06-01T04:15:00Z",
     "format": "T20",
     "trigger": "threshold",
     "training_matches": 1291,
     "new_matches_added": 50,
     "new_auc": 0.851,
     "previous_auc": 0.843,
     "outcome": "replaced",
     "model_artifact": "t20_model_20260601.json"
   }
   ```

2. Expose retrain history via `md.get_model_history(format="T20")` in the Python API.

3. Expose it via a `/model/history` REST endpoint (returns last N retrain events).

4. Model artifact filenames include a timestamp (`t20_model_YYYYMMDD.json`) so older versions
   are recoverable by date.

**Files changed**: `models/win_predictor.py`, `api/session.py`  
**New files**: `serve/routes/model.py`  
**Estimated scope**: 6-8 hours

---

### FT-013 — User-directory model storage (survives package upgrades)

**Target release**: 1.2.0  
**Priority**: High  

**Motivation**  
Current model artifacts live inside the installed package at `midwicket/models/data/*.json`.
A `pip install --upgrade midwicket` overwrites them. Any user-trained model is silently
replaced with the bundled baseline on every upgrade.

**What needs to change**

1. At model load time, check `~/.midwicket_data/models/<format>_model.json` first. If it
   exists and is newer than the bundled baseline, use it. Fall back to the bundled baseline
   if no user model exists.

2. All retrain output goes to `~/.midwicket_data/models/`, never inside the package directory.

3. Add `md.reset_model(format="T20")` to delete the user model and revert to the bundled
   baseline (useful for debugging).

4. Log at startup which model is active: `"T20 model: user-trained (AUC 0.851, 2026-06-01)"` vs
   `"T20 model: bundled baseline (AUC 0.843)"`.

**Files changed**: `models/win_predictor.py`, `compute/winprob.py`  
**New files**: none  
**Estimated scope**: 4-6 hours

---

---

## Area 4 — Advanced ML and Deep Learning Models

The current win probability model is a logistic regression trained on a flat feature vector per
delivery. It treats every ball as independent — there is no memory of momentum, wicket clustering,
or boundary streaks. Cricket is inherently sequential, and the model architecture should reflect
that. This area replaces and extends the model layer with purpose-built ML and DL architectures.

---

### Why logistic regression is the wrong long-term answer

Logistic regression plateaus at AUC ~0.84 because it cannot model:

- Momentum: three wickets in four balls changes the game; a flat feature vector cannot represent
  that without explicit hand-engineered features for every possible pattern.
- Non-linear interactions: the same RRR at over 5 and over 18 carries completely different
  pressure; logistic regression fits a single linear boundary.
- Sequential context: ball 87 is not independent of ball 86. The bowler's spell history, the
  batter's scoring rate over the last 12 balls, the fielding pressure — all of this is causal
  context that flat features discard.

---

### FT-014 — XGBoost / LightGBM win probability (ML tier 1 upgrade)

**Target release**: 1.3.0  
**Priority**: High  

**Motivation**  
XGBoost and LightGBM are gradient-boosted decision tree ensembles. They handle non-linear
interactions and feature importance natively, require no architecture design, and typically gain
3-5 AUC points over logistic regression on the same feature set. This is the lowest-risk upgrade:
same input format, no new runtime dependency beyond `xgboost` (~15MB), same training pipeline.

**What needs to change**

1. Add `xgboost>=2.0.0` to the `ml` optional extra in `pyproject.toml`.

2. Add an `XGBoostWinPredictor` class in `models/win_predictor.py` that implements the same
   interface as the existing `WinPredictor`. The dispatch logic in `compute/winprob.py` checks
   which model is available and prefers XGBoost over logistic regression.

3. Feature set for XGBoost (richer than current logistic regression):
   - balls remaining, wickets down, runs required, current RRR, required RRR
   - rolling strike rate over last 12 balls, last 24 balls
   - rolling wicket rate over last 6 overs
   - phase (Powerplay / Middle / Death)
   - venue win rate for batting team
   - batter and bowler career SR / economy at this venue

4. Train separately per format (T20 model, ODI model). Test model: `NotImplementedError`.

5. Retain logistic regression as the fallback when `xgboost` is not installed.

**Files changed**: `models/win_predictor.py`, `compute/winprob.py`, `pyproject.toml`  
**New files**: `models/data/t20_xgb_model.json`, `models/data/odi_xgb_model.json`  
**Estimated scope**: 12-18 hours

---

### FT-015 — LSTM win probability model (DL tier 2)

**Target release**: 1.3.0  
**Priority**: Medium  

**Motivation**  
An LSTM processes the innings as a time series — each ball is a timestep and the hidden state
carries forward a learned representation of everything that has happened so far. This captures
momentum (cluster of wickets, boundary runs, dot-ball pressure) without requiring hand-engineered
rolling features. An LSTM trained on all available T20 and ODI data should push AUC above 0.88.

**Architecture**

```
Input per timestep (per ball):
  batter_embedding (16-dim)    — learned from all deliveries
  bowler_embedding (16-dim)    — learned from all deliveries
  venue_embedding  (8-dim)     — learned from all venues
  over             (scalar)
  ball_in_over     (scalar)
  runs_batter      (scalar)
  is_wicket        (binary)
  extras_type      (one-hot, 5 categories)
  innings_runs_so_far (scalar, normalised)
  wickets_fallen   (scalar)
  balls_remaining  (scalar, normalised)

Hidden size: 128
Layers: 2
Dropout: 0.2

Output:
  win_probability  (sigmoid, scalar) at each timestep
```

The embeddings for batters, bowlers, and venues are learned jointly with the LSTM weights. A batter
who has never appeared in the training data gets the mean embedding (cold start).

**What needs to change**

1. Add `torch>=2.0.0` to a new `dl` optional extra in `pyproject.toml`.

2. Implement `LSTMWinPredictor` in `models/win_predictor.py`. Training script in
   `scripts/train_lstm.py`.

3. Inference path: given the current match state as a sequence of past deliveries up to the
   current ball, run a forward pass and return the scalar win probability.

4. The existing `win_probability()` function signature is unchanged. The dispatch order becomes:
   LSTM (if torch available and LSTM artifact exists) → XGBoost (if xgboost available) →
   Logistic regression (always available as baseline).

5. Model artifact: `~/.midwicket_data/models/t20_lstm.pt` (PyTorch checkpoint).

**Files changed**: `models/win_predictor.py`, `compute/winprob.py`, `pyproject.toml`  
**New files**: `scripts/train_lstm.py`, `models/lstm_arch.py`  
**Estimated scope**: 25-35 hours

---

### FT-016 — Transformer win probability model (DL tier 3)

**Target release**: 1.3.0  
**Priority**: Low  

**Motivation**  
A Transformer uses self-attention: when computing the model's representation of ball 87, it can
attend to any previous delivery in the innings — not just the most recent hidden state as in an
LSTM. For ODI cricket (300 balls), this matters: the collapse at over 12 is still causally
relevant at over 42. For T20 (120 balls) the benefit over LSTM is marginal. Primary target for
this architecture is ODI and eventually Test if a Test model is ever attempted.

**Architecture**

```
Positional encoding over ball index
4 Transformer encoder layers, 4 attention heads, d_model=128
Feed-forward dim: 256
Dropout: 0.1
Output head: linear → sigmoid → win probability
```

**What needs to change**

1. Implement `TransformerWinPredictor` in `models/win_predictor.py`.

2. Use the same input embedding scheme as the LSTM (FT-015). Both models share the
   batter/bowler/venue embedding layer so they can be compared fairly.

3. Dispatch: Transformer (if available) → LSTM → XGBoost → logistic regression.

4. Expose `md.set_win_model("transformer" | "lstm" | "xgboost" | "logistic")` for users who
   want to pin a specific model.

**Files changed**: `models/win_predictor.py`, `compute/winprob.py`  
**New files**: `models/transformer_arch.py`, `scripts/train_transformer.py`  
**Estimated scope**: 20-28 hours (lower than LSTM because infrastructure already exists from FT-015)

---

### FT-017 — Player and venue embedding model

**Target release**: 1.3.0  
**Priority**: Medium  

**Motivation**  
The 28 player analytics functions answer historical questions. They cannot answer questions like
"which batters are stylistically similar to Kohli" or "which bowler type does Rohit struggle
against" or "predict how Player X would perform on debut at a venue they have never played".
Learned embeddings answer all of these.

**Architecture**

Neural collaborative filtering trained on all batter-bowler delivery pairs:

```
Input:  batter_id, bowler_id, venue_id, match_type, phase
Embedding: 32-dim per entity (batter, bowler, venue)
Interaction: batter_emb @ bowler_emb + venue_bias + phase_bias
Outputs (multi-task):
  - expected_batter_runs  (regression head)
  - wicket_probability    (binary classification head)
  - boundary_probability  (binary classification head)
```

**What it enables**

- `md.similar_players("V Kohli", n=5)` — nearest neighbours in batter embedding space
- `md.predicted_matchup("V Kohli", "JJ Bumrah", venue="Wankhede Stadium")` — forward-looking,
  not just historical average
- Venue embeddings capture playing surface characteristics (flat pitch, seaming conditions, spin
  track) learned from scoring patterns without any ground condition labels
- Cold-start handling for new players: embed them from their first few innings, improving as
  data accumulates

**Files changed**: `api/player_analytics.py`, `midwicket/__init__.py`  
**New files**: `models/embedding_model.py`, `scripts/train_embeddings.py`  
**Estimated scope**: 20-30 hours

---

### FT-018 — LSTM player form predictor

**Target release**: 1.3.0  
**Priority**: Low  

**Motivation**  
Form is sequential. A player averaging 45 who has scored 5, 8, 3 in their last three innings is
in worse shape for the next match than a player averaging 38 who has scored 42, 55, 61. Rolling
averages and the existing `batting_form()` function treat all historical innings with equal or
linearly-decaying weight and cannot capture acceleration or collapse trajectories.

**Architecture**

```
Input: sequence of last N innings (default N=15)
  Per innings: runs scored, balls faced, dismissal type (one-hot), opposition, venue, phase
LSTM: hidden size 64, 1 layer
Output:
  - predicted_runs (regression)
  - predicted_balls_faced (regression)
  - out_probability (classification)
```

**What it enables**

- `md.predict_next_innings("V Kohli", match_format="T20")` — returns predicted score distribution
- Fantasy team selection: rank players by predicted form, not just career average
- Automatic detection of form collapse before a slump is visible in rolling averages

**Files changed**: `api/player_analytics.py`  
**New files**: `models/form_predictor.py`, `scripts/train_form.py`  
**Estimated scope**: 15-20 hours

---

### FT-019 — Model registry and A/B dispatch

**Target release**: 1.3.0  
**Priority**: Medium  

**Motivation**  
With four model tiers (logistic regression, XGBoost, LSTM, Transformer) plus per-format variants
(T20, ODI), model management becomes complex. Users need to be able to inspect which model is
active, compare two models live on the same input, and pin a specific model for reproducibility.

**What needs to change**

1. `ModelRegistry` class in `models/registry.py`:
   - Stores all available model artifacts with metadata (format, architecture, AUC, trained date,
     training match count)
   - `registry.best_model(format="T20")` returns the highest-AUC available model
   - `registry.list_models()` returns all available models sorted by AUC

2. `md.set_win_model("lstm")` to pin a specific architecture globally for the session.

3. `/model/info` REST endpoint:
   ```json
   {
     "active_models": {
       "T20": { "architecture": "lstm", "auc": 0.891, "trained": "2026-06-01", "matches": 1291 },
       "ODI": { "architecture": "xgboost", "auc": 0.874, "trained": "2026-06-01", "matches": 3134 }
     },
     "available_architectures": ["logistic", "xgboost", "lstm"]
   }
   ```

4. A/B comparison helper: `md.compare_models(input_state, models=["logistic", "xgboost", "lstm"])`
   returns predictions from all three models on the same input.

**Files changed**: `compute/winprob.py`, `midwicket/__init__.py`  
**New files**: `models/registry.py`, `serve/routes/model.py`  
**Estimated scope**: 10-14 hours

---

### FT-020 — `dl` optional dependency and graceful degradation

**Target release**: 1.3.0  
**Priority**: High  

**Motivation**  
`torch` is ~750MB installed. It must be optional. The library must work correctly when only
`scikit-learn` is installed (logistic regression), when `xgboost` is also installed (XGBoost
tier), and when `torch` is also installed (LSTM/Transformer tier). Installing
`pip install midwicket` must never pull in PyTorch silently.

**What needs to change**

1. Add `dl` extra to `pyproject.toml`:

   ```toml
   [project.optional-dependencies]
   ml  = ["scikit-learn>=1.3.0", "xgboost>=2.0.0"]
   dl  = ["midwicket[ml]", "torch>=2.0.0"]
   dev = ["midwicket[serve,viz,ml,dl]", ...]
   ```

2. All DL model classes use `pytest.importorskip`-style lazy import guards:

   ```python
   try:
       import torch
       _TORCH_AVAILABLE = True
   except ImportError:
       _TORCH_AVAILABLE = False
   ```

3. If a DL model is requested but `torch` is not installed, raise `ImportError` with a clear
   message: `"LSTM model requires PyTorch: pip install midwicket[dl]"`.

4. CI matrix: add a `dl` job that installs `midwicket[dl]` and runs DL-specific tests. The
   existing `ml` and core jobs do not install torch and must pass without it.

**Files changed**: `pyproject.toml`, `.github/workflows/ci.yml`  
**New files**: none  
**Estimated scope**: 4-6 hours

---

## Out of scope

The following items are explicitly not part of this roadmap:

- **Test match win probability model** — requires session-level features (pitch wear, day/night,
  follow-on state, batting conditions) that Cricsheet ball-by-ball JSON does not include. Would
  require an external data source.

- **Pitch map / wagon wheel / shot placement data** — Cricsheet does not record pitch coordinates
  or shot direction. `_plot_beehive` and `_plot_wagon_wheel` will remain `NotImplementedError`
  until a data source that includes this information is integrated.

- **Live ball-by-ball ingestion** — real-time data requires a commercial API (ESPN, Sportradar,
  CricAPI). Out of scope for the open-source library until a free or open real-time feed exists.
  Tracked separately in `PRODUCTION_READINESS_GAPS.md` section 2.2.

---

## Summary table

| ID | Feature | Target | Priority | Scope (hours) |
|---|---|---|---|---|
| FT-001 | Competition catalogue + multi-format download | 1.1.0 | High | 8-12 |
| FT-002 | Schema v2: match_type, gender, competition columns + migration | 1.1.0 | High | 6-8 |
| FT-003 | Format-aware phase logic (T20 / ODI / Test) | 1.1.0 | Medium | 2-3 |
| FT-004 | match_type filter on all 28 analytics functions | 1.1.0 | Medium | 6-10 |
| FT-005 | ODI win probability model (logistic regression baseline) | 1.1.0 | Medium | 15-20 |
| FT-006 | Incremental match ingestion (diff-based) | 1.2.0 | High | 6-8 |
| FT-007 | ETag-based change detection | 1.2.0 | High | 4-6 |
| FT-008 | `midwicket sync` CLI command | 1.2.0 | High | 8-10 |
| FT-009 | Built-in background sync scheduler | 1.2.0 | Medium | 8-12 |
| FT-010 | Post-sync retrain trigger | 1.2.0 | High | 4-6 |
| FT-011 | AUC safety gate on retrained model | 1.2.0 | High | 4-6 |
| FT-012 | Retrain audit log and model lineage | 1.2.0 | Medium | 6-8 |
| FT-013 | User-directory model storage | 1.2.0 | High | 4-6 |
| FT-014 | XGBoost / LightGBM win probability (ML tier 1 upgrade) | 1.3.0 | High | 12-18 |
| FT-015 | LSTM win probability model (DL tier 2) | 1.3.0 | Medium | 25-35 |
| FT-016 | Transformer win probability model (DL tier 3, ODI focus) | 1.3.0 | Low | 20-28 |
| FT-017 | Player and venue embedding model (neural collaborative filtering) | 1.3.0 | Medium | 20-30 |
| FT-018 | LSTM player form predictor | 1.3.0 | Low | 15-20 |
| FT-019 | Model registry and A/B dispatch | 1.3.0 | Medium | 10-14 |
| FT-020 | `dl` optional extra + graceful degradation across model tiers | 1.3.0 | High | 4-6 |

**Total estimated scope**: 188-250 hours  
**v1.1.0 scope** (FT-001 to FT-005): 37-53 hours  
**v1.2.0 scope** (FT-006 to FT-013): 44-62 hours  
**v1.3.0 scope** (FT-014 to FT-020): 106-151 hours

---

## Recommended phasing

### v1.1.0 — Multi-format data

- [ ] FT-001 Competition catalogue and multi-format download
- [ ] FT-002 Schema v2 + migration
- [ ] FT-003 Format-aware phase logic
- [ ] FT-004 match_type filter on analytics
- [ ] FT-005 ODI win probability model (logistic regression baseline)

### v1.2.0 — Auto-sync and auto-retrain

- [ ] FT-006 Incremental match ingestion
- [ ] FT-007 ETag-based change detection
- [ ] FT-008 midwicket sync CLI
- [ ] FT-013 User-directory model storage
- [ ] FT-010 Post-sync retrain trigger
- [ ] FT-011 AUC safety gate
- [ ] FT-009 Built-in background scheduler
- [ ] FT-012 Retrain audit log

### v1.3.0 — Advanced ML and DL models

- [ ] FT-020 `dl` optional extra + graceful degradation
- [ ] FT-014 XGBoost / LightGBM win probability (no DL dependency, quick AUC gain)
- [ ] FT-019 Model registry and A/B dispatch
- [ ] FT-017 Player and venue embedding model
- [ ] FT-015 LSTM win probability model
- [ ] FT-018 LSTM player form predictor
- [ ] FT-016 Transformer win probability model
