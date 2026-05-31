# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.0] - 2026-05-31

Developer experience and open-source adoption release. No breaking changes.
Feature-complete for the v1.1 public API surface.

### Added

#### Dataset Hub
- `midwicket.datasets.load_dataset(name, version, cache_dir, force)` — single-call
  loader for 12 Cricsheet competitions. Downloads, extracts, validates, and boots a
  `MidwicketSession` automatically. Competitions: `ipl`, `t20s`, `bbl`, `psl`, `cpl`,
  `wbbl`, `sa20`, `mlc`, `odis`, `tests`, `all_t20`, `all`.
- `force=True` parameter forces a clean rebuild of the local dataset cache.
- `list_datasets()` — returns the full registry of available competitions with
  estimated match counts, format, and source URLs.

#### Feature Store
- `build_pressure_index(session, start_date, end_date)` — per-delivery situational
  leverage score. Formula: `min(10, (wickets_lost × 0.7 + over_fraction × 0.2) /
  (wickets_remaining + 0.1))`.
- `build_bowler_quality_rating(session, start_date, end_date)` — BQR combining dot
  ball % and wicket rate. Formula: `(dot% × 60) + (wicket% × 400)`, capped at 100.
- `build_batter_intent_score(session, start_date, end_date)` — aggressive intent
  metric combining boundary rate and dot ball avoidance.
- `build_match_context_score(session)` — chase pressure index per delivery. 2nd
  innings: `(runs_needed / (balls_remaining + 0.1)) × 6 × (wickets_remaining / 10)`,
  clipped to [0, 15].
- `build_venue_bias_rating(session, start_date, end_date)` — VBR: venue first-innings
  run rate divided by global average. Venues with < 5 matches default to 1.0
  (stabilised for sparse sample sizes).
- `build_expected_runs(session)` and `build_expected_wickets(session)` — baseline
  expected outcome models using batter and bowler historical baselines.
- `build_batting_form(session, start_date, end_date)` — rolling recent-form metric
  using exponential decay weighting.
- `build_death_over_metrics(session, start_date, end_date)` — phase-specific death
  over (overs 15+) economy and wicket rate.
- `build_venue_adjusted_form(session, start_date, end_date)` — batter performance
  adjusted for VBR of the venues where they batted.
- All features support temporal scoping via `start_date` / `end_date` parameters.
  Temporal filter correctness verified — 0 leaked rows across 4 test cutoffs.

#### Scouting Reports
- `midwicket.scouting_report(player_name)` — compiles phase splits, venue performance,
  career batting/bowling, weakness detection, and recent form into a single dict.
  Resolves name aliases automatically (`"V Kohli"` = `"Virat Kohli"` = `"kohli"`).
- Case-insensitive player name resolution against both `batter` and `bowler` columns.

#### Showcase Portfolio
- 10 production-grade analytical showcases in `docs/showcases/`, each with:
  - Two publication-quality matplotlib charts (PNG, 150 DPI)
  - SQL query used to produce the result
  - Key numerical finding
  - Markdown walkthrough
- `README_SHOWCASES.md` — gallery landing page with all 20 charts embedded.
- Showcases cover: all-time run leaders, Kohli 19-season profile, Bumrah death-over
  dominance, venue scoring atlas (76 grounds), phase-wise economy heatmap, powerplay
  kings, chase specialists, wicket cluster probability, dot ball pressure, and
  18-season scoring trends.

#### Developer Documentation
- `README.md` — complete overhaul. Hero section leads with data scale and real
  insights. Architecture moved to bottom. Quick start works in 30 seconds.
- `docs/getting_started.md` — 5-minute tutorial from install to scouting report.
  Every code block is copy-paste ready with actual output shown.
- `docs/gallery.md` — 10-showcase gallery with charts, queries, key findings, and
  links to walkthroughs.
- `docs/onboarding_audit.md` — four-persona first-time user audit. 2 critical, 9
  major, 6 minor issues identified with recommended fixes ranked by adoption impact.

### Fixed

#### Data Reliability (carried forward from v1.0.0 audit)
- **Retirement classification** — `RETIRED_HURT` and `RETIRED_NOT_OUT` deliveries
  correctly marked `is_wicket=False`. `RETIRED_OUT` correctly marked `is_wicket=True`
  (valid dismissal under MCC Laws). Verified: 0 innings with > 10 wickets across
  1,239 matches.
- **int16 overflow prevention** — `over` field upcasted to `int16` in `BALL_EVENT_SCHEMA`.
  `runs_batter` and `runs_extras` upcasted to `int32` (prevents `SUM()` overflow on
  large aggregations). Comment added explaining rationale.
- **Temporal leakage** — date filters in all feature builders verified leak-proof.
  SQL-level test: `WHERE date <= X` returns 0 rows with `date > X` across all tested
  cutoffs.
- **VBR stabilisation** — venues with fewer than 5 matches forced to `VBR = 1.0`.
  Prevents single-match outliers from producing misleading bias ratings.
- **Denormalised name columns** — `batter`, `bowler`, `venue` string columns added to
  `BALL_EVENT_SCHEMA` and populated during canonicalisation. Enables SQL `WHERE batter
  = 'V Kohli'` without registry joins.

#### Ingestion
- Fresh corpus rebuild: 1,239 IPL matches canonicalised with **100% success rate**,
  0 failures, 294,757 deliveries loaded. (Previous production DB: 31 matches, stale
  schema — not addressed in this release; see onboarding audit item F-01.)

### Changed

- `README.md` structure: hero → insights → quick start → datasets → features →
  scouting → gallery → architecture (previously: problem → solution → architecture →
  quick start).
- `BALL_EVENT_SCHEMA` column order updated: denormalised name columns (`batter`,
  `bowler`, `venue`) now follow their corresponding ID columns for readability.

### Deprecated

Nothing deprecated in this release.

### Known Issues

- Production database (`data/midwicket.duckdb`) is stale — 31 matches, pre-v1.0
  schema. Needs full rebuild via `load_dataset("ipl", force=True)`. Tracked in
  onboarding audit (item F-01).
- `session.schema()` convenience accessor not yet implemented (onboarding audit M-04).
- `list_features()` discovery function not yet implemented (onboarding audit M-05).
- `session.info()` provenance method not yet implemented (onboarding audit R-01).
- Dataset table missing `date_range` and `gender` columns (onboarding audit C-03).

---

## [1.0.0] - 2026-05-30

First stable release. All 16 documented defects resolved, 0 mypy errors, 627
tests passing, and the public API surface cleaned up.

### Breaking changes

- `plot_beehive` and `plot_wagon_wheel` removed from the public API (`__all__`
  and renamed to `_plot_beehive` / `_plot_wagon_wheel`). Both functions require
  pitch-map / shot-direction data absent from the Cricsheet JSON schema and
  previously raised `NotImplementedError` unconditionally. Users relying on
  the old names should prefix calls with `_` — behaviour is unchanged.

### Fixed

- **Runtime crash in `canonicalize_match`** — `logger` was referenced before
  being defined when an unrecognised wicket kind was encountered (e.g.
  `"hit wicket"`, `"retired not out"`). Any such delivery would raise
  `NameError` and abort the entire ingestion run. Fixed by adding the missing
  `import logging` / `logger = logging.getLogger(__name__)` at module level.

- **`plot_run_pressure` crash on two-inning matches (pandas 2.x)** —
  `df.groupby('inning')['is_dot'].expanding().mean()` returned a MultiIndex
  Series that could not be assigned back to the DataFrame's RangeIndex, raising
  `TypeError` for every completed match. Fixed by switching to
  `transform(lambda x: x.expanding().mean())`.

### Added

- 34 new tests covering 8 previously-untested public functions:
  `plot_match_worm`, `plot_run_pressure`, `plot_batter_pacing`,
  `plot_momentum_swings`, `plot_manhattan`, `plot_partnership_flow`,
  `build_registry_stats` (including `matchup_stats` assertion), `serve_overlay`.

### Changed

- 106 mypy errors across 15 modules resolved; `mypy midwicket/` now reports
  0 errors in 88 source files.

---

## [0.1.2] - 2026-05-30

### Fixed

- **REST win probability endpoint now applies venue adjustments** — `/win_probability`
  was silently discarding the venue parameter before calling `win_probability()`. Added
  `venue: Optional[str]` query param so venue-specific probability adjustments are
  correctly applied via the REST API (MW-005).

- **`plot_run_pressure` stacked axes** — `ax.twinx()` was called on every inning
  iteration, creating overlapping secondary y-axes and doubled tick labels. The twin
  axis is now created once before the loop (MW-006).

- **`MidwicketAPI.run(reload=True)` crash** — uvicorn requires an import string to
  enable reload mode; passing a live `FastAPI` instance raised an opaque `TypeError`.
  `run()` now raises `ValueError` immediately with a clear message and the correct
  `uvicorn ... --reload` invocation (MW-007).

- **Non-T20 `balls_per_innings` now warned** — `win_probability()` silently accepted
  any `balls_per_innings` value and produced wrong results for ODI/Test data. A
  `logger.warning` is now emitted whenever the value differs from the T20 default of
  120 (MW-008).

- **`build_registry_stats` now populates `matchup_stats`** — The pipeline accumulated
  player and venue stats but never wrote head-to-head matchup data. Per-delivery
  accumulation and a `registry.upsert_matchup_stats()` call have been added, so
  `express.get_matchup()` uses the fast pre-built registry path after the first build
  (MW-009).

- **`plot_run_pressure` required-RR format heuristic removed** — The `max_over > 20`
  proxy could misfire for super-overs or truncated matches. `plot_run_pressure` now
  accepts an explicit `balls_per_innings: int = 120` parameter (MW-010).

- **Stale "Stage 2 MVP" comment removed** — The comment in `canonicalize.py` described
  an older incomplete design; the actual code was already correct. Replaced with a
  factual description (MW-011).

- **`MIDWICKET_DEBUG` env var now respected** — Both `api.md` and `debug_mode.md`
  documented `MIDWICKET_DEBUG=true` as a supported env var but it was never read.
  `config.py` now reads the env var at module load time so container operators
  get the expected behaviour (MW-012).

- **Unknown wicket kinds logged** — Unrecognised Cricsheet `kind` values fell through
  silently to `BOWLED`. A `logger.warning` is now emitted for each unknown kind,
  making future schema additions immediately visible in logs (MW-013).

- **`plot_beehive` and `plot_wagon_wheel` no longer produce fictional charts** — Both
  functions were generating random pitch-map and wagon-wheel positions that had no
  relation to real match data. They now raise `NotImplementedError` with a clear
  explanation: the Cricsheet schema does not include pitch-map or shot-direction fields
  (MW-014).

- **CORS startup warning when origins unset** — When `MIDWICKET_CORS_ORIGINS` is not
  configured, `allow_origins=[]` silently rejects all browser cross-origin requests
  with no log message. A `logger.warning` is now emitted at startup (MW-015).

- **Security audit CI job is now gating** — Removed `continue-on-error: true` from
  the security job so a failing `bandit -lll` (HIGH severity) blocks merges. Lint and
  type-check remain advisory until the lint-cleanup PR lands (MW-016).

---

## [0.1.1] - 2026-05-30

### Fixed

- **Win probability model output sync** — README example output updated from the
  stale `34.2%` (pre-retrain baseline) to `22.5%`, which matches the retrained
  model (AUC 0.843) shipped in this release. Eliminates the discrepancy between
  `pip install midwicket` output and documented expected output (UXDX-03).

- **Deploy/infra rename completed** — The remaining two stale `PyPitch`/`PYPITCH_*`
  references in `monitoring/prometheus.yml` are now `Midwicket`/`MIDWICKET_*`,
  making the configuration layer fully consistent with the package namespace
  the application actually reads (UXDX-01, UXDX-02, UXDX-16).

- **Version string sync** — `pyproject.toml` version bumped from `0.1.0` to `0.1.1`
  to match `midwicket.__version__`, which was already at `0.1.1`. Resolves the
  split-identity state where three different install paths (`pip install`,
  `pip install git+https://…`, editable checkout) all reported `0.1.0` but
  produced different behaviour (UXDX-06).

- **Auth guard frozen-constant fix** — `API_KEY_REQUIRED` was a module-level
  constant read once at import time; replaced with `is_api_key_required()` which
  reads `MIDWICKET_API_KEY_REQUIRED` at call time, enabling runtime toggle without
  a process restart (MW-032).

- **Dead code removed from worm.py** — Three unreachable blocks totalling ~154
  lines deleted: the superseded `plot_worm_graph` function (not exported,
  shadowed by `plot_match_worm`), an orphaned Manhattan body after an early
  `return ax` in `plot_momentum_swings`, and a dead stump-annotation block
  after an early `return ax` in `plot_beehive` (MW-034).

- **Frankenschema fixture corrected** — `test_storage_and_monitoring.py`
  read-pool test was inserting into a `ball_events` table using the legacy
  `runs_total`/`wickets_fallen`/`target`/`venue`/`timestamp` schema. Replaced
  with a neutral `probe` table that tests the read-only enforcement without
  coupling to ball_events column layout (MW-004).

- **Request audit log offloaded** — Synchronous DuckDB writes in the audit
  middleware are now deferred to Starlette `BackgroundTask` so they do not
  block the async request/response cycle (MW-006).

- **SQL injection guard extended** — `REPLACE` added to `_FORBIDDEN_TOKENS` in
  the analyze endpoint's SQL sanitizer (MW-028).

- **Connection pool maxsize** — `ConnectionPool` now passes `maxsize` to the
  underlying queue on construction, preventing the pool from exceeding its
  configured limit under concurrent load (MW-017).

### Changed

- `midwicket/docs/api.md` version references updated from `v0.1.0` to `v0.1.1`.

---

## [0.1.0] - 2026-05-28

Initial public release on PyPI.

- Express API (`midwicket.express`): `predict_win`, `get_player_stats`, `get_matchup`
- FastAPI serving layer with Prometheus metrics, CORS, rate limiting, and auth
- DuckDB-backed query engine and in-memory cache
- Win probability logistic regression model
- Cricsheet IPL data loader
- Docker + docker-compose deployment stack
- Grafana dashboard and Prometheus scrape config
