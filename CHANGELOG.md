# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
