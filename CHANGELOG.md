# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
