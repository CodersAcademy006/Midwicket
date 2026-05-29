# Midwicket — Known Bugs & Defects

> **Scope:** Full line-by-line review of `midwicket/` (~15k LOC) + tests (~9.4k LOC), packaging, CI, and repo hygiene.
> **Date:** 2026-05-28
> **How to read this:** Each entry has a stable ID, severity, exact location, the symptom, why it matters, and a suggested fix. Reference bugs by ID in commits/PRs (e.g. `fix(MW-001): ...`).
> **Important:** Several findings note that the **test suite passes anyway**. Green CI does **not** mean these are fixed — see [MW-004](#mw-004).

## Remediation status

> **Last verified:** 2026-05-29, against the working tree (not just commit messages). Every bug's detail section carries a **Status:** line; the triage tables below carry a **Status** column. Update this dashboard whenever a fix lands.
>
> **Note:** A code-level re-verification downgraded four entries that earlier commit messages had claimed fixed — **MW-006** and **MW-032** (the offending code is still in the tree) and **MW-004** and **MW-035** (only partially addressed). "RESOLVED" here means verified in code, not merely claimed in a commit.

| Status | Count | Meaning |
|--------|-------|---------|
| **RESOLVED** | 17 | Fixed and verified in code — a regression test exists or the defect is structurally gone. |
| **PARTIAL** | 6 | The concrete, low-risk part is fixed; a deeper or riskier remainder is tracked in the entry. |
| **OPEN** | 26 | Not yet addressed. |

- **Resolved (18):** MW-001, 002, 003, 005, 007, 009, 010, 012, 014, 019, 026, 029, 030, 031, 035, 036, 046, 047
- **Partial (5):** MW-004 (Frankenschema fixtures still in `test_win_model_training`, `test_player_analytics`, `test_storage_and_monitoring`), MW-016 (cache lock added; coherence + TTL unverified), MW-018 (engine honors config; runtime wire-in/removal deferred), MW-032 (`get_secret_key()` added but the `SECRET_KEY=""` module alias remains at config.py:141), MW-041 (`balls_per_innings` plumbed; T20 par constants still hardcoded)
- **Open (26):** MW-006 (audit write still synchronous inside the async middleware), 008, 011, 013, 015, 017, 020, 021, 022, 023, 024, 025, 027, 028, 033, 034, 037, 038, 039, 040, 042, 043, 044, 045, 048, 049

## Severity legend

| Level | Meaning |
|-------|---------|
| **P0** | Product-breaking. Core feature does not work on real data, or guaranteed crash / data corruption. |
| **P1** | High. Security weakness, silent wrong results, or serious concurrency/perf ceiling. |
| **P2** | Medium. Correctness bug under specific conditions, or significant tech-debt/dead code. |
| **P3** | Low. Hygiene, cleanup, cosmetic, latent landmines. |

---

## Triage summary

| ID | Severity | Area | Status | One-line |
|----|----------|------|--------|----------|
| [MW-001](#mw-001) | P0 | Data model | RESOLVED | `ball_events` has 4 incompatible schemas; analytics can't read ingest output |
| [MW-002](#mw-002) | P0 | API | RESOLVED | `/v1/players/resolve`, `/v1/venues/resolve`, `/v1/matchup` crash 500 every call |
| [MW-003](#mw-003) | P0 | Data integrity | RESOLVED | `GET /matches/{id}` double-counts rows on every hit (non-idempotent append) |
| [MW-004](#mw-004) | P0 | Tests | PARTIAL | Test suite validates a fictional Frankenschema; masks MW-001 |
| [MW-005](#mw-005) | P0 | Analytics | RESOLVED | `api/fantasy.py` + all 28 `player_analytics` features return zeros on v1 data |
| [MW-006](#mw-006) | P1 | Perf | OPEN | Audit middleware does sync DB write in async path — blocks event loop per request |
| [MW-007](#mw-007) | P1 | Security | RESOLVED | `read_only=True` is not enforced in the engine actually used |
| [MW-008](#mw-008) | P1 | Concurrency | OPEN | `RedisRateLimiter` has TOCTOU race; silently falls back per-process |
| [MW-009](#mw-009) | P1 | Perf | RESOLVED | Redis singleflight in executor makes multi-worker slower (cross-worker blind) |
| [MW-010](#mw-010) | P1 | Perf | RESOLVED | 100k-row memory cap is dead code; full result already materialized |
| [MW-011](#mw-011) | P1 | Correctness | OPEN | Strike rate counts wides as balls faced (knowingly wrong) |
| [MW-012](#mw-012) | P1 | Correctness | RESOLVED | Phase casing mismatch → wrong impact scores / silent empty filters |
| [MW-013](#mw-013) | P1 | Correctness | OPEN | Runs scored off no-balls are dropped in canonicalization |
| [MW-014](#mw-014) | P1 | Correctness | RESOLVED | Run-outs counted as bowler wickets |
| [MW-015](#mw-015) | P1 | ML | OPEN | Training selects hyperparams on the test set (leakage); reported metrics inflated |
| [MW-016](#mw-016) | P1 | Concurrency | PARTIAL | File-based `DuckDBCache` breaks under concurrency; unbounded growth |
| [MW-017](#mw-017) | P1 | Perf | OPEN | Live `QueryEngine` pool = 5 conns; registry serializes on one global lock |
| [MW-018](#mw-018) | P2 | Dead code | PARTIAL | `ThreadSafeQueryEngine` (563 LOC) never used in runtime |
| [MW-019](#mw-019) | P2 | Dead code | RESOLVED | `core/migration.py` migrates phantom tables (`deliveries`/`matches`) |
| [MW-020](#mw-020) | P2 | Dead code | OPEN | `api/validation.py` models unused; weaker inline models used instead |
| [MW-021](#mw-021) | P2 | ML | OPEN | Shipped "trained" model is hand-picked constants; ~6 coefs are 0.0 |
| [MW-022](#mw-022) | P2 | ML | OPEN | `_calculate_confidence` is arbitrary multipliers sold as statistical confidence |
| [MW-023](#mw-023) | P2 | ML | OPEN | Train/serve venue dicts diverge (`'dyanmond park'` typo); train match-id misalignment |
| [MW-024](#mw-024) | P2 | Correctness | OPEN | `DerivedStore` only materializes `venue_baselines`; planner optimization mostly dead |
| [MW-025](#mw-025) | P2 | Correctness | OPEN | Two different "venue baseline" formulas; relative SR compares mismatched units |
| [MW-026](#mw-026) | P2 | Correctness | RESOLVED | Planner `FantasyQuery` SQL gives a batter +20 points for getting out |
| [MW-027](#mw-027) | P2 | Correctness | OPEN | Live ingest writes legacy schema but v1 path needs IDs → live data can't land |
| [MW-028](#mw-028) | P2 | Security | OPEN | `sql_guard` blocks legitimate `replace()`; cardinality plan-guard likely a no-op |
| [MW-029](#mw-029) | P2 | Tooling | RESOLVED | `fix_*.py` regex patchers committed; `# type: ignore` spray neuters mypy |
| [MW-030](#mw-030) | P3 | Hygiene | RESOLVED | Scratch scripts, `pypitch/` ghost, committed PDF, dup tests in repo root |
| [MW-031](#mw-031) | P3 | Packaging | RESOLVED | `pyproject.toml` duplicate `"midwicket*"`; `create_dockerfile` ships junk |
| [MW-032](#mw-032) | P3 | Latent | PARTIAL | `config.SECRET_KEY` alias is `""` in prod; `API_KEY_REQUIRED` frozen at import |
| [MW-033](#mw-033) | P3 | Code smell | OPEN | Duplicate debug flags; 3× "try 4 dates" resolution hack; broad `except` everywhere |
| [MW-034](#mw-034) | P3 | Audit | OPEN | `visuals/worm.py` = 947 LOC/38 fns, no plotting import at top; audit for bloat |

---

## P0 — Product-breaking

### MW-001
**Status:** RESOLVED (verified against code 2026-05-29).
**`ball_events` has four mutually incompatible schemas; the analytics layer cannot read what the ingest layer writes.**
- **Producer (real data):** `core/canonicalize.py:147-153` writes the v1 schema — `batter_id`, `bowler_id`, `venue_id` (int), `runs_batter`, `runs_extras`. No name columns, no `runs_total`, no `season`. Enforced by `schema/v1.py:72` (`BALL_EVENT_SCHEMA`).
- **Consumers expect names/legacy:** `api/player_analytics.py:95,167,165` (`WHERE batter = ?`, `WHERE bowler = ?`, `runs_total - runs_extras`); `api/fantasy.py:107,143,234,75` (`batter`, `bowler`, `venue`, `season`).
- **Two more schemas exist:** `storage/thread_safe_engine.py:248` and `storage/engine.py:209` create *legacy* `ball_events` (names + `runs_total`); `core/migration.py:25-46` targets tables `deliveries`/`matches` that exist nowhere else.
- **Impact:** On canonicalized data, analytics/fantasy queries throw → swallowed → return empty/zeros. The flagship features do not work on the data the pipeline produces.
- **Fix:** Choose **one** schema (v1). Rewrite `player_analytics`, `fantasy`, and live-ingest SQL to use `batter_id`/`bowler_id`/`venue_id` (join the registry for names). Delete the legacy `CREATE TABLE ball_events` paths. This is the root cause; most P0/P1s below are symptoms.

### MW-002
**Status:** RESOLVED (verified against code 2026-05-29).
**Three public endpoints crash with HTTP 500 on every call.**
- **Location:** `serve/api.py:1305` (`/v1/players/resolve`), `:1330` (`/v1/venues/resolve`), `:1438` (`/v1/matchup`). Each builds a plain `_Req` object with `.name`/`.batter` attributes, then calls `lookup_player`/`lookup_venue`/`get_matchup_stats`, which read `request.root.name` / `request.root.batter` (`api.py:373,442`). `_Req` (and the inline Pydantic models) have no `.root`.
- **Root cause:** `fix_mypy.py` blindly ran `request.name` → `request.root.name`, assuming `RootModel`. The `except` handlers *also* use `request.root.*` (e.g. `api.py:388`), so they re-raise → unhandled 500. `# type: ignore` hides it from mypy.
- **Impact:** Guaranteed crash; the SDK client (`client.py:228,235,254`) faithfully wraps these → always errors.
- **Fix:** Replace `request.root.X` with `request.X` in `lookup_player`, `lookup_venue`, `get_matchup_stats` (and their `except` blocks). Add a real integration test that calls these routes.

### MW-003
**Status:** RESOLVED (verified against code 2026-05-29).
**`GET /matches/{match_id}` corrupts data by double-counting on every hit.**
- **Location:** route `serve/api.py:794` → `session.load_match()` → `engine.ingest_events(..., append=True)` (`api/session.py:94`). `append=True` does `INSERT INTO ball_events SELECT * FROM arrow_view` (`storage/engine.py:70`) with **no dedup/idempotency**.
- **Impact:** A cacheable, client-retried GET mutates state. Hitting the endpoint N times multiplies that match's rows by N. All subsequent stats for the match are permanently wrong until rebuild.
- **Fix:** Make `load_match` idempotent (delete-then-insert by `match_id`, or skip if already loaded). A read endpoint must never append.

### MW-004
**Status:** PARTIALLY RESOLVED — the crash-causing schema mismatch is gone, but Frankenschema fixtures (legacy `runs_total` + v1 `runs_batter` in one synthetic table) still exist in `test_win_model_training.py`, `test_player_analytics.py`, and `test_storage_and_monitoring.py`; only a handful of tests exercise the real `canonicalize_match` path (verified 2026-05-29).
**The test suite passes by validating a schema production never creates.**
- **Location:** `tests/test_player_analytics.py:33-41` builds a synthetic `ball_events` containing *every* schema's columns at once — `runs_total` (legacy) **and** `runs_batter` (v1) **and** `batter VARCHAR` (name). 14 test files reference legacy `batter`/`runs_total`; 11 reference v1 `batter_id`.
- **Impact:** `WHERE batter = ?` works in tests but not in production. Green CI gives false confidence and is the reason MW-001/MW-005 shipped undetected.
- **Fix:** Build test fixtures by running `canonicalize_match` (the real ingest path). Delete Frankenschema fixtures. Expect (and then fix) failures.

### MW-005
**Status:** RESOLVED (verified against code 2026-05-29).
**Fantasy + all 28 player-analytics features return zeros/empty on real data.**
- **Location:** `api/fantasy.py` (`fantasy_score`, `cheat_sheet`, `venue_bias`) and `api/player_analytics.py` (PA-01..PA-28). All query name/legacy columns (see MW-001). Errors are swallowed by broad `except` (`fantasy.py:185`, etc.).
- **Impact:** `/v1/players/{name}/batting|bowling|fantasy`, `/v1/venues/{venue}/fantasy`, leaderboards, etc. silently return empty. `venue_bias` always returns neutral 50/50 "INSUFFICIENT DATA".
- **Fix:** Resolved by MW-001. Until then, do **not** advertise these features as working.

---

## P1 — High

### MW-006
**Status:** OPEN — despite being claimed fixed, `audit_api_key_usage` (serve/api.py:207) is an `async def` middleware that still calls `self.session.engine.execute_sql(..., read_only=False)` synchronously (api.py:222) with no threadpool/background offload, blocking the event loop on every sensitive request (verified 2026-05-29).
**Audit middleware blocks the event loop with a synchronous DB write on every sensitive request.**
- **Location:** `serve/api.py:205-231` — an `async def` middleware calls `self.session.engine.execute_sql(... read_only=False)` (a sync DuckDB write) for every successful request to `/v1/players`, `/v1/teams`, `/matches`, `/v1/venues`.
- **Impact:** Serializes all traffic behind a write lock; violates the project's own `Agents.md` rule #2. Introduced by `add_middleware_and_export.py`.
- **Fix:** Move audit writes to a background task/queue, or run in a threadpool executor, or drop the middleware (the `/analyze` audit already exists at `api.py:1054`).

### MW-007
**Status:** RESOLVED — `QueryEngine.execute_sql` now refuses write/DDL statements when `read_only=True` (engine-level defense in depth); regression test in `test_storage_and_monitoring.py` (verified 2026-05-29).
**`read_only=True` provides no write protection in the engine actually used.**
- **Location:** `MidwicketSession` uses `QueryEngine` (`api/session.py:37`). In `storage/engine.py:149-177`, `read_only` only decides whether results are chunked — both branches use the same read-write pooled connection. The read-only-transaction enforcement lives in `ThreadSafeQueryEngine`, which is never used (see MW-018).
- **Impact:** `/analyze`'s "defense in depth" is one layer (`sql_guard`) only. Any guard bypass = writes.
- **Fix:** Either wire `ThreadSafeQueryEngine` in, or enforce `BEGIN TRANSACTION READ ONLY` in `QueryEngine` for read paths.

### MW-008
**Status:** OPEN (verified against code 2026-05-29).
**`RedisRateLimiter.is_allowed` has a TOCTOU race and silently degrades.**
- **Location:** `serve/rate_limit.py:131-148` — counts in one pipeline, then *separately* `zadd`s. Concurrent workers all read "under limit" and all add → limit bypassed. `__init__` (`:120-126`) falls back to per-process `fakeredis` if Redis is down.
- **Impact:** The multi-worker limiter it exists to provide is not atomic, and silently becomes per-process.
- **Fix:** Single atomic Lua script (zremrangebyscore + zcard + conditional zadd). Fail loudly (or to a clearly-degraded mode) when Redis is unavailable, don't silently swap to fakeredis.

### MW-009
**Status:** RESOLVED (verified against code 2026-05-29).
**Redis singleflight in the executor makes multi-worker slower, not faster.**
- **Location:** `runtime/executor.py:74-92,172-181` — the leader stores results in its **local** cache, but cross-worker waiters poll *their own* local cache, which the leader never populates → 30s poll-then-timeout. Also reads `REDIS_URL` while `rate_limit.py` reads `MIDWICKET_REDIS_URL`.
- **Impact:** In the exact scenario Redis is for, latency increases and waiters error out.
- **Fix:** Use a shared cache (Redis) for results too, or remove the distributed path and keep the in-process `threading.Event` singleflight. Unify the env var name.

### MW-010
**Status:** RESOLVED — reads now stream via `to_arrow_reader` and truncate at `MAX_RESULT_ROWS` instead of materializing the full result with `.arrow()`; regression test in `test_storage_and_monitoring.py` (verified 2026-05-29).
**The 100k-row memory cap never executes.**
- **Location:** `storage/engine.py:160` and `thread_safe_engine.py:502` guard with `if isinstance(result, pa.RecordBatchReader)`, but `con.execute(...).arrow()` returns a fully-materialized `pa.Table`. The branch is unreachable.
- **Impact:** The OOM it claims to prevent is fully present — the entire result set is in RAM before the "cap".
- **Fix:** Use `.fetch_record_batch()` for streaming, or append `LIMIT` at the SQL level before execution.

### MW-011
**Status:** OPEN (verified against code 2026-05-29).
**Strike rate counts wides as balls faced (knowingly wrong).**
- **Location:** `compute/metrics/batting.py:43` — `balls_faced = len(events)`; the comment at `:33,41` admits wides shouldn't count "for now".
- **Impact:** SR understated whenever wides are present; `relative_strike_rate` compounds it.
- **Fix:** Exclude wides (and treat no-balls per the rules) from the denominator. Requires a legal-ball indicator in the schema (see MW-013).

### MW-012
**Status:** RESOLVED (verified against code 2026-05-29).
**Phase casing mismatch produces wrong impact scores and can silently return empty filters.**
- **Location:** `compute/metrics/batting.py:96-97` matches `"Powerplay"`/`"Death"` (capitalized) while `core/canonicalize.py:8-12` writes capitalized but `storage/engine.py:_infer_phase`, `runtime/planner.py:184`, and `query/defs.py:8` use lowercase.
- **Impact:** Impact score: powerplay/death balls fall through to the "Middle" baseline (1.1 RPB). Phase filters comparing the wrong case return zero rows silently.
- **Fix:** Normalize phase casing in one place (constant/enum) and use it everywhere on write and read.

### MW-013
**Status:** OPEN (verified against code 2026-05-29).
**Runs scored off no-balls are dropped; `RunComponent` flags are computed then discarded.**
- **Location:** `core/canonicalize.py:101` calls `RunComponent.from_no_ball(...)` which hardcodes `batter_runs=0` (`schema/v1.py:48`). Canonicalize only reads `.batter_runs`/`.extras` (`:114-115`); `is_ball_faced`/`bowler_charged` are never persisted (no column exists).
- **Impact:** Batter runs off a no-ball are lost; SR/economy can't distinguish legal vs illegal deliveries.
- **Fix:** Capture `runs.batter` on no-balls; add a `legal_ball` (or `extras_type`) column to v1 and persist it.

### MW-014
**Status:** RESOLVED (verified against code 2026-05-29).
**Run-outs counted as bowler wickets.**
- **Location:** `core/canonicalize.py:117-118` sets `is_wicket=True` for all dismissals; `api/player_analytics.py:163` and `runtime/planner.py:213` count `SUM(is_wicket)` as bowler wickets. (Note: `data/pipeline.py:98` does this correctly for registry stats.)
- **Impact:** Bowling wicket counts inflated by run-outs/obstruction.
- **Fix:** Add a `bowler_wicket` flag (exclude run out / obstructing / retired) and count that for bowling.

### MW-015
**Status:** OPEN (verified against code 2026-05-29).
**Training selects hyperparameters on the test set (leakage); reported metrics are inflated.**
- **Location:** `models/train.py:251-268` — grid-searches alpha/epochs by `log_loss(y_test, ...)`, then reports that same test set's accuracy/AUC at `:295-299`. Also dumps a checkpoint pickle every epoch to CWD `checkpoints/`.
- **Impact:** All quoted metrics are optimistically biased; 150 disk writes per run pollute the filesystem.
- **Fix:** Use a validation split for selection, evaluate once on a held-out test set. Make checkpoint dir configurable and write once.

### MW-016
**Status:** PARTIALLY RESOLVED - an RLock was added, but file-mode cache coherence and CACHE_TTL pass-through are unconfirmed. (verified against code 2026-05-29).
**File-based `DuckDBCache` breaks under concurrency and grows unbounded.**
- **Location:** `runtime/cache_duckdb.py:87` — `_operation_guard` only locks for `:memory:`; file mode opens a fresh connection per call and mixes `read_only=True` (get) with `read_only=False` (set) → DuckDB config-conflict error in-process. Expired rows are filtered on read but never deleted; `CACHE_TTL` is ignored (default 3600 hardcoded at `set`).
- **Impact:** Persistent cache throws under concurrent load; disk grows forever.
- **Fix:** Single shared connection + lock for file mode too; periodic `DELETE WHERE expires_at <= now`; pass `CACHE_TTL` through.

### MW-017
**Status:** OPEN (verified against code 2026-05-29).
**Throughput ceiling: 5-connection pool + global registry lock.**
- **Location:** `storage/engine.py:22` (`max_connections=5`); `storage/registry.py` wraps every read/write in one `threading.Lock` on one connection; `serve/api.py:1359` holds `registry._lock` during `LIKE '%q%'` scans.
- **Impact:** ~5 concurrent queries max; all registry access serializes; FastAPI threadpool (~40) starves.
- **Fix:** Raise/size the pool to the worker count; give the registry a small connection pool or read replica; stop reaching into `registry._lock`/`.con` from the API layer.

---

## P2 — Medium

### MW-018
**Status:** PARTIALLY RESOLVED — the engine's pool now honors configured `threads`/`memory_limit` instead of hardcoding `2`/`1GB`. Wiring `ThreadSafeQueryEngine` into the runtime (or deleting it) is deferred: it is the safer engine but is currently used only by the live ingestor fixture and tests, so a swap/removal is a separate, higher-risk change (verified 2026-05-29).
**`ThreadSafeQueryEngine` (563 LOC) is dead code.** Referenced only in tests; runtime uses `QueryEngine` (`api/session.py:37`). Its read/write separation and read-only-transaction enforcement never run. **Fix:** wire it in (preferred — it's the safer engine) or delete it. Note it also redefines a second class named `ConnectionPool` and hardcodes `PRAGMA threads=2`/`memory_limit='1GB'` ignoring config (`thread_safe_engine.py:114-115`).

### MW-019
**Status:** RESOLVED — `_migrate_1_0_to_1_1` now guards every `ALTER` on real table existence (so it never touches phantom `deliveries`/`matches`), and `migrate_on_connect` logs the actual number of tables migrated instead of a blanket "schema updated" claim (verified 2026-05-29).
**`core/migration.py` migrates phantom tables.** `SCHEMA_VERSIONS` (`:25-46`) and `_migrate_1_0_to_1_1` (`:85-119`) target `deliveries`/`matches` tables that exist nowhere; `ALTER TABLE deliveries` fails silently (`:124-126`) yet logs *"Database schema updated. Existing data is safe."* Runs on every session init (`session.py:43`). **Fix:** delete or rewrite against the real `ball_events`.

### MW-020
**Status:** OPEN (verified against code 2026-05-29).
**`api/validation.py` (139 LOC) is unused.** `serve/api.py:38-65` defines weaker duplicate Pydantic models; `/analyze` takes raw `Dict[str, Any]`. **Fix:** use the validation models on the routes (incl. the name regex and 8KB metadata cap), delete the inline duplicates.

### MW-021
**Status:** OPEN (verified against code 2026-05-29).
**Shipped "trained" model is hand-picked constants.** `models/win_predictor.py:33-50` — defaults; six coefficients are `0.0` (`rr_gap`, `required_boundary_rate`, `runs_per_wicket_remaining`, `wickets_per_over_remaining`, `chase_progress`, `death_overs`) so those features contribute nothing. **Fix:** ship a genuinely trained `win_model_default.json` or stop describing it as a trained logistic regression.

### MW-022
**Status:** OPEN (verified against code 2026-05-29).
**`_calculate_confidence` is arbitrary multipliers presented as statistical confidence.** `win_predictor.py:177-207` multiplies 0.7/1.1/0.8/1.05/0.9 by thresholds; docstring claims "based on … sample size" but there is no sample-size input. **Fix:** compute a real interval, or rename/clearly document it as a heuristic certainty score.

### MW-023
**Status:** OPEN (verified against code 2026-05-29).
**Train/serve skew + match-id misalignment.** `models/train.py:184` venue dict has typo `'dyanmond park'` and uses spaces (`'eden gardens'`) while `win_predictor.py:53-61` uses underscores (`'eden_gardens'`) → features computed with different venue adjustments at train vs serve. `win_predictor.py:316` / `train.py:174` truncate `match_ids[:len(features)]`, which misaligns groups if any row was skipped (`prepare_training_data:81-83` skips on error while the targets loop does not → potential length-mismatch crash at `train.py:205`). **Fix:** share one venue-normalization function; build `match_ids` in lockstep with surviving feature rows.

### MW-024
**Status:** OPEN (verified against code 2026-05-29).
**Planner materialization is mostly aspirational.** `compute/derived/store.py:34-38` only knows how to build `venue_baselines`; every other preferred table (`matchup_stats`, `phase_stats`, `fantasy_points_avg`, `venue_bias`, `chase_history`) raises `ValueError`, so they're never in `derived_versions` and the planner falls back to raw scans. Also `planner.plan()` just calls `create_legacy_plan()` (`planner.py:95`) despite docstrings describing a distinction. **Fix:** implement the builders or remove the dead "materialized_view" routing claims.

### MW-025
**Status:** OPEN (verified against code 2026-05-29).
**Inconsistent venue-baseline math.** `derived/store.py:46` defines `venue_avg_sr = SUM(runs_batter+runs_extras)/COUNT(*)*100` while `:57` defines `avg_runs_per_over = AVG(runs_batter+runs_extras)*6`. `relative_strike_rate` (`batting.py`) then divides player SR (batter runs only) by a venue baseline that includes extras — mismatched units. **Fix:** one definition; compare like with like (batter runs vs batter runs).

### MW-026
**Status:** RESOLVED (verified against code 2026-05-29).
**Planner fantasy SQL credits a batter +20 for getting out.** `runtime/planner.py:231` — `SUM(CASE WHEN is_wicket THEN 20 ELSE 0 END) + SUM(runs_batter) AS avg_points`, grouped by `batter_id`. `is_wicket` on a batter's delivery means the batter was dismissed. Also it's a SUM mislabeled `avg_points`. (Duplicate, divergent logic vs `api/fantasy.py`.) **Fix:** correct the scoring sign/semantics and de-duplicate against `fantasy.py`.

### MW-027
**Status:** OPEN (verified against code 2026-05-29).
**Live ingestion can't land in a v1 table.** `live/ingestor.py` deliveries carry `batter`/`bowler` names (`LiveDeliverySchema:35-36`), but `storage/engine.py:368` v1 insert path requires `batter_id`/`bowler_id` → `DataIngestionError`. Bounds also disagree across layers (`ingestor` `ball le=10` vs `validation.py` `ball le=6` vs `api.py` model). **Fix:** resolve names→IDs in the ingestor before insert (via registry); unify delivery bounds.

### MW-028
**Status:** OPEN (verified against code 2026-05-29).
**`sql_guard` over-blocks and a plan-guard may be a no-op.** `serve/sql_guard.py:19-43` lists `REPLACE` as forbidden, which also rejects the legitimate `replace()` scalar function. `check_query_plan` (`:370`) tests `node.get("estimated_cardinality", ...)`; verify that key actually exists in DuckDB's `EXPLAIN (FORMAT JSON)` output — if the field name differs, the cardinality guard silently never fires. **Fix:** scope `REPLACE` to statement-leading DDL; confirm the plan JSON field name (and add a test that a known plan-bomb is rejected).

### MW-029
**Status:** RESOLVED (verified against code 2026-05-29).
**Codebase is being maintained by blind regex patchers.** Committed/working-tree scripts: `fix_mypy.py` (sprays `# type: ignore`, caused MW-002), `fix_mypy_ignores.py`, `fix_rest.py` (comments describe deleting failing tests), `add_middleware_and_export.py` (caused MW-006), `update_audit.py`, `update_audit_endpoint.py`, `callback.py`, `fix_train_register.py`. **Fix:** delete these from the repo; ban the workflow. Fix types/code by editing source, not string-replacing it. `warn_unused_ignores = true` is set in `pyproject.toml` — run mypy clean and remove the spray.

---

## P3 — Low / Hygiene / Latent

### MW-030
**Status:** RESOLVED (verified against code 2026-05-29).
**Repo-root junk.** Tracked scratch scripts (see MW-029) + `v1.txt` (24KB) + `virat_report.pdf` (committed despite `.gitignore` `*.pdf` "must never be committed"). `pypitch/` is a ghost: package dirs with **zero `.py` files**, only stale `__pycache__`. Duplicate test files at root (`test_metrics.py`, `test_query_types.py`, …) that CI doesn't run. **Fix:** delete; add a CI check that fails on tracked scratch files.

### MW-031
**Status:** RESOLVED (verified against code 2026-05-29).
**Packaging defects.** `pyproject.toml:94` `include = ["midwicket*", "midwicket*"]` (duplicate; was meant to be `pypitch*`). `serve/api.py:1527` `create_dockerfile()` emits `COPY . .` with a `.dockerignore` that excludes none of the scratch scripts, `.env`, `data/`, or `pypitch/`. **Fix:** correct the include glob; tighten the generated dockerignore (or remove the generator — a top-level `Dockerfile` already exists).

### MW-032
**Status:** PARTIALLY RESOLVED — `get_secret_key()` with production fail-fast was added, but the dangerous module-level alias `SECRET_KEY = os.getenv("MIDWICKET_SECRET_KEY", "")` still lives at config.py:141, so `from config import SECRET_KEY` is `""` in prod; `is_production()` vs `get_secret_key()` env semantics still disagree (verified 2026-05-29).
**Latent config landmines.** `config.py:141` `SECRET_KEY = os.getenv("MIDWICKET_SECRET_KEY", "")` — the comment claims it defers to `get_secret_key()` but it's a plain empty string in prod; any `from config import SECRET_KEY` signs JWTs with `""`. `config.py:145` `API_KEY_REQUIRED` is evaluated at import → can't be toggled post-import (the reason test scripts fought it). `is_production()` (`MIDWICKET_ENV == "production"`) and `get_secret_key()` (`!= "development"`) disagree on what "prod" means. **Fix:** remove the `SECRET_KEY` alias (force callers to `get_secret_key()`); read auth toggles at request time; unify env semantics.

### MW-033
**Status:** OPEN (verified against code 2026-05-29).
**Code smells.** Two debug flags (`config.debug` + `modes.debug_mode`). The “try `[today, 2024-01-01, 2023-01-01, 2022-01-01]`” name-resolution hack appears 3× (`session.py:107`, `express.py:136`, and effectively defeats the registry's own `match_date` requirement). Pervasive broad `except Exception` that swallows real errors and returns empty results with 200/”no data”, masking failures. `exceptions.py` defines a clean hierarchy that's almost never used for control flow. **Fix:** single debug flag; centralize resolution with the real match date; narrow exception handling so genuine failures surface.

### MW-034
**Status:** OPEN (verified against code 2026-05-29).
**Audit `visuals/worm.py`.** 947 LOC, 38 functions, no `matplotlib`/`plotly` import at module top (only `typing`). Suspiciously large for worm/manhattan charts; likely duplicated/padded. **Fix:** review for dead/duplicated code and dependency-on-demand correctness.

---

## Known-good — do not break

These modules are competently written. Preserve their behavior when refactoring:

- **`serve/sql_guard.py`** — NFKC normalization, single-statement enforcement, DuckDB `json_serialize_sql` table resolution, allowlist. (See MW-028 for two small fixes.)
- **`data/loader.py`** — atomic download, zip-slip guard, path-traversal guard, tenacity retries, schema validation.
- **`api/plugins.py`** — allowlist-based plugin loading; rejects traversal/shell metachars; off in prod by default.
- **`serve/auth.py`** — constant-time key compare; bcrypt with pbkdf2 fallback.
- **`config.get_secret_key()` + `serve/api.py:87-102`** — production fail-fast (missing secret/keys refuse boot), docs disabled in prod, CORS no-wildcard, TrustedHost, security headers, SIGTERM drain.
- **`serve/monitoring.py`** — non-blocking CPU sampling, bounded metrics, Prometheus export.
- **`data/pipeline.py`** — *correct* cricket rules for registry stats (wides, byes/leg-byes, run-out attribution).
- **`win_predictor` model loading** — SHA-256 verification, pickle path-mode disabled in prod (`compute/winprob.py:33-52`).

---

## Suggested remediation order

1. **MW-001** — unify on the v1 schema. Nothing else matters until the analytics can read the ingested data.
2. **MW-004** — rebuild test fixtures via `canonicalize_match`; let CI tell the truth.
3. **MW-002** + **MW-006** — fix the 3 crashing endpoints and the event-loop-blocking middleware; then **MW-029** (kill the regex-patcher workflow that caused them).
4. **MW-003** — make `load_match` idempotent.
5. **MW-018 / MW-019 / MW-020 / MW-010** — wire in or delete the dead "safety" systems so the code stops lying about what it does.
6. Cricket correctness: **MW-011, MW-012, MW-013, MW-014**.
7. ML integrity: **MW-015, MW-021, MW-022, MW-023**.
8. Hygiene/latent: **MW-030, MW-031, MW-032, MW-033**.

---

# Deep / Subtle Defects (second pass)

> These survive code review. They don't throw on the happy path — they return *plausible wrong answers*, drift between train and serve, or detonate under concurrency/scale weeks after deploy. This is the "looks fine in the demo, wrong in production" tier.

| ID | Severity | Area | Status | One-line |
|----|----------|------|--------|----------|
| [MW-035](#mw-035) | P0 | Caching | RESOLVED | Cache is keyed on a hardcoded `snapshot_id="latest"`, not the data version → serves stale results after every ingest |
| [MW-036](#mw-036) | P1 | Caching | RESOLVED | `SnapshotManager` is wired to nothing; the snapshot system that should drive cache coherence is decorative |
| [MW-037](#mw-037) | P1 | Identity | OPEN | Registry has no name normalization → one player becomes many entities; stats fragment across spellings |
| [MW-038](#mw-038) | P1 | Stats | OPEN | `not_outs` SQL is mathematically meaningless (`MAX(ball)` where `ball`∈1..6) → batting average wrong even after schema fix |
| [MW-039](#mw-039) | P1 | Concurrency | OPEN | Derived-schema `DROP/CREATE` runs unlocked in the live engine → concurrent readers hit dropped tables |
| [MW-040](#mw-040) | P1 | Stats | OPEN | Bowling economy excludes wides/no-balls → economy understated even on legacy schema |
| [MW-041](#mw-041) | P2 | ML | PARTIAL | Win features are half-generalized: some scale with `balls_per_innings`, others keep T20 constants (6.0, /200, /4) |
| [MW-042](#mw-042) | P2 | ML | OPEN | Trained `venue_adjustment` uses a scaled coefficient against a raw value → silent train/serve skew |
| [MW-043](#mw-043) | P2 | ML | OPEN | `overs_done` unit ambiguity: decimal overs (train) vs over.ball notation (likely user input) |
| [MW-044](#mw-044) | P2 | ML | OPEN | ModelRegistry versions collide at 1-second granularity → silent model overwrite + duplicate version list; singleton unlocked |
| [MW-045](#mw-045) | P2 | Derived | OPEN | Derived builders are orphaned and unfinished (`build_venue_stats` "logic would go here"; `build_phase_stats` counts run-outs as batter outs) |
| [MW-046](#mw-046) | P2 | Stats | RESOLVED | Three divergent phase definitions; analytics ignore the stored `phase` column and recompute from `over` |
| [MW-047](#mw-047) | P3 | Caching | RESOLVED | `MIDWICKET_CACHE_SALT` breaks cross-worker cache sharing if inconsistent; defends a threat that doesn't exist |
| [MW-048](#mw-048) | P3 | Lifecycle | OPEN | SIGTERM handler is registered per `MidwicketAPI` instance → multi-app embedding only drains the last one |
| [MW-049](#mw-049) | P3 | Privacy | OPEN | `/analyze` audit stores raw SQL (with literal filter values) readable by any admin via `/v1/audit` |

---

### MW-035
**Status:** RESOLVED — `snapshot_id` removed from queries and hardcoded calls removed; cache key natively binds to engine's `snapshot_id` and `derived_versions` in `executor.py` (verified 2026-05-29).
**The cache key is `snapshot_id`, and every caller hardcodes `snapshot_id="latest"` — so the cache serves stale results forever after new data loads.**
- **Mechanism:** `query/base.py:41-52` builds `cache_key` from `model_dump(...)`, which includes the `snapshot_id` **field** (`base.py:15`). That field is supplied by the caller — and every call site passes the literal string `"latest"`: `express.py:168`, `api/fantasy.py:212`, `api/head_to_head.py:135`, `api/sim.py:20`, `api/stats.py:40`.
- **The trap:** the engine's *real* data version is `engine._snapshot_id`, which changes on every `ingest_events` (`storage/engine.py:88`, e.g. `"match_123"`). But the cache key never sees it — it sees the constant `"latest"`. So after `session.load_match(...)` changes `ball_events`, the next identical query produces the **same `cache_key`** → `executor.execute` returns the cached pre-load result (`runtime/executor.py:154-166`).
- **Why it's nasty:** `ResultMetadata.snapshot_id` is also set to the caller's `"latest"` (`executor.py:163`), so the stale result is *labelled* current — you cannot detect staleness from the response. Default TTL is 3600s (`cache.set` called without ttl, `executor.py:288`), so wrong answers persist for up to an hour, or until the cache file is cleared.
- **Impact:** "deterministic, snapshot-aware caching" is the headline feature, and it is silently incoherent with the data. Load data → query → answer X. Load more → query → still X.
- **Fix:** Key the cache on the **engine's** `snapshot_id` + `derived_versions`, not the caller's literal. Stop letting callers pass `snapshot_id`; have the executor stamp the real version into the key and the metadata.

### MW-036
**Status:** RESOLVED (verified against code 2026-05-29).
**The snapshot system that should make MW-035 impossible exists but is wired to nothing.**
- **Location:** `storage/snapshots.py` (`SnapshotManager`, `create_snapshot`, `get_latest`). Grep shows **zero** usages outside the file. It writes `snapshots.json` that nothing reads; it has no reference to the engine, the cache, or query keys.
- **Impact:** There's a bookkeeping ledger of snapshots that influences nothing. The infrastructure to fix MW-035 was built and left unplugged.
- **Fix:** Either drive cache keys/invalidation from `SnapshotManager` (and bump it on every ingest), or delete it so it stops implying coherence that doesn't exist.

### MW-037
**Status:** OPEN (verified against code 2026-05-29).
**The identity registry creates a new entity for every spelling of a name — so one player silently becomes several, and stats fragment.**
- **Location:** `storage/registry.py:192-236` (`_resolve_generic`). The lookup/cache key is `f"{prefix}:{name}:{match_date}"` with `name` used **raw** — no case-folding, no punctuation/initials normalization. With `auto_ingest=True` (used by `canonicalize_match` and `data/pipeline.py`), an unseen string mints a brand-new entity + alias.
- **Impact:** "V Kohli", "Virat Kohli", "Kohli, V", "v kohli" → distinct `entity_id`s, each with its own `player_stats` / `matchup_stats`. Career aggregates split across variants and read low. This makes analytics subtly wrong even after the schema (MW-001) is fixed.
- **Secondary:** the in-memory `self._cache` (`registry.py:19,199`) is never invalidated when an alias's validity changes, and grows unbounded (one entry per name×date) — a slow memory leak in a long-running resolver.
- **Fix:** Normalize names before resolution/ingest (case-fold, collapse whitespace, canonical initial form), or back resolution with a curated alias table. Bound/clear the cache.

### MW-038
**Status:** OPEN (verified against code 2026-05-29).
**`not_outs` (and therefore batting average) is computed from a subquery that is mathematically meaningless.**
- **Location:** `api/player_analytics.py:82-89`:
  ```sql
  SUM(CASE WHEN is_wicket THEN 0 ELSE 1 END)
    FILTER (WHERE ball = (SELECT MAX(b2.ball) FROM ball_events b2
                          WHERE b2.match_id=... AND b2.inning=... AND b2.batter=...))
  ```
  `ball` is the ball-*within-over* (1..6), so `MAX(b2.ball)` ≈ 6 for any innings. The FILTER therefore matches "every 6th-ball-of-an-over the batter faced", not "the batter's last delivery". It has nothing to do with whether the batter was not out.
- **Impact:** `not_outs` is garbage; any average that uses dismissals derived this way is wrong. The bug is in the *logic*, so fixing MW-001 won't fix this.
- **Fix:** Determine not-outs from a per-innings dismissal flag (e.g., did a wicket with `player_out = this batter` occur in that match/innings), not from `ball` ordering. To order deliveries you need `(over, ball)` or a global delivery index, not `ball` alone.

### MW-039
**Status:** OPEN (verified against code 2026-05-29).
**Derived-table teardown runs without a lock in the engine you actually use → concurrent reads can hit a dropped schema.**
- **Location:** `storage/engine.py:95-99` (`_invalidate_derived_state`): `DROP SCHEMA IF EXISTS derived CASCADE; CREATE SCHEMA derived; self._derived_versions.clear()`. `_derived_versions` is a plain dict with **no lock** in `QueryEngine` (the only locked variant is the unused `ThreadSafeQueryEngine`; grep confirms `engine.py` has no `_state_lock`).
- **Race:** Thread A calls `ingest_events` (drops/recreates `derived`, clears versions) while Thread B is mid-query against `derived.venue_baselines` or has just checked `derived_versions.get(table) == snapshot_id` in `DerivedStore.ensure_materialized` (`compute/derived/store.py:30`) and is about to read. B reads a table that A just dropped → query error, or builds against a half-cleared version map.
- **Impact:** Intermittent 500s / inconsistent results under concurrent ingest+read. Classic "works in tests, flakes in prod."
- **Fix:** Guard derived lifecycle + `_derived_versions` with a lock (or adopt the thread-safe engine). Rebuild into a new schema and swap atomically rather than drop-then-create.

### MW-040
**Status:** OPEN (verified against code 2026-05-29).
**Bowling economy understates runs conceded — it excludes the wides and no-balls the bowler is charged for.**
- **Location:** `api/player_analytics.py:165,308,509` compute `runs_conceded = SUM(runs_total - runs_extras)` (i.e. batter runs only), then `economy = runs_conceded / overs`.
- **Cricket rule:** a bowler is charged batter runs **plus wides and no-balls** (but not byes/leg-byes). Subtracting *all* extras removes wides/no-balls too, so economy and bowling average are understated. (`data/pipeline.py:91-92` gets this right for registry stats — the analytics layer disagrees with the pipeline.)
- **Impact:** Every bowling economy/average from `player_analytics` is too low. Wrong, not just empty — so even if you only run it on legacy-schema data it lies.
- **Fix:** `runs_conceded = batter_runs + wides + no_balls`. Persist an extras breakdown (wide/no-ball/bye/legbye) in the schema so this is computable.

### MW-041
**Status:** PARTIALLY RESOLVED - balls_per_innings is now plumbed through, but the T20 magic constants (6.0 par run-rate, /200 par total, /4 boundary value) are still hardcoded. (verified against code 2026-05-29).
**Win-probability features are half-generalized beyond T20 — some scale with `balls_per_innings`, others keep hardcoded T20 constants.**
- **Location:** `models/win_features.py`. Generalized: `balls_remaining`, `target_runs_per_ball`, `chase_progress`, `death_overs` (lines 48,65-67). **Not** generalized: `momentum_factor = max(0, run_rate_current - 6.0)` (`:56`), `target_size_factor = min(target/200, 1)` (`:57`), `required_boundary_rate = (runs_remaining/4)/balls_remaining` (`:60`).
- **Impact:** For any non-T20 format (`balls_per_innings != 120`), some features use the right denominator and others use T20 magic numbers → an internally inconsistent feature vector. The "works for any format" generalization produces wrong features for the formats it claims to support.
- **Fix:** Parameterize the constants (par run rate, par total, boundary value) by format, or scope the model to T20 honestly.

### MW-042
**Status:** OPEN (verified against code 2026-05-29).
**Trained models apply a scaled `venue_adjustment` coefficient to an unscaled value.**
- **Location:** `models/win_predictor.py:103-122`. `venue_adjustment` is excluded from the scaled `linear_terms` loop (`:104`) and added separately (`:119-122`) using the **raw** value. But for a trained model, `coefs["venue_adjustment"]` came from `feature_importance` = coefficients learned on **scaled** features (`models/train.py:235,302,341-347`).
- **Impact:** The venue term is off by the feature's scale factor for any trained model — a quiet train/serve skew that only shows up once you actually train (the shipped heuristic model dodges it because it has no scaler).
- **Fix:** Apply the same scaler to `venue_adjustment` at serve time, or exclude it from scaling at train time too. Keep one path.

### MW-043
**Status:** OPEN (verified against code 2026-05-29).
**`overs_done` conflates decimal overs with cricket over.ball notation.**
- **Location:** Training derives `overs_done = over + ball/6.0` → **decimal** overs (`models/train.py:69`). Serving takes `overs_done: float` from the API/SDK (`serve/api.py:913`, `express.predict_win`), and features do `balls_bowled = int(overs_done * 6)` (`win_features.py:47`).
- **The trap:** a caller who passes `10.5` meaning "10 overs, 5 balls" (standard cricket notation) gets `int(10.5*6)=63` balls, but the real ball count is 65. There is no documentation forcing decimal overs, and the natural human input is over.ball.
- **Impact:** Off-by-a-few-balls feature errors at exactly the high-leverage death-over moments; silent train/serve unit mismatch.
- **Fix:** Accept explicit `balls_bowled` (or `overs` + `balls`) at the boundary; document and validate the unit.

### MW-044
**Status:** OPEN (verified against code 2026-05-29).
**Model versions collide at 1-second granularity → silent overwrite; the registry singleton is unlocked.**
- **Location:** `models/registry.py:121-133` — `version = f"{name}_v_{strftime('%Y%m%d_%H%M%S')}"`. Two registrations in the same second produce the **same** version → `joblib.dump` overwrites the first file (`:127`) and the `versions` list gets a duplicate entry (`:133`, append with no dedup). `delete_model` then picks `current_version = max(remaining)` (`:203`) which can point at an overwritten artifact.
- **Also:** `get_model_registry()` (`:215-219`) lazily builds the singleton with no lock; concurrent first-callers double-initialize, and `register_model` mutates `_models` + rewrites `registry.json` with no lock (last-write-wins, lost versions).
- **Impact:** In CI/retraining loops (which *do* register rapidly), models silently clobber each other and the registry index corrupts.
- **Fix:** Add a monotonic counter/uuid to the version; dedup the versions list; guard the singleton and writes with a lock.

### MW-045
**Status:** OPEN (verified against code 2026-05-29).
**The derived-table builders the planner depends on are orphaned and unfinished.**
- **Location:** `compute/derived/phase.py` (`build_phase_stats`) and `compute/derived/venue.py` (`build_venue_stats`) are only re-exported in `__init__.py` — **never called** anywhere. `DerivedStore.ensure_materialized` only knows `venue_baselines` (`store.py:34-38`), so `phase_stats`/`venue_bias`/etc. are never built → the planner's "materialized_view" path can't fire (ties to MW-024).
- **Worse, they're wrong/incomplete:** `build_venue_stats` returns the raw aggregation with a literal comment *"Logic to calculate avg score would go here"* (`venue.py:18`). `build_phase_stats` uses `('is_wicket','sum')` as a batter's `outs` (`phase.py:21`) — but `is_wicket` on a delivery includes run-outs where the *non-striker* was dismissed, so batter "outs" overcounts.
- **Fix:** Either implement + wire these into `DerivedStore` and the ingest path, or delete them and drop the materialization claims from the planner.

### MW-046
**Status:** RESOLVED (verified against code 2026-05-29).
**Three different phase definitions coexist, and the analytics ignore the phase column the ingest layer materialized.**
- **Location:** `core/canonicalize.py:8-12` writes a `phase` column (`Powerplay`/`Middle`/`Death`, boundaries `<6`/`<15`). But `api/player_analytics.py:253-256,301-305` **recompute** phase inline from `over` (`CASE WHEN over<=5 ... <=14 ...`), ignoring the stored column. `storage/engine.py:_infer_phase` is a third definition (lowercase). (See MW-012 for the casing half of this.)
- **Impact:** The materialized `phase` is dead weight; phase logic is duplicated in 3 places that can (and do) drift. Change a boundary in one and the others silently disagree.
- **Fix:** One canonical phase function used at write time; read the stored column everywhere else.

### MW-047
**Status:** RESOLVED (verified against code 2026-05-29).
**`MIDWICKET_CACHE_SALT` can silently destroy cross-worker cache sharing and guards a non-existent threat.**
- **Location:** `query/base.py:50-52` mixes an env salt into the cache key. The cache backend is an internal DuckDB file (`runtime/cache_duckdb.py`), not user-writable — there is no poisoning vector for the salt to defend.
- **Impact:** If the salt is set inconsistently across workers/replicas (or set on one deploy and not the next), the *same* query hashes to *different* keys per worker → 0% cross-worker hit rate and cache thrash, with no error. Cargo-cult security that adds an operational footgun. (Compounds MW-016.)
- **Fix:** Remove the salt, or document that it must be identical across all workers and is only for key-namespacing, not security.

### MW-048
**Status:** OPEN (verified against code 2026-05-29).
**The SIGTERM drain handler is registered per `MidwicketAPI` instance, so multi-app embedding only drains the last one.**
- **Location:** `serve/api.py:154-166` registers `signal.signal(SIGTERM, _handle_sigterm)` inside `__init__`. `create_app()` can be called more than once (tests, ASGI mounts, multi-tenant embedding). Each call overwrites the process-wide handler.
- **Impact:** Only the most-recently-constructed app drains on SIGTERM; earlier apps reject nothing and may drop in-flight work on shutdown.
- **Fix:** Register the signal handler once at process scope, or maintain a registry of apps to drain.

### MW-049
**Status:** OPEN (verified against code 2026-05-29).
**`/analyze` stores the raw user SQL (including literal filter values) in an audit table any admin can read.**
- **Location:** `serve/api.py:1062-1067` inserts `[user_id, sql, ...]` into `audit_log`; `/v1/audit` (`api.py:844`) returns `query_text` to any holder of `MIDWICKET_ADMIN_KEYS`.
- **Impact:** If users embed sensitive values in `WHERE` literals, those persist in plaintext for 30 days (`api.py:292`) and are exposed via the admin endpoint. Minor, but a data-handling surprise for a "read-only analytics" surface.
- **Fix:** Store a parameterized/normalized query shape, or redact literals, or restrict `query_text` visibility.

---

## Deep-pass remediation note

MW-035 + MW-036 are the highest-leverage subtle bugs: they make the **caching layer return confidently-labelled stale data**, which is worse than crashing because nobody notices. Fix them together — drive the cache key from the engine's real snapshot/derived versions and wire (or delete) `SnapshotManager`. MW-037 (identity normalization) is the silent accuracy-killer that will undermine every stat even after the schema work in MW-001.

---
---

# UX / DX / Experience Audit (2026-05-29)

> A separate pass from the MW-### code-defect audit above. This one ignores
> correctness-of-internals and instead asks: *how does this feel to a first-time
> user, a developer wiring it up, and an open-source contributor?* Findings are
> tagged with the experience taxonomy (not the P0–P3 scale) and use `UXDX-NN`
> IDs. Nothing here was fixed — this is a findings log only.

## First-time-user narrative (the 90-second story)

A developer finds the repo: polished README, Colab badge, "Production-Ready",
"powered by AI Agents", "Sub-Millisecond Queries", PyPI badge. They form a mental
model of *a mature, AI-driven, pip-installable analytics SDK with a one-command
Docker stack.* Then:

1. They run `pip install midwicket` (README Step 1). The Colab notebook instead
   uses `pip install git+https://…`. The two official entrypoints disagree, which
   plants the first seed of doubt about whether the PyPI package is real/current.
2. They run the headline `predict_win` example. README comment says `34.2%`; the
   code returns `22.3%`. First hands-on interaction contradicts the docs.
3. The "instant in-memory" prediction takes ~0.65s to import because it drags in
   pyarrow + pandas + the entire SDK surface.
4. They try Step 3 (`get_player_stats`) and silently get a `~/.midwicket_data`
   directory created in their home folder, plus a `None` that crashes on `.name`.
5. They try the advertised `docker-compose up -d`. It fails to build (`COPY
   pypitch/` — a directory that no longer exists). The "Enterprise Deployment"
   never starts.

Each step erodes trust. The product *over-promises in the README and
under-delivers on the first three things a user touches.* That gap — not any
single bug — is the core experience problem.

---

## CRITICAL

### UXDX-01 — [ONBOARDING] Advertised `docker-compose up` cannot build the image
**Severity:** Critical
**Category:** Onboarding / Trust
**Location:** `Dockerfile:24,42`; `docker-compose.yml:4-5`; `.github/workflows/ci.yml:133-134`
**Description:** The README's "Enterprise Deployment" tells users to `docker-compose up -d`. Compose runs `build: .`, and the Dockerfile does `COPY pypitch/ ./pypitch/` and `CMD [… "pypitch.serve.api:create_app" …]`. The `pypitch/` package no longer exists (renamed to `midwicket/`), and CI *actively asserts it stays gone* (`if [ -d pypitch ]; then … fail=1`). The `COPY` step therefore fails the build outright; even if skipped, the container would crash on startup importing a non-existent module.
**Why It Feels Wrong:** The single most prominent "how to deploy" instruction is dead on arrival. A user following the documented path verbatim gets a build error with no hint that the cause is a stale package name.
**Impact:** Anyone evaluating the "Production-Ready" claim via Docker bounces immediately. Likely the #1 repeat GitHub issue.
**Suggested Direction:** Treat the deployment surface as a first-class, tested artifact that must track the package rename; or clearly mark Docker as experimental until it works.

### UXDX-02 — [TRUST-ISSUE] `.env.example` / compose set `PYPITCH_*` vars the code never reads
**Severity:** Critical
**Category:** Trust / Onboarding
**Location:** `.env.example` (all keys); `docker-compose.yml:9-14`; `midwicket/config.py` (reads `MIDWICKET_*`)
**Description:** The documented flow is `cp .env.example .env` then bring up compose. But `.env.example` and `docker-compose.yml` define `PYPITCH_SECRET_KEY`, `PYPITCH_API_KEYS`, `PYPITCH_ENV`, `PYPITCH_DATA_DIR`, `PYPITCH_CORS_ORIGINS`, etc. The application exclusively reads `MIDWICKET_*` (e.g. `MIDWICKET_SECRET_KEY`, `MIDWICKET_ENV`, `MIDWICKET_API_KEYS`). Every configured value is silently ignored: `MIDWICKET_ENV` stays unset → defaults to *development* (despite compose intending production), no API keys are registered, secrets fall back to an ephemeral generated key.
**Why It Feels Wrong:** Configuration that is *silently ineffective* is worse than configuration that errors. The operator believes they've set a production secret and locked down auth; in reality none of it took effect.
**Impact:** Insecure-by-accident deployments; "I set the secret key but it's ignored" confusion; security-relevant because auth ends up misconfigured.
**Suggested Direction:** One canonical env-var namespace, validated at startup with a hard failure on unknown/missing required keys, so misconfiguration is loud, not silent.

---

## HIGH

### UXDX-03 — [PREDICTABILITY] Headline example output (34.2%) doesn't match reality (22.3%)
**Severity:** High
**Category:** Predictability / Trust
**Location:** `README.md:86` (`# Win Probability: 34.2%`)
**Description:** Running the README's `predict_win` example verbatim returns `win_prob = 0.2233` → `22.3%`, not the `34.2%` printed as the expected output. The model coefficients were recently retrained, but the README's hardcoded result was never updated.
**Why It Feels Wrong:** The very first interactive result contradicts the documentation. Users can't tell if they broke something, if the model is non-deterministic, or if the docs lie.
**Impact:** Immediate trust hit on the flagship feature; "why don't I get the README number?" issues/discussions.
**Suggested Direction:** Don't hardcode example outputs that can drift from the model; either compute-and-display without asserting a value, or pin example outputs to a tested snapshot updated whenever the model changes.

### UXDX-04 — [RESOURCE-LIFECYCLE] Constructing `DataLoader()` writes to the user's home directory
**Severity:** High
**Category:** Resource Management / Trust
**Location:** `midwicket/data/loader.py:108` (`self.raw_dir.mkdir(parents=True, exist_ok=True)` in `__init__`)
**Description:** Merely instantiating `DataLoader()` — with no `.download()` call — creates `~/.midwicket_data/raw/ipl/` on disk. The same home-directory creation also fires transitively via `get_player_stats`/`get_matchup` → `_auto_setup_session` → `_ensure_data_dir`, so a read-sounding "get stats" call mutates the filesystem too.
**Why It Feels Wrong:** Object construction (and "get" calls) should be side-effect-free. Users mentally model directory creation as something that happens on an explicit *download/save*, not on `__init__`. This is the classic "importing a library writes to disk" expectation violation.
**Impact:** Surprise folders in `$HOME`; "what created `~/.midwicket_data` and can I delete it?" questions; uncomfortable for users who only wanted an in-memory prediction.
**Suggested Direction:** Defer all filesystem creation to the first explicit persist/download action; keep construction and read paths pure.

### UXDX-05 — [PREDICTABILITY] `get_player_stats` / `get_matchup` return `None` silently, then docs dereference it
**Severity:** High
**Category:** Predictability / UX-Confusion
**Location:** `midwicket/express.py:99-176`; README Step 3 (`stats.name`, `matchup.matches`)
**Description:** Both functions return `None` on the common unhappy paths (no data downloaded yet, fuzzy name miss, resolution failure). The README then immediately accesses `stats.name`/`stats.runs`/`matchup.matches`, which raises `AttributeError: 'NoneType' object has no attribute …`. Note `iter_matches` *does* raise a helpful "Run loader.download() first" — so the error strategy is inconsistent across the same library.
**Why It Feels Wrong:** A `None` return with no signal forces the user to debug a cryptic downstream crash instead of being told "no data" or "player not found." Inconsistent raise-vs-None behavior breaks the mental model.
**Impact:** Confusing first failure; users can't distinguish "wrong name" from "no data" from "bug."
**Suggested Direction:** Make absence explicit and self-explaining (distinct, actionable outcomes for not-found vs no-data), and apply one consistent error contract across the Express surface.

### UXDX-06 — [PREDICTABILITY] Published PyPI `0.1.0` is frozen at an old build while the repo keeps changing under the same version
**Severity:** High
**Category:** Predictability / Onboarding / Trust
**Location:** PyPI `midwicket` 0.1.0; `pyproject.toml:7` (`version = "0.1.0"`); `README.md:66` + `notebooks/quickstart.ipynb` cell 1 (both now `pip install midwicket`)
**Description:** *(Verified against PyPI JSON API + `pip` on 2026-05-29.)* The package **is published** and legitimately owned (project_urls → `github.com/CodersAcademy006/Midwicket`, author `srjnupadhyay@gmail.com`). Only one release exists — **0.1.0, uploaded 2026-05-28 11:31 UTC**. Since that upload the repo has had a full day of material changes on 2026-05-29 (README rewrite, win-probability model **retrained** to AUC 0.843 and committed, win-prob fixes), all **still under version `0.1.0`** — the version was never bumped. So `pip install midwicket` ships the May-28 snapshot, while `pip install git+https://…` / an editable checkout ship today's different code — all three labelled `0.1.0`. (The earlier README-vs-Colab command disagreement noted in this audit was reconciled mid-session; both now point to PyPI, which is what exposes this skew.)
**Why It Feels Wrong:** Users trust that a version number identifies a specific behavior. Here one version string maps to several distinct builds (and distinct model outputs). A `pip` user silently gets stale code/model with no signal that the repo has moved on.
**Impact:** Likely the mechanical cause of UXDX-03: the PyPI 0.1.0 build predates the model retrain, so a `pip install` user may see the README's `34.2%` while anyone on current code sees `22.3%` — same version, different answer. Drives "works in the repo but not after pip install / why is my number different" confusion.
**Suggested Direction:** Bump the version on every behavior-affecting change and re-publish; never let a released version string track moving code. Treat the model artifact as part of the versioned release contract.

### UXDX-07 — [TRUST-ISSUE] "Production-Ready" observability/deploy stack is not actually wired up
**Severity:** High
**Category:** Trust / Documentation
**Location:** `README.md:36,112-126`; `docker-compose.yml:49-53` (Grafana provisioning bind-mounts commented out); `monitoring/grafana/dashboards/pypitch_api.json`
**Description:** README lists "Docker configurations, Prometheus metrics, and Grafana dashboards" as shipped, production-ready features. In compose, Grafana's provisioning and dashboard bind-mounts are commented out ("Uncomment when you create those directories"), so `docker-compose up` yields an empty Grafana with no datasource/dashboards. The dashboard file that *does* exist is named `pypitch_api.json` and targets the old package's metric names.
**Why It Feels Wrong:** Advertised, named features that don't materialize when you run the documented command. The maturity signal is marketing, not reality.
**Impact:** Wasted setup time; reinforces the "over-promise" pattern; erodes trust in every other claim.
**Suggested Direction:** Either ship the observability stack fully provisioned and verified, or downgrade the README language to "example configs / work-in-progress."

---

## MEDIUM

### UXDX-08 — [PERFORMANCE-PERCEPTION] "Instant, in-memory" prediction imports the whole data stack (~0.65s)
**Severity:** Medium
**Category:** Performance Perception / DX-Friction
**Location:** `midwicket/__init__.py:32-89`; measured `import midwicket.express` ≈ 652ms (pandas ~205ms, pyarrow ~128ms)
**Description:** `import midwicket.express` triggers the package `__init__`, which eagerly imports `api.session`, `api.stats`, `api.fantasy`, `api.sim`, 28 player-analytics functions, etc., pulling pandas + pyarrow. The README pitches `predict_win` as a zero-dependency-friction, in-memory call — but it ultimately performs a ~20-coefficient logistic dot product after loading a data-warehouse toolchain.
**Why It Feels Wrong:** "Lightweight / instant" is contradicted by a heavy import graph. A user who only wants win probability pays the full SDK import cost.
**Impact:** Sluggish first import in notebooks/scripts; perception of bloat; slower cold starts in serverless contexts.
**Suggested Direction:** Make the in-memory prediction path importable without the analytics/data stack; lazy-load heavyweight subsystems only when their features are used.

### UXDX-09 — [API-INCONSISTENCY] Express functions disagree on the default data directory
**Severity:** Medium
**Category:** API
**Location:** `midwicket/express.py:88` (`load_competition(… data_dir="./data")`) vs `:43-54` (others default to `~/.midwicket_data`)
**Description:** Within the same Express module, `load_competition` defaults data to `./data` (current working dir), while `get_player_stats`/`get_matchup`/`quick_load` default to `~/.midwicket_data`. `DataLoader` and config also center on `~/.midwicket_data`.
**Why It Feels Wrong:** Same module, same concept ("where my data lives"), two different answers. Users download to one location and query another.
**Impact:** "It downloaded but `get_player_stats` finds nothing" confusion; data scattered across CWDs.
**Suggested Direction:** One consistent default data location across the entire public surface.

### UXDX-10 — [API-SURPRISE] `predict_win(data_dir=…)` silently ignores the argument
**Severity:** Medium
**Category:** API
**Location:** `midwicket/express.py:178,197-199`
**Description:** `predict_win` exposes a `data_dir` parameter but the body never uses it — it calls `win_probability(...)` directly. Passing `data_dir` does nothing.
**Why It Feels Wrong:** A parameter that exists but is inert is a trap; users assume it routes data/model location and silently get default behavior.
**Impact:** Wasted debugging when a custom `data_dir` "doesn't take"; signals copy-paste API design.
**Suggested Direction:** Remove parameters that have no effect, or make them meaningful.

### UXDX-11 — [RESOURCE-LIFECYCLE] No way to see or remove the downloaded dataset; ~50MB zip is never cleaned up
**Severity:** Medium
**Category:** Resource Management
**Location:** `midwicket/data/loader.py` (keeps `ipl_json.zip` + extracted JSON; no purge API)
**Description:** `download()` leaves both the ~50MB `ipl_json.zip` *and* the extracted JSON in `~/.midwicket_data` (≈2× footprint), logs the location via `logging` (invisible by default — users see a tqdm bar but no path), and there is no first-class `clear_data()`/`purge()` (only in-process cache `.clear()` exists). Removal requires manually `rm -rf ~/.midwicket_data`.
**Why It Feels Wrong:** Data is written to a hidden home-dir location the user never explicitly chose, with no surfaced path and no off-ramp. Disk ownership and cleanup are entirely on the user to reverse-engineer.
**Impact:** "Where did my disk space go / how do I uninstall the data?" support requests; orphaned gigabytes over time.
**Suggested Direction:** Surface the storage location prominently, delete the intermediate archive after extraction, and provide an explicit data-management/cleanup command.

### UXDX-12 — [NAMING] "AI Agents / agentic" framing has no AI behind it
**Severity:** Medium
**Category:** Trust / Naming / Mental-Model
**Location:** `README.md:6,33-34` ("Agentic Data SDK", "powered by … AI Agents"); `Agents.md`; code has zero `openai`/`anthropic`/`llm` references
**Description:** The pitch leans on "agent-based architecture" and "AI Agents." `Agents.md` is actually honest — it defines "Agents" as *active system components* (Executor, Planner, Storage Engine, Registry, Compute) — i.e., deterministic classes. But in 2026, "AI Agents / agentic" strongly connotes LLM-driven autonomy, which is absent.
**Why It Feels Wrong:** The headline term sets an expectation (autonomous AI) the system doesn't meet; the internal doc and the marketing disagree on what "agent" means.
**Impact:** Users arrive expecting LLM features, find a query planner; "where are the AI agents?" discussions; credibility cost with technical evaluators.
**Suggested Direction:** Reserve "AI/agentic" for actual model-driven behavior; describe the internal components as an architecture/pipeline metaphor without implying AI.

### UXDX-13 — [DOCUMENTATION-GAP] Architecture doc cites class names/paths that don't exist
**Severity:** Medium
**Category:** Documentation / DX
**Location:** `Agents.md:11,46` vs code: `runtime/executor.py` defines `RuntimeExecutor` (not `Executor`), `storage/engine.py` exports `QueryEngine` (not `StorageEngine`)
**Description:** `Agents.md` lists codepaths like `midwicket.runtime.executor.Executor` and `midwicket.storage.engine.StorageEngine`. The real classes are `RuntimeExecutor` and `QueryEngine`. The README explicitly instructs contributors to "review the internal agent patterns" before submitting code.
**Why It Feels Wrong:** A new contributor follows the doc, greps for the named classes, and finds nothing — the onboarding doc fights the code.
**Impact:** Contributor friction; doc looks aspirational/stale; reduces confidence in all docs.
**Suggested Direction:** Keep the architecture doc's symbol names/paths verified against the code (treat doc-symbol drift as a doc bug).

### UXDX-14 — [WORKFLOW-BREAK] Three competing ways to acquire data; README uses the lowest-level one
**Severity:** Medium
**Category:** Workflow
**Location:** `midwicket/express.py:81-86` (`download_data`, not in `__all__`), `:201-218` (`quick_load` auto-download), README:101 (`DataLoader().download()`)
**Description:** Data can be fetched via `DataLoader().download()`, `px.download_data()`, or `px.quick_load()` (auto-downloads). The README teaches the lowest-level `DataLoader().download()`; the convenience `download_data` isn't even exported; `quick_load` is undocumented in the README.
**Why It Feels Wrong:** Multiple overlapping entrypoints with no guidance on the canonical one; the docs pick the least ergonomic option.
**Impact:** Decision paralysis; users hand-roll loaders the library already wraps; inconsistent code in the wild.
**Suggested Direction:** Designate and document one blessed data-acquisition path; demote or hide the rest.

### UXDX-15 — [DOCUMENTATION-GAP] 41 examples exist but are invisible from the README; numbering collides
**Severity:** Medium
**Category:** Documentation / Onboarding
**Location:** `examples/` (41 files, e.g. duplicate `03_ingest_world.py` and `03_player_lookup.py`)
**Description:** A substantial, numbered examples suite (setup → analysis → plugins → pipelines) is never referenced by the README, so users who don't browse the tree never find it. The numbering also collides (two `03_`).
**Why It Feels Wrong:** The best onboarding asset is hidden; the disorder (dup numbers) signals neglect.
**Impact:** Users reimplement things examples already cover; the README's thin 3-step quickstart feels like the whole story.
**Suggested Direction:** Link and curate the examples from the README; give them a coherent, collision-free index.

### UXDX-16 — [ONBOARDING] Half-finished `pypitch → midwicket` rename across infra/observability
**Severity:** Medium
**Category:** Onboarding / Documentation / Trust
**Location:** `Dockerfile`, `docker-compose.yml` (service `pypitch-api`), `monitoring/prometheus.yml`, `monitoring/grafana/dashboards/pypitch_api.json`, `.env.example` header ("PyPitch environment example"), root `test_v1_features.ipynb`
**Description:** The package was renamed but the deployment/observability/config layer still references `pypitch`. CI is the only part fully migrated (and even guards against `pypitch/` returning). The result is an inconsistent identity depending on which file you open.
**Why It Feels Wrong:** A project that calls itself two names across its own files looks unfinished and untrustworthy, and the mismatches cause the concrete failures in UXDX-01/02/07.
**Impact:** Broken deploy/monitoring; reviewer skepticism; confusion about the canonical project name.
**Suggested Direction:** Complete the rename as a single sweep across infra, monitoring, env templates, and stray notebooks; add a guard so the old name can't silently reappear.

### UXDX-17 — [DX-FRICTION] `pytest` always enforces 70% total coverage, even for one test file
**Severity:** Medium
**Category:** DX
**Location:** `pyproject.toml:71` (`addopts = "--cov=midwicket --cov-report=term-missing --cov-fail-under=70"`)
**Description:** Coverage gating is baked into default `addopts`, so a contributor running a single test (`pytest tests/test_x.py`) measures coverage over the whole package and fails the `--cov-fail-under=70` gate despite their test passing.
**Why It Feels Wrong:** The local dev inner loop inherits a CI-style global gate; "my test passed but pytest exited non-zero" is confusing.
**Impact:** Friction and false failures for contributors; people learn to distrust the exit code.
**Suggested Direction:** Keep the coverage gate in CI configuration, not in the default local test invocation.

### UXDX-18 — [DOCUMENTATION-GAP] `predict_win` docstring claims venue is unused, but venue now drives the model
**Severity:** Medium
**Category:** Documentation
**Location:** `midwicket/compute/winprob.py:77` ("venue: Optional venue (not used in baseline)"); `win_features.py` venue_adjustment + retrained `venue_adjustments`
**Description:** The docstring says venue is ignored ("not used in baseline"), but the model now applies a `venue_adjustment` learned per-venue, so the same scenario yields different probabilities by venue. The doc describes a previous model.
**Why It Feels Wrong:** Users who read the doc will (wrongly) assume venue is cosmetic and won't trust/realize venue materially changes results — or will be surprised when it does.
**Impact:** Misunderstanding of model behavior; under-/over-use of the venue argument.
**Suggested Direction:** Keep model-behavior docs in sync with the active model; state clearly that venue affects the prediction.

---

## LOW

### UXDX-19 — [UX-CONFUSION] `predict_win` returns an unexplained `confidence` field
**Severity:** Low
**Category:** UX / Documentation
**Location:** `midwicket/express.py:178-199` (returns `{'win_prob', 'confidence'}`; example yields `confidence ≈ 0.553`)
**Description:** The result includes a `confidence` value with no documented definition (is it model certainty? a calibration band? data sufficiency?). The README example ignores it entirely.
**Why It Feels Wrong:** A number presented next to the headline metric, with no meaning attached, invites misinterpretation.
**Impact:** Users either ignore a potentially important signal or invent meaning for it.
**Suggested Direction:** Define what confidence represents (and its range/interpretation), or drop it from the simple API.

### UXDX-20 — [TRUST-ISSUE] "Sub-Millisecond Queries" is asserted with no surfaced methodology
**Severity:** Low
**Category:** Trust / Performance Perception
**Location:** `README.md:32`
**Description:** A bold, specific latency claim ("sub-millisecond") is front-and-center with no benchmark, dataset size, hardware, or query type attached. A first-time user has no way to reproduce or contextualize it (and the first thing they *can* time — import — is ~650ms).
**Why It Feels Wrong:** Unqualified hard numbers read as marketing; when the user's first measurable experience is slow, the claim backfires.
**Impact:** Skepticism; "is this real?" reactions from technical readers.
**Suggested Direction:** Back performance claims with a reproducible benchmark and scope, or soften to qualitative language.

### UXDX-21 — [UX-SURPRISE] Library prints to stdout (debug toggle, download messages)
**Severity:** Low
**Category:** UX
**Location:** `midwicket/express.py:41,71,75,85` (`print(...)`)
**Description:** Express uses bare `print()` for debug-mode and download status, while `data/loader.py` uses the `logging` module. So messaging is split: some goes to stdout unconditionally, some is invisible unless logging is configured.
**Why It Feels Wrong:** Libraries writing directly to stdout is a known anti-pattern (pollutes notebooks/pipelines, can't be silenced via logging config); the inconsistency means users can neither reliably see nor reliably suppress messages.
**Impact:** Noisy notebooks; hidden download paths; no single way to control verbosity.
**Suggested Direction:** Route all library output through `logging` with a consistent, configurable verbosity story.

### UXDX-22 — [ONBOARDING] Stray dev notebook at repo root references the dead package name
**Severity:** Low
**Category:** Onboarding / Repo hygiene
**Location:** `test_v1_features.ipynb` (repo root; references `pypitch`)
**Description:** A `test_v1_features.ipynb` sits at the top level alongside README/LICENSE and references the old `pypitch` name. It reads as leftover scratch work in the project's front door.
**Why It Feels Wrong:** Root-level cruft is the first thing a browser sees on GitHub; it lowers the perceived quality bar and re-surfaces the old name.
**Impact:** Worse first impression; confusion about whether it's an official example.
**Suggested Direction:** Move real notebooks under `notebooks/`/`examples/`, remove scratch files.

### UXDX-23 — [RESOURCE-LIFECYCLE] Dev secret key is written inside the data directory
**Severity:** Low
**Category:** Resource Management / Trust
**Location:** `midwicket/config.py:97` (`~/.midwicket_data/.midwicket_dev_secret`)
**Description:** The auto-generated development secret is persisted *inside the dataset directory*. Secrets and bulk data share a lifecycle and location.
**Why It Feels Wrong:** Users reason about the data dir as deletable cache; co-locating a credential there means "clearing data" can rotate the signing key (and vice versa), an unexpected coupling.
**Impact:** Accidental key loss/rotation when clearing data; surprise when a secret turns up in a "data" folder.
**Suggested Direction:** Keep secrets in a config/credentials location distinct from cached data.

### UXDX-24 — [DX-CONFUSION] `MatchupQuery` is imported from two different modules
**Severity:** Low
**Category:** DX / Naming
**Location:** `midwicket/__init__.py:48` (`from .query.matchups import MatchupQuery`) vs `midwicket/express.py:166` (`from midwicket.query.base import MatchupQuery`)
**Description:** The same public symbol is imported from `query.matchups` in one place and `query.base` in another, implying either duplication or a re-export maze.
**Why It Feels Wrong:** Two canonical-looking import paths for one class make it unclear which is authoritative; complicates discovery and refactoring.
**Impact:** Contributor confusion; risk of divergent definitions.
**Suggested Direction:** Establish one home module for the symbol and a single documented import path.

### UXDX-25 — [MENTAL-MODEL] Quickstart never introduces the concepts the data API depends on
**Severity:** Low
**Category:** Documentation / Mental-Model
**Location:** `README.md` Quick Start vs runtime reality (registry/`build_registry_stats`, snapshots, derived tables; `get_matchup` has a fast path that needs registry stats populated)
**Description:** The README jumps from `predict_win` to `get_player_stats`/`get_matchup` without explaining snapshots, the identity registry, or that matchup stats depend on a registry being built — concepts that determine whether calls return data or `None`.
**Why It Feels Wrong:** Users operate the API with no model of *why* a call might return nothing, so failures feel random.
**Impact:** "Sometimes it works, sometimes None" confusion; under-use of the real analytics surface.
**Suggested Direction:** Add a short conceptual on-ramp (data lifecycle: download → snapshot → registry → query) before the data-dependent examples.

---

## Open-source maintainer view — issues you'll see repeatedly

- "`docker-compose up` fails: `COPY pypitch/` no such file" — **UXDX-01** (top repeat issue).
- "I set `PYPITCH_SECRET_KEY` / API keys but they're ignored" — **UXDX-02**.
- "README says 34.2% but I get 22.3% — is it broken?" — **UXDX-03**.
- "`pip install midwicket` fails / installs an old version" / "Colab uses a different command" — **UXDX-06**.
- "`get_player_stats` returns None / crashes with AttributeError" — **UXDX-05**.
- "What created `~/.midwicket_data` and how do I delete it?" — **UXDX-04 / UXDX-11**.
- "Where are the AI agents?" — **UXDX-12**.
- "Grafana is empty after compose up" — **UXDX-07**.
- "Running one test fails on coverage" — **UXDX-17**.

**Through-line:** the README promises a mature, AI-powered, one-command product; the first five things a user touches (install, predict, get-stats, data dir, docker) each under-deliver. Closing the promise-vs-reality gap — especially completing the rename and aligning docs with actual outputs — would remove most of the friction above.
