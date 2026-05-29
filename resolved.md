# Midwicket — Resolved Issues

> **Last updated:** 2026-05-29
> This log contains all verified resolved bugs/defects and their resolution details.

### MW-001
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**`ball_events` has four mutually incompatible schemas; the analytics layer cannot read what the ingest layer writes.**
- **Producer (real data):** `core/canonicalize.py:147-153` writes the v1 schema — `batter_id`, `bowler_id`, `venue_id` (int), `runs_batter`, `runs_extras`. No name columns, no `runs_total`, no `season`. Enforced by `schema/v1.py:72` (`BALL_EVENT_SCHEMA`).
- **Consumers expect names/legacy:** `api/player_analytics.py:95,167,165` (`WHERE batter = ?`, `WHERE bowler = ?`, `runs_total - runs_extras`); `api/fantasy.py:107,143,234,75` (`batter`, `bowler`, `venue`, `season`).
- **Two more schemas exist:** `storage/thread_safe_engine.py:248` and `storage/engine.py:209` create *legacy* `ball_events` (names + `runs_total`); `core/migration.py:25-46` targets tables `deliveries`/`matches` that exist nowhere else.
- **Impact:** On canonicalized data, analytics/fantasy queries throw → swallowed → return empty/zeros. The flagship features do not work on the data the pipeline produces.
- **Fix:** Choose **one** schema (v1). Rewrite `player_analytics`, `fantasy`, and live-ingest SQL to use `batter_id`/`bowler_id`/`venue_id` (join the registry for names). Delete the legacy `CREATE TABLE ball_events` paths. This is the root cause; most P0/P1s below are symptoms.

---

### MW-002
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Three public endpoints crash with HTTP 500 on every call.**
- **Location:** `serve/api.py:1305` (`/v1/players/resolve`), `:1330` (`/v1/venues/resolve`), `:1438` (`/v1/matchup`). Each builds a plain `_Req` object with `.name`/`.batter` attributes, then calls `lookup_player`/`lookup_venue`/`get_matchup_stats`, which read `request.root.name` / `request.root.batter` (`api.py:373,442`). `_Req` (and the inline Pydantic models) have no `.root`.
- **Root cause:** `fix_mypy.py` blindly ran `request.name` → `request.root.name`, assuming `RootModel`. The `except` handlers *also* use `request.root.*` (e.g. `api.py:388`), so they re-raise → unhandled 500. `# type: ignore` hides it from mypy.
- **Impact:** Guaranteed crash; the SDK client (`client.py:228,235,254`) faithfully wraps these → always errors.
- **Fix:** Replace `request.root.X` with `request.X` in `lookup_player`, `lookup_venue`, `get_matchup_stats` (and their `except` blocks). Add a real integration test that calls these routes.

---

### MW-003
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**`GET /matches/{match_id}` corrupts data by double-counting on every hit.**
- **Location:** route `serve/api.py:794` → `session.load_match()` → `engine.ingest_events(..., append=True)` (`api/session.py:94`). `append=True` does `INSERT INTO ball_events SELECT * FROM arrow_view` (`storage/engine.py:70`) with **no dedup/idempotency**.
- **Impact:** A cacheable, client-retried GET mutates state. Hitting the endpoint N times multiplies that match's rows by N. All subsequent stats for the match are permanently wrong until rebuild.
- **Fix:** Make `load_match` idempotent (delete-then-insert by `match_id`, or skip if already loaded). A read endpoint must never append.

---

### MW-005
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Fantasy + all 28 player-analytics features return zeros/empty on real data.**
- **Location:** `api/fantasy.py` (`fantasy_score`, `cheat_sheet`, `venue_bias`) and `api/player_analytics.py` (PA-01..PA-28). All query name/legacy columns (see MW-001). Errors are swallowed by broad `except` (`fantasy.py:185`, etc.).
- **Impact:** `/v1/players/{name}/batting|bowling|fantasy`, `/v1/venues/{venue}/fantasy`, leaderboards, etc. silently return empty. `venue_bias` always returns neutral 50/50 "INSUFFICIENT DATA".
- **Fix:** Resolved by MW-001. Until then, do **not** advertise these features as working.

---

## P1 — High

---

### MW-007
**Status:** RESOLVED — `QueryEngine.execute_sql` now refuses write/DDL statements when `read_only=True` (engine-level defense in depth); regression test in `test_storage_and_monitoring.py` (verified 2026-05-29).
- **Resolved Date:** 2026-05-29
**`read_only=True` provides no write protection in the engine actually used.**
- **Location:** `MidwicketSession` uses `QueryEngine` (`api/session.py:37`). In `storage/engine.py:149-177`, `read_only` only decides whether results are chunked — both branches use the same read-write pooled connection. The read-only-transaction enforcement lives in `ThreadSafeQueryEngine`, which is never used (see MW-018).
- **Impact:** `/analyze`'s "defense in depth" is one layer (`sql_guard`) only. Any guard bypass = writes.
- **Fix:** Either wire `ThreadSafeQueryEngine` in, or enforce `BEGIN TRANSACTION READ ONLY` in `QueryEngine` for read paths.

---

### MW-009
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Redis singleflight in the executor makes multi-worker slower, not faster.**
- **Location:** `runtime/executor.py:74-92,172-181` — the leader stores results in its **local** cache, but cross-worker waiters poll *their own* local cache, which the leader never populates → 30s poll-then-timeout. Also reads `REDIS_URL` while `rate_limit.py` reads `MIDWICKET_REDIS_URL`.
- **Impact:** In the exact scenario Redis is for, latency increases and waiters error out.
- **Fix:** Use a shared cache (Redis) for results too, or remove the distributed path and keep the in-process `threading.Event` singleflight. Unify the env var name.

---

### MW-010
**Status:** RESOLVED — reads now stream via `to_arrow_reader` and truncate at `MAX_RESULT_ROWS` instead of materializing the full result with `.arrow()`; regression test in `test_storage_and_monitoring.py` (verified 2026-05-29).
- **Resolved Date:** 2026-05-29
**The 100k-row memory cap never executes.**
- **Location:** `storage/engine.py:160` and `thread_safe_engine.py:502` guard with `if isinstance(result, pa.RecordBatchReader)`, but `con.execute(...).arrow()` returns a fully-materialized `pa.Table`. The branch is unreachable.
- **Impact:** The OOM it claims to prevent is fully present — the entire result set is in RAM before the "cap".
- **Fix:** Use `.fetch_record_batch()` for streaming, or append `LIMIT` at the SQL level before execution.

---

### MW-012
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Phase casing mismatch produces wrong impact scores and can silently return empty filters.**
- **Location:** `compute/metrics/batting.py:96-97` matches `"Powerplay"`/`"Death"` (capitalized) while `core/canonicalize.py:8-12` writes capitalized but `storage/engine.py:_infer_phase`, `runtime/planner.py:184`, and `query/defs.py:8` use lowercase.
- **Impact:** Impact score: powerplay/death balls fall through to the "Middle" baseline (1.1 RPB). Phase filters comparing the wrong case return zero rows silently.
- **Fix:** Normalize phase casing in one place (constant/enum) and use it everywhere on write and read.

---

### MW-014
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Run-outs counted as bowler wickets.**
- **Location:** `core/canonicalize.py:117-118` sets `is_wicket=True` for all dismissals; `api/player_analytics.py:163` and `runtime/planner.py:213` count `SUM(is_wicket)` as bowler wickets. (Note: `data/pipeline.py:98` does this correctly for registry stats.)
- **Impact:** Bowling wicket counts inflated by run-outs/obstruction.
- **Fix:** Add a `bowler_wicket` flag (exclude run out / obstructing / retired) and count that for bowling.

---

### MW-019
**Status:** RESOLVED — `_migrate_1_0_to_1_1` now guards every `ALTER` on real table existence (so it never touches phantom `deliveries`/`matches`), and `migrate_on_connect` logs the actual number of tables migrated instead of a blanket "schema updated" claim (verified 2026-05-29).
- **Resolved Date:** 2026-05-29
**`core/migration.py` migrates phantom tables.** `SCHEMA_VERSIONS` (`:25-46`) and `_migrate_1_0_to_1_1` (`:85-119`) target `deliveries`/`matches` tables that exist nowhere; `ALTER TABLE deliveries` fails silently (`:124-126`) yet logs *"Database schema updated. Existing data is safe."* Runs on every session init (`session.py:43`). **Fix:** delete or rewrite against the real `ball_events`.

---

### MW-026
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Planner fantasy SQL credits a batter +20 for getting out.** `runtime/planner.py:231` — `SUM(CASE WHEN is_wicket THEN 20 ELSE 0 END) + SUM(runs_batter) AS avg_points`, grouped by `batter_id`. `is_wicket` on a batter's delivery means the batter was dismissed. Also it's a SUM mislabeled `avg_points`. (Duplicate, divergent logic vs `api/fantasy.py`.) **Fix:** correct the scoring sign/semantics and de-duplicate against `fantasy.py`.

---

### MW-029
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Codebase is being maintained by blind regex patchers.** Committed/working-tree scripts: `fix_mypy.py` (sprays `# type: ignore`, caused MW-002), `fix_mypy_ignores.py`, `fix_rest.py` (comments describe deleting failing tests), `add_middleware_and_export.py` (caused MW-006), `update_audit.py`, `update_audit_endpoint.py`, `callback.py`, `fix_train_register.py`. **Fix:** delete these from the repo; ban the workflow. Fix types/code by editing source, not string-replacing it. `warn_unused_ignores = true` is set in `pyproject.toml` — run mypy clean and remove the spray.

---

## P3 — Low / Hygiene / Latent

---

### MW-030
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Repo-root junk.** Tracked scratch scripts (see MW-029) + `v1.txt` (24KB) + `virat_report.pdf` (committed despite `.gitignore` `*.pdf` "must never be committed"). `pypitch/` is a ghost: package dirs with **zero `.py` files**, only stale `__pycache__`. Duplicate test files at root (`test_metrics.py`, `test_query_types.py`, …) that CI doesn't run. **Fix:** delete; add a CI check that fails on tracked scratch files.

---

### MW-031
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Packaging defects.** `pyproject.toml:94` `include = ["midwicket*", "midwicket*"]` (duplicate; was meant to be `pypitch*`). `serve/api.py:1527` `create_dockerfile()` emits `COPY . .` with a `.dockerignore` that excludes none of the scratch scripts, `.env`, `data/`, or `pypitch/`. **Fix:** correct the include glob; tighten the generated dockerignore (or remove the generator — a top-level `Dockerfile` already exists).

---

### MW-035
**Status:** RESOLVED — `snapshot_id` removed from queries and hardcoded calls removed; cache key natively binds to engine's `snapshot_id` and `derived_versions` in `executor.py` (verified 2026-05-29).
- **Resolved Date:** 2026-05-29
**The cache key is `snapshot_id`, and every caller hardcodes `snapshot_id="latest"` — so the cache serves stale results forever after new data loads.**
- **Mechanism:** `query/base.py:41-52` builds `cache_key` from `model_dump(...)`, which includes the `snapshot_id` **field** (`base.py:15`). That field is supplied by the caller — and every call site passes the literal string `"latest"`: `express.py:168`, `api/fantasy.py:212`, `api/head_to_head.py:135`, `api/sim.py:20`, `api/stats.py:40`.
- **The trap:** the engine's *real* data version is `engine._snapshot_id`, which changes on every `ingest_events` (`storage/engine.py:88`, e.g. `"match_123"`). But the cache key never sees it — it sees the constant `"latest"`. So after `session.load_match(...)` changes `ball_events`, the next identical query produces the **same `cache_key`** → `executor.execute` returns the cached pre-load result (`runtime/executor.py:154-166`).
- **Why it's nasty:** `ResultMetadata.snapshot_id` is also set to the caller's `"latest"` (`executor.py:163`), so the stale result is *labelled* current — you cannot detect staleness from the response. Default TTL is 3600s (`cache.set` called without ttl, `executor.py:288`), so wrong answers persist for up to an hour, or until the cache file is cleared.
- **Impact:** "deterministic, snapshot-aware caching" is the headline feature, and it is silently incoherent with the data. Load data → query → answer X. Load more → query → still X.
- **Fix:** Key the cache on the **engine's** `snapshot_id` + `derived_versions`, not the caller's literal. Stop letting callers pass `snapshot_id`; have the executor stamp the real version into the key and the metadata.

---

### MW-036
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**The snapshot system that should make MW-035 impossible exists but is wired to nothing.**
- **Location:** `storage/snapshots.py` (`SnapshotManager`, `create_snapshot`, `get_latest`). Grep shows **zero** usages outside the file. It writes `snapshots.json` that nothing reads; it has no reference to the engine, the cache, or query keys.
- **Impact:** There's a bookkeeping ledger of snapshots that influences nothing. The infrastructure to fix MW-035 was built and left unplugged.
- **Fix:** Either drive cache keys/invalidation from `SnapshotManager` (and bump it on every ingest), or delete it so it stops implying coherence that doesn't exist.

---

### MW-038
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
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

---

### MW-046
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Three different phase definitions coexist, and the analytics ignore the phase column the ingest layer materialized.**
- **Location:** `core/canonicalize.py:8-12` writes a `phase` column (`Powerplay`/`Middle`/`Death`, boundaries `<6`/`<15`). But `api/player_analytics.py:253-256,301-305` **recompute** phase inline from `over` (`CASE WHEN over<=5 ... <=14 ...`), ignoring the stored column. `storage/engine.py:_infer_phase` is a third definition (lowercase). (See MW-012 for the casing half of this.)
- **Impact:** The materialized `phase` is dead weight; phase logic is duplicated in 3 places that can (and do) drift. Change a boundary in one and the others silently disagree.
- **Fix:** One canonical phase function used at write time; read the stored column everywhere else.

---

### MW-047
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**`MIDWICKET_CACHE_SALT` can silently destroy cross-worker cache sharing and guards a non-existent threat.**
- **Location:** `query/base.py:50-52` mixes an env salt into the cache key. The cache backend is an internal DuckDB file (`runtime/cache_duckdb.py`), not user-writable — there is no poisoning vector for the salt to defend.
- **Impact:** If the salt is set inconsistently across workers/replicas (or set on one deploy and not the next), the *same* query hashes to *different* keys per worker → 0% cross-worker hit rate and cache thrash, with no error. Cargo-cult security that adds an operational footgun. (Compounds MW-016.)
- **Fix:** Remove the salt, or document that it must be identical across all workers and is only for key-namespacing, not security.

---

### MW-011
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Strike rate counts wides as balls faced (knowingly wrong).**
- **Location:** `compute/metrics/batting.py:43` — `balls_faced = len(events)`.
- **Impact:** SR understated whenever wides are present; `relative_strike_rate` compounds it.
- **Fix:** Exclude wides (and treat no-balls per the rules) from the denominator in batting strike rate (both in python metrics and all player analytics SQL queries).

---

### MW-013
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Runs scored off no-balls are dropped; `RunComponent` flags are computed then discarded.**
- **Location:** `core/canonicalize.py:101` calls `RunComponent.from_no_ball(...)` which hardcoded `batter_runs=0`.
- **Impact:** Batter runs off a no-ball were lost; SR/economy couldn't distinguish legal vs illegal deliveries.
- **Fix:** Capture `runs.batter` on no-balls; allow `batter_runs` in `RunComponent.from_no_ball` and set `is_ball_faced=True` to count as a ball faced for the batter.

---

### MW-040
**Status:** RESOLVED (verified against code 2026-05-29).
- **Resolved Date:** 2026-05-29
**Bowling economy understates runs conceded — it excludes the wides and no-balls the bowler is charged for.**
- **Location:** `api/player_analytics.py:165,308,509` compute `runs_conceded = SUM(runs_total - runs_extras)`.
- **Impact:** Every bowling economy/average from `player_analytics` is too low.
- **Fix:** Correctly sum runs conceded as `runs_batter` plus wide and noball extras, and exclude wides and no-balls from the bowler's balls bowled count.
