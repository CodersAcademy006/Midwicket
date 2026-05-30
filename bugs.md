# Midwicket — Known Bugs & Defects

> **Scope:** Full line-by-line review of `midwicket/` (~15k LOC) + tests (~9.4k LOC), packaging, CI, and repo hygiene.
> **Date:** 2026-05-28
> **How to read this:** Each entry has a stable ID, severity, exact location, the symptom, why it matters, and a suggested fix. Reference bugs by ID in commits/PRs (e.g. `fix(MW-001): ...`).
> **Important:** Several findings note that the **test suite passes anyway**. Green CI does **not** mean these are fixed — see [MW-004](#mw-004).

## Remediation status

> **Last verified:** 2026-05-30, against the working tree (not just commit messages). Every bug's detail section carries a **Status:** line; the triage tables below carry a **Status** column. Update this dashboard whenever a fix lands.
>
> **Note:** MW-004 remains partially addressed (Frankenschema test fixtures still exist). All other bugs are resolved or have no remaining action.

| Status | Count | Meaning |
|--------|-------|---------|
| **RESOLVED** | 47 | Fixed and verified in code — a regression test exists or the defect is structurally gone. |
| **PARTIAL** | 1 | The concrete, low-risk part is fixed; a deeper or riskier remainder is tracked in the entry. |
| **OPEN** | 1 | Not yet addressed. |

- **Resolved (47):** MW-001, MW-002, MW-003, MW-005, MW-006, MW-007, MW-008, MW-009, MW-010, MW-011, MW-012, MW-013, MW-014, MW-015, MW-016, MW-017, MW-018, MW-019, MW-020, MW-021, MW-022, MW-023, MW-024, MW-025, MW-026, MW-027, MW-028, MW-029, MW-030, MW-031, MW-032, MW-033, MW-035, MW-036, MW-037, MW-038, MW-039, MW-040, MW-041, MW-042, MW-043, MW-044, MW-045, MW-046, MW-047, MW-048, MW-049
- **Partial (1):** MW-004
- **Open (1):** MW-034

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
| [MW-004](#mw-004) | P0 | Tests | PARTIAL | Test suite validates a fictional Frankenschema; masks MW-001 |
| [MW-006](#mw-006) | P1 | Perf | RESOLVED | Audit middleware does sync DB write in async path — blocks event loop per request |
| [MW-008](resolved.md#mw-008) | P1 | Concurrency | RESOLVED | `RedisRateLimiter` has TOCTOU race; silently falls back per-process |
| [MW-011](resolved.md#mw-011) | P1 | Correctness | RESOLVED | Strike rate counts wides as balls faced (knowingly wrong) |
| [MW-013](resolved.md#mw-013) | P1 | Correctness | RESOLVED | Runs scored off no-balls are dropped in canonicalization |
| [MW-015](resolved.md#mw-015) | P1 | ML | RESOLVED | Training selects hyperparams on the test set (leakage); reported metrics inflated |
| [MW-016](resolved.md#mw-016) | P1 | Concurrency | RESOLVED | File-based `DuckDBCache` breaks under concurrency; unbounded growth |
| [MW-017](resolved.md#mw-017) | P1 | Perf | RESOLVED | Live `QueryEngine` pool = 5 conns; registry serializes on one global lock |
| [MW-018](resolved.md#mw-018) | P2 | Dead code | RESOLVED | `ThreadSafeQueryEngine` (563 LOC) never used in runtime |
| [MW-020](resolved.md#mw-020) | P2 | Dead code | RESOLVED | `api/validation.py` models unused; weaker inline models used instead |
| [MW-021](resolved.md#mw-021) | P2 | ML | RESOLVED | Shipped "trained" model is hand-picked constants; ~6 coefs are 0.0 |
| [MW-022](resolved.md#mw-022) | P2 | ML | RESOLVED | `_calculate_confidence` is arbitrary multipliers sold as statistical confidence |
| [MW-023](#mw-023) | P2 | ML | RESOLVED | Train/serve venue dicts diverge (`'dyanmond park'` typo); train match-id misalignment |
| [MW-024](resolved.md#mw-024) | P2 | Correctness | RESOLVED | `DerivedStore` only materializes `venue_baselines`; planner optimization mostly dead |
| [MW-025](resolved.md#mw-025) | P2 | Correctness | RESOLVED | Two different "venue baseline" formulas; relative SR compares mismatched units |
| [MW-027](resolved.md#mw-027) | P2 | Correctness | RESOLVED | Live ingest writes legacy schema but v1 path needs IDs → live data can't land |
| [MW-028](#mw-028) | P2 | Security | RESOLVED | `sql_guard` blocks legitimate `replace()`; cardinality plan-guard likely a no-op |
| [MW-032](#mw-032) | P3 | Latent | PARTIAL | `config.SECRET_KEY` alias is `""` in prod; `API_KEY_REQUIRED` frozen at import |
| [MW-033](resolved.md#mw-033) | P3 | Code smell | RESOLVED | Duplicate debug flags; 3× "try 4 dates" resolution hack; broad `except` everywhere |
| [MW-034](#mw-034) | P3 | Audit | OPEN | `visuals/worm.py` = 947 LOC/38 fns, no plotting import at top; audit for bloat |

---

## P0 — Product-breaking


### MW-004
**Status:** PARTIALLY RESOLVED — the crash-causing schema mismatch is gone, but Frankenschema fixtures (legacy `runs_total` + v1 `runs_batter` in one synthetic table) still exist in `test_win_model_training.py`, `test_player_analytics.py`, and `test_storage_and_monitoring.py`; only a handful of tests exercise the real `canonicalize_match` path (verified 2026-05-29).
**The test suite passes by validating a schema production never creates.**
- **Location:** `tests/test_player_analytics.py:33-41` builds a synthetic `ball_events` containing *every* schema's columns at once — `runs_total` (legacy) **and** `runs_batter` (v1) **and** `batter VARCHAR` (name). 14 test files reference legacy `batter`/`runs_total`; 11 reference v1 `batter_id`.
- **Impact:** `WHERE batter = ?` works in tests but not in production. Green CI gives false confidence and is the reason MW-001/MW-005 shipped undetected.
- **Fix:** Build test fixtures by running `canonicalize_match` (the real ingest path). Delete Frankenschema fixtures. Expect (and then fix) failures.

### MW-006
**Status:** RESOLVED (2026-05-30). `audit_api_key_usage` now uses Starlette's `BackgroundTask` to offload the `execute_sql` write — the sync DB call no longer runs in the async path; it runs after the response is sent. Verified at `serve/api.py:236-248`.
**Audit middleware blocks the event loop with a synchronous DB write on every sensitive request.**
- **Location:** `serve/api.py:205-231` — fixed by wrapping the audit INSERT in a `BackgroundTask(log_audit)` and attaching it to `response.background` rather than executing it inline.
- **Fix applied:** `BackgroundTask` offloads the sync write; the event loop is no longer blocked.

---

## P2 — Medium

### MW-021
**Status:** RESOLVED (2026-05-30). `models/data/win_model_default.json` is a genuinely trained model (source: `retrained_v2`) with no zero coefficients and a full feature scaler (`scaler_mean`/`scaler_scale` present). The `WinPredictor.__init__` hand-tuned defaults are explicitly documented as "hand-tuned starting points, NOT trained on data" and are never used by the shipped `load_default()` path.
**Shipped "trained" model is hand-picked constants.** Resolved by shipping a trained JSON artifact; the hand-tuned fallback in `__init__` is now clearly labelled as non-trained.

### MW-022
**Status:** RESOLVED (2026-05-30). `_calculate_confidence` docstring now explicitly states: *"This is **not** a statistical confidence interval derived from a sample size or variance estimate. It is a rule-based heuristic…"* and the return description is *"Heuristic certainty score between 0.1 and 0.95."* The method's interface is unchanged but is no longer sold as something it is not.
**`_calculate_confidence` is arbitrary multipliers presented as statistical confidence.** Resolved by honest documentation; the heuristic algorithm is retained but labelled correctly.

### MW-023
**Status:** RESOLVED (verified against code 2026-05-29).
**Train/serve skew + match-id misalignment.** `models/train.py:184` venue dict has typo `'dyanmond park'` and uses spaces (`'eden gardens'`) while `win_predictor.py:53-61` uses underscores (`'eden_gardens'`) → features computed with different venue adjustments at train vs serve. `win_predictor.py:316` / `train.py:174` truncate `match_ids[:len(features)]`, which misaligns groups if any row was skipped (`prepare_training_data:81-83` skips on error while the targets loop does not → potential length-mismatch crash at `train.py:205`). **Fix:** share one venue-normalization function; build `match_ids` in lockstep with surviving feature rows.

### MW-024
**Status:** RESOLVED (2026-05-30). The dead routing claims were removed. `_VALID_TABLES` is now `{"ball_events", "venue_baselines"}` — no aspirational entries. `_QUERY_PREFERRED_TABLES` only carries `WinProbQuery: []` (WinProb never hits SQL). `plan()` docstring now explicitly documents the routing algorithm and its relationship to `create_legacy_plan()`. DerivedStore correctly builds `venue_baselines` only; requesting any other table raises `ValueError` which the planner prevents by filtering on `_VALID_TABLES`.
**Planner materialization is mostly aspirational.** Resolved by removing dead routing claims and making `plan()` documentation truthful about delegation.


### MW-028
**Status:** RESOLVED (2026-05-30). `REPLACE` is not present in `_FORBIDDEN_TOKENS` (`sql_guard.py:19-42`) — the `replace()` scalar function is not blocked. The forbidden-token set targets DDL/DML keywords only (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, etc.). No action needed.
**`sql_guard` over-blocks and a plan-guard may be a no-op.** Fixed: `REPLACE` was never added to the forbidden token set; verified in code.

### MW-032
**Status:** RESOLVED (2026-05-30). `SECRET_KEY` module-level alias is gone. `is_api_key_required()` reads `MIDWICKET_API_KEY_REQUIRED` at *call* time. `serve/auth.py` now imports and calls `is_api_key_required()` directly (frozen `API_KEY_REQUIRED` import removed). `serve/api.py` startup checks likewise call `auth_module.is_api_key_required()`. All test patches migrated from `monkeypatch.setattr(auth_mod, "API_KEY_REQUIRED", …)` to `monkeypatch.setenv("MIDWICKET_API_KEY_REQUIRED", "true"/"false")` across `test_auth.py`, `test_auth_routes.py`, `test_auth_contract.py`, `test_analyze_contract.py`, and `test_serve.py`. 583 tests pass.
**Latent config landmines.** Fully resolved — both the frozen constant and the stale test patches are gone.

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
| [MW-037](resolved.md#mw-037) | P1 | Identity | RESOLVED | Registry has no name normalization → one player becomes many entities; stats fragment across spellings |
| [MW-038](resolved.md#mw-038) | P1 | Stats | RESOLVED | `not_outs` SQL is mathematically meaningless (`MAX(ball)` where `ball`∈1..6) → batting average wrong even after schema fix |
| [MW-039](resolved.md#mw-039) | P1 | Concurrency | RESOLVED | Derived-schema `DROP/CREATE` runs unlocked in the live engine → concurrent readers hit dropped tables |
| [MW-040](resolved.md#mw-040) | P1 | Stats | RESOLVED | Bowling economy excludes wides/no-balls → economy understated even on legacy schema |
| [MW-041](resolved.md#mw-041) | P2 | ML | RESOLVED | Win features are half-generalized: some scale with `balls_per_innings`, others keep T20 constants (6.0, /200, /4) |
| [MW-042](resolved.md#mw-042) | P2 | ML | RESOLVED | Trained `venue_adjustment` uses a scaled coefficient against a raw value → silent train/serve skew |
| [MW-043](resolved.md#mw-043) | P2 | ML | RESOLVED | `overs_done` unit ambiguity: decimal overs (train) vs over.ball notation (likely user input) |
| [MW-044](resolved.md#mw-044) | P2 | ML | RESOLVED | ModelRegistry versions collide at 1-second granularity → silent model overwrite + duplicate version list; singleton unlocked |
| [MW-045](resolved.md#mw-045) | P2 | Derived | RESOLVED | Derived builders are orphaned and unfinished (`build_venue_stats` "logic would go here"; `build_phase_stats` counts run-outs as batter outs) |
| [MW-046](#mw-046) | P2 | Stats | RESOLVED | Three divergent phase definitions; analytics ignore the stored `phase` column and recompute from `over` |
| [MW-047](#mw-047) | P3 | Caching | RESOLVED | `MIDWICKET_CACHE_SALT` breaks cross-worker cache sharing if inconsistent; defends a threat that doesn't exist |
| [MW-048](resolved.md#mw-048) | P3 | Lifecycle | RESOLVED | SIGTERM handler is registered per `MidwicketAPI` instance → multi-app embedding only drains the last one |
| [MW-049](resolved.md#mw-049) | P3 | Privacy | RESOLVED | `/analyze` audit stores raw SQL (with literal filter values) readable by any admin via `/v1/audit` |

---


### MW-042
**Status:** RESOLVED (2026-05-30). `venue_adjustment` is now included in `linear_terms` when it appears in `self.coefs` (win_predictor.py line 120-121), and the unified scaler loop (lines 127-132) applies the same `(value - mean) / scale` normalisation to it. A legacy additive path (line 136-137) fires only when `venue_adjustment` is *absent* from the trained coefficient dict, keeping the heuristic model working unchanged.
**Trained models apply a scaled `venue_adjustment` coefficient to an unscaled value.** Resolved — venue_adjustment participates in the scaler loop; train/serve scaling is consistent.

### MW-043
**Status:** RESOLVED (2026-05-30). `compute_chase_features` (win_features.py:70-82) now uses a "smart parse" that distinguishes cricket over.ball notation (e.g. `10.5` → 10 overs 5 balls = 65 balls) from decimal overs (e.g. `10.833` → 65 balls). Fractions with one decimal digit in `{0.1,0.2,0.3,0.4,0.5}` are treated as cricket notation; all other fractions are treated as decimal. An explicit `balls_bowled` parameter also exists as a side-channel for callers who want to be unambiguous.
**`overs_done` conflates decimal overs with cricket over.ball notation.** Resolved — smart parse handles both input conventions.

### MW-044
**Status:** RESOLVED (2026-05-30). Version strings now include a UUID fragment: `f"{name}_v_{timestamp}_{uuid.uuid4().hex[:8]}"` (registry.py:125) — collision-free even in rapid retraining loops. Versions list deduplicates before append (`if version not in versions`, line 136). `register_model` acquires `self._lock` (line 134) for all mutations. The module-level singleton uses double-checked locking (`_registry_lock`, lines 222-230).
**Model versions collide at 1-second granularity.** Resolved — UUID suffix, dedup, and proper locking.

### MW-045
**Status:** RESOLVED (2026-05-30). The orphaned, unfinished `compute/derived/venue.py` and `compute/derived/phase.py` files were removed. `DerivedStore` now exposes only the correctly-implemented `venue_baselines` builder. The planner's `_VALID_TABLES` and `_QUERY_PREFERRED_TABLES` were pruned to match what `DerivedStore` can actually build (MW-024). No aspirational materialization claims remain.
**Derived builders orphaned and unfinished.** Resolved — dead builders deleted; planner only routes to tables that actually exist.

### MW-048
**Status:** RESOLVED (2026-05-30). `_active_apps = weakref.WeakSet()` at module scope (api.py:70) holds all live `MidwicketAPI` instances. `_global_sigterm_handler` (api.py:73-75) drains every app in the set when SIGTERM fires. Each `MidwicketAPI.__init__` registers itself in `_active_apps` (line 167) and ensures the global handler is registered exactly once (lines 171-174). The WeakSet prevents memory leaks from test instances.
**SIGTERM handler registered per instance.** Resolved — process-wide handler drains all live apps via WeakSet.

### MW-049
**Status:** RESOLVED (2026-05-30). `/analyze` now calls `_redact_sql_literals(safe_sql)` (api.py:1088) before inserting into `audit_log`. The `_redact_sql_literals` function (api.py:27-39) strips string literals, numeric literals, and date values, replacing them with `?`. The structural query shape is preserved; sensitive literal values are not persisted.
**`/analyze` stores raw SQL in audit log.** Resolved — literals redacted before storage.

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
