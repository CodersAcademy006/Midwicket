# Midwicket — Full Defect Register

> **Scope:** Complete line-by-line audit of `midwicket/` + `tests/` + packaging, CI, containers, monitoring, docs.
> **Date:** 2026-05-30
> **Goal:** Zero remaining issues after resolution = stable v1 release.
>
> Reference bugs by ID in commits/PRs: `fix(MW-NNN): ...`

---

## Remediation status

| Status | Count | Meaning |
|--------|-------|---------|
| **OPEN** | 0 | Not yet addressed. |
| **RESOLVED** | 16 | Fixed and merged. |

- **Resolved:** MW-001, MW-002, MW-003, MW-004, MW-005, MW-006, MW-007, MW-008, MW-009, MW-010, MW-011, MW-012, MW-013, MW-014, MW-015, MW-016

---

## Severity legend

| Level | Meaning |
|-------|---------|
| **P0** | Product-breaking. Core feature crashes on every invocation or produces silent data corruption. |
| **P1** | High. Security weakness, major feature broken in production, critical data mismatch. |
| **P2** | Medium. Correctness bug under specific conditions, schema mismatch, dead code path. |
| **P3** | Low. Hygiene, stale documentation, latent landmine, missing observability. |

---

## Triage summary

| ID | Category | Sub-category | Severity | Area | Status | One-line |
|----|----------|-------------|----------|------|--------|----------|
| [MW-005](#mw-005) | Bug | Logic | P2 | REST API | RESOLVED | `/win_probability` omits `venue`; venue adjustments never applied via REST |
| [MW-006](#mw-006) | Bug | Logic | P2 | Visuals | RESOLVED | `plot_run_pressure` calls `ax.twinx()` inside a loop — overlapping secondary axes |
| [MW-007](#mw-007) | Bug | Configuration | P2 | Serve | RESOLVED | `MidwicketAPI.run(reload=True)` crashes — uvicorn reload requires an import string |
| [MW-008](#mw-008) | Bug | Logic | P2 | Models | RESOLVED | `win_probability` `balls_per_innings` defaults to 120; ODI/Test formats silently wrong |
| [MW-009](#mw-009) | Bug | Logic | P2 | Pipeline | RESOLVED | `build_registry_stats` never populates `matchup_stats`; session triggers rebuild every start |
| [MW-010](#mw-010) | Bug | Logic | P2 | Visuals | RESOLVED | `plot_run_pressure` required-RR uses `300 if max_over > 20 else 120` proxy; not accurate |
| [MW-011](#mw-011) | Technical Debt | Configuration | P3 | Core | RESOLVED | `_ensure_schema` legacy bootstrap never updated; stale "Stage 2 MVP" comment in canonicalize |
| [MW-012](#mw-012) | Documentation Issue | Semantic | P3 | Docs | RESOLVED | `MIDWICKET_DEBUG` documented in `api.md` and `debug_mode.md` but env var never read |
| [MW-013](#mw-013) | Bug | Logic | P3 | Core | RESOLVED | Unknown Cricsheet wicket kinds silently mapped to `BOWLED` with no log warning |
| [MW-014](#mw-014) | Bug | Logic | P3 | Visuals | RESOLVED | `plot_beehive` and `plot_wagon_wheel` use random data, not real pitch/shot data |
| [MW-015](#mw-015) | Security Issue | Configuration | P3 | Serve | RESOLVED | CORS `allow_origins` passes `origins` variable but origins can be empty — browser requests rejected silently |
| [MW-016](#mw-016) | Performance Issue | Configuration | P3 | CI | RESOLVED | CI advisory jobs (lint, type-check, security) never block merges — failures invisible |

---

## P0 — Product-breaking

## P1 — High

## P2 — Medium

### MW-005
**Category:** Bug > Logic
\*\*Status:\*\* RESOLVED
**Severity:** P2
**Location:** `midwicket/serve/api.py:926–949` (`/win_probability` endpoint)

**Symptom:**
```python
result = wp_func(
    target=target,
    current_runs=current_runs,
    wickets_down=wickets_down,
    overs_done=overs_done,
    # venue omitted — defaults to None — neutral baseline always used
)
```
`win_probability()` in `midwicket/compute/winprob.py` documents: *"venue materially affects the result via per-venue adjustment factors learned during training."* The `px.predict_win()` Express path accepts `venue` correctly. Only the REST API discards it.

**Impact:** REST callers cannot get venue-adjusted win probabilities. All venues return the same neutral-baseline result.

**Fix:**
```python
venue: Optional[str] = Query(None, description="Venue name for venue-specific adjustment"),
...
result = wp_func(target=target, current_runs=current_runs,
                 wickets_down=wickets_down, overs_done=overs_done, venue=venue)
```

---

### MW-006
**Category:** Bug > Logic (UX)
\*\*Status:\*\* RESOLVED
**Severity:** P2
**Location:** `midwicket/visuals/worm.py:288–294` (`plot_run_pressure`)

**Symptom:**
```python
for i, inning in enumerate(df['inning'].unique()):
    inning_data = df[df['inning'] == inning]
    ax2 = ax.twinx()          # NEW twin axis created on every iteration
    ax2.plot(...)
    ax2.set_ylabel('Dot-ball %', color='red')
```
For a 2-innings match this creates two stacked `ax2` instances on the same plot, producing doubled right-side y-axis ticks and overlapping labels.

**Fix:**
```python
ax2 = ax.twinx()
ax2.set_ylabel('Dot-ball %', color='red')
ax2.tick_params(axis='y', labelcolor='red')
for i, inning in enumerate(df['inning'].unique()):
    inning_data = df[df['inning'] == inning]
    ax2.plot(inning_data['over_float'], inning_data['dot_pct'],
             color='red', linestyle=':', linewidth=1, alpha=0.4)
```

---

### MW-007
**Category:** Bug > Configuration
\*\*Status:\*\* RESOLVED
**Severity:** P2
**Location:** `midwicket/serve/api.py:1481–1491` (`MidwicketAPI.run`)

**Symptom:**
```python
uvicorn.run(self.app, host=host, port=port, reload=reload)
```
Uvicorn's `reload=True` requires the app to be specified as an importable string (`"midwicket.serve.api:create_app"`), not a live `FastAPI` instance. Passing an instance with `reload=True` raises:
```
TypeError: reload mode requires 'app' to be an import string
```

**Fix:**
```python
if reload:
    raise ValueError(
        "reload=True is not supported on a MidwicketAPI instance. "
        "Use: uvicorn midwicket.serve.api:create_app --reload"
    )
```

---

### MW-008
**Category:** Bug > Logic
\*\*Status:\*\* RESOLVED
**Severity:** P2
**Location:** `midwicket/compute/winprob.py:59–68`

**Symptom:**
```python
def win_probability(
    ...
    balls_per_innings: int = 120,   # hardcoded T20 default
    ...
```
The function is documented and used as a T20/IPL tool, and the default of 120 is reasonable for the current IPL-only dataset. However, there is no validation that `overs_done` and `balls_per_innings` are consistent, and passing ODI data (50 overs = 300 balls) while leaving the default at 120 silently produces wrong results. No warning is emitted.

**Impact:** Any non-T20 usage silently produces wrong win probability. There is no runtime guard.

**Fix:**
Add a validation warning:
```python
if balls_per_innings not in (120, 300) and balls_per_innings != 120:
    logger.warning(
        "win_probability: balls_per_innings=%d is non-standard. "
        "Model was trained on T20 (120-ball) data; results may be unreliable.", balls_per_innings
    )
```
Or document this limitation prominently and add an assertion.

---

### MW-009
**Category:** Bug > Logic
\*\*Status:\*\* RESOLVED
**Severity:** P2
**Location:** `midwicket/data/pipeline.py` (`build_registry_stats`)

**Symptom:**
`build_registry_stats` populates `player_stats` and `venue_stats` in the registry but never calls `registry.upsert_matchup_stats(...)`. The `matchup_stats` table is therefore always empty after a build.

In `session.py:105–109`, the session checks:
```python
try:
    self.registry.con.execute("SELECT 1 FROM matchup_stats LIMIT 1")
    matchup_table_missing = False
except Exception:
    matchup_table_missing = True
```
If `matchup_stats` is empty (which it always is after build), this query returns zero rows — not an exception — so `matchup_table_missing = False`. However, `registry_empty` is computed from `player_stats` count. After the first build, `registry_empty = False` and `matchup_table_missing = False`, so the session correctly skips rebuilds.

BUT if the registry database is deleted and recreated: `matchup_stats` exists (created by `_init_db`) but is empty. The `SELECT 1 FROM matchup_stats LIMIT 1` returns 0 rows, no exception → `matchup_table_missing = False`. Combined with `registry_empty = False` (player_stats was rebuilt), `needs_build = False`. So matchup_stats is permanently empty after the first rebuild.

**Impact:** `express.get_matchup()` and `/v1/matchup` will always hit the slow path (ball_events query) rather than the fast path (pre-built matchup_stats registry lookup). For large datasets, every matchup query scans ball_events.

**Fix:**
Either populate `matchup_stats` in `build_registry_stats`, or change the session check from testing table existence to testing table row count:
```python
matchup_count = self.registry.con.execute("SELECT COUNT(*) FROM matchup_stats").fetchone()[0]
matchup_table_missing = (matchup_count == 0)
```

---

### MW-010
**Category:** Bug > Logic
\*\*Status:\*\* RESOLVED
**Severity:** P2
**Location:** `midwicket/visuals/worm.py:278–281` (`plot_run_pressure`)

**Symptom:**
```python
max_over = df['over'].max()
total_balls = 300 if max_over > 20 else 120
remaining_balls = (total_balls - innings2['balls']).clip(lower=1)
innings2['required_rr'] = (target - innings2['cumulative']) / remaining_balls * 6
```
Format detection uses `max_over > 20` as a proxy for ODI. This is unreliable:
- A T20 match where a bowler has bowled over 20 overs (due to data errors or super-overs) would incorrectly use 300 balls.
- An interrupted ODI DLS match might have `max_over < 20` and incorrectly use 120 balls.
- The formula never accounts for the actual overs-per-innings from match metadata.

**Impact:** Required run rate on the chart is incorrect for any match where the heuristic misfires.

**Fix:**
Accept `balls_per_innings: int = 120` as a parameter to `plot_run_pressure` and use that instead of the heuristic. Callers that know the match format can pass the correct value.

---

## P3 — Low (Hygiene / Technical Debt)

### MW-011
**Category:** Technical Debt > Configuration
\*\*Status:\*\* RESOLVED
**Severity:** P3
**Location:** `midwicket/storage/thread_safe_engine.py:259–288`; `midwicket/core/canonicalize.py:65`

**Symptom 1 — stale legacy schema bootstrap:**
`_ensure_schema` was written to bootstrap an in-memory demo with legacy columns. It was never updated to match the v1 canonical schema. The full resolution is covered by MW-003; this entry tracks the cleanup task.

**Symptom 2 — stale comment:**
`canonicalize.py:65`:
```python
# so for Stage 2 MVP we use -1 or logic from the *other* inning.
```
The actual code at this point correctly computes the bowling team with `next((t for t in teams if t != batting_team), "Unknown")`. The comment describes an older design that was already implemented.

**Fix:**
Remove the "Stage 2 MVP" comment and replace with a factual description of the logic. Address the legacy schema as part of MW-003.

---

### MW-012
**Category:** Documentation Issue > Semantic
\*\*Status:\*\* RESOLVED
**Severity:** P3
**Location:** `midwicket/docs/api.md:327,333`; `midwicket/docs/debug_mode.md:54`

**Symptom:**
Both documents describe `MIDWICKET_DEBUG` as a supported env var:
```
| `MIDWICKET_DEBUG` | Enable debug mode with verbose logging | `false` |
```
Searching the entire `midwicket/` source, `MIDWICKET_DEBUG` is never read by `os.getenv` or any other mechanism. Debug mode is only set programmatically via `config.set_debug(True)` or `express.set_debug_mode(True)`.

**Impact:** Operators who set `MIDWICKET_DEBUG=true` in a container env will see no effect and no warning.

**Fix:**
Either add:
```python
# config.py
debug = os.getenv("MIDWICKET_DEBUG", "false").lower() == "true"
```
or remove the `MIDWICKET_DEBUG` references from both docs and replace them with the correct programmatic API.

---

### MW-013
**Category:** Bug > Logic
\*\*Status:\*\* RESOLVED
**Severity:** P3
**Location:** `midwicket/core/canonicalize.py:148`

**Symptom:**
```python
dismissal_type = wicket_mapping.get(wicket_kind.lower(), DismissalType.BOWLED)
```
Unknown wicket kinds fall through to `BOWLED` with no log message. Future Cricsheet format updates (e.g., adding `"timed out"`) or malformed JSON would silently inflate bowled counts.

**Fix:**
```python
known_type = wicket_mapping.get(wicket_kind.lower())
if known_type is None:
    logger.warning(
        "canonicalize_match: unknown wicket kind %r in match %s — stored as BOWLED",
        wicket_kind, match_id,
    )
    dismissal_type = DismissalType.BOWLED
else:
    dismissal_type = known_type
```

---

### MW-014
**Category:** Bug > Functional (UX)
\*\*Status:\*\* RESOLVED
**Severity:** P3
**Location:** `midwicket/visuals/worm.py` (`plot_beehive`, `plot_wagon_wheel`)

**Symptom:**
Both `plot_beehive` and `plot_wagon_wheel` generate visualizations using `random.uniform` / `random.choice` to place shot-location dots on the pitch/field rather than using real ball-by-ball pitch map and shot direction data from `ball_events`. The resulting charts look plausible but are completely fictional.

**Impact:** Any visualization produced by these functions is statistically meaningless. Broadcasting or analytical use of these charts would misrepresent actual match data.

**Fix:**
Either:
1. Mark both functions as `_demo_only` and document that they require pitch-map data not present in the Cricsheet schema.
2. Remove random-data generation and raise `NotImplementedError("Pitch map data not available in this dataset")` until the schema includes shot-direction fields.

---

### MW-015
**Category:** Security Issue > Configuration
\*\*Status:\*\* RESOLVED
**Severity:** P3
**Location:** `midwicket/serve/api.py:141–148` (CORS setup)

**Symptom:**
```python
origins = [o for o in API_CORS_ORIGINS if o and o != "*"]
self.app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=bool(origins),
    ...
)
```
When `MIDWICKET_CORS_ORIGINS` is not set (the default), `origins` is an empty list. Passing `allow_origins=[]` to `CORSMiddleware` means all cross-origin requests are silently rejected with no CORS headers — rather than returning a 403 with an explanatory message. Browser-based API clients will see opaque network errors and have no path to diagnose the issue.

**Impact:** First-time operators connecting a frontend to the API will get silent failures. No log is emitted.

**Fix:**
Log a startup warning when `origins` is empty:
```python
if not origins:
    logger.warning(
        "MIDWICKET_CORS_ORIGINS is not set. All cross-origin browser requests "
        "will be rejected. Set MIDWICKET_CORS_ORIGINS to allow browser clients."
    )
```

---

### MW-016
**Category:** Performance Issue > Configuration (CI)
\*\*Status:\*\* RESOLVED
**Severity:** P3
**Location:** `.github/workflows/ci.yml`

**Symptom:**
Advisory CI jobs (lint, type-check, security audit) are defined but do not block merges. A failing lint or type-check does not prevent a PR from being merged. Over time this leads to accumulated tech debt, type errors, and security findings going unnoticed.

**Impact:** Type errors, style regressions, and security advisories accumulate without enforcement.

**Fix:**
Add required status checks in the repository branch protection settings so that at minimum `Tests + coverage (py3.11)` and `Tests + coverage (py3.12)` are required, and consider making the lint job (`continue-on-error: false`) required as well.

---

## Known-good — do not break

These modules are well-implemented. Preserve their behavior when refactoring:

- **`serve/sql_guard.py`** — NFKC normalization, single-statement enforcement, DuckDB `json_serialize_sql` table resolution, keyword allowlist.
- **`serve/auth.py`** — Constant-time key comparison; bcrypt with pbkdf2 fallback; `is_api_key_required()` reads env at call time.
- **`api/head_to_head.py`** — Fast-path correctly returns `innings=0` (not `balls`) when `matchup_stats` lacks match count; slow-path uses ball-by-ball data for dot/boundary/six counts.
- **`api/player_analytics.py` (SQL logic)** — Uses `player_dismissed` correctly; non-striker run-outs handled via `player_dismissed = ?` subqueries; `_BOWLER_WICKET_EXPR` excludes fielding dismissals.
- **`api/fantasy.py`** — Bowling economy bonus uses per-match CTE; match-level economy computed before bonus accumulation.
- **`live/overlay.py`** — `ReuseAddressTCPServer` prevents `Address already in use` on restart; over notation uses `float(f"{int(over)}.{balls_in_over}")` (correct cricket format).
- **`data/loader.py`** — Atomic download, zip-slip guard, path-traversal guard, tenacity retries, schema validation, archive cleanup after extract.
- **`api/plugins.py`** — Allowlist-based plugin loading; rejects traversal/shell metachars; off in production by default.
- **`storage/registry.py`** — All DuckDB access under `self._lock`; bounded in-memory cache; parameterized DELETE + executemany for bulk upserts.
- **`compute/winprob.py`** — Correctly accepts `venue` and applies per-venue adjustment. Express path (`px.predict_win`) uses this correctly.
- **`serve/rate_limit.py`** — Redis rate limiter with in-process fallback; TOCTOU risk documented and accepted.
- **`core/canonicalize.py`** — Correct v1 schema construction; `player_dismissed` populated from `wickets[0].get('player')`; non-striker run-outs preserved.
