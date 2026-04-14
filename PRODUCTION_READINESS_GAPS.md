# PyPitch Production Readiness Gaps

## Overview
This document catalogs 14 critical and high-priority gaps that prevent PyPitch from being production-ready. These are organized by category and include implementation guidance for each.

## Status Update (2026-04-13)

### Recently Resolved
- API auth contract now supports `Authorization: Bearer <token>` with backward-compatible `X-API-Key`.
- `/analyze` payload contract normalized (`sql` key + positional `params` list), with backward-compatible `query` support.
- Prometheus-compatible `/metrics` exposition endpoint added and monitoring scrape path updated.
- `load_competition()` now applies competition + season filtering through `CricsheetLoader`.
- Live routes (`/live/register`, `/live/ingest`, `/live/matches`) now fail explicitly with 503 when no ingestor is configured.
- Trainable WinPredictor path is now implemented (`WinProbabilityTrainer`, `create_trained_model`, registry/path runtime loading, training script).
- `/analyze` now uses dedicated SQL guard validation and explicit read-only execution path.
- Rate limiting now supports DuckDB backend for cross-worker coordination (production default) with memory fallback.
- Readiness probes (`/ready`, `/v1/ready`) and drain mode request gating are now available.

### Still Active Critical/High Risks
- WinPredictor quality depends on data coverage/feature quality; current training pipeline is functional but needs richer feature sets and benchmark calibration for production confidence.
- `/analyze` is substantially safer, but policy hardening (table-level allowlists and cost governance) remains for high-security environments.
- DuckDB rate limiting is production-safe for moderate throughput, but Redis backend and large-scale load testing are still pending.
- Graceful lifecycle is improved with drain mode/readiness; deeper in-flight request draining metrics and orchestration playbooks are still pending.

---

## 1. ML & Analytics Core

### 1.1 WinPredictor Model Training Pipeline (CRITICAL)
**Status**: Partially implemented (training + deployment path available)  
**Impact**: Predictions are heuristic-based, not data-driven; accuracy unknown

**Current State**:
- Default shipped model still uses heuristic coefficients.
- `WinProbabilityTrainer` supports feature prep, training, CV metrics, and registry registration.
- `WinPredictor.create_trained_model()` now trains from provided data.
- Runtime loading supports default/registry/path modes via env vars.

**What's Needed**:
1. Historical match dataset (IPL T20 data from Cricsheet)
2. Feature extraction pipeline (batting averages, bowling averages, venue stats, matchup history)
3. Logistic regression training with scikit-learn
4. Cross-validation framework and performance metrics (ROC-AUC, Brier score)
5. Model versioning and persistence (joblib/pickle)
6. A/B testing framework to compare hand-tuned vs. trained models

**Dependencies**: scikit-learn, pandas, numpy (already available)  
**Estimated Scope**: 40-60 hours (data collection, cleaning, feature engineering, validation)

---

### 1.2 Player Performance Analytics (HIGH)
**Status**: Missing  
**Impact**: No player-level insights; API cannot service "best performers", "consistency analysis"

**What's Needed**:
1. Per-player aggregation queries (runs, wickets, strike rate, economy, consistency metrics)
2. Venue-specific performance (player X's record at Wankhede)
3. Form tracking (last 5 matches, 10 matches, seasonal trends)
4. Matchup-specific analytics (batter vs. bowler head-to-head)
5. API endpoints: `/players/{id}/stats`, `/players/{id}/vs/{opponent_id}`

**Dependencies**: DuckDB aggregation queries  
**Estimated Scope**: 20-30 hours

---

### 1.3 Venue & Pitch Analytics (MEDIUM)
**Status**: Missing  
**Impact**: Venue-specific predictions are rudimentary (fixed adjustment factors only)

**What's Needed**:
1. Venue-level aggregations (avg score, boundary%, wickets per innings, toss effects)
2. Pitch behavior tracking (flat pitches, rank turners, seaming pitches)
3. Historical trends per venue over seasons
4. API endpoint: `/venues/{name}/stats`

**Dependencies**: DuckDB aggregation  
**Estimated Scope**: 15-20 hours

---

### 1.4 Matchup Analysis Engine (MEDIUM)
**Status**: Missing  
**Impact**: No predictive signals for "how does Team A play against Team B?"

**What's Needed**:
1. Head-to-head win/loss records
2. Historical matchup trends (e.g., CSK vs. MI, always competitive)
3. Entity resolution for teams (franchise naming inconsistencies across data sources)
4. API endpoint: `/matchups/{team_a_id}/{team_b_id}/history`

**Dependencies**: Entity resolution tooling, DuckDB joins  
**Estimated Scope**: 15-20 hours

---

## 2. Data & Integration

### 2.1 Automated Weekly Data Pipeline (HIGH)
**Status**: Manual/offline only  
**Impact**: Match data stales; no real-time insights; requires manual Cricsheet downloads

**What's Needed**:
1. Scheduled job (cron or APScheduler) to fetch latest Cricsheet CSVs weekly
2. Incremental loading logic (skip already-ingested matches)
3. Data versioning (track ingestion dates, lineage)
4. Error handling and retry logic
5. Monitoring/alerting when pipeline fails

**Dependencies**: APScheduler, S3 or file storage for versioning  
**Estimated Scope**: 20-25 hours

---

### 2.2 Live Streaming / Real-Time Ball-by-Ball (HIGH)
**Status**: No live ingestion; data only available post-match  
**Impact**: Cannot support live predictions, live commentary

**What's Needed**:
1. Real-time data source integration (ESPN Cricinfo API, or cricket.com.au feeds)
2. Ball-by-ball ingestion with minimal lag (<5 seconds)
3. Lag compensation for predictions (adjust WinPredictor timing)
4. WebSocket support for live updates to frontend
5. Event deduplication (same ball received twice)

**Dependencies**: asyncio, websockets, real-time data API  
**Estimated Scope**: 40-60 hours

---

### 2.3 Data Quality & Validation (HIGH)
**Status**: Minimal; no schema validation on ingestion  
**Impact**: Garbage-in scenarios (malformed scores, duplicate matches, missing fields)

**What's Needed**:
1. Schema validation (pydantic models for Match, Innings, Ball)
2. Data consistency checks (e.g., runs don't exceed physical limits)
3. Duplicate detection and deduplication
4. Missing data imputation strategy
5. Data quality dashboards (% valid rows, missing fields, anomalies)

**Dependencies**: pydantic, Great Expectations (optional)  
**Estimated Scope**: 15-20 hours

---

## 3. API & Feature Completeness

### 3.1 Player Lookup & Autocomplete (MEDIUM)
**Status**: Missing  
**Impact**: Frontend cannot typeahead-search for players; /players endpoint missing

**What's Needed**:
1. `/players` endpoint with filtering (name, role, team, country)
2. Full-text search or prefix matching
3. Entity resolution for player name variations
4. API endpoint: `GET /players?q=virat` → returns `[{id, name, team, ...}]`

**Dependencies**: DuckDB text search, entity resolution  
**Estimated Scope**: 10-15 hours

---

### 3.2 Venue Lookup & Details (MEDIUM)
**Status**: Missing  
**Impact**: Frontend has no way to browse venues

**What's Needed**:
1. `/venues` endpoint with filtering (country, capacity, ground_name)
2. Venue details (coordinates, capacity, established date)
3. API endpoint: `GET /venues?country=India`

**Dependencies**: DuckDB queries  
**Estimated Scope**: 8-10 hours

---

### 3.3 Fantasy Points Calculation (HIGH)
**Status**: Missing completely  
**Impact**: Cannot support fantasy league use cases; incomplete feature set

**What's Needed**:
1. Per-player points system (configurable per league: IPL, BBL, etc.)
   - Batting: runs/4, runs/6, dismissal penalties
   - Bowling: wickets, maiden overs, economy bonuses
   - Fielding: catches, run-outs, stumpings
2. League-specific scoring variants
3. API endpoint: `GET /matches/{id}/fantasy?league=ipl` → returns player scores
4. Leaderboards / user team scoring

**Dependencies**: DuckDB queries, configurable scoring  
**Estimated Scope**: 25-35 hours

---

### 3.4 Match Filtering & Sorting (LOW)
**Status**: Partially implemented (improved)  
**Impact**: API users cannot filter by date range, format, status

**Current State**:
- Express API `load_competition()` now supports competition/season filtering.
- `/matches` endpoint filtering/sorting/pagination is still pending.

**What's Needed**:
1. Enhanced `/matches` endpoint: filter by date, format (T20/ODI), status (live/completed), venue
2. Sorting: by date, by competition, by teams
3. Pagination (limit/offset)

**Dependencies**: DuckDB WHERE clauses  
**Estimated Scope**: 10-12 hours

---

## 4. Production Hardening

### 4.1 Multi-Worker Rate Limiting (HIGH)
**Status**: Partially resolved (DuckDB backend added)  
**Impact**: Rate limiting bypassed in multi-process deployments (uvicorn --workers N)

**Current State**:
- Memory backend still exists for development.
- DuckDB backend now available and selected by default in production env.
- Backend is configurable through `PYPITCH_RATE_LIMIT_BACKEND`.

**What's Needed**:
1. Redis-backed rate limiter (or DuckDB-backed as alternative)
2. Sliding window or token bucket algorithm
3. Per-user / per-IP rate limit tracking
4. Configuration: limits per minute, per hour
5. Graceful degradation if Redis is unavailable

**Dependencies**: redis (optional), slowapi (current)  
**Estimated Scope**: 15-20 hours

---

### 4.2 SQL Injection Prevention (CRITICAL)
**Status**: Improved (guarded), further hardening recommended  
**Impact**: Potential code execution via crafted SQL

**Current State**:
- `/analyze` now has a stable payload contract (`sql`, optional positional `params`).
- Validation now runs through `pypitch.serve.sql_guard` (single statement, read-only starts, forbidden-token checks, complexity bounds, comment denial).

**What's Needed**:
1. Replace keyword blocklist with **whitelist** of allowed operations
2. Use parameterized queries for all user input
3. Enforce a read-only query path (DuckDB read-only constraints / restricted execution path)
4. Query cost estimation (reject expensive queries before execution)
5. Audit logging of all `/analyze` queries

**Dependencies**: DuckDB read-only mode, query analysis  
**Estimated Scope**: 10-15 hours

---

### 4.3 Audit Logging & Compliance (HIGH)
**Status**: Minimal; no audit trail  
**Impact**: Cannot prove data access compliance; no forensics capability

**What's Needed**:
1. Structured audit logs (who, what, when, why)
2. Log targets: /analyze queries, bulk exports, API key usage
3. Immutable log storage (append-only, signed)
4. Data retention policy (e.g., 90 days of logs)
5. GDPR-compliant data deletion tracking

**Dependencies**: structlog, audit log storage  
**Estimated Scope**: 20-25 hours

---

### 4.4 API Documentation & OpenAPI (MEDIUM)
**Status**: Partially implemented  
**Impact**: Clients lack formal schema; harder to generate SDKs

**Current State**:
- FastAPI already serves OpenAPI and docs (`/v1/openapi.json`, `/v1/docs`, `/v1/redoc`).
- Endpoint-level schema completeness and docs consistency still need hardening.

**What's Needed**:
1. Comprehensive OpenAPI 3.0 spec for all endpoints
2. Auto-generated Swagger UI (`/docs`)
3. Response schemas (Pydantic models)
4. Error code documentation
5. Rate limit headers in responses

**Dependencies**: FastAPI (already has this built-in)  
**Estimated Scope**: 8-12 hours (mostly documentation)

---

### 4.5 Health & Readiness Checks (MEDIUM)
**Status**: Substantially improved  
**Impact**: Kubernetes/orchestrators cannot distinguish "ready" from "up but broken"

**Current State**:
- `/health` and `/v1/health` are available.
- `/ready` and `/v1/ready` now exist with DB checks and drain awareness.
- Drain mode rejects new non-probe traffic with 503.

**What's Needed**:
1. `/health` → liveness (is the service running?)
2. `/ready` → readiness (can the service handle requests? Is DB connected?)
3. Dependency checks: DuckDB connectivity, data schema version
4. Graceful shutdown (drain in-flight requests, stop accepting new ones)

**Dependencies**: FastAPI lifecycle hooks  
**Estimated Scope**: 8-10 hours

---

### 4.6 Error Handling & Observability (MEDIUM)
**Status**: Partial; improving  
**Impact**: Difficult to debug; poor error context for API clients

**Current State**:
- Global exception handlers are present.
- Prometheus exposition endpoint (`/metrics`) is available.
- Structured error codes, request correlation IDs, and alert policies are still pending.

**What's Needed**:
1. Structured error responses (error_code, message, context)
2. Exception middleware (catch unhandled exceptions, log them)
3. Request ID propagation (trace a single request across logs)
4. Slow query detection and alerting
5. Error budgeting and alerting thresholds

**Dependencies**: structlog, Prometheus metrics  
**Estimated Scope**: 12-15 hours

---

## Summary Table

| Category | Issue | Priority | Scope (hours) | Dependencies |
|----------|-------|----------|---------------|--------------|
| **ML & Analytics** | WinPredictor Training | CRITICAL | 40-60 | scikit-learn, pandas |
| | Player Performance Analytics | HIGH | 20-30 | DuckDB |
| | Venue Analytics | MEDIUM | 15-20 | DuckDB |
| | Matchup Analysis | MEDIUM | 15-20 | DuckDB, entity resolution |
| **Data & Integration** | Weekly Data Pipeline | HIGH | 20-25 | APScheduler |
| | Live Streaming | HIGH | 40-60 | asyncio, real-time API |
| | Data Quality & Validation | HIGH | 15-20 | pydantic |
| **API & Features** | Player Lookup | MEDIUM | 10-15 | DuckDB |
| | Venue Lookup | MEDIUM | 8-10 | DuckDB |
| | Fantasy Points | HIGH | 25-35 | DuckDB, config system |
| | Match Filtering | LOW | 10-12 | DuckDB |
| **Production** | Multi-Worker Rate Limiting | HIGH | 15-20 | redis (optional) |
| | SQL Injection Prevention | CRITICAL | 10-15 | DuckDB, parameterized queries |
| | Audit Logging | HIGH | 20-25 | structlog |
| | OpenAPI Docs | MEDIUM | 8-12 | FastAPI (built-in) |
| | Health Checks | MEDIUM | 8-10 | FastAPI |
| | Error Handling | MEDIUM | 12-15 | structlog |

**Total Estimated Scope**: ~380-500 hours  
**Critical Path** (blockers for release): WinPredictor Training, SQL Injection Prevention, Multi-Worker Rate Limiting, Live Streaming

---

## Recommended Phasing

### Phase 1: Critical Fixes (Weeks 1-2)
- [ ] SQL Injection Prevention
- [ ] Multi-Worker Rate Limiting
- [ ] Audit Logging & Compliance

### Phase 2: Core ML & Analytics (Weeks 3-5)
- [ ] WinPredictor Training Pipeline
- [ ] Player Performance Analytics
- [ ] Venue Analytics

### Phase 3: Data Pipeline (Weeks 6-7)
- [ ] Weekly Data Pipeline
- [ ] Data Quality & Validation

### Phase 4: Live Features (Weeks 8-10)
- [ ] Live Streaming / Real-Time Ball-by-Ball
- [ ] Fantasy Points Calculation

### Phase 5: Polish (Weeks 11-12)
- [ ] Player Lookup & Autocomplete
- [ ] Venue Lookup
- [ ] Match Filtering & Sorting
- [ ] Matchup Analysis
- [ ] OpenAPI Documentation
- [ ] Health & Readiness Checks
- [ ] Error Handling & Observability

