# Bugs and Issues Identified (Current PR Exploration)

## 1) Architecture bugs/smells

- **Compute layer owns storage I/O (violates agent contract):** `DerivedStore` in `pypitch/compute/derived/store.py` executes SQL and manages materialization state through `engine`. This conflicts with `Agents.md` guidance and the "Pure Functions Only" compute rule; materialization ownership should be moved to runtime/storage.
- **Planner does runtime table-existence I/O checks (violates planner contract):** `pypitch/runtime/planner.py` checks `engine.table_exists(...)` during planning. This conflicts with the planner role described in `Agents.md`; planner should consume dependency/snapshot metadata and fail fast when requirements are unavailable, instead of probing storage directly.
- **Duplicated storage execution paths:** `pypitch/storage/engine.py` and `pypitch/storage/thread_safe_engine.py` both implement SQL execution and connection behavior, increasing drift risk.

## 2) High-risk correctness bugs

- **Read pool connections are writable:** `ConnectionPool._create_connection(read_only=...)` currently opens DuckDB with `read_only=False` in `pypitch/storage/thread_safe_engine.py`, even when called for read-pool creation.
- **Feature/target/group misalignment in training prep:** in `pypitch/models/train.py`, feature rows are appended inside a `try` block and skipped on any exception from feature construction (e.g., invalid numeric values passed into `compute_chase_features`). Targets and groups are then built by iterating full second-innings rows, so skipped feature rows can desynchronize feature/target/group row mapping.
- **Query/schema type mismatch risk for IDs:** `MatchupQuery` models IDs as strings in `pypitch/query/base.py`, while Schema V1 stores actor IDs as integers in `pypitch/schema/v1.py`.

## 3) Security/risk concerns

- **Read-only enforcement bypass risk:** writable connections in the read pool weaken guardrails for read-only code paths in `pypitch/storage/thread_safe_engine.py`.
- **`/analyze` hardening is still listed as an open production risk:** tracked in `PRODUCTION_READINESS_GAPS.md` (security/readiness sections).
- **Rate-limiting scalability hardening remains pending:** tracked in `PRODUCTION_READINESS_GAPS.md` (rate-limit backend and scale validation sections).

## 4) Suggested fix priority

- **P0 (immediate):** Fix writable read-pool connections in `pypitch/storage/thread_safe_engine.py`.
- **P1 (next):** Fix training feature/target/group alignment in `pypitch/models/train.py`.
- **P1 (next):** Align query ID types/contracts with Schema V1 in `pypitch/query/base.py`.
- **P2 (planned refactor):** Clarify runtime/compute/storage boundaries around materialization (`pypitch/runtime/*` + `pypitch/compute/derived/store.py`).
- **P2 (planned hardening):** Complete `/analyze` governance and scalable rate-limit backend work in `PRODUCTION_READINESS_GAPS.md`.
