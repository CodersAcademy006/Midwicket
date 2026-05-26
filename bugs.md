# Bugs and Issues Identified (Current PR Exploration)

## 1) Architecture bugs/smells

- **Compute layer owns storage I/O:** `DerivedStore` in `pypitch/compute/derived/store.py` executes SQL and manages materialization state through `engine`, which blurs the compute/storage boundary.
- **Planner does runtime table-existence I/O checks:** `pypitch/runtime/planner.py` checks `engine.table_exists(...)` during planning, increasing coupling between planning and storage internals.
- **Duplicated storage execution paths:** `pypitch/storage/engine.py` and `pypitch/storage/thread_safe_engine.py` both implement SQL execution and connection behavior, increasing drift risk.

## 2) High-risk correctness bugs

- **Read pool connections are writable:** `ConnectionPool._create_connection(read_only=...)` always calls `duckdb.connect(..., read_only=False)` (`pypitch/storage/thread_safe_engine.py:82-87`).
- **Potential feature/target/group misalignment in training prep:** `prepare_training_data()` may skip feature rows on exceptions, while target/group derivation is based on full second-innings iteration (`pypitch/models/train.py:64-107`, `pypitch/models/train.py:166-172`).
- **Query/schema type mismatch risk for IDs:** `MatchupQuery` models IDs as strings (`pypitch/query/base.py:52-56`) while Schema V1 stores actor IDs as integers (`pypitch/schema/v1.py:92-97`).

## 3) Security/risk concerns

- **Read-only enforcement bypass risk:** writable connections in the read pool weaken guardrails for read-only code paths (`pypitch/storage/thread_safe_engine.py:82-87`).
- **`/analyze` hardening is still listed as an open production risk:** tracked in `PRODUCTION_READINESS_GAPS.md` (`PRODUCTION_READINESS_GAPS.md:21-22`, `PRODUCTION_READINESS_GAPS.md:246-251`).
- **Rate-limiting scalability hardening remains pending:** tracked in `PRODUCTION_READINESS_GAPS.md` (`PRODUCTION_READINESS_GAPS.md:22-23`, `PRODUCTION_READINESS_GAPS.md:227-234`).

## 4) Suggested fix priority

- **P0 (immediate):** Fix writable read-pool connections in `pypitch/storage/thread_safe_engine.py`.
- **P1 (next):** Fix training feature/target/group alignment in `pypitch/models/train.py`.
- **P1 (next):** Align query ID types/contracts with Schema V1 in `pypitch/query/base.py`.
- **P2 (planned refactor):** Clarify runtime/compute/storage boundaries around materialization (`pypitch/runtime/*` + `pypitch/compute/derived/store.py`).
- **P2 (planned hardening):** Complete `/analyze` governance and scalable rate-limit backend work in `PRODUCTION_READINESS_GAPS.md`.
