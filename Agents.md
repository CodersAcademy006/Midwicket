# System Agents & Actors

This document defines the core **Agents** (active system components) responsible for the lifecycle of a query in `pypitch`. 

**Philosophy:** - Agents are **specialized**: The Storage Agent does not compute math; the Compute Agent does not touch disk.
- Agents are **contract-bound**: They communicate exclusively via Schema V1 and Query Objects.

---

## 1. The Gatekeeper (Runtime Executor)
**Codepath:** `pypitch.runtime.executor.Executor`

The Gatekeeper is the single entry point for all data retrieval. It is the only component allowed to coordinate between the Cache, the Planner, and the Storage Engine.

### Responsibilities
* **Guardrails:** Enforces `ExecutionMode` (Budget vs. Exact). Rejects queries that exceed complexity limits in Budget mode.
* **Orchestration:** Calls the Planner to determine the best path, checks the Cache, then invokes the Compute layer.
* **Metadata stamping:** Attaches `confidence`, `snapshot_id`, and `execution_time` to every result.

### Strict Rules
1.  **Never Bypass Cache Logic:** Every `execute()` call must generate a deterministic hash and check the cache first.
2.  **No Direct I/O:** It must delegate actual data fetching to the Storage Agent.
3.  **Result Integrity:** It must never modify the data returned by the Engine; it only wraps it in metadata.

---

## 2. The Planner (Query Optimizer)
**Codepath:** `pypitch.runtime.planner.QueryPlanner`

The Planner analyzes the **Intent** (Query Object) and decides the most efficient execution strategy based on the **Context** (Available Snapshots & Derived Tables).

### Responsibilities
* **Dependency Analysis:** Inspects `query.requires` to see which tables are needed.
* **Routing:** Decides whether to:
    * **Scan Raw Events:** (High cost, infinite flexibility)
    * **Fetch Materialized View:** (Low cost, pre-computed)
* **Resolution Strategy:** Determines if Identity Resolution (Name -> ID) is needed before execution.

### Strict Rules
1.  **Prefer Materialization:** Always route to a derived table if it exists and covers the query scope.
2.  **Fail Fast:** If a required table is missing from the current Snapshot, raise a `DependencyError` immediately.

---

## 3. The Archivist (Storage Engine)
**Codepath:** `pypitch.storage.engine.StorageEngine`

The Archivist manages the physical persistence layer (DuckDB/Parquet). It is the guardian of the **Schema V1 Contract**.

### Responsibilities
* **Ingestion:** Accepts incoming Arrow Tables, validates them against `schema.v1`, and rejects invalid data.
* **Snapshot Management:** Manages immutable data versions (`2024-01-01`, `2024-01-02`).
* **Query Execution:** Runs the actual SQL/Arrow compute requested by the Gatekeeper.

### Strict Rules
1.  **Schema Enforcement:** Write operations **must** fail if the data does not strictly match the Schema V1 definition.
2.  **Immutability:** Once a Snapshot is created, it is read-only. No updates, only new Snapshots.

---

## 4. The Identity Manager (Registry)
**Codepath:** `pypitch.storage.registry.IdentityRegistry`

The Identity Manager handles the complexity of "Time-Aware Identity." It ensures that entities (Players, Teams, Venues) remain consistent across decades of data.

### Responsibilities
* **Resolution:** Converts `("Virat Kohli", "2016-05-18")` -> `ID: 101`.
* **Alias Handling:** Resolves `R Pant`, `Rishabh Pant`, and `Pant, R` to the same ID.
* **Team Evolution:** Knows that "Delhi Daredevils" (2012) and "Delhi Capitals" (2020) are the same entity ID.

### Strict Rules
1.  **Time-Travel Consistency:** Resolving a name **must** require a date context.
2.  **No Duplicates:** A single entity ID must never refer to two different real-world people.

---

## 5. The Analyst (Compute Engine)
**Codepath:** `pypitch.compute`

The Analyst contains the pure mathematical logic. It is divided into **Builders** (who create tables) and **Calculators** (who produce metrics).

### Responsibilities
* **Derived Builders:** `pypitch.compute.derived` - Transforms Raw Events into optimized tables (e.g., `PhaseStats`).
* **Metric Calculators:** `pypitch.compute.metrics` - Pure functions that take tables and return scalars (e.g., `calculate_impact_score`).

### Strict Rules
1.  **Pure Functions Only:** Calculators must not access the database or cache. Input -> Math -> Output.
2.  **Vectorization:** All logic must operate on Arrow Arrays/Columns, never Python loops.