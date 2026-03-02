"""
34_query_engine_demo.py — QueryEngine, Schema & Execution Pipeline

Shows the full stack from Arrow table creation through schema validation,
ingest, and SQL execution — all in-memory, no downloads required.

Layers demonstrated:
  Schema V1 → QueryEngine.ingest_events() → execute_sql() → Arrow Table

Usage:
    python examples/34_query_engine_demo.py
"""

import pyarrow as pa
from pypitch.schema.v1 import BALL_EVENT_SCHEMA
from pypitch.storage.engine import QueryEngine
from pypitch.runtime.executor import RuntimeExecutor
from pypitch.runtime.cache import CacheInterface
from pypitch.runtime.planner import QueryPlanner
from pypitch.compute.derived import DerivedStore
from pypitch.query.base import MatchupQuery


# ---------------------------------------------------------------------------
# Minimal in-memory cache (satisfies the CacheInterface protocol)
# ---------------------------------------------------------------------------
class MemoryCache(CacheInterface):
    def __init__(self):
        self._store = {}

    def get(self, key: str):
        return self._store.get(key)

    def set(self, key: str, value, ttl: int = 3600) -> None:
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()

    def close(self) -> None:
        pass


def make_sample_table() -> pa.Table:
    """Build a tiny Schema V1-conformant table with 10 ball events."""
    n = 10
    return pa.table(
        {
            "match_id":      pa.array(["m001"] * n, type=pa.string()),
            "inning":        pa.array([1] * n,       type=pa.int32()),
            "over":          pa.array(list(range(n)), type=pa.int32()),
            "ball":          pa.array([1] * n,        type=pa.int32()),
            "batter_id":     pa.array([1] * n,        type=pa.int32()),
            "bowler_id":     pa.array([2] * n,        type=pa.int32()),
            "venue_id":      pa.array([10] * n,       type=pa.int32()),
            "runs_batter":   pa.array([4, 0, 6, 1, 2, 0, 1, 4, 6, 0], type=pa.int32()),
            "runs_extras":   pa.array([0] * n,        type=pa.int32()),
            "is_wicket":     pa.array([False] * 9 + [True],            type=pa.bool_()),
            "wicket_type":   pa.array([""] * 9 + ["caught"],           type=pa.string()),
            "phase":         pa.array(["Powerplay"] * n,               type=pa.string()),
            "season":        pa.array([2023] * n,                      type=pa.int32()),
        },
        schema=BALL_EVENT_SCHEMA,
    )


def main() -> None:
    print("PyPitch Query Engine Demo — in-memory")
    print("=" * 50)

    # ------------------------------------------------------------------
    # 1. Build engine and ingest sample data
    # ------------------------------------------------------------------
    engine = QueryEngine(db_path=":memory:")
    table  = make_sample_table()

    print(f"\n[1] Ingesting {table.num_rows} ball events…")
    engine.ingest_events(table, snapshot_tag="demo_2023", append=False)
    print("  Ingestion complete.")

    # ------------------------------------------------------------------
    # 2. Raw SQL query
    # ------------------------------------------------------------------
    print("\n[2] Raw SQL: total runs and wickets")
    result = engine.execute_sql(
        "SELECT SUM(runs_batter) AS total_runs, SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wickets FROM ball_events"
    )
    row = result.to_pandas().iloc[0]
    print(f"  Total runs : {row['total_runs']}")
    print(f"  Wickets    : {row['wickets']}")

    # ------------------------------------------------------------------
    # 3. Executor with cache
    # ------------------------------------------------------------------
    print("\n[3] RuntimeExecutor with in-memory cache")
    cache    = MemoryCache()
    executor = RuntimeExecutor(cache=cache, engine=engine)

    q = MatchupQuery(batter_id="1", bowler_id="2", snapshot_id="demo_2023")
    res = executor.execute(q)

    df = res.data.to_pandas() if hasattr(res.data, "to_pandas") else res.data
    print(f"  Query hash     : {res.meta.query_hash[:16]}…")
    print(f"  Source         : {res.meta.source}")
    print(f"  Exec time (ms) : {res.meta.execution_time_ms:.2f}")
    if hasattr(df, "to_string"):
        print(f"  Result:\n{df.to_string(index=False)}")

    # Second call — should come from cache
    res2 = executor.execute(q)
    print(f"\n  Re-run source  : {res2.meta.source}  (expected: cache)")

    # ------------------------------------------------------------------
    # 4. Schema validation — expect rejection on bad schema
    # ------------------------------------------------------------------
    print("\n[4] Schema enforcement")
    bad_table = pa.table({"col_a": [1, 2, 3]})
    try:
        engine.ingest_events(bad_table, snapshot_tag="bad")
    except ValueError as exc:
        print(f"  Bad schema correctly rejected: {exc}")

    engine.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
