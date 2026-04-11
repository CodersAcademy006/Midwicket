import time
from typing import Any, Dict, Optional, Callable
import pyarrow as pa
from pydantic import BaseModel, Field, ConfigDict

# Internal imports (mocked for structure, you must implement these interfaces)
from pypitch.query.base import BaseQuery
from pypitch.runtime.cache import CacheInterface
from pypitch.storage.engine import QueryEngine
from pypitch.runtime.planner import QueryPlanner
from pypitch.compute.derived import DerivedStore
from . import modes
from .modes import ExecutionMode  # noqa: F401 – re-exported for callers

class ResultMetadata(BaseModel):
    """
    The "Nutrition Label" for your data.
    Every result MUST carry this. No naked numbers.
    """
    query_hash: str
    snapshot_id: str
    execution_time_ms: float
    source: str = Field(..., description="cache or compute")
    engine_version: str = "v1.0.0"

class ExecutionResult(BaseModel):
    data: Any  # In production, this is a pyarrow.Table or specific Metric object
    meta: ResultMetadata

    model_config = ConfigDict(arbitrary_types_allowed=True)

class RuntimeExecutor:
    def __init__(self, cache: CacheInterface, engine: QueryEngine):
        self.cache = cache
        self.engine = engine
        self.planner = QueryPlanner(engine)
        self.derived = DerivedStore(engine)

    def execute(self, query: BaseQuery, mode: ExecutionMode = ExecutionMode.EXACT) -> ExecutionResult:
        """
        Main execute for all queries, including WinProbQuery (win probability model).

        Args:
            query:  The query object describing what data to fetch.
            mode:   ExecutionMode controlling cost/accuracy trade-offs.
                    - EXACT  (default): full raw-event scan allowed.
                    - APPROX: prefer materialized views; falls back to raw scan.
                    - BUDGET: strictly materialized views only; raises if unavailable.

        Note (0.1.x):
            Non-WinProb queries currently route through ``create_legacy_plan``
            which always performs a raw scan.  Full Planner-first routing
            (prefer-materialization, BUDGET guard) is tracked for v0.2.x.
        """
        start_time = time.perf_counter()
        query_hash = query.cache_key

        cached_data = self.cache.get(query_hash)
        if cached_data is not None:
            if modes.debug_mode and hasattr(cached_data, 'collect'):
                cached_data = cached_data.collect()
            return ExecutionResult(
                data=cached_data,
                meta=ResultMetadata(
                    query_hash=query_hash,
                    snapshot_id=query.snapshot_id,
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    source="cache"
                )
            )

        # Special handling for WinProbQuery: call robust model, not SQL
        from pypitch.query.defs import WinProbQuery
        if isinstance(query, WinProbQuery):
            from pypitch.compute.winprob import win_probability
            result = win_probability(
                target=query.target_score,
                current_runs=query.current_runs,
                wickets_down=query.current_wickets,
                overs_done=20.0 - query.overs_remaining,
                venue=None  # Optionally pass venue name/id if model supports
            )
            self.cache.set(query_hash, result)
            return ExecutionResult(
                data=result,
                meta=ResultMetadata(
                    query_hash=query_hash,
                    snapshot_id=query.snapshot_id,
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    source="compute"
                )
            )

        # ── 0.1.x: legacy planning path ──────────────────────────────────────
        # create_legacy_plan returns a dict with {"strategy", "sql", "cost"}.
        # We enforce ExecutionMode here even though full Planner-first routing
        # (prefer-materialization, cost estimation) is planned for v0.2.x.
        import logging
        _log = logging.getLogger(__name__)

        plan = self.planner.create_legacy_plan(query)

        if mode == ExecutionMode.BUDGET and plan["strategy"] == "raw_scan":
            raise RuntimeError(
                f"ExecutionMode.BUDGET forbids raw scans, but no materialized "
                f"view covers query {query.__class__.__name__}. "
                f"Either materialise the required table first or use "
                f"ExecutionMode.EXACT."
            )
        if mode == ExecutionMode.APPROX and plan["strategy"] == "raw_scan":
            _log.warning(
                "ExecutionMode.APPROX: falling back to raw_scan for %s "
                "(no materialized view available)",
                query.__class__.__name__,
            )

        result_table = self.engine.execute_sql(plan["sql"])
        if modes.debug_mode and hasattr(result_table, 'collect'):
            result_table = result_table.collect()
        self.cache.set(query_hash, result_table)
        return ExecutionResult(
            data=result_table,
            meta=ResultMetadata(
                query_hash=query_hash,
                snapshot_id=query.snapshot_id,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                source="compute"
            )
        )

    def execute_metric(self, query: BaseQuery, metric_func: Callable[..., Any]) -> ExecutionResult:
        """
        Executes a specific metric function, handling dependencies.
        """
        start_time = time.perf_counter()
        
        # 1. Hash & Cache Check (Standard)
        # We include the metric name in the hash to differentiate results
        metric_name = getattr(metric_func, "__name__", "unknown_metric")
        query_hash = f"{query.cache_key}:{metric_name}"
        
        if cached := self.cache.get(query_hash):
            return ExecutionResult(
                data=cached,
                meta=ResultMetadata(
                    query_hash=query_hash,
                    snapshot_id=query.snapshot_id,
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    source="cache"
                )
            )

        # 2. Pre-Flight: Ensure Dependencies Exist
        if hasattr(metric_func, "_pypitch_spec"):
            for req in metric_func._pypitch_spec.requirements:
                # This ensures 'derived.venue_baselines' exists in DuckDB
                self.derived.ensure_materialized(
                    req["table"], 
                    snapshot_id=query.snapshot_id
                )

        # 3. Plan: Generate the Complex SQL
        sql_plan = self.planner.create_plan(query, metric_func)

        # 4. Execute: Let DuckDB do the heavy lifting (JOIN)
        # This returns an Arrow Table with 'runs' AND 'venue_avg_sr' columns
        enriched_events = self.engine.execute_sql(sql_plan)

        # 5. Compute: Run the Pure Function
        # The metric simply expects column 'venue_avg_sr' to exist
        result_value = metric_func(enriched_events)

        # 6. Cache & Return
        self.cache.set(query_hash, result_value)

        return ExecutionResult(
            data=result_value,
            meta=ResultMetadata(
                query_hash=query_hash,
                snapshot_id=query.snapshot_id,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                source="compute"
            )
        )

