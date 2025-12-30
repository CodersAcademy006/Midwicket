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

    def execute(self, query: BaseQuery) -> ExecutionResult:
        """
        Legacy execute for simple queries (returns Arrow Table).
        """
        start_time = time.perf_counter()
        query_hash = query.cache_key
        
        cached_data = self.cache.get(query_hash)
        if cached_data is not None:
            return ExecutionResult(
                data=cached_data,
                meta=ResultMetadata(
                    query_hash=query_hash,
                    snapshot_id=query.snapshot_id,
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    source="cache"
                )
            )

        # Use legacy plan for now to maintain backward compatibility with existing tests
        plan = self.planner.create_legacy_plan(query)
        result_table = self.engine.execute_sql(plan["sql"])
        
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

        # 3. Planning (The Optimizer)
        # Never send raw logic to the engine. Send a Plan.
        # This allows you to reject "Scan Full History" queries before they run.
        plan = self.planner.create_plan(query)
        
        # 4. Execution (The Heavy Lift)
        try:
            result_data = self.engine.run(plan)
        except Exception as e:
            # You must log the query_hash here for debugging
            raise RuntimeError(f"Query {query_hash} failed: {str(e)}") from e

        # 5. Persistence (The Memory)
        # We only cache if execution succeeded.
        self.cache.set(query_hash, result_data, ttl=query.execution_opts.timeout)

        return ExecutionResult(
            data=result_data,
            meta=ResultMetadata(
                query_hash=query_hash,
                snapshot_id=query.snapshot_id,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                source="compute"
            )
        )

