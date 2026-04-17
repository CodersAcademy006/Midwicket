import logging
import time
import threading
from typing import Any, Dict, Optional, Callable, List
import pyarrow as pa
from pydantic import BaseModel, Field, ConfigDict

_log = logging.getLogger(__name__)

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
        self._inflight_guard = threading.Lock()
        self._inflight_events: Dict[str, threading.Event] = {}

    @staticmethod
    def _query_timeout_seconds(query: BaseQuery) -> Optional[float]:
        """Best-effort extraction of per-query timeout budget in seconds."""
        opts = getattr(query, "execution_opts", None)
        timeout = getattr(opts, "timeout", None)
        if timeout is None:
            return None
        try:
            return float(timeout)
        except (TypeError, ValueError):
            return None

    def _enter_inflight(self, key: str) -> tuple[bool, threading.Event]:
        """Register an in-flight key. Returns (is_leader, event)."""
        with self._inflight_guard:
            existing = self._inflight_events.get(key)
            if existing is not None:
                return False, existing
            event = threading.Event()
            self._inflight_events[key] = event
            return True, event

    def _leave_inflight(self, key: str, event: threading.Event) -> None:
        """Release in-flight key and wake waiters."""
        with self._inflight_guard:
            current = self._inflight_events.get(key)
            if current is event:
                event.set()
                del self._inflight_events[key]
            else:
                event.set()

    def _table_available_for_plan(self, table_name: str) -> bool:
        """Best-effort existence check for a planned SQL target table."""
        table_exists_fn = getattr(self.engine, "table_exists", None)
        if not callable(table_exists_fn):
            return True

        try:
            if table_name == "ball_events":
                return bool(table_exists_fn(table_name))
            return bool(table_exists_fn(table_name, schema="derived"))
        except TypeError:
            # Backward compatibility for engines with table_exists(table_name)
            return bool(table_exists_fn(table_name))
        except Exception:
            return False

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

        # Import here to avoid circular dependencies at module import time.
        from pypitch.query.defs import WinProbQuery

        precomputed_plan: Optional[Dict[str, Any]] = None
        if mode == ExecutionMode.BUDGET and not isinstance(query, WinProbQuery):
            # Enforce guardrails before cache lookup so cached EXACT/raw results
            # cannot bypass BUDGET restrictions.
            precomputed_plan = self.planner.plan(query)
            if precomputed_plan.get("strategy", "raw_scan") != "materialized_view":
                raise RuntimeError(
                    f"ExecutionMode.BUDGET forbids raw scans, but no materialized "
                    f"view covers query {query.__class__.__name__}. "
                    f"Either materialise the required table first or use "
                    f"ExecutionMode.EXACT."
                )

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

        is_leader, inflight_event = self._enter_inflight(query_hash)
        if not is_leader:
            wait_timeout = self._query_timeout_seconds(query)
            waited = inflight_event.wait(timeout=wait_timeout)
            if not waited:
                raise TimeoutError(
                    f"Timed out waiting for in-flight query result after {wait_timeout}s"
                )
            cached_after_wait = self.cache.get(query_hash)
            if cached_after_wait is not None:
                if modes.debug_mode and hasattr(cached_after_wait, 'collect'):
                    cached_after_wait = cached_after_wait.collect()
                return ExecutionResult(
                    data=cached_after_wait,
                    meta=ResultMetadata(
                        query_hash=query_hash,
                        snapshot_id=query.snapshot_id,
                        execution_time_ms=(time.perf_counter() - start_time) * 1000,
                        source="cache"
                    )
                )

        # Leader computes and populates cache; waiters reuse it.
        try:
            # Special handling for WinProbQuery: call robust model, not SQL
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

            # ── Planner routing ───────────────────────────────────────────────
            # planner.plan() is the unified entry point; it checks the built-in
            # _QUERY_PREFERRED_TABLES map and query.requires["preferred_tables"]
            # against engine.derived_versions, selecting "materialized_view" when
            # a registered table is found, otherwise "raw_scan".
            plan = precomputed_plan or self.planner.plan(query)
            strategy = plan.get("strategy", "raw_scan")

            if strategy == "materialized_view":
                _log.debug(
                    "Planner: using materialized view %r for %s",
                    plan.get("target_table"),
                    query.__class__.__name__,
                )
            elif mode == ExecutionMode.BUDGET:
                raise RuntimeError(
                    f"ExecutionMode.BUDGET forbids raw scans, but no materialized "
                    f"view covers query {query.__class__.__name__}. "
                    f"Either materialise the required table first or use "
                    f"ExecutionMode.EXACT."
                )
            elif mode == ExecutionMode.APPROX:
                _log.warning(
                    "ExecutionMode.APPROX: no materialized view available for %s, "
                    "falling back to raw_scan",
                    query.__class__.__name__,
                )
            else:
                _log.debug(
                    "Planner: raw_scan for %s (no materialized view registered)",
                    query.__class__.__name__,
                )

            if strategy == "materialized_view":
                target_table = str(plan.get("target_table", ""))
                if not self._table_available_for_plan(target_table):
                    if mode == ExecutionMode.BUDGET:
                        raise RuntimeError(
                            f"ExecutionMode.BUDGET requires materialized views, "
                            f"but target table {target_table!r} is unavailable at execution time."
                        )
                    _log.warning(
                        "Planner selected materialized view %r but it is unavailable at execution time; "
                        "falling back to raw_scan",
                        target_table,
                    )
                    fallback_sql, fallback_params = self.planner._generate_sql(query, "ball_events")
                    plan = {
                        **plan,
                        "strategy": "raw_scan",
                        "target_table": "ball_events",
                        "sql": fallback_sql,
                        "params": fallback_params,
                    }
                    strategy = "raw_scan"

            result_table = self.engine.execute_sql(
                plan["sql"],
                params=plan.get("params"),
                timeout=self._query_timeout_seconds(query),
            )
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
        finally:
            if is_leader:
                self._leave_inflight(query_hash, inflight_event)

    def execute_metric(self, query: BaseQuery, metric_func: Callable[..., Any]) -> ExecutionResult:
        """
        Executes a specific metric function, handling dependencies.
        """
        start_time = time.perf_counter()
        
        # 1. Hash & Cache Check (Standard)
        # We include the metric name in the hash to differentiate results
        metric_name = getattr(metric_func, "__name__", "unknown_metric")
        metric_qualname = getattr(metric_func, "__qualname__", metric_name)
        metric_module = getattr(metric_func, "__module__", "unknown_module")
        query_hash = f"{query.cache_key}:{metric_module}.{metric_qualname}"

        cached = self.cache.get(query_hash)
        if cached is not None:
            return ExecutionResult(
                data=cached,
                meta=ResultMetadata(
                    query_hash=query_hash,
                    snapshot_id=query.snapshot_id,
                    execution_time_ms=(time.perf_counter() - start_time) * 1000,
                    source="cache"
                )
            )

        is_leader, inflight_event = self._enter_inflight(query_hash)
        if not is_leader:
            wait_timeout = self._query_timeout_seconds(query)
            waited = inflight_event.wait(timeout=wait_timeout)
            if not waited:
                raise TimeoutError(
                    f"Timed out waiting for in-flight metric result after {wait_timeout}s"
                )
            cached_after_wait = self.cache.get(query_hash)
            if cached_after_wait is not None:
                return ExecutionResult(
                    data=cached_after_wait,
                    meta=ResultMetadata(
                        query_hash=query_hash,
                        snapshot_id=query.snapshot_id,
                        execution_time_ms=(time.perf_counter() - start_time) * 1000,
                        source="cache"
                    )
                )

        try:
            # 2. Pre-Flight: Ensure Dependencies Exist
            if hasattr(metric_func, "_pypitch_spec"):
                for req in metric_func._pypitch_spec.requirements:
                    # This ensures 'derived.venue_baselines' exists in DuckDB
                    self.derived.ensure_materialized(
                        req["table"],
                        snapshot_id=query.snapshot_id
                    )

            # 3. Plan: Generate the Complex SQL
            sql, params = self.planner.create_plan(query, metric_func)

            # 4. Execute: Let DuckDB do the heavy lifting (JOIN)
            # This returns an Arrow Table with 'runs' AND 'venue_avg_sr' columns
            enriched_events = self.engine.execute_sql(
                sql,
                params=params,
                timeout=self._query_timeout_seconds(query),
            )

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
        finally:
            if is_leader:
                self._leave_inflight(query_hash, inflight_event)

