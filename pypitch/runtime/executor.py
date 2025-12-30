import time
from typing import Dict, Any

from pypitch.query.base import BaseQuery, QueryContext
from pypitch.runtime.modes import ExecutionMode
from pypitch.runtime.cache import CacheInterface
from pypitch.storage.engine import StorageEngine

# Constants for context construction
PLANNER_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

class Executor:
    def __init__(self, engine: StorageEngine, cache: CacheInterface):
        self.engine = engine
        self.cache = cache

    def execute(self, query: BaseQuery) -> Dict[str, Any]:
        """
        The Master Logic Flow:
        1. Contextualize -> 2. Hash -> 3. Cache Check -> 4. Execute -> 5. Wrap
        """
        
        # 1. Build Context (The State of the World)
        ctx = QueryContext(
            schema_version=SCHEMA_VERSION,
            snapshot_id=self.engine.snapshot_id,
            planner_version=PLANNER_VERSION,
            derived_versions=self.engine.derived_versions
        )

        # 2. Generate Deterministic Hash
        cache_key = query.compute_cache_key(ctx)

        # 3. Check Cache
        if self.cache.exists(cache_key):
            # In production, you might log a "CACHE_HIT" metric here
            return self.cache.get(cache_key)

        # 4. Enforce Budget / Planning (Basic Logic for Stage 1)
        if query.mode == ExecutionMode.BUDGET:
             # Placeholder: logic to reject expensive queries
             # if 'ball_events' in query.requires['fallback_table']: raise BudgetExceeded(...)
             pass

        # 5. Execute (Delegated to private runner)
        # Note: A real Planner would inject the SQL generation strategy here.
        # For Stage 1, we assume a simple raw scan.
        result_arrow = self._run_computation(query)

        # 6. Wrap & Store
        response = {
            "data": result_arrow,
            "meta": {
                "execution_mode": query.mode,
                "snapshot_id": ctx.snapshot_id,
                "timestamp": time.time(),
                "confidence": 1.0 # Placeholder for Stage 1
            }
        }
        
        self.cache.set(cache_key, response)
        return response

    def _run_computation(self, query: BaseQuery):
        """
        Private method to actually run the logic.
        This will eventually be replaced by the Planner routing logic.
        """
        # TEMP: Simple hardcoded SQL generation for MatchupQuery testing
        if query.__class__.__name__ == "MatchupQuery":
            batter_tuple = tuple(query.batter_ids) if len(query.batter_ids) > 1 else f"({query.batter_ids[0]})"
            bowler_tuple = tuple(query.bowler_ids) if len(query.bowler_ids) > 1 else f"({query.bowler_ids[0]})"
            
            sql = f"""
                SELECT 
                    sum(runs_batter) as runs, 
                    count(*) as balls, 
                    sum(case when is_wicket=true then 1 else 0 end) as wickets
                FROM ball_events 
                WHERE batter_id IN {batter_tuple}
                  AND bowler_id IN {bowler_tuple}
            """
            return self.engine.execute_sql(sql)
            
        raise NotImplementedError(f"No execution strategy for {query.__class__.__name__}")
