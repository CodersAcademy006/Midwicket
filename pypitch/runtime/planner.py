from typing import Dict, Any
from pypitch.query.base import BaseQuery

class QueryPlanner:
    def __init__(self, engine):
        self.engine = engine

    def plan(self, query: BaseQuery) -> Dict[str, Any]:
        """
        Analyzes query dependencies and returns an execution plan.
        """
        reqs = query.requires
        available_tables = self.engine.derived_versions.keys()

        # 1. Check if we can use a preferred materialized view
        for table in reqs.get("preferred_tables", []):
            if table in available_tables:
                return {
                    "strategy": "materialized_view",
                    "target_table": table,
                    "cost": "low"
                }

        # 2. Fallback to raw scan
        return {
            "strategy": "raw_scan",
            "target_table": reqs.get("fallback_table", "ball_events"),
            "cost": "high"
        }
