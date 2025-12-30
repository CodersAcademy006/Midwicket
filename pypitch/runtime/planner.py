from typing import Dict, Any, Optional, Callable
from pypitch.query.base import BaseQuery

class QueryPlanner:
    def __init__(self, engine):
        self.engine = engine

    def create_plan(self, query: BaseQuery, metric_func: Optional[Callable] = None) -> str:
        """
        Constructs the optimized SQL with all necessary JOINs.
        """
        base_filters = self._build_where_clause(query)
        
        # 1. Analyze Dependencies
        joins = []
        selects = ["e.*"]  # Select all raw events
        
        if metric_func and hasattr(metric_func, "_pypitch_spec"):
            for req in metric_func._pypitch_spec.requirements:
                table = req["table"]
                key = req["key"]
                
                # Register the JOIN
                # Assuming 'derived' schema holds materialized views
                joins.append(f"""
                    LEFT JOIN derived.{table} AS {table}
                    ON e.{key} = {table}.{key}
                """)
                
                # Select the context columns (avoiding collisions if needed)
                selects.append(f"{table}.*")

        # 2. Assemble the Query
        # We assume the main event table is aliased as 'e'
        join_clause = "\n".join(joins)
        select_clause = ", ".join(selects)
        
        sql = f"""
            SELECT {select_clause}
            FROM ball_events AS e
            {join_clause}
            WHERE {base_filters}
        """
        
        return sql

    def _build_where_clause(self, query: BaseQuery) -> str:
        # Basic filter generation
        clauses = []
        # snapshot_id is handled by the engine's view registration usually, 
        # but if we had a big table we'd filter. 
        # For now, 'ball_events' IS the snapshot.
        
        if hasattr(query, 'batter_id'):
            clauses.append(f"batter_id = {query.batter_id}")
        if hasattr(query, 'bowler_id'):
            clauses.append(f"bowler_id = {query.bowler_id}")
        if hasattr(query, 'venue_id') and query.venue_id:
            clauses.append(f"venue_id = {query.venue_id}")
            
        return " AND ".join(clauses) if clauses else "1=1"

    # Legacy method kept for compatibility if needed, but we are moving to create_plan returning SQL
    def create_legacy_plan(self, query: BaseQuery) -> Dict[str, Any]:
        """
        Analyzes query dependencies and returns an execution plan.
        """
        reqs = query.requires
        available_tables = self.engine.derived_versions.keys()
        
        # Basic strategy selection
        strategy = "raw_scan"
        target_table = reqs.get("fallback_table", "ball_events")
        
        for table in reqs.get("preferred_tables", []):
            if table in available_tables:
                strategy = "materialized_view"
                target_table = table
                break
        
        # Generate SQL (Moved from Executor)
        sql = self._generate_sql(query, target_table)

        return {
            "strategy": strategy,
            "target_table": target_table,
            "sql": sql,
            "cost": "low" if strategy == "materialized_view" else "high"
        }

    def _generate_sql(self, query: BaseQuery, table: str) -> str:
        if query.__class__.__name__ == "MatchupQuery":
            # Handle single ID (str)
            batter_id = getattr(query, "batter_id")
            bowler_id = getattr(query, "bowler_id")
            
            return f"""
                SELECT 
                    sum(runs_batter) as runs, 
                    count(*) as balls, 
                    sum(case when is_wicket=true then 1 else 0 end) as wickets
                FROM {table} 
                WHERE batter_id = {batter_id}
                  AND bowler_id = {bowler_id}
            """
        raise NotImplementedError(f"No SQL generation for {query.__class__.__name__}")

