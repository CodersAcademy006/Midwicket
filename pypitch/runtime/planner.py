import logging
from typing import Dict, Any, Optional, Callable, List, Tuple
from pypitch.query.base import BaseQuery

logger = logging.getLogger(__name__)

# Whitelist of valid table identifiers to prevent SQL injection via table names.
_VALID_TABLES = frozenset({
    "ball_events", "matchup_stats", "phase_stats", "fantasy_points_avg",
    "venue_bias", "chase_history", "venue_baselines",
})


def _validate_table(name: str) -> str:
    """Ensure *name* is a known table identifier (defence-in-depth)."""
    if name not in _VALID_TABLES:
        raise ValueError(
            f"Unknown table '{name}'. "
            f"Register it in _VALID_TABLES if it is a legitimate table."
        )
    return name


class QueryPlanner:
    def __init__(self, engine: Any) -> None:
        self.engine = engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_plan(
        self,
        query: BaseQuery,
        metric_func: Optional[Callable[..., Any]] = None,
    ) -> Tuple[str, List[Any]]:
        """
        Constructs the optimised SQL with all necessary JOINs.

        Returns:
            (sql, params) — a parameterised query ready for ``execute_sql``.
        """
        where_clause, params = self._build_where_clause(query)

        # 1. Analyse Dependencies
        joins: List[str] = []
        selects = ["e.*"]

        if metric_func and hasattr(metric_func, "_pypitch_spec"):
            for req in metric_func._pypitch_spec.requirements:
                table = _validate_table(req["table"])
                key = req["key"]
                joins.append(
                    f"LEFT JOIN derived.{table} AS {table} "
                    f"ON e.{key} = {table}.{key}"
                )
                selects.append(f"{table}.*")

        join_clause = "\n".join(joins)
        select_clause = ", ".join(selects)

        sql = f"""
            SELECT {select_clause}
            FROM ball_events AS e
            {join_clause}
            WHERE {where_clause}
        """
        return sql, params

    def create_legacy_plan(self, query: BaseQuery) -> Dict[str, Any]:
        """
        Creates an execution plan by analysing query dependencies.

        Returns a dict with ``strategy``, ``target_table``, ``sql``,
        ``params``, and ``cost``.
        """
        reqs = query.requires
        available_tables = self.engine.derived_versions.keys()

        strategy = "raw_scan"
        target_table = reqs.get("fallback_table", "ball_events")

        for table in reqs.get("preferred_tables", []):
            if table in available_tables:
                strategy = "materialized_view"
                target_table = table
                break

        sql, params = self._generate_sql(query, target_table)

        return {
            "strategy": strategy,
            "target_table": target_table,
            "sql": sql,
            "params": params,
            "cost": "low" if strategy == "materialized_view" else "high",
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_where_clause(
        self, query: BaseQuery
    ) -> Tuple[str, List[Any]]:
        """Build a parameterised WHERE clause from *query* attributes."""
        clauses: List[str] = []
        params: List[Any] = []

        if hasattr(query, "batter_id"):
            clauses.append("batter_id = ?")
            params.append(query.batter_id)
        if hasattr(query, "bowler_id"):
            clauses.append("bowler_id = ?")
            params.append(query.bowler_id)
        if hasattr(query, "venue_id") and query.venue_id is not None:
            clauses.append("venue_id = ?")
            params.append(query.venue_id)
        if hasattr(query, "phase"):
            valid_phases = ("powerplay", "middle", "death", "all")
            if query.phase not in valid_phases:
                raise ValueError(
                    f"Invalid phase '{query.phase}'. "
                    f"Must be one of {valid_phases}"
                )
            if query.phase != "all":
                clauses.append("phase = ?")
                params.append(query.phase)

        where = " AND ".join(clauses) if clauses else "1=1"
        return where, params

    def _generate_sql(
        self, query: BaseQuery, table: str
    ) -> Tuple[str, List[Any]]:
        """Return ``(sql, params)`` for the given query type."""
        table = _validate_table(table)

        if query.__class__.__name__ == "MatchupQuery":
            batter_id = getattr(query, "batter_id")
            bowler_id = getattr(query, "bowler_id")
            sql = f"""
                SELECT
                    sum(runs_batter) as runs,
                    count(*) as balls,
                    sum(case when is_wicket=true then 1 else 0 end) as wickets
                FROM {table}
                WHERE batter_id = ?
                  AND bowler_id = ?
            """
            return sql, [batter_id, bowler_id]

        if query.__class__.__name__ == "FantasyQuery":
            venue_id = getattr(query, "venue_id")
            sql = f"""
                SELECT
                    batter_id as player_id,
                    SUM(runs_batter) + SUM(CASE WHEN is_wicket THEN 20 ELSE 0 END) as avg_points
                FROM {table}
                WHERE venue_id = ?
                GROUP BY batter_id
                ORDER BY avg_points DESC
            """
            return sql, [venue_id]

        # Generic fallback
        logger.warning(
            "No specialised SQL handler for %s — falling back to full scan. "
            "Consider adding an explicit handler in QueryPlanner._generate_sql.",
            query.__class__.__name__,
        )
        where, params = self._build_where_clause(query)
        return f"SELECT * FROM {table} WHERE {where}", params
