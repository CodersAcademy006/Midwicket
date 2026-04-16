import logging
from typing import Dict, Any, Optional, Callable, List, Tuple
from pypitch.query.base import BaseQuery

logger = logging.getLogger(__name__)

# Whitelist of valid table identifiers to prevent SQL injection via table names.
_VALID_TABLES = frozenset({
    "ball_events", "matchup_stats", "phase_stats", "fantasy_points_avg",
    "venue_bias", "chase_history", "venue_baselines",
})

# Map from query class name → preferred materialized table.
# The executor will pick "materialized_view" strategy when the table is loaded.
_QUERY_PREFERRED_TABLES: Dict[str, List[str]] = {
    "MatchupQuery": ["matchup_stats"],
    "FantasyQuery": ["fantasy_points_avg"],
    "PhaseQuery": ["phase_stats"],
    "VenueBiasQuery": ["venue_bias", "chase_history"],
    "WinProbQuery": [],  # routed directly to win_probability(), never hits SQL
}


def _validate_table(name: str) -> str:
    """Ensure *name* is a known table identifier (defence-in-depth)."""
    if name not in _VALID_TABLES:
        raise ValueError(
            f"Unknown table '{name}'. "
            f"Register it in _VALID_TABLES if it is a legitimate table."
        )
    return name


def _table_ref(table: str) -> str:
    """Return fully-qualified table reference for planner SQL generation."""
    table = _validate_table(table)
    return table if table == "ball_events" else f"derived.{table}"


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
        """  # nosec B608 – all interpolated values are internal schema names/params, not user input
        return sql, params

    def plan(self, query: BaseQuery) -> Dict[str, Any]:
        """
        Unified query planner — the preferred, non-legacy entry point.

        Identical contract to ``create_legacy_plan`` but uses the built-in
        ``_QUERY_PREFERRED_TABLES`` map as the primary routing authority.
        Call this from new code; ``create_legacy_plan`` is retained for
        backwards compatibility only.
        """
        return self.create_legacy_plan(query)

    def create_legacy_plan(self, query: BaseQuery) -> Dict[str, Any]:
        """
        Creates an execution plan by analysing query dependencies.

        Prefers materialized views registered in ``engine.derived_versions``
        using either the query's own ``requires`` dict or the built-in
        ``_QUERY_PREFERRED_TABLES`` map (whichever fires first).

        Returns a dict with ``strategy``, ``target_table``, ``sql``,
        ``params``, and ``cost``.
        """
        reqs = getattr(query, "requires", {})
        available_tables = getattr(self.engine, "derived_versions", {}).keys()

        # Merge preferred tables: built-in map wins over query-level hints so
        # newly-registered query types automatically participate in optimisation.
        query_type = query.__class__.__name__
        builtin_preferred = _QUERY_PREFERRED_TABLES.get(query_type, [])
        query_preferred = reqs.get("preferred_tables", [])
        preferred_tables = builtin_preferred + [
            t for t in query_preferred if t not in builtin_preferred
        ]

        strategy = "raw_scan"
        target_table = reqs.get("fallback_table", "ball_events")

        for table in preferred_tables:
            if table in available_tables:
                table_exists = True
                table_exists_fn = getattr(self.engine, "table_exists", None)
                if callable(table_exists_fn):
                    try:
                        table_exists = bool(table_exists_fn(table))
                    except Exception:
                        table_exists = False

                if table_exists:
                    strategy = "materialized_view"
                    target_table = table
                    break

                logger.warning(
                    "Planner metadata marked %s as available, but table does not exist. "
                    "Falling back to raw scan.",
                    table,
                )

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
        table_ref = _table_ref(table)

        qtype = query.__class__.__name__

        if qtype == "MatchupQuery":
            batter_id = getattr(query, "batter_id")
            bowler_id = getattr(query, "bowler_id")
            sql = f"""
                SELECT
                    sum(runs_batter)                                        AS runs,
                    count(*)                                                AS balls,
                    sum(case when is_wicket=true then 1 else 0 end)        AS wickets,
                    ROUND(sum(runs_batter)*100.0/NULLIF(count(*),0), 2)    AS strike_rate,
                    ROUND(sum(runs_batter)*1.0/NULLIF(
                        sum(case when is_wicket=true then 1 else 0 end),0), 2) AS average
                                FROM {table_ref}
                WHERE batter_id = ?
                  AND bowler_id = ?
            """  # nosec B608 – {table} is an internal constant; user values are parameterised
            return sql, [batter_id, bowler_id]

        if qtype == "FantasyQuery":
            venue_id = getattr(query, "venue_id")
            sql = f"""
                SELECT
                    batter_id                                                       AS player_id,
                    SUM(runs_batter)                                                AS runs,
                    SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)               AS fours,
                    SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)               AS sixes,
                    SUM(CASE WHEN is_wicket THEN 20 ELSE 0 END)
                        + SUM(runs_batter)                                          AS avg_points,
                    COUNT(DISTINCT match_id)                                        AS matches
                FROM {table_ref}
                WHERE venue_id = ?
                GROUP BY batter_id
                ORDER BY avg_points DESC
            """  # nosec B608
            return sql, [venue_id]

        if qtype == "PhaseQuery":
            batter_id = getattr(query, "batter_id", None)
            phase = getattr(query, "phase", "all")
            where, params = self._build_where_clause(query)
            sql = f"""
                SELECT
                    phase,
                    count(*)                                                AS balls,
                    sum(runs_batter)                                        AS runs,
                    sum(case when is_wicket then 1 else 0 end)             AS wickets,
                    ROUND(sum(runs_batter)*100.0/NULLIF(count(*),0), 2)   AS strike_rate
                FROM {table_ref}
                WHERE {where}
                GROUP BY phase
                ORDER BY phase
            """  # nosec B608
            return sql, params

        if qtype == "VenueBiasQuery":
            venue_id = getattr(query, "venue_id", None)
            sql = f"""
                SELECT
                    inning,
                    COUNT(DISTINCT match_id)        AS matches,
                    SUM(runs_batter + runs_extras)  AS total_runs,
                    AVG(runs_batter + runs_extras)  AS avg_runs_per_ball
                FROM {table_ref}
                WHERE venue_id = ?
                GROUP BY inning
            """  # nosec B608
            return sql, [venue_id]

        # Generic fallback — warns so developers know to add an explicit handler
        logger.warning(
            "No specialised SQL handler for %s — falling back to full scan. "
            "Consider adding an explicit handler in QueryPlanner._generate_sql.",
            qtype,
        )
        where, params = self._build_where_clause(query)
        return f"SELECT * FROM {table_ref} WHERE {where}", params  # nosec B608
