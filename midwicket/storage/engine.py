import contextlib
import duckdb
import pyarrow as pa
import logging
import re
import threading
import time
from datetime import date
from typing import Any, Iterator, Optional
from midwicket.schema.v1 import BALL_EVENT_SCHEMA
from midwicket.storage.connection_pool import ConnectionPool
from midwicket.exceptions import DataIngestionError, QueryExecutionError

logger = logging.getLogger(__name__)

# Hard ceiling on rows pulled into memory for a single read. Results are streamed
# in batches and truncated here so a runaway query cannot OOM the process (MW-010).
MAX_RESULT_ROWS = 100_000
_RESULT_BATCH_ROWS = 10_000

# Leading keywords that mutate data or schema. A read_only=True query that starts
# with one of these is refused at the engine boundary (MW-007), giving real write
# protection independent of the API-layer SQL guard.
_WRITE_LEADERS = frozenset({
    "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE",
    "REPLACE", "MERGE", "ATTACH", "DETACH", "COPY", "INSTALL", "LOAD",
})
_LEADING_COMMENT_RE = re.compile(r"^\s*(?:--[^\n]*\n|/\*.*?\*/)", re.DOTALL)


def _leading_keyword(sql: str) -> str:
    """Return the first SQL keyword, skipping leading comments and parentheses."""
    s = sql
    while True:
        m = _LEADING_COMMENT_RE.match(s)
        if not m:
            break
        s = s[m.end():]
    s = s.lstrip().lstrip("(").lstrip()
    m = re.match(r"[A-Za-z_]+", s)
    return m.group(0).upper() if m else ""


def _is_write_statement(sql: str) -> bool:
    """True if the statement's leading keyword mutates data or schema."""
    return _leading_keyword(sql) in _WRITE_LEADERS


class QueryEngine:
    def __init__(self, db_path: str = ":memory:") -> None:
        """
        Initializes the DuckDB engine with connection pooling.
        :memory: is fast but volatile. Use a path for persistence.
        """
        self.db_path = db_path
        self.pool = ConnectionPool(db_path, max_connections=5)

        # Initialize database schema on first connection
        with self.pool.connection() as con:
            pass  # PRAGMAs are set in ConnectionPool._create_connection

        # State tracking for deterministic hashing
        self._snapshot_id = "initial_empty"
        self._derived_versions: dict[str, str] = {}
        self._state_lock = threading.Lock()

    @property
    def snapshot_id(self) -> str:
        with self._state_lock:
            return self._snapshot_id

    @property
    def derived_versions(self) -> dict[str, str]:
        with self._state_lock:
            return self._derived_versions.copy()

    def ingest_events(self, arrow_table: pa.Table, snapshot_tag: str, append: bool = False, incremental: bool = False) -> None:
        """
        Ingests strict Schema V1 Arrow Tables.
        Rejects anything that doesn't match the contract.
        """
        # Graceful Schema Degradation: Check required names instead of strict .equals()
        expected_names = set(BALL_EVENT_SCHEMA.names)
        actual_names = set(arrow_table.schema.names)
        if not expected_names.issubset(actual_names):
            missing = expected_names - actual_names
            raise ValueError(f"Schema Violation: Missing required columns {missing}")

        # Debug: log incoming table info
        try:
            incoming_rows = getattr(arrow_table, 'num_rows', None)
        except Exception:
            incoming_rows = None
        logger.debug("ingest_events: snapshot_tag=%s append=%s incoming_rows=%s", snapshot_tag, append, incoming_rows)

        with self.pool.connection() as con:
            # Registers the Arrow table as a queryable view in DuckDB
            # This is a zero-copy operation (pointers only)
            con.register('arrow_view', arrow_table)
            try:
                exists = self.table_exists("ball_events", con)
                logger.debug("ingest_events: ball_events exists=%s", exists)

                # Persist to disk
                if append and exists:
                    logger.debug("ingest_events: appending to ball_events")
                    con.execute("INSERT INTO ball_events SELECT * FROM arrow_view")
                else:
                    logger.debug("ingest_events: creating/replacing ball_events")
                    con.execute("CREATE OR REPLACE TABLE ball_events AS SELECT * FROM arrow_view")

                # Check resulting row count for quick verification
                try:
                    res = con.execute("SELECT COUNT(*) FROM ball_events").fetchone()
                    logger.debug("ingest_events: row_count_after_write=%s", res[0] if res else "unknown")
                except Exception as e:
                    logger.debug("ingest_events: failed to fetch row count: %s", e)

                # New source data invalidates all previously materialized derived tables.
                # Keep metadata and physical derived schema in sync.
                if not incremental:
                    self._invalidate_derived_state(con)

                # Track the active snapshot tag for observability/debugging.
                with self._state_lock:
                    self._snapshot_id = snapshot_tag
            finally:
                try:
                    con.unregister('arrow_view')
                except Exception:  # nosec B110 — view may not be registered if ingest failed early
                    pass

    def _invalidate_derived_state(self, con) -> None:
        """Drop stale derived tables and clear in-memory version metadata."""
        with self._state_lock:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("DROP SCHEMA IF EXISTS derived CASCADE")
                con.execute("CREATE SCHEMA derived")
                self._derived_versions.clear()
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def execute_sql(
        self,
        sql: str,
        params: Optional[list] = None,
        read_only: bool = True,
        timeout: Optional[float] = None,
    ) -> pa.Table:
        """
        Execute a SQL query and return results as a PyArrow Table.

        Args:
            sql: SQL statement
            params: Optional positional parameters
            read_only: False for write statements
            timeout: Optional timeout in seconds. When exceeded, the query is
                interrupted and TimeoutError is raised.
        """
        if params is None:
            params = []

        # MW-007: read_only must actually prevent writes. The pooled engine used
        # in production previously ignored read_only entirely (both branches shared
        # the same read-write connection), so a guard bypass at a higher layer could
        # still mutate state. Refuse write/DDL statements here as defense in depth.
        if read_only and _is_write_statement(sql):
            raise QueryExecutionError(
                "read_only=True engine refused a write/DDL statement "
                f"('{_leading_keyword(sql)}'); pass read_only=False to write."
            )

        timeout_enabled = timeout is not None and timeout > 0
        conn_timeout = timeout if timeout_enabled else 30.0

        with self.pool.connection(timeout=conn_timeout) as con:
            timed_out = False
            timer: Optional[threading.Timer] = None
            started_at = time.perf_counter()

            def _has_timed_out() -> bool:
                if not timeout_enabled:
                    return False
                if timed_out:
                    return True
                return bool(timeout is not None and (time.perf_counter() - started_at) >= float(timeout))

            def _interrupt_query() -> None:
                nonlocal timed_out
                timed_out = True
                interrupt = getattr(con, "interrupt", None)
                if callable(interrupt):
                    with contextlib.suppress(Exception):
                        interrupt()

            if timeout_enabled:
                timer = threading.Timer(float(timeout) if timeout is not None else 0.0, _interrupt_query)
                timer.daemon = True
                timer.start()

            try:
                if not read_only:
                    result = con.execute(sql, params).arrow()
                    if _has_timed_out():
                        raise TimeoutError(f"Query timed out after {timeout}s")
                    return result

                # Stream the result in bounded batches and stop at MAX_RESULT_ROWS
                # so a runaway query can never fully materialize in memory. Unlike
                # .arrow() (which materializes eagerly, making the old cap below
                # unreachable), to_arrow_reader yields a true streaming reader, so
                # the truncation actually executes (MW-010).
                reader = con.execute(sql, params).to_arrow_reader(_RESULT_BATCH_ROWS)
                schema = reader.schema
                batches = []
                total_rows = 0
                try:
                    for batch in reader:
                        remaining = MAX_RESULT_ROWS - total_rows
                        if remaining <= 0:
                            break
                        if batch.num_rows > remaining:
                            batch = batch.slice(0, remaining)
                        batches.append(batch)
                        total_rows += batch.num_rows
                finally:
                    with contextlib.suppress(Exception):
                        reader.close()
                result = pa.Table.from_batches(batches, schema=schema) if batches else schema.empty_table()

                if _has_timed_out():
                    raise TimeoutError(f"Query timed out after {timeout}s")
                return result
            except Exception as exc:
                if _has_timed_out():
                    raise TimeoutError(f"Query timed out after {timeout}s") from exc
                raise
            finally:
                if timer is not None:
                    timer.cancel()

    def run(self, plan: dict[str, Any]) -> pa.Table:
        """
        Executes the plan.
        """
        if "sql" in plan:
            return self.execute_sql(
                plan["sql"],
                params=plan.get("params"),
                read_only=plan.get("read_only", True),
                timeout=plan.get("timeout"),
            )
        raise NotImplementedError("Plan execution without SQL not implemented")

    def insert_live_delivery(self, delivery_data: dict[str, Any]) -> None:
        """
        Insert live delivery data.
        """
        with self.pool.connection() as con:
            # Ensure table exists
            if not self.table_exists("ball_events", con):
                # Create table if not exists (simplified schema for demo)
                # Note: In real app, use full schema
                con.execute("""
                CREATE TABLE IF NOT EXISTS ball_events (
                    match_id VARCHAR, inning INTEGER, over INTEGER, ball INTEGER,
                    runs_total INTEGER, wickets_fallen INTEGER, target INTEGER,
                    venue VARCHAR, timestamp DOUBLE,
                    runs_batter INTEGER DEFAULT 0, runs_extras INTEGER DEFAULT 0, extras_type VARCHAR,
                    is_wicket BOOLEAN DEFAULT FALSE, batter VARCHAR DEFAULT '', bowler VARCHAR DEFAULT ''
                )
                """)

            columns = self._table_columns("ball_events", con)
            legacy_cols = {"runs_total", "wickets_fallen", "target", "venue", "timestamp"}
            schema_v1_cols = {
                "date", "venue_id", "batter_id", "bowler_id", "non_striker_id",
                "batting_team_id", "bowling_team_id", "runs_batter", "runs_extras",
                "is_wicket", "wicket_type", "phase",
            }

            if legacy_cols.issubset(columns):
                required_legacy = [
                    "match_id",
                    "inning",
                    "over",
                    "ball",
                    "runs_total",
                    "wickets_fallen",
                ]
                missing = [
                    field
                    for field in required_legacy
                    if delivery_data.get(field) is None
                ]
                if missing:
                    raise DataIngestionError(
                        "Missing required live delivery fields for legacy schema: "
                        + ", ".join(missing)
                    )

                con.execute(
                    """
                    INSERT INTO ball_events (
                        match_id, inning, over, ball, runs_total,
                        wickets_fallen, target, venue, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        delivery_data["match_id"],
                        delivery_data["inning"],
                        delivery_data["over"],
                        delivery_data["ball"],
                        delivery_data["runs_total"],
                        delivery_data["wickets_fallen"],
                        delivery_data.get("target"),
                        delivery_data.get("venue"),
                        delivery_data.get("timestamp"),
                    ],
                )
                return

            if schema_v1_cols.issubset(columns):
                required_schema_v1 = [
                    "match_id",
                    "inning",
                    "over",
                    "ball",
                    "venue_id",
                    "batter_id",
                    "bowler_id",
                    "non_striker_id",
                    "batting_team_id",
                    "bowling_team_id",
                ]
                missing = [
                    field
                    for field in required_schema_v1
                    if delivery_data.get(field) is None
                ]
                if missing:
                    raise DataIngestionError(
                        "Missing required live delivery fields for schema v1 table: "
                        + ", ".join(missing)
                    )

                con.execute(
                    """
                    INSERT INTO ball_events (
                        match_id, date, venue_id, inning, over, ball,
                        batter_id, bowler_id, non_striker_id,
                        batting_team_id, bowling_team_id,
                        runs_batter, runs_extras, extras_type, is_wicket, wicket_type, phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        delivery_data["match_id"],
                        delivery_data.get("date", date.today()),
                        delivery_data["venue_id"],
                        delivery_data["inning"],
                        delivery_data["over"],
                        delivery_data["ball"],
                        delivery_data["batter_id"],
                        delivery_data["bowler_id"],
                        delivery_data["non_striker_id"],
                        delivery_data["batting_team_id"],
                        delivery_data["bowling_team_id"],
                        delivery_data.get("runs_batter", 0),
                        delivery_data.get("runs_extras", 0),
                        delivery_data.get("extras_type"),
                        bool(delivery_data.get("is_wicket", False)),
                        delivery_data.get("wicket_type"),
                        delivery_data.get("phase", self._infer_phase(delivery_data.get("over"))),
                    ],
                )
                return

            raise DataIngestionError(
                "Unsupported ball_events schema for live delivery insert. "
                "Expected legacy live schema or BALL_EVENT_SCHEMA columns."
            )

    @staticmethod
    def _infer_phase(over_value: Any) -> str:
        """Infer innings phase from over index when explicit phase is unavailable."""
        try:
            over = int(over_value)
        except (TypeError, ValueError):
            return "Middle"
        if over <= 5:
            return "Powerplay"
        if over <= 14:
            return "Middle"
        return "Death"

    @staticmethod
    def _table_columns(table_name: str, con) -> set[str]:
        """Return lowercase column names for a table."""
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name],
        ).fetchall()
        return {str(row[0]).lower() for row in rows}

    def table_exists(self, table_name: str, con=None, schema: Optional[str] = None) -> bool:
        """Checks if a table exists in the database."""
        if con is None:
            with self.pool.connection() as con:
                return self._table_exists(table_name, con, schema=schema)
        else:
            return self._table_exists(table_name, con, schema=schema)

    def _table_exists(self, table_name: str, con, schema: Optional[str] = None) -> bool:
        """Checks if a table exists using the provided connection."""
        try:
            # DuckDB specific query
            if schema is None:
                res = con.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name],
                ).fetchone()
            else:
                res = con.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = ? AND table_name = ?",
                    [schema, table_name],
                ).fetchone()
            return res[0] > 0 if res else False
        except duckdb.Error as e:
            logger.warning("Error checking table existence: %s", e)
            return False

    @contextlib.contextmanager
    def raw_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """
        Yield a fresh persistent DuckDB connection to the same database.

        Use this for advanced operations such as ``ATTACH`` that must
        persist across multiple statements on the same connection.
        The caller is responsible for any cleanup (e.g. ``DETACH``).

        Example::

            with session.engine.raw_connection() as con:
                con.execute("ATTACH 'other.duckdb' AS other (READ_ONLY)")
                df = con.execute("SELECT * FROM other.main.my_table").df()
        """
        con = duckdb.connect(self.pool.connect_path)
        try:
            yield con
        finally:
            con.close()

    def close(self) -> None:
        """Close the database connection pool."""
        self.pool.close()

# Alias for backward compatibility if needed, but we will update references
StorageEngine = QueryEngine
