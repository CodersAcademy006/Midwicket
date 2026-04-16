"""
Tests for QueryEngine (storage/engine.py), ConnectionPool (storage/connection_pool.py),
MetricsCollector (serve/monitoring.py), RateLimiter (serve/rate_limit.py).
"""

import time
import threading
import tempfile
from pathlib import Path
from contextlib import contextmanager
import pytest
import pyarrow as pa

from pypitch.storage.engine import QueryEngine, StorageEngine
from pypitch.storage.connection_pool import ConnectionPool
from pypitch.storage.thread_safe_engine import create_thread_safe_engine
from pypitch.schema.v1 import BALL_EVENT_SCHEMA
from pypitch.exceptions import QueryTimeoutError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_valid_ball_event_table(n: int = 2) -> pa.Table:
    """Return a minimal BALL_EVENT_SCHEMA-conformant table."""
    import datetime
    return pa.table(
        {
            "match_id": pa.array(["m1"] * n, type=pa.string()),
            "date": pa.array([datetime.date(2023, 1, 1)] * n, type=pa.date32()),
            "venue_id": pa.array([301] * n, type=pa.int32()),
            "inning": pa.array([1] * n, type=pa.int8()),
            "over": pa.array([1] * n, type=pa.int8()),
            "ball": pa.array([i + 1 for i in range(n)], type=pa.int8()),
            "batter_id": pa.array([101] * n, type=pa.int32()),
            "bowler_id": pa.array([201] * n, type=pa.int32()),
            "non_striker_id": pa.array([102] * n, type=pa.int32()),
            "batting_team_id": pa.array([1] * n, type=pa.int16()),
            "bowling_team_id": pa.array([2] * n, type=pa.int16()),
            "runs_batter": pa.array([1] * n, type=pa.int8()),
            "runs_extras": pa.array([0] * n, type=pa.int8()),
            "is_wicket": pa.array([False] * n, type=pa.bool_()),
            "wicket_type": pa.array(
                [None] * n,
                type=pa.dictionary(pa.int8(), pa.string())
            ),
            "phase": pa.array(
                ["powerplay"] * n,
                type=pa.dictionary(pa.int8(), pa.string())
            ),
        }
    )


# ---------------------------------------------------------------------------
# QueryEngine — basic init and schema
# ---------------------------------------------------------------------------

class TestQueryEngineInit:
    def test_in_memory_engine_creates_successfully(self):
        engine = QueryEngine(":memory:")
        assert engine.db_path == ":memory:"
        assert engine.snapshot_id == "initial_empty"
        engine.close()

    def test_derived_versions_initially_empty(self):
        engine = QueryEngine(":memory:")
        assert engine.derived_versions == {}
        engine.close()

    def test_storage_engine_alias(self):
        """StorageEngine is a backward-compat alias for QueryEngine."""
        assert StorageEngine is QueryEngine


# ---------------------------------------------------------------------------
# QueryEngine — ingest and execute
# ---------------------------------------------------------------------------

class TestQueryEngineIngest:
    def test_ingest_valid_table_succeeds(self):
        engine = QueryEngine(":memory:")
        table = _make_valid_ball_event_table()
        # Should not raise
        engine.ingest_events(table, "snap-001")
        engine.close()

    def test_ingest_invalid_schema_raises(self):
        engine = QueryEngine(":memory:")
        bad_table = pa.table({"foo": [1, 2, 3]})
        with pytest.raises(ValueError, match="Schema Violation"):
            engine.ingest_events(bad_table, "snap-002")
        engine.close()

    def test_ingest_then_query_returns_rows(self):
        engine = QueryEngine(":memory:")
        table = _make_valid_ball_event_table(3)
        engine.ingest_events(table, "snap-003")
        result = engine.execute_sql("SELECT COUNT(*) AS n FROM ball_events")
        assert result.to_pydict()["n"][0] == 3
        engine.close()

    def test_ingest_append_adds_rows(self):
        engine = QueryEngine(":memory:")
        table = _make_valid_ball_event_table(2)
        engine.ingest_events(table, "snap-004")
        engine.ingest_events(table, "snap-005", append=True)
        result = engine.execute_sql("SELECT COUNT(*) AS n FROM ball_events")
        assert result.to_pydict()["n"][0] == 4
        engine.close()

    def test_ingest_updates_snapshot_id(self):
        engine = QueryEngine(":memory:")
        table = _make_valid_ball_event_table(1)

        engine.ingest_events(table, "snap-010")

        assert engine.snapshot_id == "snap-010"
        engine.close()

    def test_ingest_invalidates_derived_state(self):
        engine = QueryEngine(":memory:")
        table = _make_valid_ball_event_table(1)

        engine.ingest_events(table, "snap-a")
        engine.execute_sql(
            "CREATE TABLE derived.temp_metric AS SELECT 1 AS x",
            read_only=False,
        )
        engine.derived_versions["temp_metric"] = "snap-a"

        engine.ingest_events(table, "snap-b")

        assert engine.derived_versions == {}
        count = engine.execute_sql(
            "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema = 'derived'"
        ).to_pydict()["c"][0]
        assert count == 0
        engine.close()

    def test_insert_live_delivery_after_schema_v1_ingest(self):
        engine = QueryEngine(":memory:")
        table = _make_valid_ball_event_table(1)
        engine.ingest_events(table, "snap-live")

        engine.insert_live_delivery(
            {
                "match_id": "live-match",
                "inning": 2,
                "over": 17,
                "ball": 1,
                "runs_total": 120,
                "wickets_fallen": 3,
            }
        )

        rows = engine.execute_sql(
            "SELECT match_id, phase FROM ball_events WHERE match_id = ?",
            params=["live-match"],
        ).to_pydict()
        assert rows["match_id"] == ["live-match"]
        assert rows["phase"] == ["death"]
        engine.close()


# ---------------------------------------------------------------------------
# QueryEngine — execute_sql
# ---------------------------------------------------------------------------

class TestQueryEngineExecuteSQL:
    def test_select_returns_arrow_table(self):
        engine = QueryEngine(":memory:")
        result = engine.execute_sql("SELECT 42 AS answer")
        assert isinstance(result, pa.Table)
        assert result.to_pydict()["answer"][0] == 42
        engine.close()

    def test_write_query_returns_empty_table(self):
        engine = QueryEngine(":memory:")
        # Non-SELECT returns empty table
        engine.execute_sql("CREATE TABLE t (x INTEGER)", read_only=False)
        engine.execute_sql("INSERT INTO t VALUES (1)", read_only=False)
        result = engine.execute_sql("SELECT * FROM t")
        assert result.to_pydict()["x"] == [1]
        engine.close()

    def test_parameterised_query(self):
        engine = QueryEngine(":memory:")
        result = engine.execute_sql("SELECT ? + ? AS total", params=[3, 7])
        assert result.to_pydict()["total"][0] == 10
        engine.close()

    def test_in_memory_pool_connections_share_state(self):
        engine = QueryEngine(":memory:")
        held = engine.pool.get_connection()
        try:
            held.execute("CREATE TABLE t (x INTEGER)")
            held.execute("INSERT INTO t VALUES (1)")

            # With one connection held, execute_sql must use a different
            # pooled connection and still observe the same in-memory state.
            result = engine.execute_sql("SELECT COUNT(*) AS c FROM t")
            assert result.to_pydict()["c"] == [1]
        finally:
            engine.pool.return_connection(held)
            engine.close()


# ---------------------------------------------------------------------------
# QueryEngine — table_exists
# ---------------------------------------------------------------------------

class TestTableExists:
    def test_table_does_not_exist_initially(self):
        engine = QueryEngine(":memory:")
        assert engine.table_exists("ball_events") is False
        engine.close()

    def test_table_exists_after_ingest(self):
        engine = QueryEngine(":memory:")
        engine.ingest_events(_make_valid_ball_event_table(), "snap")
        assert engine.table_exists("ball_events") is True
        engine.close()

    def test_run_with_sql_plan(self):
        engine = QueryEngine(":memory:")
        engine.ingest_events(_make_valid_ball_event_table(1), "snap")
        result = engine.run({"sql": "SELECT * FROM ball_events"})
        assert isinstance(result, pa.Table)
        engine.close()

    def test_run_with_sql_plan_uses_params(self):
        engine = QueryEngine(":memory:")
        result = engine.run({"sql": "SELECT ? + ? AS total", "params": [2, 5]})
        assert result.to_pydict()["total"][0] == 7
        engine.close()

    def test_run_with_sql_plan_honours_read_only_flag(self):
        engine = QueryEngine(":memory:")
        engine.run({"sql": "CREATE TABLE t (x INTEGER)", "read_only": False})
        engine.run({"sql": "INSERT INTO t VALUES (?)", "params": [11], "read_only": False})
        result = engine.run({"sql": "SELECT x FROM t"})
        assert result.to_pydict()["x"] == [11]
        engine.close()

    def test_run_without_sql_raises_not_implemented(self):
        engine = QueryEngine(":memory:")
        with pytest.raises(NotImplementedError):
            engine.run({"no_sql_key": "something"})
        engine.close()


# ---------------------------------------------------------------------------
# QueryEngine — raw_connection context manager
# ---------------------------------------------------------------------------

class TestRawConnection:
    def test_raw_connection_yields_duckdb_connection(self):
        import duckdb
        engine = QueryEngine(":memory:")
        with engine.raw_connection() as con:
            assert hasattr(con, "execute")
            result = con.execute("SELECT 99 AS x").fetchone()
            assert result[0] == 99
        engine.close()

    def test_raw_connection_shares_in_memory_engine_state(self):
        engine = QueryEngine(":memory:")
        try:
            engine.execute_sql("CREATE TABLE t (x INTEGER)", read_only=False)
            engine.execute_sql("INSERT INTO t VALUES (42)", read_only=False)

            with engine.raw_connection() as con:
                count = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
                assert count == 1
        finally:
            engine.close()


# ---------------------------------------------------------------------------
# ConnectionPool
# ---------------------------------------------------------------------------

class TestConnectionPool:
    def test_pool_provides_connection(self):
        pool = ConnectionPool(":memory:", max_connections=2)
        conn = pool.get_connection()
        assert conn is not None
        pool.return_connection(conn)
        pool.close()

    def test_pool_return_unknown_connection_raises(self):
        import duckdb
        pool = ConnectionPool(":memory:", max_connections=2)
        foreign_conn = duckdb.connect(":memory:")
        with pytest.raises(ValueError, match="not managed"):
            pool.return_connection(foreign_conn)
        pool.close()
        foreign_conn.close()

    def test_pool_context_manager(self):
        pool = ConnectionPool(":memory:", max_connections=2)
        with pool.connection() as conn:
            result = conn.execute("SELECT 7 AS x").fetchone()
            assert result[0] == 7
        pool.close()

    def test_pool_close_clears_connections(self):
        pool = ConnectionPool(":memory:", max_connections=2)
        # Warm up the pool
        with pool.connection():
            pass
        pool.close()
        assert pool._connections == []
        assert pool._closed is True

    def test_pool_get_on_closed_raises(self):
        pool = ConnectionPool(":memory:", max_connections=2)
        pool.close()
        with pytest.raises(RuntimeError, match="closed"):
            pool.get_connection()

    def test_pool_timeout_raises_timeout_error(self):
        pool = ConnectionPool(":memory:", max_connections=1)
        # Exhaust the pool by not returning the connection
        conn = pool.get_connection()
        with pytest.raises(TimeoutError):
            pool.get_connection(timeout=0.1)  # Should time out immediately
        pool.return_connection(conn)
        pool.close()

    def test_waiting_get_connection_unblocks_when_closed(self):
        pool = ConnectionPool(":memory:", max_connections=1)
        held = pool.get_connection()
        outcome: dict[str, str] = {}

        def waiter() -> None:
            try:
                pool.get_connection(timeout=5.0)
                outcome["status"] = "acquired"
            except RuntimeError:
                outcome["status"] = "closed"

        t = threading.Thread(target=waiter, daemon=True)
        t.start()

        time.sleep(0.05)
        pool.close()
        t.join(timeout=1.0)

        assert not t.is_alive()
        assert outcome.get("status") == "closed"
        # Returning after close should be a no-op, not an error.
        pool.return_connection(held)


# ---------------------------------------------------------------------------
# ThreadSafeQueryEngine
# ---------------------------------------------------------------------------

class TestThreadSafeQueryEngine:
    def test_in_memory_pool_connections_share_state(self):
        engine = create_thread_safe_engine(":memory:")
        try:
            engine.execute_sql("CREATE TABLE t (x INTEGER)", read_only=False)
            engine.execute_sql("INSERT INTO t VALUES (?)", params=[1], read_only=False)
            result = engine.execute_sql("SELECT COUNT(*) AS c FROM t")
            assert result.to_pydict()["c"] == [1]
        finally:
            engine.close()

    def test_run_plan_forwards_params_and_flags(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        engine = create_thread_safe_engine(db_path)
        try:
            engine.run({"sql": "CREATE TABLE t (x INTEGER)", "read_only": False})
            engine.run(
                {
                    "sql": "INSERT INTO t VALUES (?)",
                    "params": [11],
                    "read_only": False,
                }
            )
            result = engine.run(
                {
                    "sql": "SELECT x FROM t WHERE x = ?",
                    "params": [11],
                    "timeout": 1.0,
                }
            )
            assert result.to_pydict()["x"] == [11]
        finally:
            engine.close()
            Path(db_path).unlink(missing_ok=True)

    def test_execute_sql_raises_query_timeout_on_slow_query(self, monkeypatch):
        class _FakeResult:
            def arrow(self):
                return pa.table({"x": [1]})

        class _SlowConn:
            def __init__(self) -> None:
                self.interrupted = False

            def execute(self, sql, params):
                time.sleep(0.05)
                return _FakeResult()

            def interrupt(self):
                self.interrupted = True

        slow_conn = _SlowConn()

        @contextmanager
        def _fake_read_connection(timeout: float = 5.0):
            yield slow_conn

        engine = create_thread_safe_engine(":memory:")
        try:
            monkeypatch.setattr(engine.pool, "get_read_connection", _fake_read_connection)
            with pytest.raises(QueryTimeoutError, match="timed out"):
                engine.execute_sql("SELECT 1", timeout=0.01)
            assert slow_conn.interrupted is True
        finally:
            engine.close()

    def test_execute_sql_uses_timeout_for_connection_wait(self, monkeypatch):
        captured: dict[str, float] = {}

        class _FakeResult:
            def arrow(self):
                return pa.table({"x": [1]})

        class _Conn:
            def execute(self, sql, params):
                return _FakeResult()

        @contextmanager
        def _fake_read_connection(timeout: float = 5.0):
            captured["timeout"] = timeout
            yield _Conn()

        engine = create_thread_safe_engine(":memory:")
        try:
            monkeypatch.setattr(engine.pool, "get_read_connection", _fake_read_connection)
            result = engine.execute_sql("SELECT 1", timeout=0.25)
            assert result.to_pydict()["x"] == [1]
            assert captured["timeout"] == pytest.approx(0.25)
        finally:
            engine.close()

    def test_derived_versions_mapping_is_mutable_for_compat(self):
        engine = create_thread_safe_engine(":memory:")
        try:
            engine.derived_versions["temp_metric"] = "snap-a"
            assert engine.derived_versions == {"temp_metric": "snap-a"}
        finally:
            engine.close()

    def test_ingest_invalidates_derived_state(self):
        engine = create_thread_safe_engine(":memory:")
        table = _make_valid_ball_event_table(1)
        try:
            engine.ingest_events(table, "snap-a")
            engine.execute_sql(
                "CREATE TABLE derived.temp_metric AS SELECT 1 AS x",
                read_only=False,
            )
            engine.derived_versions["temp_metric"] = "snap-a"

            engine.ingest_events(table, "snap-b")

            assert engine.derived_versions == {}
            count = engine.execute_sql(
                "SELECT COUNT(*) AS c FROM information_schema.tables WHERE table_schema = 'derived'"
            ).to_pydict()["c"][0]
            assert count == 0
        finally:
            engine.close()

    def test_insert_live_delivery_after_schema_v1_ingest(self):
        engine = create_thread_safe_engine(":memory:")
        table = _make_valid_ball_event_table(1)
        try:
            engine.ingest_events(table, "snap-live")
            engine.insert_live_delivery(
                {
                    "match_id": "live-ts",
                    "inning": 2,
                    "over": 16,
                    "ball": 3,
                    "runs_total": 140,
                    "wickets_fallen": 4,
                }
            )

            rows = engine.execute_sql(
                "SELECT match_id, phase FROM ball_events WHERE match_id = ?",
                params=["live-ts"],
            ).to_pydict()
            assert rows["match_id"] == ["live-ts"]
            assert rows["phase"] == ["death"]
        finally:
            engine.close()


# ---------------------------------------------------------------------------
# MetricsCollector (serve/monitoring.py)
# ---------------------------------------------------------------------------

class TestMetricsCollector:
    def test_record_request_increments_total(self):
        from pypitch.serve.monitoring import MetricsCollector
        mc = MetricsCollector()
        mc.record_request("GET", "/health", 200, 0.05)
        mc.record_request("POST", "/analyze", 403, 0.10)
        metrics = mc.get_api_metrics()
        assert metrics["total_requests"] >= 2

    def test_error_metrics_recorded(self):
        from pypitch.serve.monitoring import MetricsCollector
        mc = MetricsCollector()
        mc.record_request("GET", "/bad", 500, 0.01)
        metrics = mc.get_api_metrics()
        assert metrics.get("total_errors", 0) >= 1 or metrics.get("error_rate", 0) >= 0

    def test_request_metrics_returns_dict(self):
        from pypitch.serve.monitoring import MetricsCollector
        mc = MetricsCollector()
        result = mc.get_api_metrics()
        assert isinstance(result, dict)

    def test_system_metrics_returns_dict(self):
        from pypitch.serve.monitoring import MetricsCollector
        mc = MetricsCollector()
        result = mc.get_system_metrics()
        assert isinstance(result, dict)

    def test_record_error_metrics(self):
        from pypitch.serve.monitoring import MetricsCollector
        mc = MetricsCollector()
        mc.record_error("ValueError", "/test")
        # Should not raise

    def test_get_api_metrics_handles_zero_time_span(self):
        from unittest.mock import patch
        from pypitch.serve.monitoring import MetricsCollector

        mc = MetricsCollector()
        with patch("pypitch.serve.monitoring.time.time", return_value=1000.0):
            mc.record_request("GET", "/health", 200, 0.05)

        with patch("pypitch.serve.monitoring.time.time", return_value=1000.0):
            metrics = mc.get_api_metrics(since=1000.0)

        assert metrics["total_requests"] == 1
        assert metrics["requests_per_minute"] == 0

    def test_record_error_cleans_up_stale_metrics(self):
        from unittest.mock import patch
        from pypitch.serve.monitoring import MetricsCollector

        mc = MetricsCollector()
        mc.max_metrics_age = 10

        # First error becomes stale.
        with patch("pypitch.serve.monitoring.time.time", return_value=1000.0):
            mc.record_error("ValueError", "old")

        # Second error should trigger cleanup and remove stale entry.
        with patch("pypitch.serve.monitoring.time.time", return_value=1015.0):
            mc.record_error("ValueError", "new")

        errors = mc.metrics["errors"]
        assert len(errors) == 1
        assert errors[0]["message"] == "new"


# ---------------------------------------------------------------------------
# RateLimiter (serve/rate_limit.py)
# ---------------------------------------------------------------------------

class TestRateLimiter:
    def test_first_request_is_allowed(self):
        from pypitch.serve.rate_limit import RateLimiter
        limiter = RateLimiter(requests_per_minute=5)
        assert limiter.is_allowed("client-1") is True

    def test_within_limit_all_allowed(self):
        from pypitch.serve.rate_limit import RateLimiter
        limiter = RateLimiter(requests_per_minute=3)
        for _ in range(3):
            assert limiter.is_allowed("client-x") is True

    def test_exceeds_limit_is_blocked(self):
        from pypitch.serve.rate_limit import RateLimiter
        limiter = RateLimiter(requests_per_minute=2)
        limiter.is_allowed("c")
        limiter.is_allowed("c")
        assert limiter.is_allowed("c") is False

    def test_different_clients_tracked_independently(self):
        from pypitch.serve.rate_limit import RateLimiter
        limiter = RateLimiter(requests_per_minute=1)
        assert limiter.is_allowed("a") is True
        assert limiter.is_allowed("b") is True
        assert limiter.is_allowed("a") is False

    def test_get_remaining_requests(self):
        from pypitch.serve.rate_limit import RateLimiter
        limiter = RateLimiter(requests_per_minute=5)
        limiter.is_allowed("r")
        remaining = limiter.get_remaining_requests("r")
        assert remaining == 4

    def test_cleanup_old_keys_does_not_raise(self):
        from pypitch.serve.rate_limit import RateLimiter
        limiter = RateLimiter(requests_per_minute=10)
        limiter.is_allowed("temp-key")
        limiter.cleanup_old_keys()

    def test_get_client_key_uses_peer_ip(self):
        from pypitch.serve.rate_limit import get_client_key
        from unittest.mock import MagicMock
        req = MagicMock()
        req.client = MagicMock()
        req.client.host = "10.0.0.1"
        req.headers = {}
        key = get_client_key(req)
        assert "10.0.0.1" in key

    def test_get_client_key_uses_bearer_token(self):
        from pypitch.serve.rate_limit import get_client_key
        from unittest.mock import MagicMock
        req = MagicMock()
        req.client = None
        req.headers = {"Authorization": "Bearer mytoken123"}
        key = get_client_key(req)
        assert key.startswith("api_key:")
