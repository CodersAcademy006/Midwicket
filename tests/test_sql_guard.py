"""Unit tests for /analyze SQL guard validation."""

import pytest

from pypitch.serve.sql_guard import validate_read_only_query, SQLValidationError


def test_sql_guard_allows_simple_select():
    sql = validate_read_only_query("SELECT 1 AS x")
    assert sql == "SELECT 1 AS x"


def test_sql_guard_allows_cte_select():
    sql = validate_read_only_query("WITH t AS (SELECT 1 AS x) SELECT x FROM t")
    assert sql.startswith("WITH")


def test_sql_guard_rejects_write_keyword():
    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT * FROM ball_events; DELETE FROM ball_events")


def test_sql_guard_rejects_comments():
    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT 1 -- hidden tail")


def test_sql_guard_rejects_non_select_start():
    with pytest.raises(SQLValidationError):
        validate_read_only_query("UPDATE ball_events SET over = 0")
