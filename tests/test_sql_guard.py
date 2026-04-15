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


# ── Allowlist tests ───────────────────────────────────────────────────────────

def test_sql_guard_allows_public_table():
    sql = validate_read_only_query("SELECT * FROM ball_events LIMIT 10")
    assert "ball_events" in sql


def test_sql_guard_allows_all_public_tables():
    for table in ("ball_events", "matchup_stats", "phase_stats",
                  "fantasy_points_avg", "venue_bias", "chase_history", "venue_baselines"):
        validate_read_only_query(f"SELECT * FROM {table} LIMIT 1")


def test_sql_guard_allows_quoted_public_table():
    validate_read_only_query('SELECT * FROM "ball_events" LIMIT 1')


def test_sql_guard_allows_main_schema_public_table():
    validate_read_only_query("SELECT * FROM main.ball_events LIMIT 1")


def test_sql_guard_rejects_unknown_table():
    with pytest.raises(SQLValidationError, match="is not permitted for /analyze queries"):
        validate_read_only_query("SELECT * FROM secret_data")


def test_sql_guard_rejects_quoted_unknown_table():
    with pytest.raises(SQLValidationError, match="is not permitted for /analyze queries"):
        validate_read_only_query('SELECT * FROM "secret_data"')


def test_sql_guard_rejects_information_schema():
    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT * FROM information_schema.tables")


def test_sql_guard_rejects_duckdb_catalog():
    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT * FROM duckdb_tables()")


def test_sql_guard_rejects_audit_log():
    with pytest.raises(SQLValidationError):
        validate_read_only_query("SELECT * FROM audit_log")


def test_sql_guard_rejects_dangerous_table_functions():
    with pytest.raises(SQLValidationError, match="Forbidden SQL function"):
        validate_read_only_query("SELECT * FROM read_csv_auto('anything.csv')")


def test_sql_guard_allows_cte_referencing_public_table():
    sql = validate_read_only_query(
        "WITH recent AS (SELECT * FROM ball_events LIMIT 100) "
        "SELECT batter_id, SUM(runs_batter) FROM recent GROUP BY batter_id"
    )
    assert sql.startswith("WITH")


def test_sql_guard_cte_name_not_confused_with_external_table():
    # 't' is a CTE name — should not be checked against allowlist
    sql = validate_read_only_query("WITH t AS (SELECT 1 AS x) SELECT x FROM t")
    assert sql.startswith("WITH")


# ── Unicode normalization tests ───────────────────────────────────────────────

def test_sql_guard_normalizes_fullwidth_chars():
    # Fullwidth DROP — NFKC normalization should convert this to ASCII,
    # then the forbidden-keyword check catches it.
    fullwidth_drop = "\uff24\uff32\uff2f\uff30"  # ＤＲＯＰin fullwidth
    with pytest.raises(SQLValidationError):
        validate_read_only_query(f"SELECT 1; {fullwidth_drop} TABLE ball_events")


def test_sql_guard_normalizes_unicode_space_variants():
    # Non-breaking space before SELECT — should still parse correctly
    sql = validate_read_only_query("\u00a0SELECT 1 AS x")
    assert "SELECT" in sql.upper()


def test_sql_guard_allows_semicolon_inside_string_literal():
    # Valid single statement: semicolon appears inside a quoted literal.
    sql = validate_read_only_query("SELECT 'a;b' AS x")
    assert "a;b" in sql
