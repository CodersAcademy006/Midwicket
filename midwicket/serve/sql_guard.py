"""SQL validation utilities for safe read-only analysis queries."""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Iterable

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Parenthesis, Statement, TokenList
from sqlparse.tokens import Keyword


class SQLValidationError(ValueError):
    """Raised when SQL fails read-only validation."""


_FORBIDDEN_TOKENS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "REPLACE",
    "ATTACH",
    "DETACH",
    "PRAGMA",
    "GRANT",
    "REVOKE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "VACUUM",
    "CALL",
    "COPY",
    "EXPORT",
    "IMPORT",
    "LOAD",
    "INSTALL",
}

_FORBIDDEN_FUNCTIONS = (
    "read_csv",
    "read_csv_auto",
    "read_json",
    "read_json_auto",
    "read_parquet",
    "read_ndjson",
    "read_text",
    "read_blob",
    "csv_scan",
    "parquet_scan",
)

# Allowlist of tables users may query through /analyze.
# Any FROM/JOIN reference to a table not in this set is rejected.
_PUBLIC_TABLES = frozenset({
    "ball_events",
    "matchup_stats",
    "phase_stats",
    "fantasy_points_avg",
    "venue_bias",
    "chase_history",
    "venue_baselines",
})

# Block access to system catalog prefixes regardless of allowlist.
_SYSTEM_PREFIXES = (
    "information_schema.",
    "duckdb_",
    "sqlite_",
    "pg_",
    "sys.",
    "mysql.",
    "audit_log",   # internal — not a public table
)

def _strip_string_literals(sql: str) -> str:
    # Replace quoted strings with placeholders to avoid false-positive keyword matches.
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", "'X'", sql)
    sql = re.sub(r'"(?:[^"\\]|\\.)*"', '"X"', sql)
    return sql


def _normalize_ref_name(name: str) -> str:
    parts = [p.strip().strip('"') for p in str(name).split(".") if p.strip()]
    if not parts:
        return ""
    normalized = ".".join(parts).lower()
    # DuckDB default schema prefixes should not force false negatives.
    if normalized.startswith("main."):
        normalized = normalized.split(".", 1)[1]
    return normalized


def _iter_identifiers(token: TokenList) -> Iterable[Identifier]:
    if isinstance(token, IdentifierList):
        for ident in token.get_identifiers():
            if isinstance(ident, Identifier):
                yield ident
    elif isinstance(token, Identifier):
        yield token


def _identifier_to_refs(identifier: Identifier) -> set[str]:
    refs: set[str] = set()

    for child in identifier.tokens:
        if isinstance(child, Function):
            fn_name = child.get_name() or child.value
            refs.add(_normalize_ref_name(fn_name))
            return refs

    real = identifier.get_real_name()
    parent = identifier.get_parent_name()
    if real:
        refs.add(_normalize_ref_name(f"{parent}.{real}" if parent else real))
    return refs


def _extract_cte_names(parsed: Statement) -> set[str]:
    cte_names: set[str] = set()
    tokens = [t for t in parsed.tokens if not t.is_whitespace]
    if not tokens:
        return cte_names

    # Look only at initial WITH clause.
    if not str(tokens[0]).upper().startswith("WITH"):
        return cte_names

    for token in tokens[1:]:
        value_upper = str(token).upper()
        if value_upper.startswith("SELECT"):
            break
        for ident in _iter_identifiers(token):
            name = ident.get_real_name() or ident.get_name()
            if name:
                cte_names.add(_normalize_ref_name(name))

    return cte_names


def _collect_table_refs(parsed: TokenList) -> set[str]:
    refs: set[str] = set()
    tokens = list(parsed.tokens)

    i = 0
    while i < len(tokens):
        tok = tokens[i]

        if isinstance(tok, TokenList) and tok.is_group:
            refs.update(_collect_table_refs(tok))

        if tok.ttype in Keyword and str(tok).upper() in {"FROM", "JOIN"}:
            j = i + 1
            while j < len(tokens) and tokens[j].is_whitespace:
                j += 1

            if j < len(tokens):
                nxt = tokens[j]
                if isinstance(nxt, IdentifierList):
                    for ident in nxt.get_identifiers():
                        if isinstance(ident, Identifier):
                            refs.update(_identifier_to_refs(ident))
                elif isinstance(nxt, Identifier):
                    refs.update(_identifier_to_refs(nxt))
                elif isinstance(nxt, Function):
                    fn_name = nxt.get_name() or nxt.value
                    refs.add(_normalize_ref_name(fn_name))
                elif isinstance(nxt, Parenthesis):
                    # Subqueries handled recursively above.
                    pass
                else:
                    normalized = _normalize_ref_name(str(nxt))
                    if normalized:
                        refs.add(normalized)

        i += 1

    return refs


def _walk_serialized_refs(node, base: set[str], funcs: set[str]) -> None:
    """Recursively collect BASE_TABLE / TABLE_FUNCTION refs from DuckDB's
    json_serialize_sql output. This reflects the query as DuckDB actually
    parses it, so it cannot be fooled by comma-joins, parenthesized/nested
    expressions, schema qualification, or quoted identifiers."""
    if isinstance(node, dict):
        node_type = node.get("type")
        if node_type == "BASE_TABLE":
            schema = (node.get("schema_name") or "").strip()
            table = (node.get("table_name") or "").strip()
            if table:
                ref = f"{schema}.{table}" if schema else table
                base.add(_normalize_ref_name(ref))
        elif node_type == "TABLE_FUNCTION":
            fn = node.get("function")
            if isinstance(fn, dict):
                fn_name = fn.get("function_name") or fn.get("schema") or ""
                if fn_name:
                    funcs.add(_normalize_ref_name(str(fn_name)))
        for value in node.values():
            _walk_serialized_refs(value, base, funcs)
    elif isinstance(node, list):
        for value in node:
            _walk_serialized_refs(value, base, funcs)


def _duckdb_table_refs(statement: str) -> tuple[set[str], set[str]] | None:
    """Resolve table and table-function references using DuckDB's own parser.

    Returns (base_table_refs, table_function_names) on success, or ``None`` if
    DuckDB is unavailable (so the caller can fall back to the sqlparse path).
    Raises SQLValidationError if DuckDB cannot parse the statement — a query
    DuckDB cannot parse cannot be executed and must not be trusted.
    """
    try:
        import duckdb
    except Exception:  # pragma: no cover - duckdb is a hard dependency in prod
        return None

    try:
        con = duckdb.connect()
        try:
            serialized = con.execute(
                "SELECT json_serialize_sql(?)", [statement]
            ).fetchone()[0]
        finally:
            con.close()
    except Exception:
        # Could not even serialize — treat as unparseable and reject.
        raise SQLValidationError("Query could not be parsed and is not permitted")

    try:
        parsed = json.loads(serialized)
    except (ValueError, TypeError):
        raise SQLValidationError("Query could not be parsed and is not permitted")

    if isinstance(parsed, dict) and parsed.get("error"):
        raise SQLValidationError("Query could not be parsed and is not permitted")

    base: set[str] = set()
    funcs: set[str] = set()
    _walk_serialized_refs(parsed, base, funcs)
    return base, funcs


def validate_read_only_query(
    sql: str,
    *,
    max_selects: int = 5,
    max_joins: int = 8,
    max_unions: int = 3,
) -> str:
    """Validate read-only SQL and return the normalized statement.

    Rules:
    - NFKC-normalized to prevent Unicode homoglyph bypass.
    - Exactly one statement.
    - Statement must begin with SELECT or WITH.
    - No comments, no write/DDL/system keywords.
    - All FROM/JOIN table references must be in the public allowlist.
    - Complexity bounds on SELECT/JOIN/UNION counts.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise SQLValidationError("SQL must be a non-empty string")

    # NFKC normalization defeats homoglyph/fullwidth character injection
    # e.g. ＤＲＯＰcould otherwise bypass keyword matching.
    sql = unicodedata.normalize("NFKC", sql).strip()

    if "--" in sql or "/*" in sql or "*/" in sql:
        raise SQLValidationError("SQL comments are not allowed in /analyze queries")

    # Use sqlparse splitting so semicolons inside string literals do not
    # incorrectly look like statement boundaries.
    statements = [stmt.strip() for stmt in sqlparse.split(sql) if stmt.strip()]
    if len(statements) != 1:
        raise SQLValidationError("Exactly one SQL statement is allowed")

    statement = statements[0]
    stmt_upper = statement.upper().lstrip()
    if not (stmt_upper.startswith("SELECT") or stmt_upper.startswith("WITH")):
        raise SQLValidationError("Only read-only SELECT/WITH queries are allowed")

    parsed_statements = sqlparse.parse(statement)
    if len(parsed_statements) != 1:
        raise SQLValidationError("Exactly one SQL statement is allowed")
    parsed_statement = parsed_statements[0]

    scan = _strip_string_literals(statement).upper()

    for token in _FORBIDDEN_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", scan):
            raise SQLValidationError(f"Forbidden SQL keyword detected: {token}")

    for func in _FORBIDDEN_FUNCTIONS:
        if re.search(rf"\b{re.escape(func)}\s*\(", scan, re.IGNORECASE):
            raise SQLValidationError(f"Forbidden SQL function detected: {func}")

    # ── Table allowlist check ────────────────────────────────────────────────
    # Resolve every referenced table the way DuckDB actually parses the query.
    # This is the authoritative source of truth: it cannot be bypassed by
    # comma-joins after a subquery, parenthesized/nested table expressions,
    # schema-qualified names, or quoted identifiers. The legacy sqlparse-based
    # collector is retained only as a fallback for the (unexpected) case where
    # DuckDB is unavailable to import.
    cte_names = _extract_cte_names(parsed_statement)

    duck = _duckdb_table_refs(statement)
    if duck is not None:
        # DuckDB's parser is authoritative: its BASE_TABLE list is exactly the
        # set of real tables the query reads (derived-table/subquery aliases are
        # NOT base tables, so they are correctly excluded). Use it directly.
        duck_tables, duck_functions = duck
        # Any table-valued function (e.g. duckdb_tables(), read_csv) resolved by
        # DuckDB is never an allowlisted table — reject outright.
        for fn in duck_functions:
            if fn not in _PUBLIC_TABLES:
                raise SQLValidationError(
                    f"Table function {fn!r} is not permitted for /analyze queries"
                )
        table_refs = duck_tables
    else:
        # Fallback only when DuckDB is unavailable: hand-rolled sqlparse scan.
        table_refs = _collect_table_refs(parsed_statement)

    external_refs = table_refs - cte_names

    for ref in external_refs:
        # Block system catalog prefixes
        for prefix in _SYSTEM_PREFIXES:
            if ref.startswith(prefix.lower()) or ref == prefix.lower().rstrip("."):
                raise SQLValidationError(
                    f"Access to system table {ref!r} is not permitted"
                )
        # Enforce public allowlist
        if ref not in _PUBLIC_TABLES:
            raise SQLValidationError(
                f"Table {ref!r} is not permitted for /analyze queries"
            )

    select_count = len(re.findall(r"\bSELECT\b", scan))
    join_count = len(re.findall(r"\bJOIN\b", scan))
    union_count = len(re.findall(r"\bUNION\b", scan))

    if select_count > max_selects:
        raise SQLValidationError("Query too complex: too many SELECT clauses")
    if join_count > max_joins:
        raise SQLValidationError("Query too complex: too many JOIN clauses")
    if union_count > max_unions:
        raise SQLValidationError("Query too complex: too many UNION clauses")

    return statement


def check_query_plan(plan_json_str: str) -> None:
    """Analyze a DuckDB physical query plan (JSON) and reject unbounded operations."""
    try:
        plan = json.loads(plan_json_str)
    except Exception:
        raise SQLValidationError("Invalid query plan format")

    def _walk(node):
        if isinstance(node, dict):
            if node.get("name") == "CROSS_PRODUCT":
                raise SQLValidationError("Query rejected: unbounded CROSS JOIN detected, which exceeds cost limits.")
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for v in node:
                _walk(v)

    _walk(plan)
