"""SQL validation utilities for safe read-only analysis queries."""

from __future__ import annotations

import re


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
}


def _strip_string_literals(sql: str) -> str:
    # Replace quoted strings with placeholders to avoid false-positive keyword matches.
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", "'X'", sql)
    sql = re.sub(r'"(?:[^"\\]|\\.)*"', '"X"', sql)
    return sql


def validate_read_only_query(
    sql: str,
    *,
    max_selects: int = 5,
    max_joins: int = 8,
    max_unions: int = 3,
) -> str:
    """Validate read-only SQL and return the normalized statement.

    Rules:
    - Exactly one statement.
    - Statement must begin with SELECT or WITH.
    - No comments, no write/DDL/system keywords.
    - Complexity bounds on SELECT/JOIN/UNION counts.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise SQLValidationError("SQL must be a non-empty string")

    sql = sql.strip()

    if "--" in sql or "/*" in sql or "*/" in sql:
        raise SQLValidationError("SQL comments are not allowed in /analyze queries")

    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    if len(statements) != 1:
        raise SQLValidationError("Exactly one SQL statement is allowed")

    statement = statements[0]
    stmt_upper = statement.upper().lstrip()
    if not (stmt_upper.startswith("SELECT") or stmt_upper.startswith("WITH")):
        raise SQLValidationError("Only read-only SELECT/WITH queries are allowed")

    scan = _strip_string_literals(statement).upper()

    for token in _FORBIDDEN_TOKENS:
        if re.search(rf"\b{re.escape(token)}\b", scan):
            raise SQLValidationError(f"Forbidden SQL keyword detected: {token}")

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
