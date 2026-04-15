"""SQL validation utilities for safe read-only analysis queries."""

from __future__ import annotations

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

    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
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
    # Collect parsed table names, subtract CTE names, and ensure every external
    # reference is from the public allowlist.
    table_refs = _collect_table_refs(parsed_statement)
    cte_names = _extract_cte_names(parsed_statement)
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
