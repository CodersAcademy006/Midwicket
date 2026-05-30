# Midwicket — Resolved Bugs Log

## MW-001: `_get_con()` returns a context manager, not a connection
- **Severity:** P0
- **Area:** Player Analytics
- **Fix:** Added `borrow_connection` to `QueryEngine` and updated `_get_con` in `player_analytics.py` to use it instead of `raw_connection()`. Exposed `get_session` at the module level in `session.py`.
- **Date Resolved:** 2026-05-30
- **PR:** #57
