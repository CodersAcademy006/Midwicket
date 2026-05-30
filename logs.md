# Midwicket — Resolved Bugs Log

## MW-001: `_get_con()` returns a context manager, not a connection
- **Severity:** P0
- **Area:** Player Analytics
- **Fix:** Added `borrow_connection` to `QueryEngine` and updated `_get_con` in `player_analytics.py` to use it instead of `raw_connection()`. Exposed `get_session` at the module level in `session.py`.
- **Date Resolved:** 2026-05-30
- **PR:** #57

## MW-002: `sim.predict_win` crashes with `ValueError` on every call (missing `match_date`)
- **Severity:** P0
- **Area:** API
- **Fix:** Added `match_date` optional parameter to `predict_win()` in `midwicket/api/sim.py` and defaulted it to `date.today()`, passing it to the registry's `resolve_venue()` call.
- **Date Resolved:** 2026-05-30
- **PR:** #58

