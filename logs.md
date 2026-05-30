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

## MW-003: `ThreadSafeQueryEngine._ensure_schema` creates legacy `ball_events` table
- **Severity:** P1
- **Area:** Schema
- **Fix:** Replaced legacy columns in `_ensure_schema` inside `midwicket/storage/thread_safe_engine.py` with the full unified V1 schema definition, and added self-healing migration alterations to update existing legacy schema databases dynamically on start.
- **Date Resolved:** 2026-05-30
- **PR:** #59

## MW-004: `LiveDeliverySchema` uses legacy fields; live data invisible to all v1 analytics queries
- **Severity:** P1
- **Area:** Live
- **Fix:** Added missing v1 schema fields to `LiveDeliverySchema` in `midwicket/live/ingestor.py`, mapped `player_out` to `player_dismissed` in `_ingest_delivery_data` dynamically, and updated the storage engines to gracefully resolve missing live V1 fields to "Unknown" placeholder registry identities.
- **Date Resolved:** 2026-05-30
- **PR:** #60



