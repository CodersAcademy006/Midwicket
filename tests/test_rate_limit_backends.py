"""Tests for rate limiter backends and multi-worker behavior."""

from pathlib import Path

from pypitch.serve.rate_limit import DuckDBRateLimiter, RateLimiter, _build_rate_limiter


def test_duckdb_rate_limiter_shared_state(tmp_path):
    db = tmp_path / "rate_limit.duckdb"

    limiter_a = DuckDBRateLimiter(requests_per_minute=2, db_path=str(db))
    limiter_b = DuckDBRateLimiter(requests_per_minute=2, db_path=str(db))
    try:
        assert limiter_a.is_allowed("client:1") is True
        assert limiter_b.is_allowed("client:1") is True
        # Third request across a different limiter instance should be blocked.
        assert limiter_a.is_allowed("client:1") is False
    finally:
        limiter_a.close()
        limiter_b.close()


def test_build_rate_limiter_defaults_to_memory_for_dev(monkeypatch):
    monkeypatch.delenv("PYPITCH_RATE_LIMIT_BACKEND", raising=False)
    monkeypatch.setenv("PYPITCH_ENV", "development")

    limiter = _build_rate_limiter()
    assert isinstance(limiter, RateLimiter)


def test_build_rate_limiter_uses_duckdb_when_configured(monkeypatch, tmp_path):
    db = tmp_path / "rl.duckdb"
    monkeypatch.setenv("PYPITCH_RATE_LIMIT_BACKEND", "duckdb")
    monkeypatch.setenv("PYPITCH_RATE_LIMIT_DB_PATH", str(db))

    limiter = _build_rate_limiter()
    try:
        assert isinstance(limiter, DuckDBRateLimiter)
        assert Path(limiter.db_path) == db
    finally:
        limiter.close()
