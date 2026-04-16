"""Tests for configuration env parsing robustness."""

from __future__ import annotations

import importlib

import pytest


def _reload_config_module():
    import pypitch.config as cfg
    return importlib.reload(cfg)


def test_invalid_api_port_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYPITCH_API_PORT", "not-a-port")
    cfg = _reload_config_module()
    assert cfg.API_PORT == 8000


def test_out_of_range_api_port_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYPITCH_API_PORT", "70000")
    cfg = _reload_config_module()
    assert cfg.API_PORT == 8000


@pytest.mark.parametrize("bad_ttl", ["not-a-number", "0", "-1"])
def test_invalid_cache_ttl_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
    bad_ttl: str,
) -> None:
    monkeypatch.setenv("PYPITCH_CACHE_TTL", bad_ttl)
    cfg = _reload_config_module()
    assert cfg.CACHE_TTL == 3600


def test_db_threads_validation_remains_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYPITCH_DB_THREADS", "invalid")
    with pytest.raises(ValueError, match="PYPITCH_DB_THREADS must be an integer"):
        _reload_config_module()
