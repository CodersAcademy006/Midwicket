"""Tests for DataLoader download retry behavior."""

from __future__ import annotations

import importlib
import requests
import pytest

from pypitch.data.loader import DataLoader


def _reload_loader_module():
    import pypitch.data.loader as loader_mod
    return importlib.reload(loader_mod)


class _FakeResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.headers = {"content-length": str(sum(len(c) for c in chunks))}

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 8192):
        del chunk_size
        for chunk in self._chunks:
            yield chunk


def test_download_retries_then_succeeds(monkeypatch, tmp_path):
    calls = {"n": 0}

    def _fake_get(url, stream=True, timeout=60):
        del url, stream, timeout
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.exceptions.ConnectionError("transient network error")
        return _FakeResponse([b"1234"])

    monkeypatch.setattr("pypitch.data.loader.requests.get", _fake_get)
    monkeypatch.setattr(DataLoader, "_extract", lambda self: None)

    loader = DataLoader(str(tmp_path))
    loader.download(force=True)

    assert calls["n"] == 3
    assert loader.zip_path.exists()


def test_download_exhausted_retries_raises(monkeypatch, tmp_path):
    def _always_fail(url, stream=True, timeout=60):
        del url, stream, timeout
        raise requests.exceptions.Timeout("network timeout")

    monkeypatch.setattr("pypitch.data.loader.requests.get", _always_fail)
    monkeypatch.setattr(DataLoader, "_extract", lambda self: None)

    loader = DataLoader(str(tmp_path))
    with pytest.raises(ConnectionError):
        loader.download(force=True)

    assert not loader.zip_path.exists()


def test_loader_invalid_int_env_values_fall_back(monkeypatch):
    monkeypatch.setenv("PYPITCH_DOWNLOAD_TIMEOUT", "not-int")
    monkeypatch.setenv("PYPITCH_EXTRACT_TIMEOUT", "0")
    monkeypatch.setenv("PYPITCH_DOWNLOAD_RETRIES", "-3")

    loader_mod = _reload_loader_module()

    assert loader_mod._DOWNLOAD_TIMEOUT == 60
    assert loader_mod._EXTRACT_TIMEOUT == 120
    assert loader_mod._DOWNLOAD_RETRY_ATTEMPTS == 3


def test_loader_invalid_float_env_values_fall_back(monkeypatch):
    monkeypatch.setenv("PYPITCH_DOWNLOAD_RETRY_BACKOFF_BASE", "not-float")
    monkeypatch.setenv("PYPITCH_DOWNLOAD_RETRY_BACKOFF_MAX", "-1")

    loader_mod = _reload_loader_module()

    assert loader_mod._DOWNLOAD_RETRY_BACKOFF_BASE == 0.5
    assert loader_mod._DOWNLOAD_RETRY_BACKOFF_MAX == 8.0
