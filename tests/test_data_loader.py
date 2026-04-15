"""Tests for DataLoader download retry behavior."""

from __future__ import annotations

import requests
import pytest

from pypitch.data.loader import DataLoader


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
