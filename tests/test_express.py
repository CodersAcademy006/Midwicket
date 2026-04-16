"""Tests for express session bootstrap and cache behavior."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pypitch.express as px


def _seed_local_raw_data(base_dir: Path) -> None:
    raw_dir = base_dir / "raw" / "ipl"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "dummy_match.json").write_text("{}", encoding="utf-8")


class _FakeLoader:
    def __init__(self, data_dir: str):
        self.raw_dir = Path(data_dir) / "raw" / "ipl"

    def download(self) -> None:
        raise AssertionError("download() should not be called when raw data exists")


def test_auto_setup_session_is_thread_safe(monkeypatch, tmp_path: Path):
    _seed_local_raw_data(tmp_path)

    created: list[str] = []
    created_lock = threading.Lock()

    class _FakeSession:
        def __init__(self, data_dir: str):
            # Increase overlap so races become visible if locking is removed.
            time.sleep(0.02)
            with created_lock:
                created.append(data_dir)
            self.data_dir = data_dir

    monkeypatch.setattr(px, "DataLoader", _FakeLoader)
    monkeypatch.setattr(px, "PyPitchSession", _FakeSession)
    monkeypatch.setattr(px, "_cached_session", None)
    monkeypatch.setattr(px, "_cached_session_dir", None)

    start = threading.Barrier(8)
    errors: list[Exception] = []
    sessions = []

    def _worker() -> None:
        try:
            start.wait()
            sessions.append(px._auto_setup_session(str(tmp_path)))
        except Exception as exc:  # pragma: no cover - defensive capture
            errors.append(exc)

    threads = [threading.Thread(target=_worker, daemon=True) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors
    assert len(created) == 1
    assert len({id(s) for s in sessions}) == 1


def test_auto_setup_session_rebuilds_for_different_dirs(monkeypatch, tmp_path: Path):
    d1 = tmp_path / "a"
    d2 = tmp_path / "b"
    _seed_local_raw_data(d1)
    _seed_local_raw_data(d2)

    created: list[str] = []

    class _FakeSession:
        def __init__(self, data_dir: str):
            created.append(data_dir)
            self.data_dir = data_dir

    monkeypatch.setattr(px, "DataLoader", _FakeLoader)
    monkeypatch.setattr(px, "PyPitchSession", _FakeSession)
    monkeypatch.setattr(px, "_cached_session", None)
    monkeypatch.setattr(px, "_cached_session_dir", None)

    s1 = px._auto_setup_session(str(d1))
    s2 = px._auto_setup_session(str(d2))

    assert s1 is not s2
    assert created == [str(d1), str(d2)]
