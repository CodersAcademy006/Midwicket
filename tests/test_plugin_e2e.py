"""
End-to-end tests for the PyPitch plugin system.

Exercises the full load → call cycle:
  discover_plugins  →  load_plugin  →  get_metric / get_report
using the sample plugin shipped in examples/plugins/sample_plugin.py.
"""

import os
import sys
import pytest

# Make the examples/ directory importable as a package during tests.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from pypitch.api.plugins import PluginManager, PluginSpec


SAMPLE_ENTRY_POINT = "examples.plugins.sample_plugin"


@pytest.fixture
def manager_with_allowlist(monkeypatch):
    """Return a PluginManager whose allowlist permits the sample plugin."""
    monkeypatch.setenv("PYPITCH_PLUGIN_ALLOWLIST", SAMPLE_ENTRY_POINT)
    return PluginManager()


@pytest.fixture
def loaded_manager(manager_with_allowlist):
    """Return a PluginManager with the sample plugin already loaded."""
    spec = PluginSpec(
        name="sample",
        entry_point=SAMPLE_ENTRY_POINT,
        version="1.0.0",
        description="Sample plugin for e2e testing",
    )
    success = manager_with_allowlist.load_plugin(spec)
    assert success, "load_plugin returned False — check PYPITCH_PLUGIN_ALLOWLIST"
    return manager_with_allowlist


class TestPluginDiscovery:
    def test_discover_returns_spec_when_env_set(self, manager_with_allowlist, monkeypatch):
        monkeypatch.setenv("PYPITCH_PLUGINS", f"sample:{SAMPLE_ENTRY_POINT}")
        specs = manager_with_allowlist.discover_plugins()
        assert any(s.entry_point == SAMPLE_ENTRY_POINT for s in specs)

    def test_discover_empty_when_no_plugins_env(self, manager_with_allowlist, monkeypatch):
        monkeypatch.delenv("PYPITCH_PLUGINS", raising=False)
        assert manager_with_allowlist.discover_plugins() == []

    def test_discover_skips_blocklisted_module(self, monkeypatch):
        monkeypatch.setenv("PYPITCH_PLUGIN_ALLOWLIST", "only_trusted")
        monkeypatch.setenv("PYPITCH_PLUGINS", f"evil:{SAMPLE_ENTRY_POINT}")
        mgr = PluginManager()
        specs = mgr.discover_plugins()
        assert specs == []


class TestPluginLoad:
    def test_load_registers_metric_functions(self, loaded_manager):
        assert loaded_manager.get_metric("strike_rate") is not None
        assert loaded_manager.get_metric("economy_rate") is not None

    def test_load_registers_report_functions(self, loaded_manager):
        assert loaded_manager.get_report("text_scorecard") is not None

    def test_load_rejects_module_not_in_allowlist(self, monkeypatch):
        monkeypatch.setenv("PYPITCH_PLUGIN_ALLOWLIST", "only_trusted")
        mgr = PluginManager()
        spec = PluginSpec(name="bad", entry_point=SAMPLE_ENTRY_POINT)
        result = mgr.load_plugin(spec)
        assert result is False

    def test_load_rejects_empty_allowlist(self, monkeypatch):
        monkeypatch.delenv("PYPITCH_PLUGIN_ALLOWLIST", raising=False)
        mgr = PluginManager()
        spec = PluginSpec(name="any", entry_point=SAMPLE_ENTRY_POINT)
        result = mgr.load_plugin(spec)
        assert result is False

    def test_load_rejects_path_traversal_in_entry_point(self, monkeypatch):
        monkeypatch.setenv("PYPITCH_PLUGIN_ALLOWLIST", "../evil")
        mgr = PluginManager()
        spec = PluginSpec(name="evil", entry_point="../evil/module")
        result = mgr.load_plugin(spec)
        assert result is False

    def test_load_rejects_dep_not_in_allowlist(self, monkeypatch):
        monkeypatch.setenv("PYPITCH_PLUGIN_ALLOWLIST", SAMPLE_ENTRY_POINT)
        mgr = PluginManager()
        spec = PluginSpec(
            name="sample",
            entry_point=SAMPLE_ENTRY_POINT,
            dependencies=["os"],  # 'os' is not in the allowlist prefix
        )
        result = mgr.load_plugin(spec)
        assert result is False


class TestPluginCallCycle:
    def test_strike_rate_calculation(self, loaded_manager):
        fn = loaded_manager.get_metric("strike_rate")
        assert fn(runs=50, balls=40) == pytest.approx(125.0)

    def test_strike_rate_zero_balls(self, loaded_manager):
        fn = loaded_manager.get_metric("strike_rate")
        assert fn(runs=10, balls=0) == 0.0

    def test_economy_rate_calculation(self, loaded_manager):
        fn = loaded_manager.get_metric("economy_rate")
        assert fn(runs_conceded=24, overs=4) == pytest.approx(6.0)

    def test_text_scorecard(self, loaded_manager):
        fn = loaded_manager.get_report("text_scorecard")
        result = fn({"info": {"teams": ["MI", "CSK"], "dates": ["2023-05-14"]}})
        assert "MI" in result and "CSK" in result and "2023-05-14" in result

    def test_missing_metric_returns_none(self, loaded_manager):
        assert loaded_manager.get_metric("nonexistent") is None

    def test_loaded_plugins_registry(self, loaded_manager):
        assert "sample" in loaded_manager._loaded_plugins
