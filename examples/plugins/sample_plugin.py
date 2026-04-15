"""
Sample PyPitch plugin — demonstrates the full plugin contract.

To load this plugin:
    export PYPITCH_PLUGIN_ALLOWLIST=examples.plugins.sample_plugin
    export PYPITCH_PLUGINS=sample:examples.plugins.sample_plugin

Or load it directly in code:

    from pypitch.api.plugins import PluginManager, PluginSpec
    import os

    os.environ["PYPITCH_PLUGIN_ALLOWLIST"] = "examples.plugins.sample_plugin"
    manager = PluginManager()
    spec = PluginSpec(
        name="sample",
        entry_point="examples.plugins.sample_plugin",
        version="1.0.0",
        description="Example plugin for PyPitch",
    )
    manager.load_plugin(spec)
    strike_rate_fn = manager.get_metric("strike_rate")
    print(strike_rate_fn(runs=45, balls=30))   # → 150.0
"""

from typing import Any, Dict


# ── Metrics ──────────────────────────────────────────────────────────────────

def _strike_rate(runs: int, balls: int) -> float:
    """Batter strike rate: runs per 100 balls."""
    if balls <= 0:
        return 0.0
    return round(runs / balls * 100, 2)


def _economy_rate(runs_conceded: int, overs: float) -> float:
    """Bowler economy rate: runs per over."""
    if overs <= 0:
        return 0.0
    return round(runs_conceded / overs, 2)


def register_metrics() -> Dict[str, Any]:
    """Return a mapping of metric_name → callable for the PluginManager."""
    return {
        "strike_rate": _strike_rate,
        "economy_rate": _economy_rate,
    }


# ── Reports ───────────────────────────────────────────────────────────────────

def _text_scorecard(match_data: Dict[str, Any]) -> str:
    """Minimal text scorecard from a normalised match dict."""
    info = match_data.get("info", {})
    teams = info.get("teams", ["Team A", "Team B"])
    date = info.get("dates", ["unknown"])[0]
    return f"Match: {' vs '.join(teams)} on {date}"


def register_reports() -> Dict[str, Any]:
    """Return a mapping of report_name → callable for the PluginManager."""
    return {
        "text_scorecard": _text_scorecard,
    }
