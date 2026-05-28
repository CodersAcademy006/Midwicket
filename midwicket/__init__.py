"""
Midwicket — The Advanced Open-Source Cricket Intelligence SDK.

Midwicket is a high-performance cricket intelligence platform engineered for deep analytics,
statistical modeling, and robust data serving. Built on a modern data stack featuring
DuckDB, PyArrow, and FastAPI, it empowers developers, analysts, and enthusiasts with
blazingly fast insights and seamless API integrations.

Quick start::

    import midwicket as md

    # One-liner player stats
    stats = md.express.get_player_stats("V Kohli")

    # Win probability
    prob = md.express.predict_win(
        venue="Wankhede Stadium",
        target=180,
        current_score=95,
        wickets_down=3,
        overs_done=10.0,
    )

    # Head-to-head matchup
    result = md.express.get_matchup("V Kohli", "JJ Bumrah")
"""

from typing import Any

# Core session
from .api.session import MidwicketSession, init

# Sub-packages (used via md.data.*, md.visuals.*, etc.)
from . import data
from . import visuals

# Top-level convenience modules
from . import api
from . import express

# Stats / fantasy / sim namespaces
import midwicket.api.stats as stats
import midwicket.api.fantasy as fantasy
import midwicket.api.sim as sim

# Common query objects
from .query.matchups import MatchupQuery

# Debug / mode helpers
from .runtime.modes import set_debug_mode

# Head-to-head analysis (new convenience API)
from .api.head_to_head import head_to_head, HeadToHeadSummary

# Player analytics (PA-01 to PA-28)
from .api.player_analytics import (
    career_batting,
    career_bowling,
    career_fielding,
    batting_by_phase,
    bowling_by_phase,
    batting_by_venue,
    bowling_by_venue,
    best_worst_venues,
    batting_by_season,
    bowling_by_season,
    batting_form,
    bowling_form,
    batting_vs_teams,
    bowling_vs_teams,
    weakness_detector,
    batting_by_innings_number,
    batting_in_chases,
    batting_under_pressure,
    death_over_specialist,
    highest_score,
    best_bowling_figures,
    match_streaks,
    milestones_and_failures,
    compare_players,
    batting_leaderboard,
    bowling_leaderboard,
    batting_vs_bowler_hand,
    bowling_vs_batter_hand,
)

# Match configuration
from .core.match_config import MatchConfig


# --- Lazy imports for heavyweight modules (avoid scikit-learn at import time) ---
def __getattr__(name: str) -> Any:
    if name == "WinPredictor":
        from .models.win_predictor import WinPredictor
        return WinPredictor
    if name == "win_probability":
        from .compute.winprob import win_probability
        return win_probability
    if name == "set_win_model":
        from .compute.winprob import set_win_model
        return set_win_model
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# Serve helper (lazy import avoids pulling fastapi at install time)
def serve(*args: Any, **kwargs: Any) -> None:
    """Start the Midwicket REST API server. Requires the ``serve`` extra."""
    from .serve import serve as _serve
    return _serve(*args, **kwargs)


__version__ = "0.1.0"
__author__ = "Midwicket Team"
__email__ = "srjnupadhyay@gmail.com"

__all__ = [
    # Session
    "MidwicketSession",
    "init",
    # Modules
    "data",
    "visuals",
    "api",
    "express",
    # Namespaces
    "stats",
    "fantasy",
    "sim",
    # Query objects
    "MatchupQuery",
    # Helpers
    "set_debug_mode",
    "serve",
    # ML (lazy)
    "WinPredictor",
    "win_probability",
    "set_win_model",
    # Config
    "MatchConfig",
    # Head-to-head
    "head_to_head",
    "HeadToHeadSummary",
    # Player analytics (PA-01 to PA-28)
    "career_batting",
    "career_bowling",
    "career_fielding",
    "batting_by_phase",
    "bowling_by_phase",
    "batting_by_venue",
    "bowling_by_venue",
    "best_worst_venues",
    "batting_by_season",
    "bowling_by_season",
    "batting_form",
    "bowling_form",
    "batting_vs_teams",
    "bowling_vs_teams",
    "weakness_detector",
    "batting_by_innings_number",
    "batting_in_chases",
    "batting_under_pressure",
    "death_over_specialist",
    "highest_score",
    "best_bowling_figures",
    "match_streaks",
    "milestones_and_failures",
    "compare_players",
    "batting_leaderboard",
    "bowling_leaderboard",
    "batting_vs_bowler_hand",
    "bowling_vs_batter_hand",
    # Meta
    "__version__",
]
