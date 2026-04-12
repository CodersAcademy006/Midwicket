"""
PyPitch — The Open Source Cricket Intelligence SDK.

Quick start::

    import pypitch as pp

    # One-liner player stats
    stats = pp.express.get_player_stats("V Kohli")

    # Win probability
    prob = pp.express.predict_win(
        venue="Wankhede Stadium",
        target=180,
        current_score=95,
        wickets_down=3,
        overs_done=10.0,
    )

    # Head-to-head matchup
    result = pp.express.get_matchup("V Kohli", "JJ Bumrah")
"""

from typing import Any

# Core session
from .api.session import PyPitchSession, init

# Sub-packages (used via pp.data.*, pp.visuals.*, etc.)
from . import data
from . import visuals

# Top-level convenience modules
from . import api
from . import express

# Stats / fantasy / sim namespaces
import pypitch.api.stats as stats
import pypitch.api.fantasy as fantasy
import pypitch.api.sim as sim

# Common query objects
from .query.matchups import MatchupQuery

# Debug / mode helpers
from .runtime.modes import set_debug_mode

# Head-to-head analysis (new convenience API)
from .api.head_to_head import head_to_head, HeadToHeadSummary

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
    """Start the PyPitch REST API server. Requires the ``serve`` extra."""
    from .serve import serve as _serve
    return _serve(*args, **kwargs)


__version__ = "0.1.0"
__author__ = "PyPitch Team"
__email__ = "srjnupadhyay@gmail.com"

__all__ = [
    # Session
    "PyPitchSession",
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
    # Meta
    "__version__",
]
