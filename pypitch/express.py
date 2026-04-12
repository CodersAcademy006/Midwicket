"""
PyPitch Express: Simplified API for Beginners

Inspired by Plotly Express, this module provides one-liner access to PyPitch features
with sensible defaults. Hides complexity while keeping power features available for pros.

Usage:
    import pypitch.express as px
    ipl = px.load_competition("ipl", 2023)
    stats = px.get_player_stats("V Kohli")
"""

import os
from pathlib import Path
from typing import Optional, Any, Dict
from pypitch.api.session import PyPitchSession
from pypitch.data.loader import DataLoader
from pypitch.storage.engine import QueryEngine
from pypitch.runtime.executor import RuntimeExecutor
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.core.match_config import MatchConfig
from pypitch.sources.cricsheet_loader import CricsheetLoader

# Global debug mode
_DEBUG_MODE = False

# Global session cache for quick_load
_cached_session: Optional[PyPitchSession] = None
_cached_session_dir: Optional[str] = None

def set_debug_mode(enabled: bool = True) -> None:
    """Enable debug mode for eager execution and verbose logging."""
    global _DEBUG_MODE
    _DEBUG_MODE = enabled
    if enabled:
        print("[PyPitch] Debug mode enabled: Queries will execute eagerly for immediate error feedback.")

def _get_default_data_dir() -> Path:
    """Get default data directory (~/.pypitch_data)."""
    return Path.home() / ".pypitch_data"

def _ensure_data_dir(data_dir: Optional[str] = None) -> Path:
    """Ensure data directory exists."""
    if data_dir:
        path = Path(data_dir)
    else:
        path = _get_default_data_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path

def _auto_setup_session(data_dir: Optional[str] = None) -> PyPitchSession:
    """Auto-setup session with defaults and caching."""
    global _cached_session, _cached_session_dir

    resolved = str(_ensure_data_dir(data_dir))

    # Return cached session only if the data dir matches
    if _cached_session is not None and _cached_session_dir == resolved:
        return _cached_session

    data_path = Path(resolved)

    # Download data on first run if not already present
    loader = DataLoader(str(data_path))
    raw_present = loader.raw_dir.exists() and bool(list(loader.raw_dir.glob("*.json")))

    if not raw_present:
        if _DEBUG_MODE:
            print("No local data found. Downloading IPL dataset (~50 MB)...")
        try:
            loader.download()
        except Exception as exc:
            if _DEBUG_MODE:
                print(f"Download failed: {exc}. Continuing without data.")

    _cached_session = PyPitchSession(str(data_path))
    _cached_session_dir = resolved
    return _cached_session

def load_competition(competition: str, season: int, data_dir: str = "./data"):
    """
    Loads all matches for a competition and season with one line.

    .. note::
       Competition/season filtering is not yet implemented (0.1.x).
       Currently returns all available matches regardless of arguments.
       This will be resolved in v0.2.x.

    Example::

        ipl = px.load_competition("ipl", 2023)
    """
    import logging as _log
    _log.getLogger(__name__).warning(
        "load_competition(competition=%r, season=%r): filtering by "
        "competition/season is not yet implemented. Returning all matches.",
        competition, season,
    )
    loader = CricsheetLoader(data_dir)
    return loader

def get_player_stats(player_name: str, data_dir: Optional[str] = None) -> Optional[Any]:
    """
    Get player statistics by name.

    Args:
        player_name: Player name (fuzzy matched)
        data_dir: Optional custom data directory

    Returns:
        PlayerStats dataclass or None

    Example:
        stats = px.get_player_stats("Virat Kohli")
        print(f"Matches: {stats.matches}, Runs: {stats.runs}")
    """
    session = _auto_setup_session(data_dir)
    return session.get_player_stats(player_name)

def get_matchup(batter: str, bowler: str, data_dir: Optional[str] = None) -> Optional[Any]:
    """
    Get head-to-head matchup statistics.

    Args:
        batter: Batter name
        bowler: Bowler name
        data_dir: Optional custom data directory

    Returns:
        MatchupResult dataclass or None

    Example:
        result = px.get_matchup("V Kohli", "JJ Bumrah")
        print(f"Matches: {result.matches}, Avg: {result.average}")
    """
    from datetime import date
    from pypitch.query.base import MatchupQuery

    session = _auto_setup_session(data_dir)
    registry = session.registry

    # Resolve names to entity IDs
    dates_to_try = [date.today(), date(2024, 1, 1), date(2023, 1, 1), date(2022, 1, 1)]

    def _resolve(name: str) -> Optional[int]:
        for d in dates_to_try:
            try:
                eid = registry.resolve_player(name, d)
                if eid:
                    return eid
            except Exception:
                continue
        return None

    batter_id = _resolve(batter)
    bowler_id = _resolve(bowler)
    if batter_id is None or bowler_id is None:
        return None

    query = MatchupQuery(
        batter_id=str(batter_id),
        bowler_id=str(bowler_id),
        snapshot_id="latest",
    )
    result = session.executor.execute(query)
    return result.data

def predict_win(venue: str, target: int, current_score: int, wickets_down: int, overs_done: float, data_dir: Optional[str] = None) -> Dict[str, float]:
    """
    Predict win probability.

    Args:
        venue: Venue name
        target: Target score
        current_score: Current score
        wickets_down: Wickets fallen
        overs_done: Overs completed (so overs_remaining = 20 - overs_done)
        data_dir: Optional custom data directory

    Returns:
        Dict with 'win_prob' and 'confidence' keys containing probability (0.0 to 1.0) and confidence score

    Example:
        prob = px.predict_win("Wankhede", 180, 120, 5, 15.0)
        print(f"Win probability: {prob['win_prob']:.2%}, Confidence: {prob['confidence']:.1%}")
    """
    # Use the compute win probability function directly for express API
    from pypitch.compute.winprob import win_probability
    return win_probability(target, current_score, wickets_down, overs_done, venue)

def quick_load(data_dir: Optional[str] = None) -> PyPitchSession:
    """
    Return a ready-to-use PyPitchSession, downloading data automatically
    on first call if no local data is found (~50 MB IPL dataset).

    Subsequent calls return the cached session instantly.

    Args:
        data_dir: Optional custom data directory (default: ~/.pypitch_data)

    Returns:
        Initialised PyPitchSession.

    Example:
        session = px.quick_load()
        session.load_match("1234567")
    """
    return _auto_setup_session(data_dir)

# Export convenience functions
__all__ = [
    'load_competition',
    'get_player_stats',
    'get_matchup',
    'predict_win',
    'quick_load',
    'set_debug_mode'
]
