"""
Midwicket Express: Simplified API for Beginners

Inspired by Plotly Express, this module provides one-liner access to Midwicket features
with sensible defaults. Hides complexity while keeping power features available for pros.

Usage:
    import midwicket.express as px
    ipl = px.load_competition("ipl", 2023)
    stats = px.get_player_stats("V Kohli")
"""

import os
from pathlib import Path
from typing import Optional, Any, Dict
from midwicket.api.session import MidwicketSession
from midwicket.data.loader import DataLoader
from midwicket.storage.engine import QueryEngine
from midwicket.runtime.executor import RuntimeExecutor
from midwicket.runtime.cache_duckdb import DuckDBCache
from midwicket.core.match_config import MatchConfig
from midwicket.sources.cricsheet_loader import CricsheetLoader

# Global debug mode — protected by lock (M3)
import threading as _threading
_DEBUG_MODE = False
_debug_lock = _threading.Lock()

# Global session cache for quick_load
_cached_session: Optional[MidwicketSession] = None
_cached_session_dir: Optional[str] = None
_session_lock = _threading.Lock()


def set_debug_mode(enabled: bool = True) -> None:
    """Enable debug mode for eager execution and verbose logging."""
    global _DEBUG_MODE
    with _debug_lock:
        _DEBUG_MODE = enabled
    # Unify duplicate debug flags (MW-033)
    try:
        import midwicket.config as config
        config.debug = enabled
    except Exception:
        pass
    try:
        import midwicket.runtime.modes as modes
        modes.debug_mode = enabled
    except Exception:
        pass
    if enabled:
        print("[Midwicket] Debug mode enabled: Queries will execute eagerly for immediate error feedback.")

def _get_default_data_dir() -> Path:
    """Get default data directory (~/.midwicket_data)."""
    return Path.home() / ".midwicket_data"

def _ensure_data_dir(data_dir: Optional[str] = None) -> str:
    """Ensure data directory exists or return ':memory:'."""
    if data_dir is None:
        return ":memory:"
    path = Path(data_dir)
    if str(path) != ":memory:":
        path.mkdir(parents=True, exist_ok=True)
    return str(path)

def _auto_setup_session(data_dir: Optional[str] = None, auto_download: bool = False) -> MidwicketSession:
    """Auto-setup session with caching. Does not download data by default."""
    global _cached_session, _cached_session_dir

    resolved = _ensure_data_dir(data_dir)
    with _session_lock:
        if _cached_session is not None and _cached_session_dir == resolved:
            return _cached_session

        if resolved == ":memory:":
            _cached_session = MidwicketSession(":memory:")
            _cached_session_dir = resolved
            return _cached_session

        data_path = Path(resolved)
        
        if auto_download:
            loader = DataLoader(str(data_path))
            raw_present = loader.raw_dir.exists() and bool(list(loader.raw_dir.glob("*.json")))
            if not raw_present:
                print("No local data found. Downloading IPL dataset (~50 MB)...")
                try:
                    loader.download()
                except Exception as exc:
                    print(f"Download failed: {exc}. Continuing without data.")

        _cached_session = MidwicketSession(str(data_path))
        _cached_session_dir = resolved
        return _cached_session

def download_data(data_dir: Optional[str] = None) -> None:
    """Explicitly download the historical dataset."""
    path = _ensure_data_dir(data_dir)
    if path == ":memory:":
        print("Running in in-memory mode, dataset already pre-cached in RAM.")
        return
    loader = DataLoader(path)
    print("Downloading historical dataset...")
    loader.download()

def load_competition(competition: str, season: int, data_dir: Optional[str] = None) -> CricsheetLoader:
    """
    Returns a CricsheetLoader filtered to a specific competition and season.

    Example::

        ipl = px.load_competition("ipl", 2023)
        match_ids = ipl.get_match_ids()
    """
    if data_dir is None:
        loader_dir = ":memory:"
    else:
        loader_dir = data_dir
    return CricsheetLoader(loader_dir, competition=competition, season=season)

def _check_dataset_exists(data_dir: Optional[str] = None) -> None:
    resolved = _ensure_data_dir(data_dir)
    if resolved == ":memory:":
        # In memory mode, the dataset is always loaded from bundled zip in RAM, so it always exists!
        return
    path = Path(resolved)
    raw_dir = path / "raw" / "ipl"
    if not raw_dir.exists() or not any(raw_dir.glob("*.json")):
        raise FileNotFoundError("Dataset not loaded. Please run DataLoader().download() or px.download_data() first.")

def get_player_stats(player_name: str, data_dir: Optional[str] = None) -> Any:
    """
    Get player statistics by name.

    Args:
        player_name: Player name (fuzzy matched)
        data_dir: Optional custom data directory

    Returns:
        PlayerStats dataclass

    Example:
        stats = px.get_player_stats("Virat Kohli")
        print(f"Matches: {stats.matches}, Runs: {stats.runs}")
    """
    _check_dataset_exists(data_dir)
    if not player_name:
        raise ValueError("Player name cannot be empty.")
    
    session = _auto_setup_session(data_dir)
    stats = session.get_player_stats(player_name)
    if stats is None:
        from midwicket.storage.registry import EntityNotFoundError
        raise EntityNotFoundError(f"Player '{player_name}' not found in the registry.")
    return stats

def get_matchup(batter: str, bowler: str, data_dir: Optional[str] = None) -> Any:
    """
    Get head-to-head matchup statistics.

    Args:
        batter: Batter name
        bowler: Bowler name
        data_dir: Optional custom data directory

    Returns:
        MatchupResult dataclass

    Example:
        result = px.get_matchup("V Kohli", "JJ Bumrah")
        print(f"Matches: {result.matches}, Avg: {result.average}")
    """
    from datetime import date
    from midwicket.storage.registry import EntityNotFoundError

    _check_dataset_exists(data_dir)
    if not batter or not bowler:
        raise ValueError("Batter and bowler names cannot be empty.")

    session = _auto_setup_session(data_dir)
    registry = session.registry

    # Resolve names to entity IDs
    dates_to_try = [date.today(), date(2024, 1, 1), date(2023, 1, 1), date(2022, 1, 1)]

    import logging as _log
    _express_logger = _log.getLogger(__name__)

    def _resolve(name: str) -> Optional[int]:
        for d in dates_to_try:
            try:
                eid = registry.resolve_player(name, d)
                if eid:
                    return eid
            except Exception as _exc:  # nosec B112
                _express_logger.debug("resolve_player(%r, %s) failed: %s", name, d, _exc)
                continue
        return None

    batter_id = _resolve(batter)
    if batter_id is None:
        raise EntityNotFoundError(f"Player '{batter}' (batter) not found in the registry.")
    bowler_id = _resolve(bowler)
    if bowler_id is None:
        raise EntityNotFoundError(f"Player '{bowler}' (bowler) not found in the registry.")

    # Fast path: query registry matchup_stats (populated by build_registry_stats)
    stats = registry.get_matchup_stats(batter_id, bowler_id)
    if stats is not None:
        return stats

    # Slow path: fall back to ball_events engine (requires explicit match loading)
    from midwicket.query.base import MatchupQuery
    try:
        query = MatchupQuery(
            batter_id=str(batter_id),
            bowler_id=str(bowler_id),
        )
        result = session.executor.execute(query)
        if result.data is None:
            raise EntityNotFoundError(f"No matchup data found between '{batter}' and '{bowler}'.")
        return result.data
    except Exception as exc:
        if isinstance(exc, EntityNotFoundError):
            raise
        raise EntityNotFoundError(f"No matchup data found between '{batter}' and '{bowler}': {exc}") from exc

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
    if data_dir:
        from midwicket.models.registry import ModelRegistry
        from midwicket.exceptions import ModelNotFoundError, ModelTrainingError
        from pathlib import Path
        model_path = Path(data_dir) / "models"
        if model_path.exists():
            try:
                registry = ModelRegistry(str(model_path))
                model = registry.get_model("win_predictor")
                prob, conf = model.predict(target, current_score, wickets_down, overs_done, venue)
                return {"win_prob": prob, "confidence": conf}
            except (ModelNotFoundError, ModelTrainingError):
                pass

    # Use the compute win probability function directly for express API
    from midwicket.compute.winprob import win_probability
    return win_probability(target, current_score, wickets_down, overs_done, venue)

def quick_load(data_dir: Optional[str] = None) -> MidwicketSession:
    """
    Return a ready-to-use MidwicketSession, downloading data automatically
    on first call if no local data is found (~50 MB IPL dataset).

    Subsequent calls return the cached session instantly.

    Args:
        data_dir: Optional custom data directory (default: ~/.midwicket_data)

    Returns:
        Initialised MidwicketSession.

    Example:
        session = px.quick_load()
        session.load_match("1234567")
    """
    return _auto_setup_session(data_dir, auto_download=True)

# Export convenience functions
__all__ = [
    'load_competition',
    'get_player_stats',
    'get_matchup',
    'predict_win',
    'quick_load',
    'set_debug_mode'
]
