"""
Midwicket Express: Simplified API for Beginners

Inspired by Plotly Express, this module provides one-liner access to Midwicket features
with sensible defaults. Hides complexity while keeping power features available for pros.

Usage:
    import midwicket.express as px
    ipl = px.load_competition("ipl", 2023)
    stats = px.get_player_stats("V Kohli")
"""

# Only stdlib and typing at import time — all heavy SDK modules are imported
# lazily inside the functions that use them (UXDX-08).  This keeps
# `import midwicket.express as px; px.predict_win(...)` from loading the full
# DuckDB / PyArrow / session stack unless it is actually needed.

import logging
import os
import threading as _threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from midwicket.api.session import MidwicketSession
    from midwicket.sources.cricsheet_loader import CricsheetLoader

_log = logging.getLogger(__name__)

# Global debug mode — protected by lock (M3)
_DEBUG_MODE = False
_debug_lock = _threading.Lock()

# Global session cache for quick_load (Any avoids importing MidwicketSession at module load)
_cached_session: Optional[Any] = None
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
        _log.info("Debug mode enabled: Queries will execute eagerly for immediate error feedback.")


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


def _auto_setup_session(data_dir: Optional[str] = None, auto_download: bool = False) -> Any:
    """Auto-setup session with caching. Does not download data by default."""
    global _cached_session, _cached_session_dir

    resolved = _ensure_data_dir(data_dir)
    with _session_lock:
        if _cached_session is not None and _cached_session_dir == resolved:
            return _cached_session

        from midwicket.api.session import MidwicketSession  # lazy
        from midwicket.data.loader import DataLoader  # lazy

        if resolved == ":memory:":
            _cached_session = MidwicketSession(":memory:")
            _cached_session_dir = resolved
            return _cached_session

        data_path = Path(resolved)

        if auto_download:
            loader = DataLoader(str(data_path))
            raw_present = (
                loader.raw_dir is not None
                and loader.raw_dir.exists()
                and bool(list(loader.raw_dir.glob("*.json")))
            )
            if not raw_present:
                _log.info("No local data found. Downloading IPL dataset (~50 MB)...")
                try:
                    loader.download()
                except Exception as exc:
                    _log.warning("Download failed: %s. Continuing without data.", exc)

        _cached_session = MidwicketSession(str(data_path))
        _cached_session_dir = resolved
        return _cached_session


def download_data(data_dir: Optional[str] = None) -> None:
    """Download the full historical IPL dataset (~50 MB from Cricsheet).

    The data is stored under ``data_dir`` (default: ``./data``).
    Subsequent calls skip the download if data already exists.

    Args:
        data_dir: Directory to store downloaded data.  Defaults to ``./data``
                  in the current working directory.  Pass an absolute path to
                  use a different location.
    """
    if data_dir is None:
        data_dir = "./data"
    path = _ensure_data_dir(data_dir)
    if path == ":memory:":
        _log.info("In-memory mode: dataset is pre-loaded from the bundled ZIP.")
        return
    from midwicket.data.loader import DataLoader  # lazy
    loader = DataLoader(path)
    _log.info("Downloading historical dataset to %s ...", path)
    loader.download()


def load_competition(competition: str, season: int, data_dir: Optional[str] = None) -> "CricsheetLoader":
    """Return a CricsheetLoader filtered to a specific competition and season.

    Defaults to the in-memory bundled dataset (same default as all other
    Express functions).  Pass ``data_dir`` to read from a locally downloaded
    dataset instead.

    Args:
        competition: Competition identifier, e.g. ``"ipl"``.
        season: Season year, e.g. ``2023``.
        data_dir: Optional path to a local dataset directory.

    Example::

        ipl = px.load_competition("ipl", 2023)
        match_ids = ipl.get_match_ids()
    """
    from midwicket.sources.cricsheet_loader import CricsheetLoader  # lazy
    loader_dir = data_dir if data_dir is not None else ":memory:"
    return CricsheetLoader(loader_dir, competition=competition, season=season)


def _check_dataset_exists(data_dir: Optional[str] = None) -> None:
    resolved = _ensure_data_dir(data_dir)
    if resolved == ":memory:":
        # In memory mode, the dataset is always loaded from bundled zip in RAM, so it always exists!
        return
    path = Path(resolved)
    raw_dir = path / "raw" / "ipl"
    if not raw_dir.exists() or not any(raw_dir.glob("*.json")):
        raise FileNotFoundError("Dataset not loaded. Please run px.download_data() first.")


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
    from midwicket.storage.registry import EntityNotFoundError

    _check_dataset_exists(data_dir)
    if not batter or not bowler:
        raise ValueError("Batter and bowler names cannot be empty.")

    session = _auto_setup_session(data_dir)
    registry = session.registry

    # Resolve names to entity IDs using centralized helper (MW-033)
    batter_id = registry.resolve_player_without_date(batter)
    if batter_id is None:
        raise EntityNotFoundError(f"Player '{batter}' (batter) not found in the registry.")
    bowler_id = registry.resolve_player_without_date(bowler)
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


def predict_win(
    venue: str,
    target: int,
    current_score: int,
    wickets_down: int,
    overs_done: float,
    data_dir: Optional[str] = None,
) -> Dict[str, float]:
    """Predict win probability for the chasing team.

    Uses the bundled retrained logistic model.  Venue materially affects
    the result via per-venue adjustment factors learned during training.

    This function is intentionally lightweight: it only imports the
    win-probability model, not the full session/storage stack.

    Args:
        venue: Venue name (e.g. ``"Wankhede Stadium"``).  Drives a
               venue-specific probability adjustment; an unrecognised venue
               name falls back to a neutral baseline.
        target: Target score to chase.
        current_score: Runs scored so far by the chasing team.
        wickets_down: Wickets fallen (0–10).
        overs_done: Overs completed (decimal, e.g. 10.5 = 10 overs 3 balls).
        data_dir: Optional path to a local dataset that contains a
                  ``models/`` sub-directory with a custom win predictor.
                  When provided and the model file exists, that model is used
                  instead of the bundled default.

    Returns:
        Dict with two keys:

        * ``'win_prob'``: probability of a win for the chasing team (0.0–1.0).
        * ``'confidence'``: heuristic certainty indicator (0.1–0.95).
          This is **not** a statistical confidence interval; it reflects
          prediction extremity and situational factors (wickets in hand,
          balls remaining).  Interpret it as a qualitative certainty signal,
          not a calibrated probability.

    Example:
        prob = px.predict_win("Wankhede Stadium", 180, 120, 5, 15.0)
        print(f"Win probability: {prob['win_prob']:.1%}")
    """
    if data_dir:
        from midwicket.exceptions import ModelNotFoundError, ModelTrainingError
        from midwicket.models.registry import ModelRegistry
        model_path = Path(data_dir) / "models"
        if model_path.exists():
            try:
                registry = ModelRegistry(str(model_path))
                model = registry.get_model("win_predictor")
                prob, conf = model.predict(target, current_score, wickets_down, overs_done, venue)
                return {"win_prob": prob, "confidence": conf}
            except (ModelNotFoundError, ModelTrainingError):
                pass

    from midwicket.compute.winprob import win_probability  # lazy
    return win_probability(target, current_score, wickets_down, overs_done, venue)


def quick_load(data_dir: Optional[str] = None) -> Any:
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
# download_data is the canonical way to fetch the full historical dataset.
__all__ = [
    'load_competition',
    'get_player_stats',
    'get_matchup',
    'predict_win',
    'quick_load',
    'download_data',
    'set_debug_mode',
]
