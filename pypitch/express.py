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
from pypitch.storage.registry import IdentityRegistry
from pypitch.runtime.executor import RuntimeExecutor
from pypitch.runtime.cache_duckdb import DuckDBCache
from pypitch.core.match_config import MatchConfig
from pypitch.sources.cricsheet_loader import CricsheetLoader

# Global debug mode
_DEBUG_MODE = False

# Global session cache for quick_load
_cached_session: Optional[PyPitchSession] = None

def set_debug_mode(enabled: bool = True) -> None:
    """Enable debug mode for eager execution and verbose logging."""
    global _DEBUG_MODE
    _DEBUG_MODE = enabled
    if enabled:
        print("🐛 Debug mode enabled: Queries will execute eagerly for immediate error feedback.")

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

def _load_bundled_registry(registry: IdentityRegistry, bundled_dir: Path) -> None:
    """Load bundled registry data with improved error handling and efficiency."""
    import pyarrow.parquet as pq
    
    # Check if registry already has bundled data (check for a specific entity that should be in bundled data)
    try:
        result = registry.con.execute("SELECT count(*) FROM entities WHERE id = 1").fetchone()
        if result and result[0] > 0:
            if _DEBUG_MODE:
                print("📦 Bundled data already loaded.")
            return
    except Exception:
        pass  # Table might not exist yet, continue with loading
    
    if _DEBUG_MODE:
        print("📦 Loading bundled registry data...")
    
    try:
        # Load entities
        entities_file = bundled_dir / "entities.parquet"
        if entities_file.exists():
            entities_table = pq.read_table(str(entities_file))
            if _DEBUG_MODE:
                print(f"   Loading {entities_table.num_rows} entities...")
            entities_data = list(zip(
                entities_table['id'].to_pylist(),
                entities_table['type'].to_pylist(),
                entities_table['primary_name'].to_pylist()
            ))
            registry.con.executemany("INSERT INTO entities VALUES (?, ?, ?)", entities_data)
        
        # Load aliases
        aliases_file = bundled_dir / "aliases.parquet"
        if aliases_file.exists():
            aliases_table = pq.read_table(str(aliases_file))
            if _DEBUG_MODE:
                print(f"   Loading {aliases_table.num_rows} aliases...")
            aliases_data = list(zip(
                aliases_table['alias'].to_pylist(),
                aliases_table['entity_id'].to_pylist(),
                aliases_table['valid_from'].to_pylist(),
                aliases_table['valid_to'].to_pylist()
            ))
            registry.con.executemany("INSERT INTO aliases VALUES (?, ?, ?, ?)", aliases_data)
        
        # Load player stats
        player_stats_file = bundled_dir / "player_stats.parquet"
        if player_stats_file.exists():
            stats_table = pq.read_table(str(player_stats_file))
            if _DEBUG_MODE:
                print(f"   Loading {stats_table.num_rows} player stats...")
            stats_data = list(zip(
                stats_table['entity_id'].to_pylist(),
                stats_table['matches'].to_pylist(),
                stats_table['runs'].to_pylist(),
                stats_table['balls_faced'].to_pylist(),
                stats_table['wickets'].to_pylist(),
                stats_table['balls_bowled'].to_pylist(),
                stats_table['runs_conceded'].to_pylist()
            ))
            registry.con.executemany("INSERT INTO player_stats VALUES (?, ?, ?, ?, ?, ?, ?)", stats_data)
        
        # Load venue stats
        venue_stats_file = bundled_dir / "venue_stats.parquet"
        if venue_stats_file.exists():
            venue_table = pq.read_table(str(venue_stats_file))
            if _DEBUG_MODE:
                print(f"   Loading {venue_table.num_rows} venue stats...")
            venue_data = list(zip(
                venue_table['entity_id'].to_pylist(),
                venue_table['matches'].to_pylist(),
                venue_table['total_runs'].to_pylist(),
                venue_table['first_innings_runs'].to_pylist(),
                venue_table['first_innings_count'].to_pylist()
            ))
            registry.con.executemany("INSERT INTO venue_stats VALUES (?, ?, ?, ?, ?)", venue_data)
        
        if _DEBUG_MODE:
            print("✅ Bundled data loaded successfully.")
            
    except Exception as e:
        print(f"❌ Error loading bundled data: {e}")
        raise

def _auto_setup_session(data_dir: Optional[str] = None) -> PyPitchSession:
    """Auto-setup session with defaults and caching."""
    global _cached_session
    
    # Return cached session if available and data_dir matches
    if _cached_session is not None:
        return _cached_session
    
    data_path = _ensure_data_dir(data_dir)

    # Check if we have bundled data (first check package data, then user data)
    bundled_dir = Path(__file__).parent.parent / "data" / "bundled"
    if not bundled_dir.exists():
        bundled_dir = data_path / "bundled"
    
    if bundled_dir.exists() and (bundled_dir / "entities.parquet").exists():
        if _DEBUG_MODE:
            print("📦 Using bundled sample data for quick start.")
        # Create session with bundled data directory (skip registry build)
        _cached_session = PyPitchSession(str(data_path), skip_registry_build=True)
        
        # Load bundled registry data
        _load_bundled_registry(_cached_session.registry, bundled_dir)
        return _cached_session

    # Otherwise, download if needed
    loader = DataLoader(str(data_path))
    if not (loader.raw_dir.exists() and list(loader.raw_dir.glob("*.json"))):
        print("⬇️  Downloading sample data (IPL 2023)...")
        loader.download()

    _cached_session = PyPitchSession(str(data_path))
    return _cached_session

def load_competition(competition: str, season: int, data_dir: str = "./data"):
    """
    Loads all matches for a competition and season with one line.
    Example:
        ipl = px.load_competition("ipl", 2023)
    Returns a loader object with match_ids and match_data access.
    """
    # For now, just use CricsheetLoader. In future, can route by competition.
    loader = CricsheetLoader(data_dir)
    # Optionally filter match_ids by competition/season here
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
    session = _auto_setup_session(data_dir)
    # TODO: Implement matchup logic
    return None

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

def quick_load() -> PyPitchSession:
    """
    Quick load with bundled data (no download required).

    Returns:
        PyPitchSession with sample World Cup data

    Example:
        session = px.quick_load()
        # Instantly ready for analysis
    """
    return _auto_setup_session()

# Export convenience functions
__all__ = [
    'load_competition',
    'get_player_stats',
    'get_matchup',
    'predict_win',
    'quick_load',
    'set_debug_mode'
]
