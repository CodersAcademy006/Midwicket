from typing import List, Optional, Dict, Any, Literal
from pydantic import Field, field_validator
from pypitch.query.base import BaseQuery, MatchupQuery

__all__ = ["FantasyQuery", "WinProbQuery", "MatchupQuery"]

# Type aliases for consistency
Phase = Literal["powerplay", "middle", "death", "all"]
Role = Literal["batter", "bowler", "all-rounder"]

class FantasyQuery(BaseQuery):
    """
    Intent: Get aggregated player value/points for fantasy selection.
    Used for: Cheat Sheets, Captain Optimizers.
    """
    venue_id: int
    roles: List[Role] = ["all"]  # Type-safe role list
    budget_cap: Optional[float] = None
    min_matches: int = 10

    @property
    def requires(self) -> Dict[str, Any]:
        return {
            "preferred_tables": ["fantasy_points_avg", "venue_bias"],
            "fallback_table": "ball_events",
            "entities": ["venue", "player"],
            "granularity": "match"
        }

class WinProbQuery(BaseQuery):
    """
    Intent: Calculate win probability for a live match state.
    Used for: Simulation, Live Broadcast features.
    """
    venue_id: int
    target_score: int
    current_runs: int
    current_wickets: int
    overs_remaining: float
    
    @field_validator('overs_remaining')
    @classmethod
    def validate_overs_remaining(cls, v: float) -> float:
        """Ensure overs_remaining is within valid cricket match bounds."""
        if v < 0 or v > 50:
            raise ValueError(f"overs_remaining must be between 0 and 50, got {v}")
        return v
    
    @property
    def requires(self) -> Dict[str, Any]:
        return {
            "preferred_tables": ["chase_history"],
            "fallback_table": "ball_events",
            "entities": ["venue"],
            "granularity": "match"
        }
