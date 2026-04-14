from typing import List, Optional, Dict, Any, Literal
from pydantic import Field, field_validator
from pypitch.query.base import BaseQuery, MatchupQuery

__all__ = ["FantasyQuery", "WinProbQuery", "MatchupQuery"]

# Type aliases for consistency
Phase = Literal["powerplay", "middle", "death", "all"]
Role = Literal["batter", "bowler", "all-rounder", "all"]

class FantasyQuery(BaseQuery):
    """
    Intent: Get aggregated player value/points for fantasy selection.
    Used for: Cheat Sheets, Captain Optimizers.
    """
    venue_id: int
    roles: List[Role] = ["all"]  # "all" = no role filter applied
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
        """Ensure overs_remaining is within T20 match bounds.

        The executor computes overs_done = 20.0 - overs_remaining, so
        values greater than 20 would produce a negative overs_done and
        raise a ValueError inside win_probability().  Cap at 20.
        """
        if v < 0 or v > 20:
            raise ValueError(f"overs_remaining must be between 0 and 20, got {v}")
        return v
    
    @property
    def requires(self) -> Dict[str, Any]:
        return {
            "preferred_tables": ["chase_history"],
            "fallback_table": "ball_events",
            "entities": ["venue"],
            "granularity": "match"
        }
