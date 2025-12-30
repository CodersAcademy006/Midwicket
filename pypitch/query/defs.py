from typing import List, Optional
from pydantic import Field
from pypitch.query.base import BaseQuery

class MatchupQuery(BaseQuery):
    """
    Intent: Compare Batters vs Bowlers.
    Used for: Head-to-Head stats, Argument Enders.
    """
    batter_ids: List[int]
    bowler_ids: List[int]
    phases: List[str] = Field(default_factory=lambda: ["Powerplay", "Middle", "Death"])
    venue_ids: Optional[List[int]] = None
    
    @property
    def requires(self):
        return {
            "preferred_tables": ["matchup_stats", "phase_stats"],
            "fallback_table": "ball_events",
            "entities": ["batter", "bowler"],
            "granularity": "ball" 
        }

class FantasyQuery(BaseQuery):
    """
    Intent: Get aggregated player value/points for fantasy selection.
    Used for: Cheat Sheets, Captain Optimizers.
    """
    venue_id: int
    roles: List[str] = ["all"] # 'batter', 'bowler', 'all'
    budget_cap: Optional[float] = None
    min_matches: int = 10

    @property
    def requires(self):
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
    
    @property
    def requires(self):
        return {
            "preferred_tables": ["chase_history"],
            "fallback_table": "ball_events",
            "entities": ["venue"],
            "granularity": "match"
        }
