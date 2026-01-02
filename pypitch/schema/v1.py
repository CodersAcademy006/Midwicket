import pyarrow as pa
from enum import Enum, auto
from typing import Optional

# Enums for categorical data to prevent string inconsistencies
class DismissalType(Enum):
    BOWLED = auto()
    CAUGHT = auto()
    LBW = auto()
    RUN_OUT = auto()
    STUMPED = auto()
    CAUGHT_AND_BOWLED = auto()
    HIT_WICKET = auto()
    OBSTRUCTING_THE_FIELD = auto()
    DOUBLE_HIT = auto()
    HANDLED_THE_BALL = auto()
    RETIRED_HURT = auto()
    RETIRED_OUT = auto()
    RETIRED_NOT_OUT = auto()

class Phase(Enum):
    POWERPLAY = auto()
    MIDDLE = auto()
    DEATH = auto()

class RunComponent:
    """
    Explicit handling of cricket extras to prevent bugs.
    
    Rules:
    - Wides/No-balls: Count to Team score and Bowler runs, but NOT Batter balls faced.
    - Byes/Leg Byes: Count to Team score, but NOT Bowler runs or Batter runs.
    """
    def __init__(self, batter_runs: int = 0, extras: int = 0, 
                 is_ball_faced: bool = True, bowler_charged: bool = True):
        self.batter_runs = batter_runs
        self.extras = extras
        self.is_ball_faced = is_ball_faced
        self.bowler_charged = bowler_charged
    
    @property
    def total_runs(self) -> int:
        """Total runs scored (batter + extras)"""
        return self.batter_runs + self.extras
    
    @classmethod
    def from_wide(cls, runs: int) -> 'RunComponent':
        """Wide: counts to team and bowler, not batter balls faced"""
        return cls(batter_runs=0, extras=runs, is_ball_faced=False, bowler_charged=True)
    
    @classmethod
    def from_no_ball(cls, runs: int) -> 'RunComponent':
        """No-ball: counts to team and bowler, not batter balls faced"""
        return cls(batter_runs=0, extras=runs, is_ball_faced=False, bowler_charged=True)
    
    @classmethod
    def from_bye(cls, runs: int) -> 'RunComponent':
        """Bye: counts to team only"""
        return cls(batter_runs=0, extras=runs, is_ball_faced=True, bowler_charged=False)
    
    @classmethod
    def from_leg_bye(cls, runs: int) -> 'RunComponent':
        """Leg bye: counts to team only"""
        return cls(batter_runs=0, extras=runs, is_ball_faced=True, bowler_charged=False)
    
    @classmethod
    def from_boundary(cls, runs: int) -> 'RunComponent':
        """Normal boundary or runs"""
        return cls(batter_runs=runs, extras=0, is_ball_faced=True, bowler_charged=True)

# Metadata to track schema evolution and compatibility
SCHEMA_META = {
    "version": "1.0.0",
    "frozen_at": "2024-01-01",
    "compatibility": "backward-only",
    "doc": "Ball-by-ball event data with materialized phases and context."
}

# The Strict V1 Schema Definition
BALL_EVENT_SCHEMA = pa.schema([
    # --- Identity (Who & Where) ---
    ('match_id', pa.string()),
    ('date', pa.date32()),
    ('venue_id', pa.int32()),
    
    # --- State (When) ---
    ('inning', pa.int8()),
    ('over', pa.int8()),
    ('ball', pa.int8()),
    
    # --- Actors (IDs from Registry) ---
    ('batter_id', pa.int32()),
    ('bowler_id', pa.int32()),
    ('non_striker_id', pa.int32()),
    ('batting_team_id', pa.int16()),
    ('bowling_team_id', pa.int16()),
    
    # --- Metrics (What Happened) ---
    ('runs_batter', pa.int8()),
    ('runs_extras', pa.int8()),
    ('is_wicket', pa.bool_()),
    # Dictionary encoding saves memory for repetitive strings like "caught", "bowled"
    ('wicket_type', pa.dictionary(pa.int8(), pa.string())),
    
    # --- Derived Context (Materialized) ---
    # Pre-computing this during ingestion saves massive compute later
    ('phase', pa.dictionary(pa.int8(), pa.string())), # 'Powerplay', 'Middle', 'Death'
], metadata=SCHEMA_META)
