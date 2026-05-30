import pyarrow as pa
from enum import Enum, auto

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
        return self.batter_runs + self.extras
    
    @classmethod
    def from_wide(cls, runs: int) -> 'RunComponent':
        return cls(batter_runs=0, extras=runs, is_ball_faced=False, bowler_charged=True)
    
    @classmethod
    def from_no_ball(cls, runs: int, batter_runs: int = 0) -> 'RunComponent':
        return cls(batter_runs=batter_runs, extras=runs, is_ball_faced=True, bowler_charged=True)
    
    @classmethod
    def from_bye(cls, runs: int) -> 'RunComponent':
        return cls(batter_runs=0, extras=runs, is_ball_faced=True, bowler_charged=False)
    
    @classmethod
    def from_leg_bye(cls, runs: int) -> 'RunComponent':
        return cls(batter_runs=0, extras=runs, is_ball_faced=True, bowler_charged=False)
    
    @classmethod
    def from_boundary(cls, runs: int) -> 'RunComponent':
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
    # Upcasted to int32 to prevent DuckDB SUM() overflow
    ('runs_batter', pa.int32()),
    ('runs_extras', pa.int32()),
    ('extras_type', pa.string()),
    ('is_wicket', pa.bool_()),
    ('wicket_type', pa.string()),
    
    # --- Derived Context (Materialized) ---
    ('phase', pa.string()), # 'Powerplay', 'Middle', 'Death'

    # --- Denormalized Names (convenience for analytics queries) ---
    # These mirror the registry-resolved IDs above. They are populated
    # during canonicalization from the raw source data and allow SQL
    # queries to filter/group by human-readable names without needing
    # cross-database JOINs against the identity registry.
    ('batter', pa.string()),
    ('bowler', pa.string()),
    ('venue', pa.string()),
    ('player_dismissed', pa.string()),
], metadata=SCHEMA_META)
