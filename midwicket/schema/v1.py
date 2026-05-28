import pyarrow as pa

# Metadata to track schema evolution and compatibility
SCHEMA_META = {
    "version": "1.0.0",
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
    ('is_wicket', pa.bool_()),
    ('wicket_type', pa.string()),
    
    # --- Derived Context (Materialized) ---
    ('phase', pa.string()), # 'Powerplay', 'Middle', 'Death'
], metadata=SCHEMA_META)
