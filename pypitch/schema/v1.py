import pyarrow as pa

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
