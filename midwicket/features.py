"""
Midwicket Feature Store: Local Feature Generation and Caching

Provides high-leverage, deterministic, and testable feature sets for cricket analytics,
fantasy modeling, and scouting.
"""

import logging
from typing import Optional, Dict, Any
import pandas as pd

# Internal Imports
from midwicket.api.session import MidwicketSession

logger = logging.getLogger(__name__)

def _get_where_clause(start_date: Optional[str] = None, end_date: Optional[str] = None, prefix: str = "WHERE") -> str:
    clauses = []
    if start_date:
        clauses.append(f"date >= '{start_date}'")
    if end_date:
        clauses.append(f"date <= '{end_date}'")
    if not clauses:
        return ""
    return f"{prefix} " + " AND ".join(clauses)

# Registered features and their builders
FEATURE_REGISTRY = {}

def register_feature(name: str):
    """Decorator to register a feature builder."""
    def decorator(func):
        FEATURE_REGISTRY[name] = func
        return func
    return decorator

@register_feature("pressure_index")
def build_pressure_index(session: MidwicketSession, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    1. PRESSURE INDEX (Formula 1):
    PressureIndex = Min(10, (Wickets Lost * 0.7 + Over Fraction * 0.2) / (Wickets Remaining + 0.1))
    """
    where_clause = _get_where_clause(start_date, end_date)
    query = f"""
    SELECT 
        match_id,
        inning,
        over,
        ball,
        batter_id,
        bowler_id,
        is_wicket,
        -- Wickets remaining before this ball
        CAST(10 - SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) 
            OVER (PARTITION BY match_id, inning ORDER BY over, ball ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS DOUBLE) as wickets_remaining
    FROM ball_events
    {where_clause}
    """
    df = session.engine.execute_sql(query).to_pandas()
    
    df['wickets_remaining'] = df['wickets_remaining'].fillna(10.0).astype(float)
    df['wickets_lost'] = 10.0 - df['wickets_remaining']
    df['over_fraction'] = df['over'].astype(float) + (df['ball'].astype(float) / 6.0)
    
    # Precise leverage formula
    df['pressure_index'] = ((df['wickets_lost'] * 0.7 + df['over_fraction'] * 0.2) / (df['wickets_remaining'] + 0.1)).clip(0.0, 10.0).round(2)
    
    return df[['match_id', 'inning', 'over', 'ball', 'batter_id', 'bowler_id', 'pressure_index']]

@register_feature("bowler_quality_rating")
def build_bowler_quality_rating(session: MidwicketSession, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    2. BOWLER QUALITY RATING (Formula 2):
    BQR = (Dot Balls % * 60) + (Wickets % * 400)
    """
    where_clause = _get_where_clause(start_date, end_date)
    query = f"""
    SELECT
        bowler_id,
        COUNT(*) as total_balls,
        CAST(SUM(CASE WHEN runs_batter = 0 AND COALESCE(extras_type, '') != 'wides' THEN 1 ELSE 0 END) AS DOUBLE) as dot_balls,
        CAST(SUM(CASE WHEN is_wicket = true AND wicket_type NOT IN ('RUN_OUT','OBSTRUCTING_THE_FIELD','RETIRED_HURT','RETIRED_OUT','RETIRED_NOT_OUT') THEN 1 ELSE 0 END) AS DOUBLE) as wickets
    FROM ball_events
    {where_clause}
    GROUP BY bowler_id
    HAVING total_balls >= 12
    """
    df = session.engine.execute_sql(query).to_pandas()
    
    df['dot_ratio'] = df['dot_balls'] / df['total_balls'].astype(float)
    df['wicket_ratio'] = df['wickets'] / df['total_balls'].astype(float)
    
    df['bowler_quality_rating'] = (df['dot_ratio'] * 60.0 + df['wicket_ratio'] * 400.0).clip(0.0, 100.0).round(2)
    return df[['bowler_id', 'total_balls', 'dot_balls', 'wickets', 'bowler_quality_rating']]

@register_feature("batter_intent_score")
def build_batter_intent_score(session: MidwicketSession, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    3. BATTER INTENT SCORE (Formula 3):
    BIS = ((Boundaries * 1.5) + (Runs - Boundaries)) / (Balls Faced + 0.1) * 100
    """
    where_clause = _get_where_clause(start_date, end_date)
    query = f"""
    SELECT
        batter_id,
        COUNT(*) as total_balls,
        CAST(SUM(CASE WHEN runs_batter = 4 OR runs_batter = 6 THEN 1 ELSE 0 END) AS DOUBLE) as boundaries,
        CAST(SUM(runs_batter) AS DOUBLE) as total_runs
    FROM ball_events
    {where_clause}
    GROUP BY batter_id
    HAVING total_balls >= 12
    """
    df = session.engine.execute_sql(query).to_pandas()
    
    boundary_runs = df['boundaries'] * 4.0 # approximate boundary runs
    df['intent_score'] = (((df['boundaries'] * 1.5) + (df['total_runs'] - boundary_runs)) / (df['total_balls'].astype(float) + 0.1) * 100.0).clip(0.0, 200.0).round(2)
    
    return df[['batter_id', 'total_balls', 'total_runs', 'intent_score']]

@register_feature("match_context_score")
def build_match_context_score(session: MidwicketSession) -> pd.DataFrame:
    """
    4. MATCH CONTEXT SCORE (Formula 4):
    First Innings: MCS = CRR * (Wickets Remaining / 10.0)
    Second Innings: MCS = (Runs Needed / (Balls Remaining + 0.1)) * (Wickets Remaining / 10.0)
    """
    query = """
    WITH first_inn_totals AS (
        SELECT match_id, SUM(runs_batter + runs_extras) as first_inn_total
        FROM ball_events
        WHERE inning = 1
        GROUP BY match_id
    )
    SELECT 
        b.match_id,
        b.inning,
        b.over,
        b.ball,
        b.batter_id,
        b.bowler_id,
        CAST(COALESCE(SUM(b.runs_batter + b.runs_extras) 
            OVER (PARTITION BY b.match_id, b.inning ORDER BY b.over, b.ball), 0) AS DOUBLE) as running_score,
        CAST(COALESCE(SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END) 
            OVER (PARTITION BY b.match_id, b.inning ORDER BY b.over, b.ball), 0) AS DOUBLE) as running_wickets,
        t.first_inn_total
    FROM ball_events b
    LEFT JOIN first_inn_totals t ON b.match_id = t.match_id
    """
    df = session.engine.execute_sql(query).to_pandas()
    
    import numpy as np
    
    df['wickets_remaining'] = 10.0 - df['running_wickets']
    df['over_fraction'] = df['over'].astype(float) + (df['ball'].astype(float) / 6.0)
    df['crr'] = df['running_score'] / (df['over_fraction'] + 0.1)
    
    # 2nd Innings Target-based Logic
    # Default to 120 balls (T20) or 300 balls (ODI) based on maximum over in each match
    match_max_over = df.groupby('match_id')['over'].transform('max')
    total_balls = np.where(match_max_over > 20, 300.0, 120.0)
    balls_remaining = (total_balls - (df['over_fraction'] * 6.0)).clip(lower=0.0)
    
    # Target runs to win
    target = df['first_inn_total'].fillna(150.0).astype(float) + 1.0
    runs_needed = (target - df['running_score']).clip(lower=0.0)
    
    # Required Run Rate (runs needed per ball, converted to runs per over by multiplying by 6.0)
    required_rate = runs_needed / (balls_remaining + 0.1)
    required_rpo = required_rate * 6.0
    
    # Score assignment based on innings
    df['match_context_score'] = np.where(
        df['inning'] == 1,
        df['crr'] * (df['wickets_remaining'] / 10.0),
        required_rpo * (df['wickets_remaining'] / 10.0)
    )
    
    df['match_context_score'] = df['match_context_score'].clip(0.0, 15.0).round(2)
    return df[['match_id', 'inning', 'over', 'ball', 'batter_id', 'bowler_id', 'match_context_score']]

@register_feature("venue_bias_rating")
def build_venue_bias_rating(session: MidwicketSession, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    5. VENUE BIAS RATING (Formula 5):
    VBR = Venue First Innings Average Runs / Global First Innings Average Runs
    """
    where_clause_and = _get_where_clause(start_date, end_date, prefix="AND")
    # Global average first innings runs
    global_query = f"SELECT CAST(AVG(runs_batter + runs_extras) * 120.0 AS DOUBLE) as global_avg FROM ball_events WHERE inning = 1 {where_clause_and}"
    res = session.engine.execute_sql(global_query).to_pydict()
    global_avg = res["global_avg"][0] if res and res["global_avg"] and res["global_avg"][0] else 150.0
    
    venue_query = f"""
    SELECT
        venue_id,
        COUNT(DISTINCT match_id) as matches,
        CAST(AVG(runs_batter + runs_extras) * 120.0 / {global_avg} AS DOUBLE) as venue_bias_rating
    FROM ball_events
    WHERE inning = 1 {where_clause_and}
    GROUP BY venue_id
    """
    df = session.engine.execute_sql(venue_query).to_pandas()
    # Stabilize VBR rating: fallback to 1.0 (global average) if a venue has hosted < 5 matches
    df.loc[df['matches'] < 5, 'venue_bias_rating'] = 1.0
    df['venue_bias_rating'] = df['venue_bias_rating'].round(3)
    return df[['venue_id', 'matches', 'venue_bias_rating']]

@register_feature("expected_runs")
def build_expected_runs(session: MidwicketSession) -> pd.DataFrame:
    """
    6. EXPECTED RUNS (xRuns) (Formula 6):
    xRuns = 0.5 * (Batter Strike Rate / 100.0) + 0.5 * (Bowler Economy / 6.0)
    """
    # Batter baseline
    bat_query = "SELECT batter_id, CAST(AVG(runs_batter) AS DOUBLE) as bat_avg_runs FROM ball_events GROUP BY batter_id"
    df_bat = session.engine.execute_sql(bat_query).to_pandas()
    
    # Bowler baseline
    bowl_query = "SELECT bowler_id, CAST(AVG(runs_batter + runs_extras) AS DOUBLE) as bowl_avg_runs FROM ball_events GROUP BY bowler_id"
    df_bowl = session.engine.execute_sql(bowl_query).to_pandas()
    
    # Base match state query
    base_query = "SELECT match_id, inning, over, ball, batter_id, bowler_id FROM ball_events"
    df = session.engine.execute_sql(base_query).to_pandas()
    
    # Merge baselines
    df = pd.merge(df, df_bat, on="batter_id", how="left")
    df = pd.merge(df, df_bowl, on="bowler_id", how="left")
    
    df['bat_avg_runs'] = df['bat_avg_runs'].fillna(0.7).astype(float)
    df['bowl_avg_runs'] = df['bowl_avg_runs'].fillna(0.75).astype(float)
    
    # Calculate Expected Runs (xRuns)
    df['expected_runs'] = (df['bat_avg_runs'] * 0.5 + df['bowl_avg_runs'] * 0.5).round(3)
    return df[['match_id', 'inning', 'over', 'ball', 'batter_id', 'bowler_id', 'expected_runs']]

@register_feature("expected_wickets")
def build_expected_wickets(session: MidwicketSession) -> pd.DataFrame:
    """
    7. EXPECTED WICKETS (xWickets) (Formula 7):
    xWickets = 0.5 * (Bowler Wicket % / 5.0) + 0.5 * (Batter Dismissal % / 30.0)
    """
    # Batter baseline dismissal probability
    bat_query = """
    SELECT 
        batter_id, 
        CAST(SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS DOUBLE) as bat_dismiss_pct
    FROM ball_events 
    GROUP BY batter_id
    """
    df_bat = session.engine.execute_sql(bat_query).to_pandas()
    
    # Bowler baseline wicket probability
    bowl_query = """
    SELECT 
        bowler_id, 
        CAST(SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS DOUBLE) as bowl_wicket_pct
    FROM ball_events 
    GROUP BY bowler_id
    """
    df_bowl = session.engine.execute_sql(bowl_query).to_pandas()
    
    # Base match state query
    base_query = "SELECT match_id, inning, over, ball, batter_id, bowler_id FROM ball_events"
    df = session.engine.execute_sql(base_query).to_pandas()
    
    # Merge baselines
    df = pd.merge(df, df_bat, on="batter_id", how="left")
    df = pd.merge(df, df_bowl, on="bowler_id", how="left")
    
    df['bat_dismiss_pct'] = df['bat_dismiss_pct'].fillna(0.04).astype(float)
    df['bowl_wicket_pct'] = df['bowl_wicket_pct'].fillna(0.045).astype(float)
    
    # Calculate Expected Wickets (xWickets)
    df['expected_wickets'] = (df['bat_dismiss_pct'] * 0.5 + df['bowl_wicket_pct'] * 0.5).round(4)
    return df[['match_id', 'inning', 'over', 'ball', 'batter_id', 'bowler_id', 'expected_wickets']]

@register_feature("batting_form")
def build_batting_form(session: MidwicketSession) -> pd.DataFrame:
    """
    Computes the running batting average and strike rate for the last 5 matches for each batter.
    """
    query = """
    WITH match_runs AS (
        SELECT 
            batter_id,
            match_id,
            date,
            SUM(runs_batter) as match_runs,
            SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END) as balls_faced,
            SUM(CASE WHEN player_dismissed IS NOT NULL AND player_dismissed != '' THEN 1 ELSE 0 END) as dismissed
        FROM ball_events
        GROUP BY batter_id, match_id, date
    ),
    running_stats AS (
        SELECT
            batter_id,
            match_id,
            date,
            match_runs,
            balls_faced,
            dismissed,
            ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY date DESC) as match_rank
        FROM match_runs
    )
    SELECT
        batter_id,
        CAST(AVG(match_runs) AS DOUBLE) as avg_runs_last_5,
        SUM(match_runs) * 100.0 / NULLIF(SUM(balls_faced), 0) as strike_rate_last_5,
        SUM(dismissed) as dismissals_last_5
    FROM running_stats
    WHERE match_rank <= 5
    GROUP BY batter_id
    """
    return session.engine.execute_sql(query).to_pandas()

@register_feature("death_over_metrics")
def build_death_over_metrics(session: MidwicketSession) -> pd.DataFrame:
    """
    Extracts key metrics for batters and bowlers in death overs (overs 16-20, i.e. over >= 15).
    """
    query_batting = """
    SELECT
        batter_id,
        SUM(runs_batter) as death_runs,
        SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END) as death_balls_faced,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as death_dismissals,
        SUM(runs_batter) * 100.0 / NULLIF(SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END), 0) as death_strike_rate
    FROM ball_events
    WHERE over >= 15
    GROUP BY batter_id
    """
    
    query_bowling = """
    SELECT
        bowler_id,
        SUM(runs_batter + runs_extras) as death_runs_conceded,
        SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END) as death_balls_bowled,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as death_wickets,
        SUM(runs_batter + runs_extras) * 6.0 / NULLIF(SUM(CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END), 0) as death_economy
    FROM ball_events
    WHERE over >= 15
    GROUP BY bowler_id
    """
    
    df_bat = session.engine.execute_sql(query_batting).to_pandas()
    df_bowl = session.engine.execute_sql(query_bowling).to_pandas()
    
    # Merge both perspectives into a single unified dataframe
    merged = pd.merge(df_bat, df_bowl, left_on="batter_id", right_on="bowler_id", how="outer")
    merged['player_id'] = merged['batter_id'].combine_first(merged['bowler_id']).astype(int)
    merged = merged.drop(columns=['batter_id', 'bowler_id'])
    
    return merged

@register_feature("venue_adjusted_form")
def build_venue_adjusted_form(session: MidwicketSession, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Computes a player's recent form adjusted by the historical average scoring rate at the venues they played in.
    """
    where_clause = _get_where_clause(start_date, end_date)
    # 1. Get venue averages (cast AVG calculation to DOUBLE to prevent Decimal multiplication crash in Python)
    venue_query = f"""
    SELECT 
        venue_id,
        CAST(AVG(runs_batter + runs_extras) * 6.0 AS DOUBLE) as venue_avg_runs_per_over
    FROM ball_events
    {where_clause}
    GROUP BY venue_id
    """
    df_venue = session.engine.execute_sql(venue_query).to_pandas()
    
    # 2. Get player match runs & venues
    player_query = f"""
    SELECT 
        batter_id,
        venue_id,
        CAST(SUM(runs_batter) AS DOUBLE) as match_runs
    FROM ball_events
    {where_clause}
    GROUP BY batter_id, venue_id, match_id
    """
    df_player = session.engine.execute_sql(player_query).to_pandas()
    
    # Merge and calculate venue adjustment
    merged = pd.merge(df_player, df_venue, on="venue_id", how="left")
    
    # Group by player to get venue-adjusted scoring
    merged['adjusted_diff'] = merged['match_runs'] - (merged['venue_avg_runs_per_over'] * 2.0)
    
    adjusted = merged.groupby('batter_id')['adjusted_diff'].mean().reset_index()
    adjusted = adjusted.rename(columns={'adjusted_diff': 'venue_adjusted_rating'})
    return adjusted

def load_features(name: str, session: Optional[MidwicketSession] = None,
                  start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Load a pre-calculated or dynamically computed feature set with optional temporal bounds.

    Args:
        name: The target feature set name.
        session: Active Midwicket session (default is singleton).
        start_date: Optional start date filter (ISO format, YYYY-MM-DD).
        end_date: Optional end date filter (ISO format, YYYY-MM-DD).

    Returns:
        pd.DataFrame containing the requested features.
    """
    canonical_name = name.lower().strip()
    if canonical_name not in FEATURE_REGISTRY:
        raise ValueError(
            f"Feature set '{name}' is not supported. "
            f"Supported features: {list(FEATURE_REGISTRY.keys())}"
        )
        
    active_session = session or MidwicketSession.get()
    
    logger.info("Computing feature set '%s'...", canonical_name)
    builder = FEATURE_REGISTRY[canonical_name]
    
    import inspect
    sig = inspect.signature(builder)
    if 'start_date' in sig.parameters:
        return builder(active_session, start_date=start_date, end_date=end_date)
    return builder(active_session)
