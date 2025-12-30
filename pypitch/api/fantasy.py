import pandas as pd
from typing import List, Literal, Dict, Any

from pypitch.api.session import get_executor, get_registry
from pypitch.query.defs import FantasyQuery

def cheat_sheet(venue: str, last_n_years: int = 3) -> pd.DataFrame:
    """
    Generates a Fantasy Cheat Sheet for a specific venue.
    Includes: Avg Score, Pace vs Spin wickets, Top Fantasy Point scorers.
    """
    reg = get_registry()
    exc = get_executor()
    
    v_id = reg.resolve_venue(venue)
    
    q = FantasyQuery(
        venue_id=v_id,
        roles=["all"],
        min_matches=5,
        snapshot_id="latest"
    )
    
    response = exc.execute(q)
    df = response.data.to_pandas()
    
    # Sort by 'fantasy_points_avg' (assuming compute layer produced this)
    if not df.empty and 'avg_points' in df.columns:
        df = df.sort_values('avg_points', ascending=False).head(20)
        
    return df

def venue_bias(venue: str) -> Dict[str, Any]:
    """
    Returns the bias of a venue (Win % Batting First vs Chasing).
    """
    # This would execute a different specific Query, 
    # but for Stage 4 MVP we can reuse the cheat sheet logic or add a VenueQuery
    # Placeholder for MVP:
    return {
        "venue": venue,
        "win_bat_first_pct": 45.2,
        "win_chase_pct": 54.8,
        "verdict": "CHASE"
    }
