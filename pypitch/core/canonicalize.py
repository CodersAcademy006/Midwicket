import pyarrow as pa
from datetime import datetime
from typing import Dict, Any

from pypitch.schema.v1 import BALL_EVENT_SCHEMA
from pypitch.storage.registry import IdentityRegistry

def _determine_phase(over_num: int) -> str:
    """Materialization Logic: 0-5 (PP), 6-14 (Middle), 15+ (Death)"""
    if over_num < 6: return "Powerplay"
    if over_num < 15: return "Middle"
    return "Death"

def canonicalize_match(match_data: Dict[str, Any], registry: IdentityRegistry) -> pa.Table:
    """
    Transform Raw Cricsheet JSON -> Strict V1 Arrow Table.
    """
    info = match_data.get('info', {})
    
    # --- 1. Extract Global Match Metadata ---
    # Fallback: Use filename ID if not in JSON (logic handled by caller usually)
    match_id = str(info.get('match_type_number', 0)) 
    
    # Parse Date (Handle ISO strings: '2023-05-21')
    date_str = info.get('dates', ['1970-01-01'])[0]
    try:
        match_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        match_date_obj = datetime(1970, 1, 1).date()

    # Resolve Venue
    venue_name = info.get('venue', 'Unknown Venue')
    venue_id = registry.resolve_venue(venue_name, match_date=match_date_obj, auto_ingest=True)

    # --- 2. Prepare Columnar Buffers ---
    # We build lists first, then convert to Arrow arrays (Speed optimization)
    buffers = {
        'match_id': [], 'date': [], 'venue_id': [],
        'inning': [], 'over': [], 'ball': [],
        'batter_id': [], 'bowler_id': [], 'non_striker_id': [],
        'batting_team_id': [], 'bowling_team_id': [],
        'runs_batter': [], 'runs_extras': [],
        'is_wicket': [], 'wicket_type': [],
        'phase': []
    }

    # --- 3. Iterate & Flatten ---
    # Note: Cricsheet format varies. This assumes the standard new format.
    for inning_idx, inning_data in enumerate(match_data.get('innings', [])):
        batting_team = inning_data.get('team')
        bat_team_id = registry.resolve_team(batting_team, match_date=match_date_obj, auto_ingest=True)
        
        # We don't easily know bowling team without looking ahead/behind, 
        # so for Stage 2 MVP we use -1 or logic from the *other* inning.
        # Ideally, we parse 'teams' from info first.
        teams = info.get('teams', [])
        bowl_team_name = next((t for t in teams if t != batting_team), "Unknown")
        bowl_team_id = registry.resolve_team(bowl_team_name, match_date=match_date_obj, auto_ingest=True)
        
        for over_data in inning_data.get('overs', []):
            over_num = over_data['over'] # 0-indexed in new Cricsheet
            phase = _determine_phase(over_num)
            
            for ball_idx, delivery in enumerate(over_data['deliveries']):
                # --- A. Resolve Identities ---
                b_id = registry.resolve_player(delivery['batter'], match_date_obj, auto_ingest=True)
                bo_id = registry.resolve_player(delivery['bowler'], match_date_obj, auto_ingest=True)
                ns_id = registry.resolve_player(delivery['non_striker'], match_date_obj, auto_ingest=True)

                # --- B. Fill Buffers ---
                buffers['match_id'].append(match_id)
                buffers['date'].append(match_date_obj)
                buffers['venue_id'].append(venue_id)
                
                buffers['inning'].append(inning_idx + 1)
                buffers['over'].append(over_num)
                buffers['ball'].append(ball_idx + 1)
                
                buffers['batter_id'].append(b_id)
                buffers['bowler_id'].append(bo_id)
                buffers['non_striker_id'].append(ns_id)
                buffers['batting_team_id'].append(bat_team_id)
                buffers['bowling_team_id'].append(bowl_team_id)
                
                runs = delivery.get('runs', {})
                buffers['runs_batter'].append(runs.get('batter', 0))
                buffers['runs_extras'].append(runs.get('extras', 0))
                
                wickets = delivery.get('wickets', [])
                buffers['is_wicket'].append(len(wickets) > 0)
                buffers['wicket_type'].append(wickets[0]['kind'] if wickets else None)
                
                buffers['phase'].append(phase)

    # --- 4. Build Arrow Table ---
    table = pa.Table.from_pydict(buffers)
    
    # --- 5. Enforce Schema V1 (Casting) ---
    # This is the "Gatekeeper" check. If data types don't align, this fails.
    # We cast to ensure strict adherence to the contract
    try:
        table = table.cast(BALL_EVENT_SCHEMA)
    except Exception as e:
        raise ValueError(f"Schema Violation: {e}")

    return table
