from typing import Dict, cast, Any, Optional
from datetime import date
from midwicket.api.session import get_executor, get_registry
from midwicket.query.defs import WinProbQuery

def predict_win(venue: str, target: int, current_runs: int, wickets_down: int, overs_done: float, match_date: Optional[date] = None) -> Dict[str, float]:
    """
    Returns win probability for the chasing team.
    """
    reg = get_registry()
    exc = get_executor()
    
    if match_date is None:
        match_date = date.today()
        
    v_id = reg.resolve_venue(venue, match_date=match_date)
    
    q = WinProbQuery(
        venue_id=v_id,
        target_score=target,
        current_runs=current_runs,
        current_wickets=wickets_down,
        overs_remaining=20.0 - overs_done
    )
    
    # For Stage 1, this will likely hit a 'NotImplemented' in executor 
    # until we wire up the actual Sim model, but the API contract is valid.
    response = exc.execute(q)
    return cast(Dict[str, float], response.data) # Expecting {'win_prob': 0.45}
