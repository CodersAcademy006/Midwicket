from typing import Dict
from pypitch.api.session import get_executor, get_registry
from pypitch.query.defs import WinProbQuery

def predict_win(venue: str, target: int, current_runs: int, wickets_down: int, overs_done: float) -> Dict[str, float]:
    """
    Returns win probability for the chasing team.
    """
    reg = get_registry()
    exc = get_executor()
    
    v_id = reg.resolve_venue(venue)
    
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
    return response['data'] # Expecting {'win_prob': 0.45}
