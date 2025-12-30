import pandas as pd
from datetime import date
from typing import List, Optional, Union

from pypitch.api.session import get_executor, get_registry
from pypitch.query.defs import MatchupQuery

def matchup(
    batter: str, 
    bowler: str, 
    venue: Optional[str] = None,
    phases: List[str] = ["Powerplay", "Middle", "Death"]
) -> pd.DataFrame:
    """
    Analyzes the Head-to-Head record between a batter and bowler.
    
    Example:
        >>> df = pp.stats.matchup("V Kohli", "JJ Bumrah")
    """
    reg = get_registry()
    exc = get_executor()
    
    # 1. Resolve Identities (Defaulting to 'today' for latest ID mapping)
    # In v2, allow users to pass specific years.
    today = date.today()
    b_id = reg.resolve_player(batter, today)
    bo_id = reg.resolve_player(bowler, today)
    
    venue_ids = None
    if venue:
        v_id = reg.resolve_venue(venue)
        venue_ids = [v_id]

    # 2. Build Query
    q = MatchupQuery(
        batter_ids=[b_id],
        bowler_ids=[bo_id],
        phases=phases,
        venue_ids=venue_ids
    )

    # 3. Execute
    response = exc.execute(q)
    
    # 4. Convert Arrow -> Pandas for the user
    arrow_table = response['data']
    df = arrow_table.to_pandas()
    
    # Add human-readable names back
    df['batter_name'] = batter
    df['bowler_name'] = bowler
    
    return df
