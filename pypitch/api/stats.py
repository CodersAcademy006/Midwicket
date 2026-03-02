import pandas as pd
from datetime import date
from typing import List, Optional

from pypitch.api.session import get_executor, get_registry
from pypitch.api.models import MatchupResult
from pypitch.query.base import MatchupQuery


def matchup(
    batter: str,
    bowler: str,
    venue: Optional[str] = None,
    phases: List[str] = ["Powerplay", "Middle", "Death"],
) -> MatchupResult:
    """
    Analyze the head-to-head record between a batter and bowler.

    Returns a MatchupResult object with aggregated statistics.

    Example:
        >>> result = pp.stats.matchup("V Kohli", "JJ Bumrah")
        >>> print(f"Average: {result.average}")
    """
    reg = get_registry()
    exc = get_executor()

    today = date.today()
    b_id = str(reg.resolve_player(batter, today))
    bo_id = str(reg.resolve_player(bowler, today))

    v_id = None
    if venue:
        v_id = str(reg.resolve_venue(venue, today))

    q = MatchupQuery(
        snapshot_id="latest",
        batter_id=b_id,
        bowler_id=bo_id,
        venue_id=v_id,
    )

    response = exc.execute(q)

    arrow_table = response.data
    df = arrow_table.to_pandas() if hasattr(arrow_table, "to_pandas") else pd.DataFrame()

    return MatchupResult.from_dataframe(df, batter, bowler, venue)
