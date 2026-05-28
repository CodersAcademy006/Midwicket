import pandas as pd
from datetime import date
from typing import List, Optional

from midwicket.api.session import get_executor, get_registry
from midwicket.api.models import MatchupResult
from midwicket.query.base import MatchupQuery


def matchup(
    batter: str,
    bowler: str,
    venue: Optional[str] = None,
    phases: Optional[List[str]] = None,
) -> MatchupResult:
    """
    Analyze the head-to-head record between a batter and bowler.

    Returns a MatchupResult object with aggregated statistics.

    Example:
        >>> result = md.stats.matchup("V Kohli", "JJ Bumrah")
        >>> print(f"Average: {result.average}")
    """
    if phases is None:
        phases = ["Powerplay", "Middle", "Death"]

    reg = get_registry()
    exc = get_executor()

    today = date.today()
    b_id = reg.resolve_player(batter, today)
    bo_id = reg.resolve_player(bowler, today)

    v_id = None
    if venue:
        v_id = reg.resolve_venue(venue, today)

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
