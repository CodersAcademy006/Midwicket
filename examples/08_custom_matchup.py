"""Build and execute a MatchupQuery manually - more control than matchup()."""

from datetime import date
from midwicket.api.session import get_executor, get_registry
from midwicket.query.defs import MatchupQuery

reg, today = get_registry(), date.today()
q = MatchupQuery(
    batter_id=str(reg.resolve_player("MS Dhoni",  today)),
    bowler_id=str(reg.resolve_player("SP Narine", today)),
    venue_id=None,
)
print(get_executor().execute(q).data)
