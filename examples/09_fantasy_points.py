"""Run a FantasyQuery directly for a venue's players."""

from datetime import date
from midwicket.api.session import get_executor, get_registry
from midwicket.query.defs import FantasyQuery

vid = get_registry().resolve_venue("MA Chidambaram Stadium", date.today())
q = FantasyQuery(venue_id=vid, roles=["all"], min_matches=2)
print(get_executor().execute(q).data.to_pandas().head())
