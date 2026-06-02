"""Midwicket IdentityRegistry - register players/venues/teams, resolve, upsert + get stats, raw SQL."""

from datetime import date
from midwicket.storage.registry import IdentityRegistry

reg = IdentityRegistry(db_path=":memory:")
md = date(2023, 5, 1)

for name in ("V Kohli", "JJ Bumrah", "MS Dhoni", "R Sharma"):
    print("player", name, "->", reg.resolve_player(name, md, auto_ingest=True))
for name in ("Wankhede Stadium", "Eden Gardens", "Chepauk"):
    print("venue ", name, "->", reg.resolve_venue(name, md, auto_ingest=True))
for name in ("Mumbai Indians", "Chennai Super Kings"):
    print("team  ", name, "->", reg.resolve_team(name, md, auto_ingest=True))

kid = reg.resolve_player("V Kohli", md)
reg.upsert_player_stats({kid: {"matches": 237, "runs": 7263, "balls_faced": 5268,
                               "wickets": 4, "balls_bowled": 156, "runs_conceded": 231}})
print("Kohli stats:", reg.get_player_stats(kid))

wid = reg.resolve_venue("Wankhede Stadium", md)
reg.upsert_venue_stats({wid: {"matches": 98, "total_runs": 168320,
                              "first_innings_runs": 84160, "first_innings_count": 98}})
print("Wankhede   :", reg.get_venue_stats(wid))

print(reg.con.execute("SELECT id, type, primary_name FROM entities ORDER BY type, primary_name").fetchall())
reg.close()
