"""Midwicket - complete capability tour, all in-memory."""

from datetime import date
import logging
import pyarrow as pa

import midwicket as md
import midwicket.express as px
from midwicket.compute.metrics.batting import calculate_strike_rate
from midwicket.compute.metrics.bowling import calculate_economy
from midwicket.compute.metrics.team    import calculate_team_win_rate
from midwicket.compute.winprob          import win_probability
from midwicket.schema.v1                import BALL_EVENT_SCHEMA
from midwicket.storage.engine           import QueryEngine
from midwicket.storage.registry         import IdentityRegistry
from midwicket.logging_config           import setup_logging

print("midwicket", md.__version__, "by", md.__author__, "|", ", ".join(md.__all__[:8]), "...")

# 1. Pure compute metrics
print("SR  :", calculate_strike_rate(pa.array([68, 45, 30]), pa.array([42, 38, 28])).to_pylist())
print("Econ:", calculate_economy(pa.array([28, 32, 22]), pa.array([24, 24, 24])).to_pylist())
print("Win%:", calculate_team_win_rate(pa.array([9, 10, 7]), pa.array([14, 14, 14])).to_pylist())

# 2. Win probability across three scenarios
for kw in [dict(target=180, current_score=100, wickets_down=2, overs_done=10.0),
           dict(target=180, current_score=100, wickets_down=5, overs_done=14.0),
           dict(target=180, current_score=170, wickets_down=2, overs_done=19.0)]:
    print(win_probability(**kw, venue=None))

# 3. In-memory QueryEngine + Schema V1 + SQL
n = 8
sample = pa.table({
    "match_id":        pa.array(["m1"] * n,                pa.string()),
    "date":            pa.array([date(2023, 4, 1)] * n,    pa.date32()),
    "venue_id":        pa.array([10] * n,                  pa.int32()),
    "inning":          pa.array([1] * n,                   pa.int8()),
    "over":            pa.array(range(n),                  pa.int8()),
    "ball":            pa.array([1] * n,                   pa.int8()),
    "batter_id":       pa.array([1, 1, 1, 2, 2, 2, 1, 1], pa.int32()),
    "bowler_id":       pa.array([3] * n,                   pa.int32()),
    "non_striker_id":  pa.array([2] * n,                   pa.int32()),
    "batting_team_id": pa.array([1] * n,                   pa.int16()),
    "bowling_team_id": pa.array([2] * n,                   pa.int16()),
    "runs_batter":     pa.array([4, 0, 6, 1, 2, 0, 1, 4], pa.int8()),
    "runs_extras":     pa.array([0] * n,                   pa.int8()),
    "is_wicket":       pa.array([False] * 7 + [True],      pa.bool_()),
    "wicket_type":     pa.array([""] * 7 + ["bowled"],     pa.dictionary(pa.int8(), pa.string())),
    "phase":           pa.array(["Powerplay"] * n,         pa.dictionary(pa.int8(), pa.string())),
}, schema=BALL_EVENT_SCHEMA)
eng = QueryEngine(db_path=":memory:")
eng.ingest_events(sample, snapshot_tag="tour")
print(eng.execute_sql(
    "SELECT batter_id, SUM(runs_batter) runs FROM ball_events GROUP BY batter_id ORDER BY runs DESC"
).to_pandas())
eng.close()

# 4. IdentityRegistry
reg = IdentityRegistry(db_path=":memory:")
kid = reg.resolve_player("V Kohli", date(2024, 4, 1), auto_ingest=True)
reg.upsert_player_stats({kid: {"matches": 237, "runs": 7263, "balls_faced": 5268,
                               "wickets": 4, "balls_bowled": 156, "runs_conceded": 231}})
print("Kohli:", reg.get_player_stats(kid))
reg.close()

# 5. Express API + debug mode + logging
setup_logging(level=logging.WARNING)
try:
    print(px.get_player_stats("V Kohli"))
except Exception as e:
    print("(express skipped:", e, ")")
md.set_debug_mode(True); md.set_debug_mode(False)
