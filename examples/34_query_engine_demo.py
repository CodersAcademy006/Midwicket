"""Midwicket query engine - Schema V1 ingest, raw SQL, executor + cache, schema enforcement."""

from datetime import date
import pyarrow as pa
from midwicket.schema.v1         import BALL_EVENT_SCHEMA
from midwicket.storage.engine    import QueryEngine
from midwicket.runtime.executor  import RuntimeExecutor
from midwicket.runtime.cache     import CacheInterface
from midwicket.query.base        import MatchupQuery


class MemoryCache(CacheInterface):
    def __init__(self):            self.s = {}
    def get(self, k):              return self.s.get(k)
    def set(self, k, v, ttl=3600): self.s[k] = v
    def delete(self, k):           self.s.pop(k, None)
    def clear(self):               self.s.clear()
    def close(self):               pass


n = 10
table = pa.table({
    "match_id":        pa.array(["m001"] * n,              pa.string()),
    "date":            pa.array([date(2023, 1, 1)] * n,    pa.date32()),
    "inning":          pa.array([1] * n,                   pa.int8()),
    "over":            pa.array(range(n),                  pa.int8()),
    "ball":            pa.array([1] * n,                   pa.int8()),
    "batter_id":       pa.array([1] * n,                   pa.int32()),
    "non_striker_id":  pa.array([3] * n,                   pa.int32()),
    "bowler_id":       pa.array([2] * n,                   pa.int32()),
    "batting_team_id": pa.array([100] * n,                 pa.int16()),
    "bowling_team_id": pa.array([200] * n,                 pa.int16()),
    "venue_id":        pa.array([10] * n,                  pa.int32()),
    "runs_batter":     pa.array([4, 0, 6, 1, 2, 0, 1, 4, 6, 0], pa.int8()),
    "runs_extras":     pa.array([0] * n,                   pa.int8()),
    "is_wicket":       pa.array([False] * 9 + [True],      pa.bool_()),
    "wicket_type":     pa.array([""] * 9 + ["caught"],     pa.dictionary(pa.int8(), pa.string())),
    "phase":           pa.array(["Powerplay"] * n,         pa.dictionary(pa.int8(), pa.string())),
}, schema=BALL_EVENT_SCHEMA)

engine = QueryEngine(db_path=":memory:")
engine.ingest_events(table, snapshot_tag="demo_2023", append=False)
print(engine.execute_sql(
    "SELECT SUM(runs_batter) runs, SUM(is_wicket::INT) wkts FROM ball_events"
).to_pandas())

executor = RuntimeExecutor(cache=MemoryCache(), engine=engine)
q = MatchupQuery(batter_id="1", bowler_id="2")
r1, r2 = executor.execute(q), executor.execute(q)
print(f"sources: {r1.meta.source} -> {r2.meta.source} (second hits cache)")

try:
    engine.ingest_events(pa.table({"col_a": [1, 2, 3]}), snapshot_tag="bad")
except ValueError as e:
    print("schema enforced:", e)

engine.close()
