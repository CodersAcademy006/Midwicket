"""
36_full_library_tour.py — Complete PyPitch Library Tour

A single script that walks through every major capability of PyPitch.
Designed to be the canonical "show me what this library does" script.

No server, no downloads needed for sections 1–4.
Sections 5–6 need data (run 01_setup_data.py first).

Usage:
    python examples/36_full_library_tour.py
"""

import os
import pyarrow as pa

os.environ.setdefault("PYPITCH_ENV", "development")


# ── helpers ──────────────────────────────────────────────────────────────────

def banner(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print("─" * 60)


# ═══════════════════════════════════════════════════════════════════
# SECTION 1 — Package metadata & version
# ═══════════════════════════════════════════════════════════════════
banner("1 · Package metadata")

import pypitch as pp
print(f"  pypitch v{pp.__version__}  |  author: {pp.__author__}")
print(f"  Public API: {', '.join(pp.__all__[:8])} …")


# ═══════════════════════════════════════════════════════════════════
# SECTION 2 — Pure compute metrics (no DB)
# ═══════════════════════════════════════════════════════════════════
banner("2 · Pure compute metrics (no database)")

from pypitch.compute.metrics.batting import calculate_strike_rate
from pypitch.compute.metrics.bowling import calculate_economy, calculate_pressure_index
from pypitch.compute.metrics.team    import calculate_team_win_rate

runs  = pa.array([68, 45, 30], type=pa.int64())
balls = pa.array([42, 38, 28], type=pa.int64())
sr    = calculate_strike_rate(runs, balls)
print("  Strike rates:", [round(sr[i].as_py(), 1) for i in range(3)])

r_con   = pa.array([28, 32, 22], type=pa.int64())
lb      = pa.array([24, 24, 24], type=pa.int64())
econ    = calculate_economy(r_con, lb)
print("  Economy rates:", [round(econ[i].as_py(), 2) for i in range(3)])

wins    = pa.array([9, 10, 7], type=pa.int64())
matches = pa.array([14, 14, 14], type=pa.int64())
wr      = calculate_team_win_rate(wins, matches)
print("  Win rates (%):", [round(wr[i].as_py(), 1) for i in range(3)])


# ═══════════════════════════════════════════════════════════════════
# SECTION 3 — Win probability model
# ═══════════════════════════════════════════════════════════════════
banner("3 · Win probability model")

from pypitch.compute.winprob import win_probability

scenarios = [
    ("Easy chase",   dict(target=180, current_runs=100, wickets_down=2, overs_done=10.0)),
    ("Tight game",   dict(target=180, current_runs=100, wickets_down=5, overs_done=14.0)),
    ("Near certain", dict(target=180, current_runs=170, wickets_down=2, overs_done=19.0)),
]
for label, kw in scenarios:
    p = win_probability(**kw, venue=None)
    print(f"  {label:<18}  win_prob = {p.get('win_prob', 'N/A')}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 4 — In-memory query engine + schema validation
# ═══════════════════════════════════════════════════════════════════
banner("4 · QueryEngine — in-memory schema + SQL")

from pypitch.schema.v1 import BALL_EVENT_SCHEMA
from pypitch.storage.engine import QueryEngine

n = 8
sample = pa.table(
    {
        "match_id":    pa.array(["m1"] * n,               type=pa.string()),
        "inning":      pa.array([1] * n,                   type=pa.int32()),
        "over":        pa.array(list(range(n)),             type=pa.int32()),
        "ball":        pa.array([1] * n,                   type=pa.int32()),
        "batter_id":   pa.array([1, 1, 1, 2, 2, 2, 1, 1], type=pa.int32()),
        "bowler_id":   pa.array([3] * n,                   type=pa.int32()),
        "venue_id":    pa.array([10] * n,                  type=pa.int32()),
        "runs_batter": pa.array([4, 0, 6, 1, 2, 0, 1, 4], type=pa.int32()),
        "runs_extras": pa.array([0] * n,                   type=pa.int32()),
        "is_wicket":   pa.array([False] * 7 + [True],      type=pa.bool_()),
        "wicket_type": pa.array([""] * 7 + ["bowled"],     type=pa.string()),
        "phase":       pa.array(["Powerplay"] * n,         type=pa.string()),
        "season":      pa.array([2023] * n,                type=pa.int32()),
    },
    schema=BALL_EVENT_SCHEMA,
)

engine = QueryEngine(":memory:")
engine.ingest_events(sample, snapshot_tag="tour")

res = engine.execute_sql(
    "SELECT batter_id, SUM(runs_batter) AS runs, COUNT(*) AS balls "
    "FROM ball_events GROUP BY batter_id ORDER BY runs DESC"
)
print("  Batter summary:")
for row in res.to_pandas().itertuples(index=False):
    print(f"    batter_id={row.batter_id}  runs={row.runs}  balls={row.balls}")

engine.close()


# ═══════════════════════════════════════════════════════════════════
# SECTION 5 — Registry (in-memory)
# ═══════════════════════════════════════════════════════════════════
banner("5 · IdentityRegistry — name ↔ integer ID")

from datetime import date
from pypitch.storage.registry import IdentityRegistry

reg = IdentityRegistry(":memory:")
d   = date(2024, 4, 1)
kid = reg.resolve_player("V Kohli",  d, auto_ingest=True)
bid = reg.resolve_player("JJ Bumrah", d, auto_ingest=True)
wid = reg.resolve_venue("Wankhede Stadium", d, auto_ingest=True)
print(f"  V Kohli     → {kid}")
print(f"  JJ Bumrah   → {bid}")
print(f"  Wankhede    → {wid}")

reg.upsert_player_stats({kid: {"matches":237,"runs":7263,"balls_faced":5268,
                               "wickets":4,"balls_bowled":156,"runs_conceded":231}})
stats = reg.get_player_stats(kid)
print(f"  Kohli stats : {stats}")
reg.close()


# ═══════════════════════════════════════════════════════════════════
# SECTION 6 — Express API (requires downloaded data)
# ═══════════════════════════════════════════════════════════════════
banner("6 · Express API (requires 01_setup_data.py)")

try:
    import pypitch.express as px
    stats = px.get_player_stats("V Kohli")
    if stats:
        print(f"  V Kohli: {stats.runs} runs in {stats.matches} matches")
        print(f"  SR: {stats.strike_rate:.1f}  Economy: {stats.economy:.2f}" if stats.economy else "")
    else:
        print("  (no data — registry empty. Run 01_setup_data.py first)")
except Exception as exc:
    print(f"  Skipped: {exc}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 7 — Debug & logging
# ═══════════════════════════════════════════════════════════════════
banner("7 · Debug mode & structured logging")

from pypitch.logging_config import setup_logging, get_logger
import logging

setup_logging(level=logging.WARNING)  # suppress noise for demo
logger = get_logger(__name__)

pp.set_debug_mode(True)
logger.warning("Debug mode enabled — eager execution active")
pp.set_debug_mode(False)
print("  Logging + debug mode: OK")

print("\n" + "═" * 60)
print("  Tour complete. PyPitch is ready to deploy.")
print("═" * 60)
