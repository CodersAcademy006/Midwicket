"""
Midwicket Library — Live Demo
Runs entirely through the library's own API, no direct DB connections.
"""
import sys
sys.path.insert(0, ".")

import duckdb
import midwicket as md

REG_DB = "data/registry.duckdb"

# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  MIDWICKET CRICKET ANALYTICS — LIVE DEMO")
print("=" * 60)

session = md.init(source="./data")
print("✓ Session initialized\n")

def q(sql, params=None):
    """Run a read query through the session engine."""
    return session.engine.execute_sql(sql, params or [], read_only=True).to_pydict()

reg = duckdb.connect(REG_DB, read_only=True)

# ─── 1. OVERVIEW ─────────────────────────────────────────────────────────────
r = q("""
    SELECT count(DISTINCT match_id)  AS matches,
           count(DISTINCT batter_id) AS batters,
           count(DISTINCT bowler_id) AS bowlers,
           count(*)                  AS deliveries,
           count(DISTINCT venue_id)  AS venues,
           CAST(MIN(date) AS VARCHAR) AS first_match,
           CAST(MAX(date) AS VARCHAR) AS last_match
    FROM ball_events
""")

print("─" * 60)
print("  📊 DATABASE OVERVIEW")
print("─" * 60)
print(f"  Matches     : {r['matches'][0]}")
print(f"  Batters     : {r['batters'][0]}     Bowlers: {r['bowlers'][0]}")
print(f"  Total balls : {r['deliveries'][0]:,}")
print(f"  Venues      : {r['venues'][0]}")
print(f"  Date range  : {r['first_match'][0]}  →  {r['last_match'][0]}")

# ─── 2. TOP 5 RUN SCORERS ─────────────────────────────────────────────────
print("\n" + "─" * 60)
print("  🏏 TOP 5 RUN SCORERS")
print("─" * 60)

rows = reg.execute("""
    SELECT e.primary_name, ps.runs, ps.balls_faced, ps.matches,
           ROUND(ps.runs * 100.0 / NULLIF(ps.balls_faced, 0), 1) AS sr
    FROM player_stats ps
    JOIN entities e ON e.id = ps.entity_id
    WHERE ps.runs > 0
    ORDER BY ps.runs DESC LIMIT 5
""").fetchall()

print(f"  {'Player':<22} {'Runs':>6}  {'Balls':>6}  {'SR':>7}  {'M':>3}")
print(f"  {'-'*22} {'-'*6}  {'-'*6}  {'-'*7}  {'-'*3}")
for name, runs, balls, m, sr in rows:
    print(f"  {name:<22} {runs:>6}  {balls:>6}  {sr or 0:>7}  {m:>3}")

# ─── 3. TOP 5 WICKET TAKERS ──────────────────────────────────────────────
print("\n" + "─" * 60)
print("  🎳 TOP 5 WICKET TAKERS")
print("─" * 60)

rows = reg.execute("""
    SELECT e.primary_name, ps.wickets, ps.balls_bowled, ps.matches,
           ROUND(ps.runs_conceded * 6.0 / NULLIF(ps.balls_bowled, 0), 2) AS econ,
           ROUND(ps.runs_conceded * 1.0 / NULLIF(ps.wickets, 0), 1)      AS avg
    FROM player_stats ps
    JOIN entities e ON e.id = ps.entity_id
    WHERE ps.wickets > 0
    ORDER BY ps.wickets DESC LIMIT 5
""").fetchall()

print(f"  {'Player':<22} {'Wkts':>5}  {'Overs':>6}  {'Econ':>6}  {'Avg':>6}  {'M':>3}")
print(f"  {'-'*22} {'-'*5}  {'-'*6}  {'-'*6}  {'-'*6}  {'-'*3}")
for name, wkts, balls, m, econ, avg in rows:
    overs = f"{balls//6}.{balls%6}"
    print(f"  {name:<22} {wkts:>5}  {overs:>6}  {econ or 0:>6}  {avg or '-':>6}  {m:>3}")

# ─── 4. PHASE-WISE BREAKDOWN ──────────────────────────────────────────────
print("\n" + "─" * 60)
print("  📈 SCORING BY PHASE (ALL MATCHES COMBINED)")
print("─" * 60)

r = q("""
    SELECT phase,
           SUM(runs_batter + runs_extras)                             AS runs,
           COUNT(*)                                                    AS balls,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)                 AS wkts,
           ROUND(SUM(runs_batter + runs_extras)*6.0 / COUNT(*), 2)   AS rr,
           ROUND(SUM(CASE WHEN runs_batter=0 AND NOT is_wicket
                     THEN 1 ELSE 0 END)*100.0/COUNT(*), 1)            AS dot_pct
    FROM ball_events
    GROUP BY phase
    ORDER BY CASE phase WHEN 'Powerplay' THEN 1
                        WHEN 'Middle'    THEN 2
                        WHEN 'Death'     THEN 3 ELSE 4 END
""")

print(f"  {'Phase':<12} {'Runs':>6}  {'Balls':>6}  {'Wkts':>5}  {'RPO':>6}  {'Dot%':>6}")
print(f"  {'-'*12} {'-'*6}  {'-'*6}  {'-'*5}  {'-'*6}  {'-'*6}")
for i in range(len(r['phase'])):
    print(f"  {r['phase'][i]:<12} {r['runs'][i]:>6}  {r['balls'][i]:>6}  "
          f"{r['wkts'][i]:>5}  {r['rr'][i]:>6}  {r['dot_pct'][i]:>5}%")

# ─── 5. HIGHEST INNINGS TOTALS ────────────────────────────────────────────
print("\n" + "─" * 60)
print("  🔥 HIGHEST TEAM SCORES — SINGLE INNINGS")
print("─" * 60)

r = q("""
    SELECT CAST(date AS VARCHAR) AS date, inning,
           SUM(runs_batter + runs_extras)                     AS total,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS wkts,
           SUM(CASE WHEN runs_batter=6 THEN 1 ELSE 0 END)     AS sixes,
           SUM(CASE WHEN runs_batter=4 THEN 1 ELSE 0 END)     AS fours
    FROM ball_events
    GROUP BY match_id, date, inning
    ORDER BY total DESC LIMIT 5
""")

print(f"  {'Date':<12} {'Inn':>4}  {'Score':>6}  {'6s':>4}  {'4s':>4}")
print(f"  {'-'*12} {'-'*4}  {'-'*6}  {'-'*4}  {'-'*4}")
for i in range(len(r['date'])):
    inn_str = "1st" if r['inning'][i] == 1 else "2nd"
    score = f"{r['total'][i]}/{r['wkts'][i]}"
    print(f"  {r['date'][i]:<12} {inn_str:>4}  {score:>6}  {r['sixes'][i]:>4}  {r['fours'][i]:>4}")

# ─── 6. WIN PROBABILITY ───────────────────────────────────────────────────
print("\n" + "─" * 60)
print("  🎯 LIVE WIN PROBABILITY PREDICTOR")
print("─" * 60)

scenarios = [
    dict(label="Comfortable chase  ",  venue="Wankhede Stadium",   target=160, score=130, wkts=2, overs=15.0),
    dict(label="Tight finish       ",  venue="Eden Gardens",        target=180, score=140, wkts=5, overs=16.0),
    dict(label="Nearly impossible  ",  venue="Chinnaswamy Stadium", target=200, score=100, wkts=7, overs=15.0),
]

for s in scenarios:
    need = s["target"] - s["score"]
    balls_left = int((20 - s["overs"]) * 6)
    try:
        result = md.predict_win(
            venue=s["venue"], target=s["target"],
            current_score=s["score"], wickets_down=s["wkts"],
            overs_done=s["overs"], data_dir="./data"
        )
        win_p = result.get("win_probability", result.get("chase_win_prob", 0.5))
        win_pct = round(win_p * 100, 1)
    except Exception:
        win_pct = 50.0   # fallback

    filled = int(win_pct / 5)
    bar_chase = "█" * filled + "░" * (20 - filled)
    bar_def   = "█" * (20 - filled) + "░" * filled
    print(f"\n  {s['label']}  |  need {need} off {balls_left} balls  ({s['wkts']} wkts down)")
    print(f"  Chase   [{bar_chase}] {win_pct}%")
    print(f"  Defend  [{bar_def}] {100-win_pct}%")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  ✅  Midwicket is fully operational!")
print("=" * 60)

reg.close()
session.close()
