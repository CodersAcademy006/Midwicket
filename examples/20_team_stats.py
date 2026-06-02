"""Team run rates - aggregated from ball_events + registry team names."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    SELECT e.primary_name                                              AS team,
           COUNT(DISTINCT b.match_id)                                  AS matches,
           SUM(b.runs_batter + b.runs_extras)                          AS total_runs,
           ROUND(SUM(b.runs_batter + b.runs_extras)*6.0/COUNT(*), 2)   AS run_rate
    FROM ball_events b
    JOIN registry.main.entities e ON b.batting_team_id = e.id
    GROUP BY e.primary_name ORDER BY run_rate DESC
""").to_pandas())
