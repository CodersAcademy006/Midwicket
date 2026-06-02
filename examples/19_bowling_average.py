"""Best bowling average (min 20 wickets) - runs conceded / wickets."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    SELECT e.primary_name                                                                                                     AS bowler,
           SUM(b.runs_batter + b.runs_extras)                                                                                  AS runs_conceded,
           SUM(CASE WHEN b.is_wicket AND b.wicket_type != 'run out' THEN 1 ELSE 0 END)                                         AS wickets,
           ROUND(SUM(b.runs_batter + b.runs_extras)*1.0/NULLIF(SUM(CASE WHEN b.is_wicket AND b.wicket_type != 'run out' THEN 1 ELSE 0 END), 0), 2) AS average
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    GROUP BY e.primary_name HAVING wickets > 20
    ORDER BY average ASC LIMIT 10
""").to_pandas())
