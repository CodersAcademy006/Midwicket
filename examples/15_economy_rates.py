"""Best bowling economy (min 20 overs)."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    SELECT e.primary_name                                            AS bowler,
           SUM(b.runs_batter + b.runs_extras)                        AS runs_conceded,
           COUNT(*)                                                  AS balls,
           ROUND(SUM(b.runs_batter + b.runs_extras)*6.0/COUNT(*), 2) AS economy
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    GROUP BY e.primary_name HAVING balls >= 120
    ORDER BY economy ASC LIMIT 10
""").to_pandas())
