"""Highest dot ball % (min 300 balls bowled)."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    SELECT e.primary_name                                                                          AS bowler,
           COUNT(*)                                                                                AS balls_bowled,
           SUM(CASE WHEN b.runs_batter = 0 AND b.runs_extras = 0 THEN 1 ELSE 0 END)                AS dots,
           ROUND(SUM(CASE WHEN b.runs_batter = 0 AND b.runs_extras = 0 THEN 1 ELSE 0 END)*100.0/COUNT(*), 2) AS dot_pct
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    GROUP BY e.primary_name HAVING balls_bowled > 300
    ORDER BY dot_pct DESC LIMIT 10
""").to_pandas())
