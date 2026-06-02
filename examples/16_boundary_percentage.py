"""Highest boundary % (min 500 runs)."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    SELECT e.primary_name                                                                                  AS batter,
           SUM(b.runs_batter)                                                                              AS total_runs,
           SUM(CASE WHEN b.runs_batter = 4 THEN 4 ELSE 0 END)                                              AS runs_in_fours,
           SUM(CASE WHEN b.runs_batter = 6 THEN 6 ELSE 0 END)                                              AS runs_in_sixes,
           ROUND(SUM(CASE WHEN b.runs_batter IN (4,6) THEN b.runs_batter ELSE 0 END)*100.0/SUM(b.runs_batter), 2) AS boundary_pct
    FROM ball_events b
    JOIN registry.main.entities e ON b.batter_id = e.id
    GROUP BY e.primary_name HAVING total_runs > 500
    ORDER BY boundary_pct DESC LIMIT 10
""").to_pandas())
