"""Highest batting average (min 500 runs) - runs / dismissals."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    SELECT e.primary_name                                                                            AS batter,
           SUM(b.runs_batter)                                                                        AS runs,
           SUM(CASE WHEN b.is_wicket AND b.wicket_type != 'run out' THEN 1 ELSE 0 END)               AS dismissals,
           ROUND(SUM(b.runs_batter)*1.0/NULLIF(SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END), 0), 2)  AS average
    FROM ball_events b
    JOIN registry.main.entities e ON b.batter_id = e.id
    GROUP BY e.primary_name HAVING runs > 500
    ORDER BY average DESC LIMIT 10
""").to_pandas())
