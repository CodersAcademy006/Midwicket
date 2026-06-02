"""Player consistency - avg, stddev, coefficient of variation (min 20 innings)."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    WITH match_scores AS (
        SELECT batter_id, match_id, SUM(runs_batter) AS runs
        FROM ball_events GROUP BY batter_id, match_id
    )
    SELECT e.primary_name                          AS batter,
           COUNT(*)                                AS innings,
           AVG(m.runs)                             AS avg_runs,
           STDDEV(m.runs)                          AS std_dev,
           ROUND(STDDEV(m.runs)/AVG(m.runs), 2)    AS cv
    FROM match_scores m
    JOIN registry.main.entities e ON m.batter_id = e.id
    GROUP BY e.primary_name HAVING innings > 20
    ORDER BY avg_runs DESC LIMIT 10
""").to_pandas())
