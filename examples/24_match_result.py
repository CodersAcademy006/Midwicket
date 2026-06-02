"""Match winners by joining inning scores (first 10 matches)."""

from midwicket.api.session import MidwicketSession

s = MidwicketSession.get(); s.registry.close()
rp = s.registry_path.replace("\\", "/")
s.engine.con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
print(s.engine.execute_sql("""
    WITH inning_scores AS (
        SELECT match_id, inning, batting_team_id, SUM(runs_batter + runs_extras) AS total_runs
        FROM ball_events GROUP BY match_id, inning, batting_team_id
    )
    SELECT i1.match_id,
           t1.primary_name AS team_1, i1.total_runs AS score_1,
           t2.primary_name AS team_2, i2.total_runs AS score_2,
           CASE WHEN i1.total_runs > i2.total_runs THEN t1.primary_name
                WHEN i2.total_runs > i1.total_runs THEN t2.primary_name
                ELSE 'Tie' END AS winner
    FROM inning_scores i1
    JOIN inning_scores i2 ON i1.match_id = i2.match_id AND i1.inning = 1 AND i2.inning = 2
    JOIN registry.main.entities t1 ON i1.batting_team_id = t1.id
    JOIN registry.main.entities t2 ON i2.batting_team_id = t2.id
    LIMIT 10
""").to_pandas())
