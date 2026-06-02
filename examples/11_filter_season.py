"""Filter ball_events by season (year)."""

from midwicket.api.session import MidwicketSession

print(MidwicketSession.get().engine.execute_sql("""
    SELECT COUNT(DISTINCT match_id)                   AS matches_played,
           SUM(runs_batter + runs_extras)             AS total_runs,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS total_wickets
    FROM ball_events
    WHERE YEAR(date) = 2023
""").to_pandas())
