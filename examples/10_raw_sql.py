"""Raw SQL against the ball_events table - phase breakdown."""

from midwicket.api.session import MidwicketSession

print(MidwicketSession.get().engine.execute_sql("""
    SELECT phase,
           COUNT(*)                                   AS balls,
           SUM(runs_batter)                           AS total_runs,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wickets
    FROM ball_events
    GROUP BY phase
    ORDER BY total_runs DESC
""").to_pandas())
