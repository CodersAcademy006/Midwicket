"""Phase breakdown - Powerplay / Middle / Death."""

from midwicket.api.session import MidwicketSession

print(MidwicketSession.get().engine.execute_sql("""
    SELECT phase,
           SUM(runs_batter)                                                               AS runs,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)                                     AS wickets,
           ROUND(SUM(runs_batter) * 1.0 / SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END), 2)  AS avg,
           ROUND(SUM(runs_batter) * 6.0 / COUNT(*), 2)                                    AS run_rate
    FROM ball_events
    GROUP BY phase
    ORDER BY run_rate DESC
""").to_pandas())
