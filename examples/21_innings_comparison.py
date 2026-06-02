"""1st vs 2nd innings comparison."""

from midwicket.api.session import MidwicketSession

print(MidwicketSession.get().engine.execute_sql("""
    SELECT inning,
           COUNT(*)                                                                                            AS balls,
           SUM(runs_batter + runs_extras)                                                                      AS total_runs,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)                                                          AS wickets,
           ROUND(SUM(runs_batter + runs_extras)*6.0/COUNT(*), 2)                                               AS run_rate,
           ROUND(SUM(runs_batter + runs_extras)*1.0/NULLIF(SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END), 0), 2)  AS avg
    FROM ball_events WHERE inning <= 2 GROUP BY inning
""").to_pandas())
