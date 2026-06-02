"""Average partnership runs by wicket-number, via SQL window functions."""

from midwicket.api.session import MidwicketSession

print(MidwicketSession.get().engine.execute_sql("""
    WITH partnerships AS (
        SELECT match_id, inning, runs_batter + runs_extras AS runs,
               SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)
                 OVER (PARTITION BY match_id, inning ORDER BY over, ball
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS partnership_idx
        FROM ball_events
    )
    SELECT partnership_idx + 1     AS wicket_partnership,
           COUNT(*)                 AS partnerships_count,
           AVG(partnership_runs)    AS avg_runs
    FROM (SELECT match_id, inning, partnership_idx, SUM(runs) AS partnership_runs
          FROM partnerships GROUP BY match_id, inning, partnership_idx)
    GROUP BY partnership_idx ORDER BY partnership_idx
""").to_pandas())
