"""Top 10 wicket takers (run-outs excluded) - ATTACH + JOIN."""

from midwicket.api.session import MidwicketSession

session = MidwicketSession.get()
rp = session.registry_path.replace("\\", "/")
with session.engine.raw_connection() as con:
    con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
    print(con.execute("""
        SELECT e.primary_name                                            AS bowler,
               SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END)              AS wickets,
               COUNT(*)                                                  AS balls_bowled,
               ROUND(SUM(b.runs_batter + b.runs_extras)*6.0/COUNT(*), 2) AS economy
        FROM ball_events b
        JOIN registry.main.entities e ON b.bowler_id = e.id
        WHERE b.wicket_type != 'run out'
        GROUP BY e.primary_name ORDER BY wickets DESC LIMIT 10
    """).df().to_string(index=False))
