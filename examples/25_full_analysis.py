"""Best death bowlers at a specific venue (min 10 overs) - venue + phase + JOIN combined."""

from midwicket.api.session import MidwicketSession

session = MidwicketSession.get()
rp = session.registry_path.replace("\\", "/")
with session.engine.raw_connection() as con:
    con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
    print(con.execute("""
        SELECT e.primary_name                                            AS bowler,
               COUNT(*)                                                  AS balls,
               SUM(b.runs_batter + b.runs_extras)                        AS runs,
               SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END)              AS wickets,
               ROUND(SUM(b.runs_batter + b.runs_extras)*6.0/COUNT(*), 2) AS economy
        FROM ball_events b
        JOIN registry.main.entities e ON b.bowler_id = e.id
        WHERE b.phase = 'Death'
          AND b.venue_id IN (SELECT id FROM registry.main.entities
                             WHERE primary_name = 'Wankhede Stadium' AND type = 'venue')
        GROUP BY e.primary_name HAVING balls > 60
        ORDER BY economy ASC LIMIT 10
    """).df().to_string(index=False))
