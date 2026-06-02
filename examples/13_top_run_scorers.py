"""Top 10 run scorers - ATTACH registry + JOIN for player names."""

from midwicket.api.session import MidwicketSession

session = MidwicketSession.get()
rp = session.registry_path.replace("\\", "/")
with session.engine.raw_connection() as con:
    con.execute(f"ATTACH '{rp}' AS registry (READ_ONLY)")
    print(con.execute("""
        SELECT e.primary_name                              AS batter,
               SUM(b.runs_batter)                          AS runs,
               COUNT(*)                                    AS balls,
               ROUND(SUM(b.runs_batter)*100.0/COUNT(*), 2) AS strike_rate
        FROM ball_events b
        JOIN registry.main.entities e ON b.batter_id = e.id
        GROUP BY e.primary_name ORDER BY runs DESC LIMIT 10
    """).df().to_string(index=False))
