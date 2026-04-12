"""
14_top_wicket_takers.py

Finds the top wicket takers by joining ball_events with the registry.
Uses engine.raw_connection() for the ATTACH + JOIN pattern.

Prerequisites: run 03_ingest_world.py first to populate data.
"""

from pypitch.api.session import PyPitchSession


def main():
    session = PyPitchSession.get()

    registry_path = session.registry_path.replace("\\", "/")

    sql = """
    SELECT
        e.primary_name as bowler,
        SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END) as wickets,
        COUNT(*) as balls_bowled,
        ROUND(SUM(b.runs_batter + b.runs_extras) * 6.0 / COUNT(*), 2) as economy
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    WHERE b.wicket_type != 'run out'
    GROUP BY e.primary_name
    ORDER BY wickets DESC
    LIMIT 10
    """

    print("Top 10 Wicket Takers:")
    try:
        with session.engine.raw_connection() as con:
            con.execute(f"ATTACH '{registry_path}' AS registry (READ_ONLY)")
            df = con.execute(sql).df()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
