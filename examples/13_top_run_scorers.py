"""
13_top_run_scorers.py

This script demonstrates an ADVANCED technique:
Using engine.raw_connection() to ATTACH the Registry database and join
entity names with ball-level stats in a single SQL query.

Prerequisites: run 03_ingest_world.py first to populate data.
"""

from pypitch.api.session import PyPitchSession


def main():
    session = PyPitchSession.get()

    registry_path = session.registry_path.replace("\\", "/")

    sql = """
    SELECT
        e.primary_name as batter,
        SUM(b.runs_batter) as runs,
        COUNT(*) as balls,
        ROUND(SUM(b.runs_batter) * 100.0 / COUNT(*), 2) as strike_rate
    FROM ball_events b
    JOIN registry.main.entities e ON b.batter_id = e.id
    GROUP BY e.primary_name
    ORDER BY runs DESC
    LIMIT 10
    """

    print("Top 10 Run Scorers (All Time):")
    try:
        with session.engine.raw_connection() as con:
            con.execute(f"ATTACH '{registry_path}' AS registry (READ_ONLY)")
            df = con.execute(sql).df()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
