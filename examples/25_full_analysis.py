"""
25_full_analysis.py

Comprehensive analysis combining venue filter, phase filter, and registry join.
Uses engine.raw_connection() for the ATTACH + JOIN pattern.

Prerequisites: run 03_ingest_world.py first to populate data.
"""

from pypitch.api.session import PyPitchSession


def main():
    session = PyPitchSession.get()

    registry_path = session.registry_path.replace("\\", "/")
    venue_name = "Wankhede Stadium"

    sql = f"""
    SELECT
        e.primary_name as bowler,
        COUNT(*) as balls,
        SUM(b.runs_batter + b.runs_extras) as runs,
        SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END) as wickets,
        ROUND((SUM(b.runs_batter + b.runs_extras) * 6.0) / COUNT(*), 2) as economy
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    WHERE b.phase = 'Death'
      AND b.venue_id IN (
            SELECT id FROM registry.main.entities
            WHERE primary_name = '{venue_name}' AND type = 'venue'
      )
    GROUP BY e.primary_name
    HAVING balls > 60
    ORDER BY economy ASC
    LIMIT 10
    """

    print(f"Best Death Bowlers at {venue_name} (Min 10 Overs):")
    try:
        with session.engine.raw_connection() as con:
            con.execute(f"ATTACH '{registry_path}' AS registry (READ_ONLY)")
            df = con.execute(sql).df()
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
