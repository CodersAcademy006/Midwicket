"""
19_bowling_average.py

This script calculates Bowling Average: Runs Conceded / Wickets Taken.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    session.registry.close()
    
    registry_path = session.registry_path.replace("\\", "/")
    try:
        session.engine.con.execute(f"ATTACH '{registry_path}' AS registry (READ_ONLY);")
    except Exception as e:
        print(f"Warning: Attach failed: {e}")
        
    sql = """
    SELECT 
        e.primary_name as bowler,
        SUM(b.runs_batter + b.runs_extras) as runs_conceded,
        SUM(CASE WHEN b.is_wicket AND b.wicket_type != 'run out' THEN 1 ELSE 0 END) as wickets,
        ROUND(
            SUM(b.runs_batter + b.runs_extras) * 1.0 / NULLIF(SUM(CASE WHEN b.is_wicket AND b.wicket_type != 'run out' THEN 1 ELSE 0 END), 0), 
            2
        ) as average
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    GROUP BY e.primary_name
    HAVING wickets > 20
    ORDER BY average ASC
    LIMIT 10
    """
    
    print("Best Bowling Average (Min 20 Wickets):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
