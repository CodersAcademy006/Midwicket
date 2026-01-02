"""
18_batting_average.py

This script calculates Batting Average: Total Runs / Number of Dismissals.
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
        e.primary_name as batter,
        SUM(b.runs_batter) as runs,
        SUM(CASE WHEN b.is_wicket AND b.wicket_type != 'run out' THEN 1 ELSE 0 END) as dismissals,
        ROUND(
            SUM(b.runs_batter) * 1.0 / NULLIF(SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END), 0), 
            2
        ) as average
    FROM ball_events b
    JOIN registry.main.entities e ON b.batter_id = e.id
    GROUP BY e.primary_name
    HAVING runs > 500
    ORDER BY average DESC
    LIMIT 10
    """
    
    print("Highest Batting Average (Min 500 Runs):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
