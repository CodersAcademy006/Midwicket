"""
17_dot_ball_percentage.py

This script calculates the dot ball percentage for bowlers.
High dot ball % usually indicates good control.
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
        COUNT(*) as balls_bowled,
        SUM(CASE WHEN b.runs_batter = 0 AND b.runs_extras = 0 THEN 1 ELSE 0 END) as dots,
        ROUND(
            (SUM(CASE WHEN b.runs_batter = 0 AND b.runs_extras = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 
            2
        ) as dot_pct
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    GROUP BY e.primary_name
    HAVING balls_bowled > 300
    ORDER BY dot_pct DESC
    LIMIT 10
    """
    
    print("Highest Dot Ball % (Min 300 Balls):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
