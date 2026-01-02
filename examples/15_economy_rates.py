"""
15_economy_rates.py

This script calculates economy rates for bowlers who have bowled at least 120 balls (20 overs).
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
        COUNT(*) as balls,
        ROUND((SUM(b.runs_batter + b.runs_extras) * 6.0) / COUNT(*), 2) as economy
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    GROUP BY e.primary_name
    HAVING balls >= 120
    ORDER BY economy ASC
    LIMIT 10
    """
    
    print("Best Economy Rates (Min 20 Overs):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
