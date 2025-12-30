"""
14_top_wicket_takers.py

This script finds the top wicket takers by joining with the registry.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    # Close existing connection to avoid locks
    session.registry.close()
    
    registry_path = session.registry_path.replace("\\", "/")
    try:
        session.engine.con.execute(f"ATTACH '{registry_path}' AS registry (READ_ONLY);")
    except Exception as e:
        print(f"Warning: Attach failed: {e}")
        
    sql = """
    SELECT 
        e.primary_name as bowler,
        SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END) as wickets,
        COUNT(*) as balls_bowled
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    WHERE b.wicket_type != 'run out' -- Exclude run outs from bowler stats
    GROUP BY e.primary_name
    ORDER BY wickets DESC
    LIMIT 10
    """
    
    print("Top 10 Wicket Takers:")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
