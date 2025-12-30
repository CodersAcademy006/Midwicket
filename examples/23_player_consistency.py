"""
23_player_consistency.py

This script calculates the Standard Deviation of runs scored by a batter per match.
Lower StdDev (relative to average) implies higher consistency.
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
    WITH match_scores AS (
        SELECT 
            batter_id,
            match_id,
            SUM(runs_batter) as runs
        FROM ball_events
        GROUP BY batter_id, match_id
    )
    SELECT 
        e.primary_name as batter,
        COUNT(*) as innings,
        AVG(m.runs) as avg_runs,
        STDDEV(m.runs) as std_dev,
        ROUND(STDDEV(m.runs) / AVG(m.runs), 2) as cv -- Coefficient of Variation
    FROM match_scores m
    JOIN registry.main.entities e ON m.batter_id = e.id
    GROUP BY e.primary_name
    HAVING innings > 20
    ORDER BY avg_runs DESC
    LIMIT 10
    """
    
    print("Player Consistency (Avg & StdDev):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
