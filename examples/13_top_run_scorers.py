"""
13_top_run_scorers.py

This script demonstrates an ADVANCED technique:
Attaching the Registry database to the Query Engine to join IDs with Names.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    # 1. Attach the Registry DB
    # We need to close the existing registry connection to avoid locking issues
    session.registry.close()
    
    # We need to escape backslashes for Windows paths in SQL, or use forward slashes
    registry_path = session.registry_path.replace("\\", "/")
    attach_sql = f"ATTACH '{registry_path}' AS registry (READ_ONLY);"
    
    try:
        session.engine.con.execute(attach_sql)
    except Exception as e:
        print(f"Warning: Attach failed (might be already attached): {e}")
        
    # 2. Query with Join
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
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
