"""
10_raw_sql.py

This script demonstrates how to execute raw SQL queries against the underlying DuckDB engine.
This is useful for ad-hoc analysis not covered by the standard Query objects.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    # The main table is registered as 'ball_events'
    sql = """
    SELECT 
        phase,
        COUNT(*) as balls,
        SUM(runs_batter) as total_runs,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
    FROM ball_events
    GROUP BY phase
    ORDER BY total_runs DESC
    """
    
    print("Executing Raw SQL...")
    print(sql)
    
    try:
        arrow_table = session.engine.execute_sql(sql)
        df = arrow_table.to_pandas()
        
        print("\nResults:")
        print(df)
        
    except Exception as e:
        print(f"SQL Execution failed: {e}")

if __name__ == "__main__":
    main()
