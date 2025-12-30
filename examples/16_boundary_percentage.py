"""
16_boundary_percentage.py

This script calculates the percentage of runs scored in boundaries (4s and 6s) vs running.
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
        SUM(b.runs_batter) as total_runs,
        SUM(CASE WHEN b.runs_batter = 4 THEN 4 ELSE 0 END) as runs_in_fours,
        SUM(CASE WHEN b.runs_batter = 6 THEN 6 ELSE 0 END) as runs_in_sixes,
        ROUND(
            (SUM(CASE WHEN b.runs_batter IN (4, 6) THEN b.runs_batter ELSE 0 END) * 100.0) / SUM(b.runs_batter), 
            2
        ) as boundary_pct
    FROM ball_events b
    JOIN registry.main.entities e ON b.batter_id = e.id
    GROUP BY e.primary_name
    HAVING total_runs > 500
    ORDER BY boundary_pct DESC
    LIMIT 10
    """
    
    print("Highest Boundary % (Min 500 Runs):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
