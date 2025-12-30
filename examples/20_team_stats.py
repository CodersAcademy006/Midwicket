"""
20_team_stats.py

This script aggregates stats by Team.
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
        e.primary_name as team,
        COUNT(DISTINCT b.match_id) as matches,
        SUM(b.runs_batter + b.runs_extras) as total_runs,
        ROUND(SUM(b.runs_batter + b.runs_extras) * 6.0 / COUNT(*), 2) as run_rate
    FROM ball_events b
    JOIN registry.main.entities e ON b.batting_team_id = e.id
    GROUP BY e.primary_name
    ORDER BY run_rate DESC
    """
    
    print("Team Run Rates:")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
