"""
24_match_result.py

This script determines the winner of each match by aggregating runs per inning.
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
    WITH inning_scores AS (
        SELECT 
            match_id,
            inning,
            batting_team_id,
            SUM(runs_batter + runs_extras) as total_runs
        FROM ball_events
        GROUP BY match_id, inning, batting_team_id
    )
    SELECT 
        i1.match_id,
        t1.primary_name as team_1,
        i1.total_runs as score_1,
        t2.primary_name as team_2,
        i2.total_runs as score_2,
        CASE 
            WHEN i1.total_runs > i2.total_runs THEN t1.primary_name
            WHEN i2.total_runs > i1.total_runs THEN t2.primary_name
            ELSE 'Tie'
        END as winner
    FROM inning_scores i1
    JOIN inning_scores i2 ON i1.match_id = i2.match_id AND i1.inning = 1 AND i2.inning = 2
    JOIN registry.main.entities t1 ON i1.batting_team_id = t1.id
    JOIN registry.main.entities t2 ON i2.batting_team_id = t2.id
    LIMIT 10
    """
    
    print("Match Results (First 10):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
