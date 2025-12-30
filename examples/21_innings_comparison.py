"""
21_innings_comparison.py

This script compares batting performance in the 1st vs 2nd innings.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    sql = """
    SELECT 
        inning,
        COUNT(*) as balls,
        SUM(runs_batter + runs_extras) as total_runs,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
        ROUND(SUM(runs_batter + runs_extras) * 6.0 / COUNT(*), 2) as run_rate,
        ROUND(SUM(runs_batter + runs_extras) * 1.0 / NULLIF(SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END), 0), 2) as avg
    FROM ball_events
    WHERE inning <= 2
    GROUP BY inning
    """
    
    print("1st vs 2nd Innings:")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
