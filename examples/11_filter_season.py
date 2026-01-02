"""
11_filter_season.py

This script demonstrates how to filter data by season (year) using SQL.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    # Filter for matches in 2023
    sql = """
    SELECT 
        COUNT(DISTINCT match_id) as matches_played,
        SUM(runs_batter + runs_extras) as total_runs,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as total_wickets
    FROM ball_events
    WHERE YEAR(date) = 2023
    """
    
    print("Stats for Season 2023:")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
