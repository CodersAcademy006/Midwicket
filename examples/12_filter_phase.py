"""
12_filter_phase.py

This script demonstrates how to analyze performance across different phases of play:
Powerplay (0-6), Middle (7-15), Death (16-20).
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    sql = """
    SELECT 
        phase,
        SUM(runs_batter) as runs,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
        ROUND(SUM(runs_batter) * 1.0 / SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END), 2) as avg,
        ROUND(SUM(runs_batter) * 6.0 / COUNT(*), 2) as run_rate
    FROM ball_events
    GROUP BY phase
    ORDER BY run_rate DESC
    """
    
    print("Phase Analysis:")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
