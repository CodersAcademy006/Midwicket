"""
22_partnership_stats.py

This script demonstrates ADVANCED SQL Window Functions to calculate partnerships.
It identifies partnerships by tracking cumulative wickets within an inning.
"""

from pypitch.api.session import PyPitchSession

def main():
    session = PyPitchSession.get()
    
    # 1. Calculate Partnership ID for every ball
    # 2. Aggregate by Partnership ID
    sql = """
    WITH partnerships AS (
        SELECT 
            match_id,
            inning,
            runs_batter + runs_extras as runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) 
                OVER (PARTITION BY match_id, inning ORDER BY over, ball ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
                as partnership_idx
        FROM ball_events
    )
    SELECT 
        partnership_idx + 1 as wicket_partnership,
        COUNT(*) as partnerships_count,
        AVG(partnership_runs) as avg_runs
    FROM (
        SELECT 
            match_id, inning, partnership_idx, SUM(runs) as partnership_runs
        FROM partnerships
        GROUP BY match_id, inning, partnership_idx
    )
    GROUP BY partnership_idx
    ORDER BY partnership_idx
    """
    
    print("Average Partnership Runs by Wicket:")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
