"""
25_full_analysis.py

This script combines multiple techniques to perform a comprehensive analysis:
1. Filter by Venue
2. Filter by Phase
3. Join with Registry
4. Calculate multiple metrics
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
        
    # Analysis: Best Death Bowlers at Wankhede Stadium
    venue_name = "Wankhede Stadium"
    
    # First, get venue ID (using SQL subquery or Python)
    # We'll use a subquery in the main SQL for elegance
    
    sql = f"""
    SELECT 
        e.primary_name as bowler,
        COUNT(*) as balls,
        SUM(b.runs_batter + b.runs_extras) as runs,
        SUM(CASE WHEN b.is_wicket THEN 1 ELSE 0 END) as wickets,
        ROUND((SUM(b.runs_batter + b.runs_extras) * 6.0) / COUNT(*), 2) as economy
    FROM ball_events b
    JOIN registry.main.entities e ON b.bowler_id = e.id
    WHERE b.phase = 'Death'
      AND b.venue_id IN (SELECT id FROM registry.main.entities WHERE primary_name = '{venue_name}' AND type = 'venue')
    GROUP BY e.primary_name
    HAVING balls > 60
    ORDER BY economy ASC
    LIMIT 10
    """
    
    print(f"Best Death Bowlers at {venue_name} (Min 10 Overs):")
    try:
        df = session.engine.execute_sql(sql).to_pandas()
        print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
