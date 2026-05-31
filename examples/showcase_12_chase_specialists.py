"""
Showcase 12: Best Batters in Second-Innings Chases
Identifies batters who maintain high averages during chases.
"""
import midwicket as md

def main():
    print("=== Second-Innings Chase Specialists ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, SUM(runs_batter) as chase_runs, COUNT(*) as balls
        FROM ball_events
        WHERE inning = 2
        GROUP BY batter_id
        HAVING balls >= 30
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
