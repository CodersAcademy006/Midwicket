"""
Showcase 19: Bowler Performance Stamina Decay
Analyzes average scoring rate allowed per over in bowler spells.
"""
import midwicket as md

def main():
    print("=== Bowler Stamina Decay ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT bowler_id, over, COUNT(*) as balls, AVG(runs_batter + runs_extras) as avg_runs
        FROM ball_events
        GROUP BY bowler_id, over
        HAVING balls >= 10
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
