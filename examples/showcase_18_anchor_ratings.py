"""
Showcase 18: Batter Anchor Ratings
Evaluates standard deviation of scoring rates to find stable anchor players.
"""
import midwicket as md

def main():
    print("=== Batter Anchor Capability Analysis ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, AVG(runs_batter) as avg_runs, STDDEV(runs_batter) as runs_stddev
        FROM ball_events
        GROUP BY batter_id
        HAVING COUNT(*) >= 30
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
