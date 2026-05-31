"""
Showcase 20: Clutch Playoff Performances
Ranks players by historical playoff stats.
"""
import midwicket as md

def main():
    print("=== Playoff Clutch Statistics ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, COUNT(DISTINCT match_id) as matches, SUM(runs_batter) as total_runs
        FROM ball_events
        GROUP BY batter_id
        HAVING matches >= 3
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
