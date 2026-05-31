"""
Showcase 16: Expected Matchup Outcomes
Models projected expected runs based on historical batter-bowler matchup data.
"""
import midwicket as md

def main():
    print("=== Matchup Outcome Leverage ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, bowler_id, COUNT(*) as balls, SUM(runs_batter) as runs
        FROM ball_events
        GROUP BY batter_id, bowler_id
        HAVING balls >= 5
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
