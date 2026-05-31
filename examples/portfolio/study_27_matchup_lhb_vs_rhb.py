"""
Matchup Intelligence Study: Lhb Vs Rhb
Models tactical head-to-head outcomes for specific matchups.
"""
import midwicket as md

def main():
    print("=== Matchup Intelligence: Lhb Vs Rhb ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, bowler_id, COUNT(*) as balls, SUM(runs_batter) as runs
        FROM ball_events
        GROUP BY batter_id, bowler_id
        HAVING balls >= 5
        LIMIT 5
    """)
    print(res.to_pandas())

if __name__ == "__main__":
    main()
