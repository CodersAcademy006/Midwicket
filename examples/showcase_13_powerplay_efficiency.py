"""
Showcase 13: Powerplay Inning Acceleration
Extracts scoring metrics during fielding restrictions (overs 0-5).
"""
import midwicket as md

def main():
    print("=== Powerplay Efficiency Analysis ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, SUM(runs_batter) as pp_runs, COUNT(*) as pp_balls
        FROM ball_events
        WHERE over < 6
        GROUP BY batter_id
        HAVING pp_balls >= 24
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
