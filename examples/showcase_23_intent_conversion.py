"""
Showcase 23: Batter Attacking Shot Boundary Conversion
Measures ratio of boundary runs to total runs.
"""
import midwicket as md

def main():
    print("=== Batter Intent Conversion ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batter_id, SUM(runs_batter) as total_runs,
               SUM(CASE WHEN runs_batter = 4 OR runs_batter = 6 THEN runs_batter ELSE 0 END) * 100.0 / NULLIF(SUM(runs_batter), 0) as boundary_runs_pct
        FROM ball_events
        GROUP BY batter_id
        HAVING total_runs >= 50
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
