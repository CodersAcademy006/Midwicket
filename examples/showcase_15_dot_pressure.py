"""
Showcase 15: Dot-Ball Pressure Build
Measures bowler dot ball percentages inside initial over spells.
"""
import midwicket as md

def main():
    print("=== Dot Ball Pressure Metrics ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT bowler_id, COUNT(*) as balls,
               SUM(CASE WHEN runs_batter = 0 AND runs_extras = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as dot_pct
        FROM ball_events
        GROUP BY bowler_id
        HAVING balls >= 30
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
