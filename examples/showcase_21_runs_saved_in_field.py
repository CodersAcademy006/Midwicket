"""
Showcase 21: Fielding Impact Runs Saved
Tracks fielding efficiencies from run-outs and catches.
"""
import midwicket as md

def main():
    print("=== Fielding Runs Saved Index ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT player_dismissed, COUNT(*) as wickets
        FROM ball_events
        WHERE is_wicket = true AND wicket_type = 'RUN_OUT'
        GROUP BY player_dismissed
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
