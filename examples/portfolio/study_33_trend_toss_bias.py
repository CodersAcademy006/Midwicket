"""
Historical Trend Study: Toss Bias over Seasons
Analyzes scoring rate evolution and tactical shifts across match years.
"""
import midwicket as md

def main():
    print("=== Historical Trend: Toss Bias ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT over, AVG(runs_batter + runs_extras) * 6.0 as rpo
        FROM ball_events
        GROUP BY over
        ORDER BY over
        LIMIT 5
    """)
    print(res.to_pandas())

if __name__ == "__main__":
    main()
