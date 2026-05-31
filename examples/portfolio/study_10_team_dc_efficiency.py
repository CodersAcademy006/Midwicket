"""
Team Analysis Study: Inning Scoring Efficiency for DC
Analyzes team run rates and boundary percentages.
"""
import midwicket as md

def main():
    print("=== Team Analysis: DC ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT batting_team_id, COUNT(*) as balls, SUM(runs_batter + runs_extras) as total_runs
        FROM ball_events
        GROUP BY batting_team_id
        LIMIT 5
    """)
    print(res.to_pandas())

if __name__ == "__main__":
    main()
