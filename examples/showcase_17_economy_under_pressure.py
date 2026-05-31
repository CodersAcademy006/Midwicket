"""
Showcase 17: Bowler Economy Defending Small Totals
Ranks bowlers by economy rate when required run rates are under pressure.
"""
import midwicket as md

def main():
    print("=== Bowler Economy Defending Small Totals ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT bowler_id, COUNT(*) as balls,
               SUM(runs_batter + runs_extras) * 6.0 / COUNT(*) as pressure_econ
        FROM ball_events
        GROUP BY bowler_id
        HAVING balls >= 24
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
