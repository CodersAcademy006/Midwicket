"""
Showcase 14: Bowler Wicket Clusters
Calculates bowler probability to take multiple wickets inside a single match.
"""
import midwicket as md

def main():
    print("=== Bowler Wicket-in-Cluster Probability ===")
    session = md.datasets.load_dataset("mlc")
    res = session.engine.execute_sql("""
        SELECT bowler_id, match_id, COUNT(CASE WHEN is_wicket THEN 1 END) as wickets
        FROM ball_events
        GROUP BY bowler_id, match_id
        HAVING wickets >= 2
    """)
    print(res.to_pandas().head(10))

if __name__ == "__main__":
    main()
