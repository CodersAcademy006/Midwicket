"""
Showcase 8: Bowler Quality Rating (BQR) Leaderboards
Ranks all active bowlers by their BQR metric.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Bowler Quality Rating (BQR) Leaderboard ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("bowler_quality_rating", session)
    
    top_bowlers = df.sort_values(by="bowler_quality_rating", ascending=True)
    print(top_bowlers.head(10))

if __name__ == "__main__":
    main()
