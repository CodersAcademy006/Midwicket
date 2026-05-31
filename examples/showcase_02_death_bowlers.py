"""
Showcase 2: Best Death Bowlers Analysis
Identifies bowlers with the lowest economy and highest wicket counts in overs 16-20.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Best Death Bowlers (Overs 16-20) ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("death_over_metrics", session)
    
    # Filter bowlers with at least 12 balls bowled in death overs
    bowlers = df[df['death_balls_bowled'] >= 12].sort_values(by="death_economy", ascending=True)
    print(bowlers[['player_id', 'death_balls_bowled', 'death_wickets', 'death_economy']].head(10))

if __name__ == "__main__":
    main()
