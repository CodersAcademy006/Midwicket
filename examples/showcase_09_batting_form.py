"""
Showcase 9: Batting Form Decay tracker
Tracks the sliding averages of batters over their last 5 innings.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Batting Form Decay ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("batting_form", session)
    print(df.sort_values(by="avg_runs_last_5", ascending=False).head(10))

if __name__ == "__main__":
    main()
