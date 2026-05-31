"""
BQR Validation Study: Wickets Correlation
Analyzes Bowler Quality Rating correlation with economy rates.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== BQR Validation: Wickets Correlation ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("bowler_quality_rating", session)
    print(df.head(5))

if __name__ == "__main__":
    main()
