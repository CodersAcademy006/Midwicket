"""
Showcase 3: Venue Bias Analysis
Analyzes first vs. second innings scoring distributions across venues.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Venue Bias and Scoring Skew ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("venue_adjusted_form", session)
    print(df.head(10))

if __name__ == "__main__":
    main()
