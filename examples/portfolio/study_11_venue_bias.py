"""
Venue Analysis Study: Venue Bias Indexing
Determines venue scoring skew and pitches characteristics.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Venue Analysis: Bias ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("venue_bias_rating", session)
    print(df.head(5))

if __name__ == "__main__":
    main()
