"""
Pressure Index Validation Study: Death Overs Leverage
Analyzes runs and wickets under extreme pressure indexes.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Pressure Index: Death Overs Leverage ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("pressure_index", session)
    print(df.head(5))

if __name__ == "__main__":
    main()
