"""
Pressure Index Validation Study: Wickets Pressure
Analyzes runs and wickets under extreme pressure indexes.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Pressure Index: Wickets Pressure ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("pressure_index", session)
    print(df.head(5))

if __name__ == "__main__":
    main()
