"""
Pressure Index Validation Study: Rpo Pressure
Analyzes runs and wickets under extreme pressure indexes.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Pressure Index: Rpo Pressure ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("pressure_index", session)
    print(df.head(5))

if __name__ == "__main__":
    main()
