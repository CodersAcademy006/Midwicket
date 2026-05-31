"""
Showcase 22: Expected Fantasy Point Yields
Projecting fantasy yields based on bowling quality and batting form.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Expected Fantasy Point Projection ===")
    session = md.datasets.load_dataset("mlc")
    df_bqr = load_features("bowler_quality_rating", session)
    print(df_bqr.head(10))

if __name__ == "__main__":
    main()
