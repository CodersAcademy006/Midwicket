"""
Fantasy Optimization Study: Differential Models
Leverages Bowler Quality Rating and batting form to optimize lineups.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Fantasy Model: Differential ===")
    session = md.datasets.load_dataset("mlc")
    df_bqr = load_features("bowler_quality_rating", session)
    df_form = load_features("batting_form", session)
    print(df_bqr.head(3))
    print(df_form.head(3))

if __name__ == "__main__":
    main()
