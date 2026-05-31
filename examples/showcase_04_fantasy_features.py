"""
Showcase 4: Fantasy Feature Engineering
Compiles bowling quality, recent form, and pressure resistance into a single training dataframe.
"""
import midwicket as md
from midwicket.features import load_features
import pandas as pd

def main():
    print("=== Fantasy Feature Store Compiler ===")
    session = md.datasets.load_dataset("mlc")
    
    df_bqr = load_features("bowler_quality_rating", session)
    df_form = load_features("batting_form", session)
    
    print("Bowler Quality Features:")
    print(df_bqr.head(5))
    print("\nBatting Form Features:")
    print(df_form.head(5))

if __name__ == "__main__":
    main()
