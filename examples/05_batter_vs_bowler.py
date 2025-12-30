"""
05_batter_vs_bowler.py

This script demonstrates the high-level `matchup` API to analyze
Head-to-Head records between a batter and a bowler.
"""

from pypitch.api.stats import matchup

def main():
    batter = "V Kohli"
    bowler = "JJ Bumrah"
    
    print(f"Analyzing {batter} vs {bowler}...")
    
    try:
        df = matchup(batter, bowler)
        
        if df.empty:
            print("No matchup data found.")
        else:
            print(df)
            
            # You can calculate aggregate stats from the dataframe
            # Note: The default MatchupQuery returns aggregated stats directly
            total_runs = df['runs'].iloc[0]
            balls_faced = df['balls'].iloc[0]
            outs = df['wickets'].iloc[0]
            
            print("\nSummary:")
            print(f"Runs: {total_runs}")
            print(f"Balls: {balls_faced}")
            print(f"Outs: {outs}")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
