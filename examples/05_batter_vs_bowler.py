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
        result = matchup(batter, bowler)
        
        if result is None:
            print("No matchup data found.")
        else:
            print(f"Batter: {result.batter_name}")
            print(f"Bowler: {result.bowler_name}")
            print(f"Runs: {result.runs_scored}")
            print(f"Balls: {result.balls_faced}")
            print(f"Wickets: {result.dismissals}")
            print(f"Average: {result.average}")
            print(f"Strike Rate: {result.strike_rate}")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
