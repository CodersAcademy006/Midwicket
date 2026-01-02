"""
06_venue_stats.py

This script demonstrates how to generate a Fantasy Cheat Sheet for a specific venue.
It shows average scores and top performing players.
"""

from pypitch.api.fantasy import cheat_sheet

def main():
    venue = "Wankhede Stadium"
    
    print(f"Generating Cheat Sheet for {venue}...")
    
    try:
        df = cheat_sheet(venue, last_n_years=3)
        
        if df.empty:
            print("No data available for this venue.")
        else:
            print("\nTop Fantasy Picks:")
            print(df.head(10))
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
