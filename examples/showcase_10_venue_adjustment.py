"""
Showcase 10: Venue-Adjusted Player Ratings
Evaluates batter averages normalized by the average scoring rates at the stadiums played in.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Venue-Adjusted Form Ratings ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("venue_adjusted_form", session)
    print(df.sort_values(by="venue_adjusted_rating", ascending=False).head(10))

if __name__ == "__main__":
    main()
