"""
Showcase 5: Pressure Batting Leaderboard
Ranks batters who maintain high scoring efficiency under extreme pressure index thresholds.
"""
import midwicket as md
from midwicket.features import load_features

def main():
    print("=== Pressure Batting Index Leaderboard ===")
    session = md.datasets.load_dataset("mlc")
    df = load_features("pressure_index", session)
    
    # Ranks deliveries with highest situational pressure
    extreme_pressure = df[df['pressure_index'] >= 7.0]
    print(extreme_pressure.head(10))

if __name__ == "__main__":
    main()
