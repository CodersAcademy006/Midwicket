"""
analysis/venue_bias.py

Computes win probability for a chasing team at match start (0/0 after 0 overs)
across all major IPL venues for a fixed target.

A neutral venue shows ~50%. Venues significantly below 50% have a structural
batting-first advantage in the historical data.

Usage:
    pip install midwicket
    python analysis/venue_bias.py
"""

import midwicket.express as px

VENUES = [
    "Wankhede Stadium",
    "M Chinnaswamy Stadium",
    "Eden Gardens",
    "Arun Jaitley Stadium",
    "MA Chidambaram Stadium",
    "Rajiv Gandhi International Cricket Stadium",
    "Punjab Cricket Association Stadium",
    "Sawai Mansingh Stadium",
    "DY Patil Stadium",
    "Brabourne Stadium",
]

TARGET = 175


def main():
    print(f"\nIPL Venue Bias Analysis — Chasing {TARGET}")
    print("=" * 72)
    print(f"{'Venue':<44} {'Chase WP at 0/0 0ov':>20} {'Verdict':>8}")
    print("-" * 72)

    results = []
    for venue in VENUES:
        wp = px.predict_win(
            venue=venue, target=TARGET,
            current_score=0, wickets_down=0, overs_done=0.0,
        )["win_prob"]
        verdict = "Bat First" if wp < 0.48 else ("Chase" if wp > 0.52 else "Neutral")
        results.append((venue, wp, verdict))

    for venue, wp, verdict in sorted(results, key=lambda x: x[1]):
        print(f"{venue:<44} {wp:>18.1%}  {verdict:>8}")

    print()
    print("Chase WP < 48%  = structural batting-first advantage")
    print("Chase WP > 52%  = structural chasing advantage")
    print("48-52%          = venue is neutral")
    print()
    print("Analysis powered by Midwicket")
    print("https://github.com/CodersAcademy006/Midwicket")


if __name__ == "__main__":
    main()
