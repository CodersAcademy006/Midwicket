"""
analysis/critical_over.py

For each over in a T20 innings, computes how much win probability drops
if the chasing team suffers a 2-wicket collapse for 2 runs in that over.

The over with the highest WP swing is where matches are most commonly decided.
Challenges the conventional wisdom that death overs (17-20) are always decisive.

Usage:
    pip install midwicket
    python analysis/critical_over.py
"""

import midwicket.express as px

# Average IPL chase trajectory (approximated from historical data)
# Format: (over, cumulative_score, cumulative_wickets)
OVER_STATES = [
    (1,   8, 0), (2,  17, 0), (3,  28, 0), (4,  38, 0), (5,  48, 0), (6,  58, 1),
    (7,  67, 1), (8,  77, 2), (9,  86, 2), (10, 95, 2), (11, 104, 3), (12, 112, 3),
    (13, 120, 3), (14, 128, 4), (15, 137, 4), (16, 146, 4), (17, 154, 5),
    (18, 161, 5), (19, 166, 6),
]

TARGET = 170


def main():
    print(f"\nCritical Over Analysis — Chasing {TARGET} in IPL")
    print("WP Swing = drop if 2 wickets fall for 2 runs in that over")
    print("=" * 72)
    print(f"{'Over':>5}  {'Normal WP':>10}  {'After Collapse':>15}  {'WP Swing':>10}  Bar")
    print("-" * 72)

    swings = []
    for over, score, wkts in OVER_STATES:
        normal_wp = px.predict_win(
            venue="Neutral", target=TARGET,
            current_score=score, wickets_down=wkts, overs_done=float(over),
        )["win_prob"]
        collapse_wp = px.predict_win(
            venue="Neutral", target=TARGET,
            current_score=score + 2, wickets_down=min(wkts + 2, 10),
            overs_done=float(over + 1),
        )["win_prob"]
        swing = abs(collapse_wp - normal_wp)
        swings.append((over, normal_wp, collapse_wp, swing))
        bar = "|" * int(swing * 50)
        print(f"{over:>5}  {normal_wp:>9.1%}  {collapse_wp:>14.1%}  {swing:>9.1%}  {bar}")

    most_critical = max(swings, key=lambda x: x[3])
    print()
    print(f"Most decisive over: Over {most_critical[0]}")
    print(f"  Normal WP     : {most_critical[1]:.1%}")
    print(f"  After collapse: {most_critical[2]:.1%}")
    print(f"  WP swing      : {most_critical[3]:.1%}")
    print()
    print("Analysis powered by Midwicket")
    print("https://github.com/CodersAcademy006/Midwicket")


if __name__ == "__main__":
    main()
