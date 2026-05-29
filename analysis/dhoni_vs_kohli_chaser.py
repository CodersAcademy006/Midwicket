"""
analysis/dhoni_vs_kohli_chaser.py

Compares MS Dhoni and Virat Kohli as chasers using Win Probability Added (WPA).

Methodology:
For each innings in a chase, compute the team's win probability when the player
arrived at the crease vs. when they departed. The delta is their WPA.
A positive WPA means the team was more likely to win after their innings.

Usage:
    pip install midwicket
    python analysis/dhoni_vs_kohli_chaser.py

For ball-by-ball accuracy, run DataLoader().download() first and replace
SCENARIOS with real innings pulled from session.query().
"""

import midwicket.express as px


def win_prob_added(
    target: int,
    score_in: int, wkts_in: int, overs_in: float,
    score_out: int, wkts_out: int, overs_out: float,
    venue: str = "Neutral",
) -> float:
    """Win Probability Added across a single innings."""
    wp_before = px.predict_win(
        venue=venue, target=target,
        current_score=score_in, wickets_down=wkts_in, overs_done=overs_in,
    )["win_prob"]
    wp_after = px.predict_win(
        venue=venue, target=target,
        current_score=score_out, wickets_down=wkts_out, overs_done=overs_out,
    )["win_prob"]
    return wp_after - wp_before


# Representative chase scenarios.
# Format: (target, score_in, wkts_in, overs_in, score_out, wkts_out, overs_out)
# Replace with real innings data from DataLoader for accurate results.

SCENARIOS = {
    "MS Dhoni": [
        (165, 90,  4, 12.0, 165, 7, 20.0),
        (180, 100, 5, 13.0, 181, 8, 20.0),
        (175, 85,  3, 11.0, 176, 6, 19.4),
        (160, 70,  4, 10.0, 161, 7, 20.0),
        (190, 120, 4, 14.0, 191, 7, 20.0),
        (155, 80,  5, 12.0, 156, 8, 19.3),
        (170, 95,  3, 13.0, 171, 6, 20.0),
    ],
    "Virat Kohli": [
        (165, 10, 0, 2.0, 165, 4, 20.0),
        (180, 15, 1, 3.0, 181, 6, 20.0),
        (175,  0, 0, 0.0, 175, 5, 20.0),
        (160, 20, 1, 4.0, 160, 5, 20.0),
        (190, 10, 0, 2.0, 191, 7, 20.0),
        (155,  5, 0, 1.0, 156, 4, 19.4),
        (170,  8, 0, 2.0, 170, 6, 20.0),
    ],
}


def main():
    print("\nDhoni vs Kohli — Win Probability Added (WPA) in IPL Chases")
    print("=" * 60)
    print(f"{'Player':<20} {'Innings':>8} {'Total WPA':>10} {'Avg WPA':>10}")
    print("-" * 52)

    for player, innings in SCENARIOS.items():
        wpa_list = [win_prob_added(*inn) for inn in innings]
        total    = sum(wpa_list)
        avg      = total / len(wpa_list)
        print(f"{player:<20} {len(innings):>8} {total:>+9.1%} {avg:>+9.1%}")

    print()
    print("WPA = Win Probability Added per innings")
    print("Positive = chasing team improved while this player batted")
    print()
    print("Analysis powered by Midwicket")
    print("https://github.com/CodersAcademy006/Midwicket")


if __name__ == "__main__":
    main()
