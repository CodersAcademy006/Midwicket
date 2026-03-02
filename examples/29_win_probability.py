"""
29_win_probability.py — Win Probability Across Match Scenarios

Demonstrates the win_probability compute function directly,
without needing a live database session.

Usage:
    python examples/29_win_probability.py
"""

from pypitch.compute.winprob import win_probability


def print_scenario(label: str, **kwargs) -> None:
    result = win_probability(**kwargs)
    win_pct = result.get("win_prob", 0) * 100 if isinstance(result, dict) else 0
    print(f"  {label:<45}  {win_pct:>6.1f}%")


def main() -> None:
    print("=" * 65)
    print("Win Probability — Scenario Analysis")
    print("=" * 65)
    print(f"  {'Scenario':<45}  {'Chase Win %':>10}")
    print("-" * 65)

    # Comfortable chase
    print_scenario(
        "180 target | 95/3 after 10 overs (WR=8.5)",
        target=180, current_runs=95, wickets_down=3,
        overs_done=10.0, venue=None,
    )

    # Tight chase
    print_scenario(
        "180 target | 120/5 after 15 overs (WR=12.0)",
        target=180, current_runs=120, wickets_down=5,
        overs_done=15.0, venue=None,
    )

    # Near impossible
    print_scenario(
        "180 target | 50/7 after 15 overs (WR=26.0)",
        target=180, current_runs=50, wickets_down=7,
        overs_done=15.0, venue=None,
    )

    # Match won
    print_scenario(
        "180 target | 181/4 — chase complete",
        target=180, current_runs=181, wickets_down=4,
        overs_done=19.3, venue=None,
    )

    # Early overs — high uncertainty
    print_scenario(
        "180 target | 20/0 after 3 overs",
        target=180, current_runs=20, wickets_down=0,
        overs_done=3.0, venue=None,
    )

    print("-" * 65)
    print("\nNote: probabilities are from the built-in Duckworth-Lewis-style")
    print("model. Wire in a trained ML model via pp.set_win_model() for")
    print("venue-adjusted predictions.")


if __name__ == "__main__":
    main()
