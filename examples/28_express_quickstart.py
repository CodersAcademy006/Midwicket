"""
28_express_quickstart.py — PyPitch Express API: One-liner Access

The express module mirrors Plotly Express: sensible defaults, zero boilerplate.
Data is downloaded automatically on first run (~50 MB IPL dataset).

Usage:
    python examples/28_express_quickstart.py
"""

import pypitch.express as px


def main() -> None:
    print("=" * 60)
    print("PyPitch Express — Quick Start Demo")
    print("=" * 60)

    # ------------------------------------------------------------------
    # 1. Player statistics
    # ------------------------------------------------------------------
    print("\n[1] Player Stats — Virat Kohli")
    stats = px.get_player_stats("V Kohli")
    if stats:
        print(f"  Matches      : {stats.matches}")
        print(f"  Runs         : {stats.runs}")
        print(f"  Strike Rate  : {stats.strike_rate:.1f}" if stats.strike_rate else "  Strike Rate  : N/A")
        print(f"  Batting Avg  : {stats.average:.1f}" if stats.average else "  Batting Avg  : N/A")
    else:
        print("  (player not found — run 01_setup_data.py first)")

    # ------------------------------------------------------------------
    # 2. Head-to-head matchup
    # ------------------------------------------------------------------
    print("\n[2] Matchup — Kohli vs Bumrah")
    matchup = px.get_matchup("V Kohli", "JJ Bumrah")
    if matchup is not None:
        print(f"  Result: {matchup}")
    else:
        print("  (no data — registry may be empty)")

    # ------------------------------------------------------------------
    # 3. Win probability
    # ------------------------------------------------------------------
    print("\n[3] Win Probability")
    print("  Scenario: chasing 180, score 95/3 after 10 overs at Wankhede")
    prob = px.predict_win(
        venue="Wankhede Stadium",
        target=180,
        current_score=95,
        wickets_down=3,
        overs_done=10.0,
    )
    print(f"  Chase win probability : {prob.get('win_prob', 'N/A')}")

    # ------------------------------------------------------------------
    # 4. Debug mode (eager execution for development)
    # ------------------------------------------------------------------
    print("\n[4] Enable debug mode")
    px.set_debug_mode(True)
    print("  Debug mode is ON — queries execute eagerly")
    px.set_debug_mode(False)

    print("\nDone.")


if __name__ == "__main__":
    main()
