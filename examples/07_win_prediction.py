"""
07_win_prediction.py

Demonstrates the Win Probability API using the built-in
Duckworth-Lewis-style model (no data download required).

For venue-adjusted predictions, wire in a trained ML model
via pp.set_win_model() — see 29_win_probability.py for details.
"""

import pypitch.express as px


def main():
    scenarios = [
        dict(venue="Eden Gardens",  target=180, current_score=150, wickets_down=3, overs_done=16.0),
        dict(venue="Wankhede Stadium", target=165, current_score=80, wickets_down=5, overs_done=12.0),
        dict(venue="Chinnaswamy",   target=200, current_score=50, wickets_down=7, overs_done=10.0),
    ]

    print(f"{'Scenario':<45}  {'Win %':>6}  {'Conf':>6}")
    print("-" * 62)
    for s in scenarios:
        label = f"{s['target']} target | {s['current_score']}/{s['wickets_down']} in {s['overs_done']} ov"
        try:
            result = px.predict_win(**s)
            wp = result.get("win_prob", 0.0)
            cf = result.get("confidence", 0.0)
            print(f"{label:<45}  {wp*100:>5.1f}%  {cf*100:>5.1f}%")
        except Exception as e:
            print(f"{label:<45}  ERROR: {e}")


if __name__ == "__main__":
    main()
