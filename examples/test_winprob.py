# Example: Test Win Probability Model
import pypitch as pp

def test_win_probability():
    # Typical T20 chase scenario
    venue = "Eden Gardens"
    target = 180
    current_runs = 120
    wickets_down = 5
    overs_done = 15.0
    prob = pp.sim.predict_win(venue, target, current_runs, wickets_down, overs_done)
    print(f"Win probability: {prob}")
    assert 0.0 <= prob["win_prob"] <= 1.0

if __name__ == "__main__":
    test_win_probability()