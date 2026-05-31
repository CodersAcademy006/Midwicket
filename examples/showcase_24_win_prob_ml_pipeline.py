"""
Showcase 24: Win Probability Predictor Calibration Test
Simulates win probability curves on final match states.
"""
import midwicket as md

def main():
    print("=== Predictor Curve Simulation ===")
    session = md.datasets.load_dataset("mlc")
    # Verify model is ready
    prob = md.express.predict_win(
        venue="Grand Prairie Stadium, Dallas",
        target=160,
        current_score=100,
        wickets_down=4,
        overs_done=12.0
    )
    print(f"Simulated Win Probability: {prob['win_prob']:.2%}")

if __name__ == "__main__":
    main()
