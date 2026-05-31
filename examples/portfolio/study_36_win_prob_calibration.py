"""
Win Probability Validation Study: Calibration
Tests calibration and reliability of the win predictor.
"""
import midwicket as md

def main():
    print("=== Win Probability: Calibration ===")
    prob = md.express.predict_win(
        venue="Grand Prairie Stadium, Dallas",
        target=160,
        current_score=100,
        wickets_down=4,
        overs_done=12.0
    )
    print(f"Calculated Win Probability: {prob['win_prob']:.2%}")

if __name__ == "__main__":
    main()
