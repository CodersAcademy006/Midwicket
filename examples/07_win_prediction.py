"""
07_win_prediction.py

This script demonstrates the Win Probability API.
Note: In early versions of PyPitch, this might return a placeholder or raise NotImplementedError
if the simulation model is not yet fully trained.
"""

from pypitch.api.sim import predict_win

def main():
    venue = "Eden Gardens"
    target = 180
    current_runs = 150
    wickets_down = 3
    overs_done = 16.0
    
    print(f"Scenario: Chasing {target} at {venue}")
    print(f"Score: {current_runs}/{wickets_down} in {overs_done} overs")
    
    try:
        result = predict_win(venue, target, current_runs, wickets_down, overs_done)
        print("\nPrediction:")
        print(f"Win Probability: {result.get('win_prob', 0.0) * 100:.1f}%")
        
    except NotImplementedError:
        print("\nNote: Win Prediction model is not yet connected to the SQL engine.")
        print("This feature requires the Simulation Agent (coming in v0.2).")
    except Exception as e:
        print(f"Prediction failed: {e}")

if __name__ == "__main__":
    main()
