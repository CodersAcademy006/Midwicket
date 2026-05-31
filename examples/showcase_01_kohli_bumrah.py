"""
Showcase 1: Virat Kohli vs Jasprit Bumrah Matchup Analysis
Determines head-to-head matchup statistics using Midwicket's matchup engine.
"""
import midwicket as md

def main():
    print("=== Virat Kohli vs Jasprit Bumrah Head-to-Head ===")
    session = md.datasets.load_dataset("mlc") # test session
    try:
        report = md.scouting_report("Virat Kohli")
        print(f"Role: {report['role']}")
        print(f"Strengths: {report['strengths']}")
    except Exception as e:
        print(f"No local data for Kohli in MLC dataset. Loading custom head-to-head mapping...")
        # Simulating matchup profile
        matchup = md.head_to_head("V Kohli", "JJ Bumrah")
        print(f"Matchup parsed successfully: {matchup}")

if __name__ == "__main__":
    main()
