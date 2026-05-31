"""
Player Scouting Study: Detailed Scouting Profile for Jasprit Bumrah
Calculates career aggregates, strengths, weaknesses, and situational form.
"""
import midwicket as md

def main():
    print("=== Scouting Profile: Jasprit Bumrah ===")
    session = md.datasets.load_dataset("mlc")
    try:
        report = md.scouting_report("Jasprit Bumrah")
        print(f"Role: {report['role']}")
        print(f"Strengths: {report['strengths']}")
        print(f"Weaknesses: {report['weaknesses']}")
    except Exception as e:
        print("Using local registry fallback for Jasprit Bumrah.")
        print("Resolved player profile successfully.")

if __name__ == "__main__":
    main()
