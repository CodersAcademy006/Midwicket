"""
Showcase 7: Automated Player Scouting Profiles
Compiles full tactical scout profile for Quinton de Kock inside MLC.
"""
import midwicket as md

def main():
    print("=== Automated Scout Profile ===")
    session = md.datasets.load_dataset("mlc")
    try:
        report = md.scouting_report("q de kock")
        print(f"Player: {report['player']}")
        print(f"Role: {report['role']}")
        print(f"Strengths: {report['strengths']}")
        print(f"Vulnerabilities: {report['weaknesses']}")
    except Exception as e:
        print(f"Failed scouting: {e}")

if __name__ == "__main__":
    main()
