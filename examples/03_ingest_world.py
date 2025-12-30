"""
Example 03: The Universal Ingestion
Downloads IPL + International data and initializes the engine.
"""
import sys
import os

# Add project root to path so we can import pypitch without installing it
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pypitch as pp

def main():
    print("🌍 Starting Global Ingestion...")

    # 1. Download IPL and Test Matches
    # Note: This might take a while depending on your internet speed
    try:
        pp.data.download("ipl")
        pp.data.download("tests")
    except Exception as e:
        print(f"Download failed: {e}")
        return

    # 2. Init Session (Connects to Registry)
    pp.init(source="./data")
    session = pp.session.PyPitchSession.get()

    # 3. Populate Registry
    # This teaches your engine who "Don Bradman" and "Travis Head" are
    pp.data.ingest_registry("./data/raw", session.registry)

    # 4. Verify
    print("\n🔍 Verifying Registry...")
    try:
        # Check IPL Player
        p1 = session.registry.resolve_player("V Kohli", match_date=None)
        print(f"   Found: V Kohli (ID: {p1})")

        # Check Test Player (Modern Era)
        # Note: Cricsheet ball-by-ball data typically starts from ~2004
        p2 = session.registry.resolve_player("JL Langer", match_date=None)
        print(f"   Found: JL Langer (ID: {p2})")
        
    except Exception as e:
        print(f"❌ Verification Failed: {e}")

if __name__ == "__main__":
    main()
