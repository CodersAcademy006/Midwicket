"""
03_ingest_world.py

Downloads IPL data from Cricsheet and populates the registry.
Run this once before using examples that query players or venues.
"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pypitch as pp
from pypitch.data.loader import DataLoader
from pypitch.data.pipeline import build_registry_stats


def main():
    print("Starting data ingestion...")

    data_dir = "./data"

    # 1. Download IPL JSON files (~50 MB) from Cricsheet
    loader = DataLoader(data_dir)
    try:
        loader.download()
        print("Download complete.")
    except Exception as e:
        print(f"Download failed: {e}")
        return

    # 2. Initialize session
    session = pp.init(source=data_dir)

    # 3. Populate registry (player/venue identity resolution)
    print("Building registry from raw match data...")
    try:
        build_registry_stats(session.loader, session.registry)
        print("Registry built successfully.")
    except Exception as e:
        print(f"Registry build failed: {e}")
        return

    # 4. Verify
    print("\nVerifying Registry...")
    from datetime import date
    for name in ["V Kohli", "JJ Bumrah", "Wankhede Stadium"]:
        try:
            eid = session.registry.resolve_player(name, date.today())
            print(f"  Player '{name}' -> ID {eid}")
        except Exception:
            try:
                eid = session.registry.resolve_venue(name, date.today())
                print(f"  Venue  '{name}' -> ID {eid}")
            except Exception as e:
                print(f"  Not found: {name} ({e})")


if __name__ == "__main__":
    main()
