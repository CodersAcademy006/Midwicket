"""
02_basic_session.py

This script demonstrates how to access the Midwicket session singleton.
The session manages the database connection, identity registry, and cache.
"""

from midwicket.api.session import MidwicketSession

def main():
    # Get the singleton instance
    session = MidwicketSession.get()
    
    print(f"Session initialized.")
    print(f"Data Directory: {session.data_dir}")
    print(f"DB Path: {session.db_path}")
    print(f"Registry Path: {session.registry_path}")
    
    # Check if engine is ready
    print(f"Current Snapshot ID: {session.engine.snapshot_id}")

if __name__ == "__main__":
    main()
