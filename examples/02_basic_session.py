"""
02_basic_session.py

This script demonstrates how to access the PyPitch session singleton.
The session manages the database connection, identity registry, and cache.
"""

from pypitch.api.session import PyPitchSession

def main():
    # Get the singleton instance
    session = PyPitchSession.get()
    
    print(f"Session initialized.")
    print(f"Data Directory: {session.data_dir}")
    print(f"DB Path: {session.db_path}")
    print(f"Registry Path: {session.registry_path}")
    
    # Check if engine is ready
    print(f"Current Snapshot ID: {session.engine.snapshot_id}")

if __name__ == "__main__":
    main()
