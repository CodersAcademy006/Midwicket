"""
01_setup_data.py

This script is now OPTIONAL.
PyPitch automatically sets up the database on first use.

However, you can run this script to force a re-download and re-ingestion
if your data is corrupted or you want to update to the latest dataset.
"""

import sys
import os
# Add project root to path so we can import pypitch without installing it
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pypitch.api.session import PyPitchSession

def main():
    print("Forcing PyPitch Setup...")
    
    # Initialize session (this will trigger auto-setup if DB is missing)
    session = PyPitchSession.get()
    
    # If DB already exists, we can force a re-run manually
    print("Re-running setup logic...")
    session._setup_db()

if __name__ == "__main__":
    main()
