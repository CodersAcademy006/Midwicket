"""
04_venue_lookup.py

This script demonstrates how to resolve venue names to IDs.
"""

from datetime import date
from pypitch.api.session import get_registry

def main():
    registry = get_registry()
    
    venue_name = "M Chinnaswamy Stadium"
    today = date.today()
    
    try:
        venue_id = registry.resolve_venue(venue_name, match_date=today)
        print(f"Venue: {venue_name}")
        print(f"ID: {venue_id}")
        
    except ValueError:
        print(f"Venue '{venue_name}' not found in registry.")
        print("Try running 01_setup_data.py first.")

if __name__ == "__main__":
    main()
