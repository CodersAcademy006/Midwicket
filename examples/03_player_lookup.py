"""
03_player_lookup.py

This script demonstrates how to use the IdentityRegistry to resolve player names to IDs.
PyPitch handles aliases (e.g., "V Kohli", "Virat Kohli") and time-travel consistency.
"""

from datetime import date
from pypitch.api.session import get_registry

def main():
    registry = get_registry()
    
    # Resolve a player
    name = "V Kohli"
    today = date.today()
    
    try:
        player_id = registry.resolve_player(name, match_date=today)
        print(f"Player: {name}")
        print(f"ID: {player_id}")
        
        # Try an alias
        alias = "Virat Kohli"
        try:
            alias_id = registry.resolve_player(alias, match_date=today)
            print(f"Alias: {alias} -> ID: {alias_id}")
            assert player_id == alias_id
            print("Identity resolution confirmed: Both names map to the same ID.")
        except ValueError:
            print(f"Alias '{alias}' not found. This is expected if aliases haven't been manually added.")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure you have run 01_setup_data.py to populate the registry.")

if __name__ == "__main__":
    main()
