"""
09_fantasy_points.py

This script demonstrates how to use the FantasyQuery directly to get player performance data.
"""

from datetime import date
from pypitch.api.session import get_executor, get_registry
from pypitch.query.defs import FantasyQuery

def main():
    reg = get_registry()
    exc = get_executor()
    
    venue_name = "MA Chidambaram Stadium"
    try:
        venue_id = reg.resolve_venue(venue_name, date.today())
    except ValueError:
        print(f"Venue {venue_name} not found.")
        return

    # Query for all players at this venue
    query = FantasyQuery(
        venue_id=venue_id,
        roles=["all"],
        min_matches=2,
        snapshot_id="latest"
    )
    
    print(f"Fetching fantasy stats for {venue_name}...")
    response = exc.execute(query)
    df = response.data.to_pandas()
    
    print(df.head())

if __name__ == "__main__":
    main()
