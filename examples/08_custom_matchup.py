"""
08_custom_matchup.py

This script demonstrates how to manually build and execute a MatchupQuery.
This gives you more control than the high-level `matchup()` function.
"""

from datetime import date
from pypitch.api.session import get_executor, get_registry
from pypitch.query.defs import MatchupQuery

def main():
    reg = get_registry()
    exc = get_executor()
    
    # Resolve IDs
    batter_id = str(reg.resolve_player("MS Dhoni", date.today()))
    bowler_id = str(reg.resolve_player("SP Narine", date.today()))
    
    # Build Query Object
    query = MatchupQuery(
        snapshot_id="latest",
        batter_id=batter_id,
        bowler_id=bowler_id,
        venue_id=None # Global stats
    )
    
    print(f"Executing Custom Query: {query}")
    
    # Execute
    response = exc.execute(query)
    df = response.data.to_pandas()
    
    print(f"\nResult Rows: {len(df)}")
    if not df.empty:
        # MatchupQuery returns aggregated stats
        print(df[['runs', 'balls', 'wickets']].head())

if __name__ == "__main__":
    main()
