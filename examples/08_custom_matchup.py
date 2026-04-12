"""
08_custom_matchup.py

Demonstrates how to manually build and execute a MatchupQuery.
This gives more control than the high-level matchup() function.

Prerequisites: run 03_ingest_world.py first.
"""

from datetime import date
from pypitch.api.session import get_executor, get_registry
from pypitch.query.defs import MatchupQuery


def main():
    reg = get_registry()
    exc = get_executor()

    # Resolve IDs
    try:
        batter_id = str(reg.resolve_player("MS Dhoni", date.today()))
        bowler_id = str(reg.resolve_player("SP Narine", date.today()))
    except Exception as e:
        print(f"Player not found: {e}")
        print("Run 03_ingest_world.py first to populate the registry.")
        return

    # Build Query Object
    query = MatchupQuery(
        snapshot_id="latest",
        batter_id=batter_id,
        bowler_id=bowler_id,
        venue_id=None,  # Global stats
    )

    print(f"Executing Custom Query: {query}")

    try:
        response = exc.execute(query)
        data = response.data
        # data may be a dict (from registry matchup_stats) or an Arrow table
        if isinstance(data, dict):
            print(f"\nMatchup stats:")
            for k, v in data.items():
                print(f"  {k}: {v}")
        elif hasattr(data, "to_pandas"):
            df = data.to_pandas()
            print(f"\nResult Rows: {len(df)}")
            if not df.empty:
                print(df[["runs", "balls", "wickets"]].head())
    except Exception as e:
        print(f"Query execution failed: {e}")


if __name__ == "__main__":
    main()
