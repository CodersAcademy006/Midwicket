"""
33_session_and_registry.py — Session, Registry & Identity Resolution

Shows how to:
  • Initialize a PyPitchSession manually
  • Register players, teams, and venues in the IdentityRegistry
  • Resolve names ↔ integer IDs across match dates
  • Query player and venue stats

Run after 01_setup_data.py if you want real data; this script seeds
a tiny in-memory dataset of its own so it works standalone.

Usage:
    python examples/33_session_and_registry.py
"""

import sys
import duckdb
from datetime import date

# Ensure UTF-8 stdout on Windows (CP1252 crashes on non-ASCII output)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

from pypitch.storage.registry import IdentityRegistry


def main() -> None:
    print("PyPitch Session & Registry Demo")
    print("=" * 50)

    # Use an in-memory registry — no files needed
    reg = IdentityRegistry(db_path=":memory:")

    # ------------------------------------------------------------------
    # 1. Register entities (auto-ingest = True adds unknown names)
    # ------------------------------------------------------------------
    print("\n[1] Registering entities…")
    match_date = date(2023, 5, 1)

    players = [
        ("V Kohli",   "player"),
        ("JJ Bumrah", "player"),
        ("MS Dhoni",  "player"),
        ("R Sharma",  "player"),
    ]
    venues = [
        ("Wankhede Stadium", "venue"),
        ("Eden Gardens",     "venue"),
        ("Chepauk",          "venue"),
    ]
    teams = [
        ("Mumbai Indians",     "team"),
        ("Chennai Super Kings","team"),
    ]

    for name, entity_type in players + venues + teams:
        if entity_type == "player":
            eid = reg.resolve_player(name, match_date, auto_ingest=True)
        elif entity_type == "venue":
            eid = reg.resolve_venue(name, match_date, auto_ingest=True)
        else:
            eid = reg.resolve_team(name, match_date, auto_ingest=True)
        print(f"  {entity_type:<8} '{name}' → ID {eid}")

    # ------------------------------------------------------------------
    # 2. Round-trip: resolve by name, get stats
    # ------------------------------------------------------------------
    print("\n[2] Round-trip name → ID resolution")
    kohli_id = reg.resolve_player("V Kohli", match_date)
    print(f"  'V Kohli' resolves to entity ID: {kohli_id}")

    # Seed some stats manually
    reg.upsert_player_stats({
        kohli_id: {
            "matches": 237,
            "runs": 7263,
            "balls_faced": 5268,
            "wickets": 4,
            "balls_bowled": 156,
            "runs_conceded": 231,
        }
    })

    stats = reg.get_player_stats(kohli_id)
    print(f"  Stats for ID {kohli_id}: {stats}")

    # ------------------------------------------------------------------
    # 3. Venue stats
    # ------------------------------------------------------------------
    print("\n[3] Venue stats")
    wankhede_id = reg.resolve_venue("Wankhede Stadium", match_date)
    reg.upsert_venue_stats({
        wankhede_id: {
            "matches": 98,
            "total_runs": 168320,
            "first_innings_runs": 84160,
            "first_innings_count": 98,
        }
    })

    vstats = reg.get_venue_stats(wankhede_id)
    print(f"  Wankhede (ID {wankhede_id}): {vstats}")

    # ------------------------------------------------------------------
    # 4. Direct DuckDB query on registry
    # ------------------------------------------------------------------
    print("\n[4] Raw SQL against registry DB")
    rows = reg.con.execute(
        "SELECT id, type, primary_name FROM entities ORDER BY type, primary_name"
    ).fetchall()
    print(f"  {'ID':>4}  {'Type':<8}  Name")
    print("  " + "-" * 40)
    for row in rows:
        print(f"  {row[0]:>4}  {row[1]:<8}  {row[2]}")

    reg.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
