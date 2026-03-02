"""
Data pipeline utilities for building registry and summary statistics.

Implements the ETL pass that seeds the IdentityRegistry (Agent 4) and
pre-computes derived tables consumed by the Archivist (Agent 3).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Dict


def build_registry_stats(loader: Any, registry: Any) -> None:
    """
    Build registry and summary statistics from raw match data.

    Iterates every raw match JSON through the DataLoader, registers
    players/teams/venues with the IdentityRegistry (auto-creating IDs if
    absent), accumulates per-player and per-venue aggregates, then writes
    them back via the registry's bulk-upsert helpers.

    Args:
        loader:   DataLoader instance used to fetch raw match files.
        registry: IdentityRegistry instance to populate with entity IDs.
    """
    # Accumulators keyed by entity_id
    player_stats: Dict[int, Dict[str, int]] = defaultdict(
        lambda: dict(matches=0, runs=0, balls_faced=0, wickets=0, balls_bowled=0, runs_conceded=0)
    )
    venue_stats: Dict[int, Dict[str, int]] = defaultdict(
        lambda: dict(matches=0, total_runs=0, first_innings_runs=0, first_innings_count=0)
    )

    for match in loader.iter_matches():
        info = match.get("info", {})
        innings_list = match.get("innings", [])

        # --- Resolve date ---
        raw_dates = info.get("dates", [])
        match_date: date = date.fromisoformat(raw_dates[0]) if raw_dates else date.today()

        # --- Register venue ---
        venue_name: str = info.get("venue", "Unknown Venue")
        venue_id = registry.resolve_venue(venue_name, match_date=match_date, auto_ingest=True)

        # --- Register players and track match participation ---
        players_in_match: Dict[str, int] = {}
        for team_players in info.get("players", {}).values():
            for pname in team_players:
                pid = registry.resolve_player(pname, match_date=match_date, auto_ingest=True)
                players_in_match[pname] = pid
                player_stats[pid]["matches"] += 1

        # --- Process ball-by-ball data ---
        match_total_runs = 0
        for innings_idx, innings in enumerate(innings_list):
            innings_runs = 0
            for over in innings.get("overs", []):
                for delivery in over.get("deliveries", []):
                    extras = delivery.get("extras", {})
                    is_wide = "wides" in extras
                    is_noball = "noballs" in extras

                    batter_name: str = delivery.get("batter", "")
                    bowler_name: str = delivery.get("bowler", "")

                    runs_batter: int = delivery.get("runs", {}).get("batter", 0)
                    runs_total: int = delivery.get("runs", {}).get("total", 0)

                    innings_runs += runs_total

                    # Batter stats — wides don't count as a legal ball faced
                    if batter_name and batter_name in players_in_match:
                        bid = players_in_match[batter_name]
                        player_stats[bid]["runs"] += runs_batter
                        if not is_wide:
                            player_stats[bid]["balls_faced"] += 1

                    # Bowler stats — wides and no-balls are not legal deliveries
                    if bowler_name and bowler_name in players_in_match:
                        oid = players_in_match[bowler_name]
                        player_stats[oid]["runs_conceded"] += runs_total
                        if not is_wide and not is_noball:
                            player_stats[oid]["balls_bowled"] += 1

                    # Wicket attribution
                    for wicket in delivery.get("wickets", []):
                        kind = wicket.get("kind", "")
                        # Run-outs and obstructions aren't credited to the bowler
                        if kind not in ("run out", "obstructing the field", "retired hurt", "retired out"):
                            if bowler_name and bowler_name in players_in_match:
                                oid = players_in_match[bowler_name]
                                player_stats[oid]["wickets"] += 1

            match_total_runs += innings_runs
            if innings_idx == 0:
                venue_stats[venue_id]["first_innings_runs"] += innings_runs
                venue_stats[venue_id]["first_innings_count"] += 1

        # --- Venue aggregate ---
        venue_stats[venue_id]["matches"] += 1
        venue_stats[venue_id]["total_runs"] += match_total_runs

    # --- Persist to registry ---
    registry.upsert_player_stats(dict(player_stats))
    registry.upsert_venue_stats(dict(venue_stats))
