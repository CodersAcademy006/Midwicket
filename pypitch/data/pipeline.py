"""
Data pipeline: ETL pass that seeds the IdentityRegistry and
pre-computes derived summary tables from raw Cricsheet JSON files.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger(__name__)


def build_registry_stats(loader: Any, registry: Any) -> None:
    """
    Iterate every raw match file, resolve entity identities with
    auto-ingest, aggregate career statistics, then persist via the
    registry bulk-upsert helpers.

    Args:
        loader:   DataLoader — provides iter_matches().
        registry: IdentityRegistry — target for entity + stats writes.
    """
    # Accumulators keyed by entity_id
    player_stats: Dict[int, Dict[str, int]] = defaultdict(lambda: {
        "matches": 0, "runs": 0, "balls_faced": 0,
        "wickets": 0, "balls_bowled": 0, "runs_conceded": 0,
    })
    venue_stats: Dict[int, Dict[str, int]] = defaultdict(lambda: {
        "matches": 0, "total_runs": 0,
        "first_innings_runs": 0, "first_innings_count": 0,
    })

    match_count = 0

    for match_data in loader.iter_matches():
        match_count += 1
        info = match_data.get("info", {})

        # --- parse match date ---
        date_str = (info.get("dates") or ["1970-01-01"])[0]
        try:
            match_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            match_date = datetime(1970, 1, 1).date()

        # --- resolve venue ---
        venue_name = info.get("venue", "Unknown Venue")
        venue_id = registry.resolve_venue(venue_name, match_date, auto_ingest=True)

        venue_stats[venue_id]["matches"] += 1

        # Track players seen in this match to count match appearances once
        batters_this_match: set = set()
        bowlers_this_match: set = set()

        for inning_idx, inning in enumerate(match_data.get("innings", [])):
            inning_runs = 0

            for over_data in inning.get("overs", []):
                for delivery in over_data.get("deliveries", []):
                    # --- resolve players ---
                    b_id = registry.resolve_player(
                        delivery["batter"], match_date, auto_ingest=True
                    )
                    bo_id = registry.resolve_player(
                        delivery["bowler"], match_date, auto_ingest=True
                    )

                    runs_data = delivery.get("runs", {})
                    extras_data = delivery.get("extras", {})
                    batter_runs = runs_data.get("batter", 0)
                    extras = runs_data.get("extras", 0)

                    # --- batter stats ---
                    batters_this_match.add(b_id)
                    is_wide = "wides" in extras_data
                    if not is_wide:
                        player_stats[b_id]["balls_faced"] += 1
                    player_stats[b_id]["runs"] += batter_runs

                    # --- bowler stats ---
                    bowlers_this_match.add(bo_id)
                    if not is_wide:
                        player_stats[bo_id]["balls_bowled"] += 1
                    player_stats[bo_id]["runs_conceded"] += batter_runs + extras

                    # --- wickets ---
                    for wicket in delivery.get("wickets", []):
                        kind = wicket.get("kind", "")
                        # run out is not credited to the bowler
                        if kind not in ("run out", "obstructing the field"):
                            player_stats[bo_id]["wickets"] += 1

                    inning_runs += batter_runs + extras

            # --- venue first-innings totals ---
            if inning_idx == 0:
                venue_stats[venue_id]["first_innings_runs"] += inning_runs
                venue_stats[venue_id]["first_innings_count"] += 1
            venue_stats[venue_id]["total_runs"] += inning_runs

        # Increment match counter once per player/match
        for pid in batters_this_match | bowlers_this_match:
            player_stats[pid]["matches"] += 1

    logger.info(
        "pipeline: processed %d matches, %d players, %d venues",
        match_count, len(player_stats), len(venue_stats),
    )

    registry.upsert_player_stats(dict(player_stats))
    registry.upsert_venue_stats(dict(venue_stats))
