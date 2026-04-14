"""
Fantasy analytics for PyPitch.

cheat_sheet — top players at a venue by fantasy value.
venue_bias  — win% batting first vs chasing at a venue.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_SORT_CANDIDATES = ("avg_points", "fantasy_points_avg", "runs", "strike_rate")


def cheat_sheet(venue: str, last_n_years: int = 3) -> pd.DataFrame:
    """
    Fantasy cheat sheet for a venue.

    Tries the materialized fantasy_points_avg table first; falls back to
    aggregating raw ball_events when that table is not loaded.

    Returns a DataFrame sorted by best available fantasy-value column.
    At minimum returns career batting SR and runs at the venue.
    """
    from pypitch.api.session import get_executor, get_registry, get_session
    from pypitch.query.defs import FantasyQuery

    reg = get_registry()
    exc = get_executor()

    v_id = reg.resolve_venue(venue, date.today())

    q = FantasyQuery(
        venue_id=v_id,
        roles=["all"],
        min_matches=5,
        snapshot_id="latest",
    )

    df: Optional[pd.DataFrame] = None
    try:
        response = exc.execute(q)
        df = response.data.to_pandas()
    except (RuntimeError, AttributeError, TypeError, ValueError) as e:
        logger.debug("cheat_sheet: executor failed for venue=%r: %s — falling back", venue, e)

    # Fallback: aggregate from ball_events directly
    if df is None or df.empty:
        try:
            session = get_session()
            result = session.engine.execute_sql(
                """  -- nosec B608
                SELECT
                    batter AS player,
                    COUNT(DISTINCT match_id)                            AS matches,
                    COUNT(*)                                            AS balls,
                    SUM(runs_batter)                                    AS runs,
                    ROUND(SUM(runs_batter) * 100.0 / NULLIF(COUNT(*), 0), 2) AS strike_rate,
                    SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)   AS fours,
                    SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)   AS sixes
                FROM ball_events
                WHERE venue = ? AND batter != ''
                GROUP BY batter
                HAVING COUNT(DISTINCT match_id) >= 2
                ORDER BY runs DESC
                LIMIT 20
                """,
                params=[venue],
            )
            df = result.to_pandas()
        except (RuntimeError, AttributeError, TypeError, ValueError) as e:
            logger.warning("cheat_sheet: ball_events fallback also failed for venue=%r: %s", venue, e, exc_info=True)
            return pd.DataFrame()

    if df.empty:
        return df

    # Sort by best available fantasy-value column
    sort_col = next((c for c in _SORT_CANDIDATES if c in df.columns), None)
    if sort_col:
        df = df.sort_values(sort_col, ascending=False)

    return df.head(20).reset_index(drop=True)


def venue_bias(venue: str) -> Dict[str, Any]:
    """
    Win % batting first vs chasing at a venue, computed from ball_events.

    Falls back to neutral 50/50 when no data is available.
    """
    from pypitch.api.session import get_session

    try:
        session = get_session()
        result = session.engine.execute_sql(
            """  -- nosec B608
            SELECT
                inning,
                COUNT(DISTINCT match_id) AS matches
            FROM ball_events
            WHERE venue = ?
            GROUP BY inning
            """,
            params=[venue],
        )
        rows = result.to_pydict()
        if not rows.get("inning"):
            raise ValueError("no data")

        inning_map: Dict[int, int] = {}
        for i in range(len(rows["inning"])):
            inning_map[rows["inning"][i]] = rows["matches"][i]

        # Approximate: matches where inning 1 > inning 2 totals = batting-first wins.
        # Without explicit result column we use total balls as proxy for match count.
        total = sum(inning_map.values()) / 2  # each match has 2 innings
        if total == 0:
            raise ValueError("no match data")

        # Better proxy: query run totals per inning per match
        runs_result = session.engine.execute_sql(
            """  -- nosec B608
            SELECT
                match_id,
                inning,
                SUM(runs_batter + runs_extras) AS total_runs
            FROM ball_events
            WHERE venue = ?
            GROUP BY match_id, inning
            ORDER BY match_id, inning
            """,
            params=[venue],
        )
        run_rows = runs_result.to_pydict()
        if not run_rows.get("match_id"):
            raise ValueError("no run data")

        match_innings: Dict[str, Dict[int, int]] = {}
        for i in range(len(run_rows["match_id"])):
            mid = run_rows["match_id"][i]
            inn = run_rows["inning"][i]
            runs = int(run_rows["total_runs"][i] or 0)
            match_innings.setdefault(mid, {})[inn] = runs

        bat_first_wins = chase_wins = 0
        for mid, innings in match_innings.items():
            if 1 in innings and 2 in innings:
                if innings[1] > innings[2]:
                    bat_first_wins += 1
                else:
                    chase_wins += 1

        total_matches = bat_first_wins + chase_wins
        if total_matches == 0:
            raise ValueError("insufficient match data")

        bat_pct = round(bat_first_wins / total_matches * 100, 1)
        chase_pct = round(chase_wins / total_matches * 100, 1)
        verdict = "BAT FIRST" if bat_pct >= chase_pct else "CHASE"

        return {
            "venue": venue,
            "total_matches_analysed": total_matches,
            "win_bat_first_pct": bat_pct,
            "win_chase_pct": chase_pct,
            "verdict": verdict,
        }
    except (RuntimeError, AttributeError, TypeError, ValueError, ZeroDivisionError) as e:
        logger.debug("venue_bias: no data for %r (%s), returning neutral", venue, e)
        return {
            "venue": venue,
            "total_matches_analysed": 0,
            "win_bat_first_pct": 50.0,
            "win_chase_pct": 50.0,
            "verdict": "INSUFFICIENT DATA",
        }
