"""
Fantasy analytics for PyPitch.

cheat_sheet   — top players at a venue by fantasy value.
venue_bias    — win% batting first vs chasing at a venue.
fantasy_score — per-player career fantasy point estimate from ball_events.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# Proxy helpers — defined at module level so tests can patch them via
# "pypitch.api.fantasy.get_session" etc. without circular-import issues.
def get_session():
    from pypitch.api.session import PyPitchSession
    return PyPitchSession.get()


def get_executor():
    from pypitch.api.session import get_executor as _get_executor
    return _get_executor()


def get_registry():
    from pypitch.api.session import get_registry as _get_registry
    return _get_registry()

_SORT_CANDIDATES = ("avg_points", "fantasy_points_avg", "runs", "strike_rate")

# Standard T20 fantasy scoring weights (customisable by league)
_DEFAULT_SCORING = {
    "run": 1,
    "four": 1,        # bonus on top of run
    "six": 2,         # bonus on top of run
    "fifty": 30,
    "hundred": 50,
    "wicket": 10,
    "economy_bonus_lt7": 10,   # bowling economy < 7 per over
    "economy_bonus_lt8": 5,    # bowling economy < 8 per over
}


def fantasy_score(
    player_name: str,
    season: Optional[str] = None,
    scoring: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """
    Estimate fantasy points for a player from ball_events data.

    Computes per-match-average using standard T20 fantasy scoring:
      Batting: 1pt/run + 1pt/four + 2pt/six + 30pt/50 + 50pt/100
      Bowling: 10pt/wicket + economy bonus (lt7 = +10, lt8 = +5)

    Falls back to empty stats dict when no data is available.

    Args:
        player_name: Exact batter/bowler name as stored in ball_events.
        season:      Optional season filter (e.g. "2023").
        scoring:     Override default scoring weights (dict of weight keys).

    Returns:
        Dict with player, season, matches, batting_pts, bowling_pts,
        total_pts, per_match_avg, and breakdown sub-dicts.
    """
    weights = {**_DEFAULT_SCORING, **(scoring or {})}

    season_clause = "AND season = ?" if season else ""
    params_bat = [player_name] + ([season] if season else [])
    params_bowl = [player_name] + ([season] if season else [])

    result: Dict[str, Any] = {
        "player": player_name,
        "season": season or "all",
        "matches": 0,
        "batting_pts": 0.0,
        "bowling_pts": 0.0,
        "total_pts": 0.0,
        "per_match_avg": 0.0,
        "batting_breakdown": {},
        "bowling_breakdown": {},
        "source": "ball_events",
        "scoring_weights": weights,
    }

    try:
        session = get_session()

        # --- Batting stats ---
        # Aggregate to per-match totals first so 50s/100s are counted on
        # match-level scores, not individual delivery values (which top out at 6).
        bat_sql = f"""  -- nosec B608
            WITH per_match AS (
                SELECT
                    match_id,
                    SUM(runs_batter)                                    AS match_runs,
                    SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)   AS match_fours,
                    SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)   AS match_sixes
                FROM ball_events
                WHERE batter = ? {season_clause}
                GROUP BY match_id
            )
            SELECT
                COUNT(*)                                                     AS matches,
                SUM(match_runs)                                              AS runs,
                SUM(match_fours)                                             AS fours,
                SUM(match_sixes)                                             AS sixes,
                COUNT(CASE WHEN match_runs >= 50 AND match_runs < 100 THEN 1 END) AS fifties,
                COUNT(CASE WHEN match_runs >= 100 THEN 1 END)               AS hundreds
            FROM per_match
        """
        bat_res = session.engine.execute_sql(bat_sql, params=params_bat).to_pydict()
        matches = int((bat_res.get("matches") or [0])[0] or 0)
        runs = int((bat_res.get("runs") or [0])[0] or 0)
        fours = int((bat_res.get("fours") or [0])[0] or 0)
        sixes = int((bat_res.get("sixes") or [0])[0] or 0)
        fifties = int((bat_res.get("fifties") or [0])[0] or 0)
        hundreds = int((bat_res.get("hundreds") or [0])[0] or 0)

        bat_pts = (
            runs * weights["run"]
            + fours * weights["four"]
            + sixes * weights["six"]
            + fifties * weights["fifty"]
            + hundreds * weights["hundred"]
        )

        # --- Bowling stats ---
        bowl_sql = f"""  -- nosec B608
            SELECT
                COUNT(DISTINCT match_id)                                  AS matches,
                COUNT(*)                                                  AS balls,
                SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)               AS wickets,
                SUM(runs_batter + runs_extras)                            AS runs_conceded
            FROM ball_events
            WHERE bowler = ? {season_clause}
        """
        bowl_res = session.engine.execute_sql(bowl_sql, params=params_bowl).to_pydict()
        bowl_balls = int((bowl_res.get("balls") or [0])[0] or 0)
        wickets = int((bowl_res.get("wickets") or [0])[0] or 0)
        runs_conceded = int((bowl_res.get("runs_conceded") or [0])[0] or 0)

        economy = (runs_conceded / (bowl_balls / 6)) if bowl_balls >= 6 else None
        econ_bonus = 0
        if economy is not None:
            if economy < 7:
                econ_bonus = weights["economy_bonus_lt7"]
            elif economy < 8:
                econ_bonus = weights["economy_bonus_lt8"]

        bowl_pts = float(wickets * weights["wicket"] + econ_bonus)

        total = bat_pts + bowl_pts
        matches_all = max(matches, int((bowl_res.get("matches") or [0])[0] or 0))
        per_match = round(total / matches_all, 2) if matches_all > 0 else 0.0

        result.update({
            "matches": matches_all,
            "batting_pts": float(bat_pts),
            "bowling_pts": bowl_pts,
            "total_pts": float(total),
            "per_match_avg": per_match,
            "batting_breakdown": {
                "runs": runs, "fours": fours, "sixes": sixes,
                "fifties": fifties, "hundreds": hundreds,
            },
            "bowling_breakdown": {
                "wickets": wickets, "balls": bowl_balls,
                "runs_conceded": runs_conceded,
                "economy": round(economy, 2) if economy is not None else None,
                "economy_bonus": econ_bonus,
            },
        })
        logger.debug(
            "fantasy_score: %r season=%r matches=%d total_pts=%.1f",
            player_name, season, matches_all, total,
        )
    except (RuntimeError, AttributeError, TypeError, ValueError) as e:
        logger.warning("fantasy_score: failed for player=%r: %s", player_name, e)

    return result


def cheat_sheet(venue: str, last_n_years: int = 3) -> pd.DataFrame:
    """
    Fantasy cheat sheet for a venue.

    Tries the materialized fantasy_points_avg table first; falls back to
    aggregating raw ball_events when that table is not loaded.

    Returns a DataFrame sorted by best available fantasy-value column.
    At minimum returns career batting SR and runs at the venue.
    """
    from pypitch.query.defs import FantasyQuery

    df: Optional[pd.DataFrame] = None
    try:
        reg = get_registry()
        exc = get_executor()
        v_id = reg.resolve_venue(venue, date.today())
        q = FantasyQuery(
            venue_id=v_id,
            roles=["all"],
            min_matches=5,
            snapshot_id="latest",
        )
        response = exc.execute(q)
        df = response.data.to_pandas()
    except (RuntimeError, AttributeError, TypeError, ValueError) as e:
        logger.debug("cheat_sheet: executor/registry failed for venue=%r: %s — falling back", venue, e)

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
