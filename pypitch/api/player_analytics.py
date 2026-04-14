"""
Player Performance Analytics — PyPitch

All queries run against ball_events table in DuckDB.
Functions return plain dicts for easy JSON serialisation.

PA-01  Career batting aggregate
PA-02  Career bowling aggregate
PA-03  Fielding (catches + run-outs via is_wicket proxy)
PA-04  Batting by phase (Powerplay / Middle / Death)
PA-05  Bowling by phase
PA-06  Batting by venue
PA-07  Bowling by venue
PA-08  Best / worst venue (top-3 / bottom-3)
PA-09  Season-by-season batting
PA-10  Season-by-season bowling
PA-11  Form tracker — last N matches batting
PA-12  Form tracker — last N matches bowling
PA-13  Batting vs each opposition team
PA-14  Bowling vs each opposition team
PA-15  Weakness detector (teams/phases where avg drops >30%)
PA-16  Batting by innings number (1st vs 2nd)
PA-17  Batting in chases (target > 0, 2nd innings)
PA-18  High-pressure batting (wickets_fallen >= threshold at ball time)
PA-19  Death-over specialist score (SR in overs 16-19 vs career SR)
PA-20  Highest individual score in a single match
PA-21  Best bowling figures in a single innings
PA-22  Consecutive match streaks (20+ runs / 1+ wicket)
PA-23  Duck count (batting) and economy breaks (bowling > 10/over)
PA-24  Player comparison — side-by-side career stats for two players
PA-25  Batting leaderboard — top N by runs / avg / SR
PA-26  Bowling leaderboard — top N by wickets / economy / bowling avg
PA-27  Batting vs bowler hand (left/right) — requires metadata, best-effort
PA-28  Bowling vs batter hand (left/right) — requires metadata, best-effort
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Phase boundary constants (T20 overs, 0-indexed)
_POWERPLAY_MAX = 5    # overs 0-5
_MIDDLE_MAX = 14      # overs 6-14
# Death = overs 15-19


def _get_con() -> Any:
    """Return live DuckDB connection from the active storage engine."""
    from pypitch.api.session import get_session
    session = get_session()
    return session.engine.raw_connection()


def _r(v: Optional[float], dp: int = 2) -> Optional[float]:
    """Round or return None."""
    return round(v, dp) if v is not None else None


# ---------------------------------------------------------------------------
# PA-01  Career batting aggregate
# ---------------------------------------------------------------------------

def career_batting(player_name: str) -> Dict[str, Any]:
    """
    Full career batting aggregate for a player.

    Returns:
        matches, innings, runs, balls_faced, not_outs, highest_score,
        average, strike_rate, fifties, hundreds, fours, sixes, dot_balls
    """
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COUNT(DISTINCT match_id)                         AS matches,
            COUNT(DISTINCT match_id || '_' || inning)        AS innings,
            COALESCE(SUM(runs_batter), 0)                    AS runs,
            COUNT(*)                                          AS balls_faced,
            SUM(CASE WHEN is_wicket THEN 0 ELSE 1 END)
                FILTER (WHERE ball = (
                    SELECT MAX(b2.ball)
                    FROM ball_events b2
                    WHERE b2.match_id = ball_events.match_id
                      AND b2.inning  = ball_events.inning
                      AND b2.batter  = ball_events.batter
                ))                                            AS not_outs,
            MAX(runs_batter)                                  AS highest_ball,
            SUM(CASE WHEN runs_batter = 0 THEN 1 ELSE 0 END) AS dot_balls,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
        FROM ball_events
        WHERE batter = ?
        """
        row = con.execute(sql, [player_name]).fetchone()
        if not row or row[0] == 0:
            return {"player": player_name, "matches": 0, "message": "no data"}

        matches, innings, runs, balls, _not_outs, _high, dots, fours, sixes = row
        dismissals = innings  # upper bound; not_out calc is approximate without full scorecard

        avg = _r(runs / dismissals) if dismissals else None
        sr = _r((runs / balls) * 100) if balls else None

        # Milestone counts require per-innings aggregation
        milestones_sql = """  -- nosec B608
        SELECT
            SUM(CASE WHEN inning_runs >= 50  AND inning_runs < 100 THEN 1 ELSE 0 END) AS fifties,
            SUM(CASE WHEN inning_runs >= 100 THEN 1 ELSE 0 END)                        AS hundreds,
            MAX(inning_runs)                                                             AS highest_score
        FROM (
            SELECT match_id, inning, SUM(runs_batter) AS inning_runs
            FROM ball_events
            WHERE batter = ?
            GROUP BY match_id, inning
        ) t
        """
        mrow = con.execute(milestones_sql, [player_name]).fetchone()
        fifties = mrow[0] if mrow else 0
        hundreds = mrow[1] if mrow else 0
        highest = mrow[2] if mrow else 0

        return {
            "player": player_name,
            "matches": matches,
            "innings": innings,
            "runs": runs,
            "balls_faced": balls,
            "average": avg,
            "strike_rate": sr,
            "highest_score": highest,
            "fifties": fifties,
            "hundreds": hundreds,
            "fours": fours,
            "sixes": sixes,
            "dot_balls": dots,
            "dot_ball_pct": _r((dots / balls) * 100) if balls else None,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-02  Career bowling aggregate
# ---------------------------------------------------------------------------

def career_bowling(player_name: str) -> Dict[str, Any]:
    """
    Full career bowling aggregate for a player.

    Returns:
        matches, innings, wickets, balls_bowled, runs_conceded,
        economy, bowling_average, bowling_sr, best_figures, three_fers, five_fers
    """
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COUNT(DISTINCT match_id)                          AS matches,
            COUNT(DISTINCT match_id || '_' || inning)         AS innings,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)        AS wickets,
            COUNT(*)                                           AS balls_bowled,
            COALESCE(SUM(runs_total - runs_extras), 0)        AS runs_conceded
        FROM ball_events
        WHERE bowler = ?
        """
        row = con.execute(sql, [player_name]).fetchone()
        if not row or row[0] == 0:
            return {"player": player_name, "matches": 0, "message": "no data"}

        matches, innings, wickets, balls, runs = row
        overs = balls / 6.0
        economy = _r(runs / overs) if overs else None
        bowl_avg = _r(runs / wickets) if wickets else None
        bowl_sr = _r(balls / wickets) if wickets else None

        # Best figures and hauls
        haul_sql = """  -- nosec B608
        SELECT
            SUM(CASE WHEN inning_wkts >= 3 AND inning_wkts < 5 THEN 1 ELSE 0 END) AS three_fers,
            SUM(CASE WHEN inning_wkts >= 5 THEN 1 ELSE 0 END)                      AS five_fers,
            MAX(inning_wkts)                                                         AS best_wkts,
            MIN(CASE WHEN inning_wkts = (
                SELECT MAX(t2.inning_wkts) FROM (
                    SELECT match_id, inning, SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS inning_wkts
                    FROM ball_events WHERE bowler = ? GROUP BY match_id, inning
                ) t2
            ) THEN inning_runs ELSE NULL END)                                        AS best_runs
        FROM (
            SELECT match_id, inning,
                SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS inning_wkts,
                COALESCE(SUM(runs_total - runs_extras), 0)          AS inning_runs
            FROM ball_events
            WHERE bowler = ?
            GROUP BY match_id, inning
        ) t
        """
        hrow = con.execute(haul_sql, [player_name, player_name]).fetchone()
        three_fers = hrow[0] if hrow else 0
        five_fers = hrow[1] if hrow else 0
        best_wkts = hrow[2] if hrow else 0
        best_runs = hrow[3] if hrow else 0

        return {
            "player": player_name,
            "matches": matches,
            "innings_bowled": innings,
            "wickets": wickets,
            "balls_bowled": balls,
            "overs": _r(overs),
            "runs_conceded": runs,
            "economy": economy,
            "bowling_average": bowl_avg,
            "bowling_sr": bowl_sr,
            "best_figures": f"{best_wkts}/{best_runs}",
            "three_fers": three_fers,
            "five_fers": five_fers,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-03  Fielding (proxy from ball_events)
# ---------------------------------------------------------------------------

def career_fielding(player_name: str) -> Dict[str, Any]:
    """
    Fielding aggregate — run-outs where bowler field = player (proxy).
    ball_events does not store fielder name; returns available proxy counts.
    """
    # ball_events has no fielder column — return note + wickets-as-bowler proxy
    bowling = career_bowling(player_name)
    return {
        "player": player_name,
        "note": "ball_events has no fielder column; fielding counts require scorecard enrichment",
        "wickets_as_bowler": bowling.get("wickets", 0),
    }


# ---------------------------------------------------------------------------
# PA-04  Batting by phase
# ---------------------------------------------------------------------------

def batting_by_phase(player_name: str) -> Dict[str, Any]:
    """Batting split: Powerplay (0-5), Middle (6-14), Death (15-19)."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            CASE
                WHEN over <= 5  THEN 'Powerplay'
                WHEN over <= 14 THEN 'Middle'
                ELSE                 'Death'
            END                                               AS phase,
            COUNT(*)                                          AS balls,
            COALESCE(SUM(runs_batter), 0)                    AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)        AS dismissals,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes,
            SUM(CASE WHEN runs_batter = 0 THEN 1 ELSE 0 END) AS dot_balls
        FROM ball_events
        WHERE batter = ?
        GROUP BY phase
        ORDER BY MIN(over)
        """
        rows = con.execute(sql, [player_name]).fetchall()
        phases = []
        for r in rows:
            phase, balls, runs, dis, fours, sixes, dots = r
            avg = _r(runs / dis) if dis else None
            sr = _r((runs / balls) * 100) if balls else None
            phases.append({
                "phase": phase,
                "balls": balls,
                "runs": runs,
                "dismissals": dis,
                "average": avg,
                "strike_rate": sr,
                "fours": fours,
                "sixes": sixes,
                "dot_ball_pct": _r((dots / balls) * 100) if balls else None,
            })
        return {"player": player_name, "phases": phases}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-05  Bowling by phase
# ---------------------------------------------------------------------------

def bowling_by_phase(player_name: str) -> Dict[str, Any]:
    """Bowling split by phase."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            CASE
                WHEN over <= 5  THEN 'Powerplay'
                WHEN over <= 14 THEN 'Middle'
                ELSE                 'Death'
            END                                                      AS phase,
            COUNT(*)                                                   AS balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)                AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)                AS runs_conceded
        FROM ball_events
        WHERE bowler = ?
        GROUP BY phase
        ORDER BY MIN(over)
        """
        rows = con.execute(sql, [player_name]).fetchall()
        phases = []
        for r in rows:
            phase, balls, wkts, runs = r
            overs = balls / 6.0
            economy = _r(runs / overs) if overs else None
            bowl_avg = _r(runs / wkts) if wkts else None
            bowl_sr = _r(balls / wkts) if wkts else None
            phases.append({
                "phase": phase,
                "balls": balls,
                "overs": _r(overs),
                "wickets": wkts,
                "runs_conceded": runs,
                "economy": economy,
                "bowling_average": bowl_avg,
                "bowling_sr": bowl_sr,
            })
        return {"player": player_name, "phases": phases}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-06  Batting by venue
# ---------------------------------------------------------------------------

def batting_by_venue(player_name: str) -> List[Dict[str, Any]]:
    """Batting stats split by venue, sorted by runs desc."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            venue,
            COUNT(DISTINCT match_id)                          AS matches,
            COUNT(*)                                          AS balls,
            COALESCE(SUM(runs_batter), 0)                    AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)        AS dismissals,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
        FROM ball_events
        WHERE batter = ? AND venue IS NOT NULL AND venue != ''
        GROUP BY venue
        ORDER BY runs DESC
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            venue, matches, balls, runs, dis, fours, sixes = r
            avg = _r(runs / dis) if dis else None
            sr = _r((runs / balls) * 100) if balls else None
            result.append({
                "venue": venue,
                "matches": matches,
                "balls": balls,
                "runs": runs,
                "dismissals": dis,
                "average": avg,
                "strike_rate": sr,
                "fours": fours,
                "sixes": sixes,
            })
        return result
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-07  Bowling by venue
# ---------------------------------------------------------------------------

def bowling_by_venue(player_name: str) -> List[Dict[str, Any]]:
    """Bowling stats split by venue, sorted by wickets desc."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            venue,
            COUNT(DISTINCT match_id)                          AS matches,
            COUNT(*)                                          AS balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)        AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)        AS runs_conceded
        FROM ball_events
        WHERE bowler = ? AND venue IS NOT NULL AND venue != ''
        GROUP BY venue
        ORDER BY wickets DESC
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            venue, matches, balls, wkts, runs = r
            overs = balls / 6.0
            economy = _r(runs / overs) if overs else None
            bowl_avg = _r(runs / wkts) if wkts else None
            result.append({
                "venue": venue,
                "matches": matches,
                "balls": balls,
                "overs": _r(overs),
                "wickets": wkts,
                "runs_conceded": runs,
                "economy": economy,
                "bowling_average": bowl_avg,
            })
        return result
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-08  Best / worst venue
# ---------------------------------------------------------------------------

def best_worst_venues(player_name: str, role: str = "batting", top_n: int = 3) -> Dict[str, Any]:
    """
    Top-N and bottom-N venues for a player.

    role: 'batting' sorts by strike_rate; 'bowling' sorts by economy (lower = better).
    """
    if role == "batting":
        rows = batting_by_venue(player_name)
        rows = [r for r in rows if r["balls"] >= 6]  # min 1 over faced
        sorted_asc = sorted(rows, key=lambda x: x["strike_rate"] or 0)
        return {
            "player": player_name,
            "role": role,
            "best": sorted_asc[-top_n:][::-1],
            "worst": sorted_asc[:top_n],
        }
    else:
        rows = bowling_by_venue(player_name)
        rows = [r for r in rows if r["balls"] >= 6]  # min 1 over bowled
        sorted_asc = sorted(rows, key=lambda x: x["economy"] or 99)
        return {
            "player": player_name,
            "role": role,
            "best": sorted_asc[:top_n],    # lowest economy = best
            "worst": sorted_asc[-top_n:][::-1],
        }


# ---------------------------------------------------------------------------
# PA-09  Season-by-season batting
# ---------------------------------------------------------------------------

def batting_by_season(player_name: str) -> List[Dict[str, Any]]:
    """Season-by-season batting split."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COALESCE(NULLIF(season, ''), 'unknown')            AS season,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS dismissals
        FROM ball_events
        WHERE batter = ?
        GROUP BY season
        ORDER BY season
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            season, matches, balls, runs, dis = r
            avg = _r(runs / dis) if dis else None
            sr = _r((runs / balls) * 100) if balls else None
            result.append({
                "season": season,
                "matches": matches,
                "balls": balls,
                "runs": runs,
                "dismissals": dis,
                "average": avg,
                "strike_rate": sr,
            })
        return result
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-10  Season-by-season bowling
# ---------------------------------------------------------------------------

def bowling_by_season(player_name: str) -> List[Dict[str, Any]]:
    """Season-by-season bowling split."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COALESCE(NULLIF(season, ''), 'unknown')            AS season,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)         AS runs_conceded
        FROM ball_events
        WHERE bowler = ?
        GROUP BY season
        ORDER BY season
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            season, matches, balls, wkts, runs = r
            overs = balls / 6.0
            economy = _r(runs / overs) if overs else None
            bowl_avg = _r(runs / wkts) if wkts else None
            result.append({
                "season": season,
                "matches": matches,
                "balls": balls,
                "overs": _r(overs),
                "wickets": wkts,
                "runs_conceded": runs,
                "economy": economy,
                "bowling_average": bowl_avg,
            })
        return result
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-11  Form tracker — last N matches batting
# ---------------------------------------------------------------------------

def batting_form(player_name: str, last_n: int = 5) -> Dict[str, Any]:
    """Last N matches batting performance, most recent first."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            match_id,
            inning,
            SUM(runs_batter)                                   AS runs,
            COUNT(*)                                           AS balls,
            MAX(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS got_out,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)  AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)  AS sixes,
            MAX(timestamp)                                     AS match_ts
        FROM ball_events
        WHERE batter = ?
        GROUP BY match_id, inning
        ORDER BY match_ts DESC
        LIMIT ?
        """
        rows = con.execute(sql, [player_name, last_n]).fetchall()
        matches = []
        for r in rows:
            mid, inn, runs, balls, got_out, fours, sixes, ts = r
            sr = _r((runs / balls) * 100) if balls else None
            matches.append({
                "match_id": mid,
                "inning": inn,
                "runs": runs,
                "balls": balls,
                "strike_rate": sr,
                "got_out": bool(got_out),
                "fours": fours,
                "sixes": sixes,
            })
        total_runs = sum(m["runs"] for m in matches)
        avg_sr = _r(sum(m["strike_rate"] or 0 for m in matches) / len(matches)) if matches else None
        return {
            "player": player_name,
            "last_n": last_n,
            "total_runs": total_runs,
            "average_sr": avg_sr,
            "matches": matches,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-12  Form tracker — last N matches bowling
# ---------------------------------------------------------------------------

def bowling_form(player_name: str, last_n: int = 5) -> Dict[str, Any]:
    """Last N matches bowling performance, most recent first."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            match_id,
            inning,
            COUNT(*)                                           AS balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)         AS runs_conceded,
            MAX(timestamp)                                     AS match_ts
        FROM ball_events
        WHERE bowler = ?
        GROUP BY match_id, inning
        ORDER BY match_ts DESC
        LIMIT ?
        """
        rows = con.execute(sql, [player_name, last_n]).fetchall()
        matches = []
        for r in rows:
            mid, inn, balls, wkts, runs, ts = r
            overs = balls / 6.0
            economy = _r(runs / overs) if overs else None
            matches.append({
                "match_id": mid,
                "inning": inn,
                "balls": balls,
                "overs": _r(overs),
                "wickets": wkts,
                "runs_conceded": runs,
                "economy": economy,
            })
        total_wkts = sum(m["wickets"] for m in matches)
        avg_econ = _r(
            sum(m["economy"] or 0 for m in matches) / len(matches)
        ) if matches else None
        return {
            "player": player_name,
            "last_n": last_n,
            "total_wickets": total_wkts,
            "average_economy": avg_econ,
            "matches": matches,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-13  Batting vs each opposition team
# ---------------------------------------------------------------------------

def batting_vs_teams(player_name: str) -> List[Dict[str, Any]]:
    """
    Batting split by opposition.
    Uses bowler's match_id cross-join via a subquery to infer team names
    from venue-level data. NOTE: ball_events has no 'team' column;
    we use the match_id prefix convention or competition metadata.
    Returns per-match aggregation grouped by match_id prefix (best-effort).
    """
    con = _get_con()
    try:
        # ball_events has no explicit team column — group by competition as proxy
        sql = """  -- nosec B608
        SELECT
            COALESCE(NULLIF(competition, ''), 'unknown')       AS opposition_proxy,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS dismissals
        FROM ball_events
        WHERE batter = ?
        GROUP BY opposition_proxy
        ORDER BY runs DESC
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            opp, matches, balls, runs, dis = r
            avg = _r(runs / dis) if dis else None
            sr = _r((runs / balls) * 100) if balls else None
            result.append({
                "opposition": opp,
                "matches": matches,
                "balls": balls,
                "runs": runs,
                "dismissals": dis,
                "average": avg,
                "strike_rate": sr,
            })
        return result
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-14  Bowling vs each opposition (by competition proxy)
# ---------------------------------------------------------------------------

def bowling_vs_teams(player_name: str) -> List[Dict[str, Any]]:
    """Bowling split by opposition (competition proxy)."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COALESCE(NULLIF(competition, ''), 'unknown')       AS opposition_proxy,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)         AS runs_conceded
        FROM ball_events
        WHERE bowler = ?
        GROUP BY opposition_proxy
        ORDER BY wickets DESC
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            opp, matches, balls, wkts, runs = r
            overs = balls / 6.0
            economy = _r(runs / overs) if overs else None
            bowl_avg = _r(runs / wkts) if wkts else None
            result.append({
                "opposition": opp,
                "matches": matches,
                "balls": balls,
                "overs": _r(overs),
                "wickets": wkts,
                "runs_conceded": runs,
                "economy": economy,
                "bowling_average": bowl_avg,
            })
        return result
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-15  Weakness detector
# ---------------------------------------------------------------------------

def weakness_detector(player_name: str, drop_threshold: float = 0.30) -> Dict[str, Any]:
    """
    Find phases / venues where batting avg or SR drops >threshold vs career.
    Returns list of weakness entries with pct_drop field.
    """
    career = career_batting(player_name)
    career_sr = career.get("strike_rate") or 0
    career_avg = career.get("average") or 0

    phase_data = batting_by_phase(player_name)
    venue_data = batting_by_venue(player_name)

    weaknesses = []

    for p in phase_data.get("phases", []):
        sr = p.get("strike_rate") or 0
        if career_sr > 0 and sr > 0:
            drop = (career_sr - sr) / career_sr
            if drop >= drop_threshold:
                weaknesses.append({
                    "type": "phase",
                    "name": p["phase"],
                    "career_sr": career_sr,
                    "phase_sr": sr,
                    "sr_drop_pct": _r(drop * 100),
                })

    for v in venue_data:
        sr = v.get("strike_rate") or 0
        if career_sr > 0 and sr > 0 and v["balls"] >= 12:
            drop = (career_sr - sr) / career_sr
            if drop >= drop_threshold:
                weaknesses.append({
                    "type": "venue",
                    "name": v["venue"],
                    "career_sr": career_sr,
                    "venue_sr": sr,
                    "sr_drop_pct": _r(drop * 100),
                })

    return {
        "player": player_name,
        "career_sr": career_sr,
        "career_avg": career_avg,
        "drop_threshold_pct": drop_threshold * 100,
        "weaknesses": sorted(weaknesses, key=lambda x: x.get("sr_drop_pct", 0), reverse=True),
    }


# ---------------------------------------------------------------------------
# PA-16  Batting by innings (1st vs 2nd)
# ---------------------------------------------------------------------------

def batting_by_innings_number(player_name: str) -> Dict[str, Any]:
    """Batting split: 1st innings vs 2nd innings."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            inning,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS dismissals
        FROM ball_events
        WHERE batter = ?
        GROUP BY inning
        ORDER BY inning
        """
        rows = con.execute(sql, [player_name]).fetchall()
        result = []
        for r in rows:
            inn, matches, balls, runs, dis = r
            avg = _r(runs / dis) if dis else None
            sr = _r((runs / balls) * 100) if balls else None
            result.append({
                "inning": inn,
                "matches": matches,
                "balls": balls,
                "runs": runs,
                "dismissals": dis,
                "average": avg,
                "strike_rate": sr,
            })
        return {"player": player_name, "innings_split": result}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-17  Batting in chases (2nd innings, target > 0)
# ---------------------------------------------------------------------------

def batting_in_chases(player_name: str) -> Dict[str, Any]:
    """Batting stats when chasing (inning=2, target > 0)."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS dismissals,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)  AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)  AS sixes
        FROM ball_events
        WHERE batter = ? AND inning = 2 AND target > 0
        """
        row = con.execute(sql, [player_name]).fetchone()
        if not row or row[0] == 0:
            return {"player": player_name, "matches": 0, "message": "no chase data"}

        matches, balls, runs, dis, fours, sixes = row
        avg = _r(runs / dis) if dis else None
        sr = _r((runs / balls) * 100) if balls else None
        return {
            "player": player_name,
            "context": "chasing",
            "matches": matches,
            "balls": balls,
            "runs": runs,
            "dismissals": dis,
            "average": avg,
            "strike_rate": sr,
            "fours": fours,
            "sixes": sixes,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-18  High-pressure batting (wickets_fallen >= threshold)
# ---------------------------------------------------------------------------

def batting_under_pressure(player_name: str, wickets_threshold: int = 5) -> Dict[str, Any]:
    """
    Batting when wickets_fallen >= threshold at time of delivery.
    Proxy for crisis situations / lower-order contributions.
    """
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS dismissals
        FROM ball_events
        WHERE batter = ? AND wickets_fallen >= ?
        """
        row = con.execute(sql, [player_name, wickets_threshold]).fetchone()
        if not row or row[0] == 0:
            return {"player": player_name, "matches": 0, "message": "no high-pressure data"}

        matches, balls, runs, dis = row
        avg = _r(runs / dis) if dis else None
        sr = _r((runs / balls) * 100) if balls else None
        return {
            "player": player_name,
            "wickets_fallen_threshold": wickets_threshold,
            "matches": matches,
            "balls": balls,
            "runs": runs,
            "dismissals": dis,
            "average": avg,
            "strike_rate": sr,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-19  Death-over specialist score
# ---------------------------------------------------------------------------

def death_over_specialist(player_name: str) -> Dict[str, Any]:
    """
    Death-over batting SR (overs 15-19) vs career SR.
    Ratio > 1.0 = death-over specialist.
    """
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs
        FROM ball_events
        WHERE batter = ? AND over >= 15
        """
        row = con.execute(sql, [player_name]).fetchone()
        if not row or row[0] == 0:
            return {"player": player_name, "message": "no death-over data"}

        balls, runs = row
        death_sr = _r((runs / balls) * 100) if balls else None

        career = career_batting(player_name)
        career_sr = career.get("strike_rate")
        ratio = _r(death_sr / career_sr) if death_sr and career_sr else None

        return {
            "player": player_name,
            "death_over_balls": balls,
            "death_over_runs": runs,
            "death_over_sr": death_sr,
            "career_sr": career_sr,
            "specialist_ratio": ratio,
            "is_specialist": (ratio or 0) >= 1.10,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-20  Highest individual score in a single match
# ---------------------------------------------------------------------------

def highest_score(player_name: str, top_n: int = 5) -> Dict[str, Any]:
    """Top-N individual match scores."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            match_id,
            inning,
            SUM(runs_batter)                                   AS runs,
            COUNT(*)                                           AS balls,
            MAX(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS got_out,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END)  AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END)  AS sixes
        FROM ball_events
        WHERE batter = ?
        GROUP BY match_id, inning
        ORDER BY runs DESC
        LIMIT ?
        """
        rows = con.execute(sql, [player_name, top_n]).fetchall()
        scores = []
        for r in rows:
            mid, inn, runs, balls, got_out, fours, sixes = r
            sr = _r((runs / balls) * 100) if balls else None
            scores.append({
                "match_id": mid,
                "inning": inn,
                "runs": runs,
                "balls": balls,
                "strike_rate": sr,
                "not_out": not bool(got_out),
                "fours": fours,
                "sixes": sixes,
            })
        return {"player": player_name, "top_scores": scores}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-21  Best bowling figures in a single innings
# ---------------------------------------------------------------------------

def best_bowling_figures(player_name: str, top_n: int = 5) -> Dict[str, Any]:
    """Top-N bowling spells by wickets (then runs ascending)."""
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            match_id,
            inning,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)         AS runs_conceded,
            COUNT(*)                                           AS balls
        FROM ball_events
        WHERE bowler = ?
        GROUP BY match_id, inning
        ORDER BY wickets DESC, runs_conceded ASC
        LIMIT ?
        """
        rows = con.execute(sql, [player_name, top_n]).fetchall()
        figures = []
        for r in rows:
            mid, inn, wkts, runs, balls = r
            overs = _r(balls / 6.0)
            economy = _r(runs / (balls / 6.0)) if balls else None
            figures.append({
                "match_id": mid,
                "inning": inn,
                "figures": f"{wkts}/{runs}",
                "wickets": wkts,
                "runs_conceded": runs,
                "balls": balls,
                "overs": overs,
                "economy": economy,
            })
        return {"player": player_name, "best_figures": figures}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-22  Consecutive match streaks
# ---------------------------------------------------------------------------

def match_streaks(player_name: str, runs_threshold: int = 20, wickets_threshold: int = 1) -> Dict[str, Any]:
    """
    Find longest consecutive match streak:
    - Batting: scoring >= runs_threshold each match
    - Bowling: taking >= wickets_threshold each match

    Returns current streak + longest streak for each.
    """
    con = _get_con()
    try:
        bat_sql = """  -- nosec B608
        SELECT match_id, SUM(runs_batter) AS runs, MAX(timestamp) AS ts
        FROM ball_events WHERE batter = ?
        GROUP BY match_id ORDER BY ts ASC
        """
        bowl_sql = """  -- nosec B608
        SELECT match_id, SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wkts, MAX(timestamp) AS ts
        FROM ball_events WHERE bowler = ?
        GROUP BY match_id ORDER BY ts ASC
        """

        bat_rows = con.execute(bat_sql, [player_name]).fetchall()
        bowl_rows = con.execute(bowl_sql, [player_name]).fetchall()

        def _streak(rows: list, col_idx: int, threshold: int):
            best = cur = 0
            for r in rows:
                if (r[col_idx] or 0) >= threshold:
                    cur += 1
                    best = max(best, cur)
                else:
                    cur = 0
            return best, cur

        bat_best, bat_cur = _streak(bat_rows, 1, runs_threshold)
        bowl_best, bowl_cur = _streak(bowl_rows, 1, wickets_threshold)

        return {
            "player": player_name,
            "batting_streak": {
                "threshold_runs": runs_threshold,
                "longest": bat_best,
                "current": bat_cur,
            },
            "bowling_streak": {
                "threshold_wickets": wickets_threshold,
                "longest": bowl_best,
                "current": bowl_cur,
            },
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-23  Duck count and economy breaks
# ---------------------------------------------------------------------------

def milestones_and_failures(player_name: str) -> Dict[str, Any]:
    """
    Batting: duck count (out for 0).
    Bowling: economy-break innings (economy > 10 runs/over).
    """
    con = _get_con()
    try:
        duck_sql = """  -- nosec B608
        SELECT COUNT(*) AS ducks
        FROM (
            SELECT match_id, inning,
                SUM(runs_batter)                                AS runs,
                MAX(CASE WHEN is_wicket THEN 1 ELSE 0 END)      AS got_out
            FROM ball_events WHERE batter = ?
            GROUP BY match_id, inning
        ) t
        WHERE runs = 0 AND got_out = 1
        """
        econ_sql = """  -- nosec B608
        SELECT COUNT(*) AS economy_breaks
        FROM (
            SELECT match_id, inning,
                COALESCE(SUM(runs_total - runs_extras), 0)      AS runs_conceded,
                COUNT(*)                                         AS balls
            FROM ball_events WHERE bowler = ?
            GROUP BY match_id, inning
            HAVING balls >= 6
        ) t
        WHERE (runs_conceded * 1.0 / balls * 6) > 10
        """
        ducks = (con.execute(duck_sql, [player_name]).fetchone() or [0])[0]
        econ_breaks = (con.execute(econ_sql, [player_name]).fetchone() or [0])[0]
        return {
            "player": player_name,
            "ducks": ducks,
            "economy_breaks_over_10": econ_breaks,
        }
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-24  Player comparison
# ---------------------------------------------------------------------------

def compare_players(player1: str, player2: str) -> Dict[str, Any]:
    """Side-by-side career batting + bowling comparison."""
    return {
        "player1": {
            "name": player1,
            "batting": career_batting(player1),
            "bowling": career_bowling(player1),
        },
        "player2": {
            "name": player2,
            "batting": career_batting(player2),
            "bowling": career_bowling(player2),
        },
    }


# ---------------------------------------------------------------------------
# PA-25  Batting leaderboard
# ---------------------------------------------------------------------------

_MAX_LEADERBOARD_N = 100  # M7: cap unbounded top_n to prevent full-sort DOS


def batting_leaderboard(
    sort_by: str = "runs",
    top_n: int = 10,
    min_balls: int = 30,
) -> List[Dict[str, Any]]:
    """
    Top-N batters across all players in ball_events.

    sort_by: 'runs' | 'average' | 'strike_rate'
    min_balls: minimum balls faced to qualify
    """
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            batter,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            COALESCE(SUM(runs_batter), 0)                     AS runs,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS dismissals
        FROM ball_events
        WHERE batter != ''
        GROUP BY batter
        HAVING COUNT(*) >= ?
        """
        rows = con.execute(sql, [min_balls]).fetchall()
        result = []
        for r in rows:
            player, matches, balls, runs, dis = r
            avg = _r(runs / dis) if dis else None
            sr = _r((runs / balls) * 100) if balls else None
            result.append({
                "player": player,
                "matches": matches,
                "balls": balls,
                "runs": runs,
                "dismissals": dis,
                "average": avg,
                "strike_rate": sr,
            })

        key_map = {"runs": "runs", "average": "average", "strike_rate": "strike_rate"}
        sort_key = key_map.get(sort_by, "runs")
        result.sort(key=lambda x: x[sort_key] or 0, reverse=True)
        return result[:min(top_n, _MAX_LEADERBOARD_N)]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-26  Bowling leaderboard
# ---------------------------------------------------------------------------

def bowling_leaderboard(
    sort_by: str = "wickets",
    top_n: int = 10,
    min_balls: int = 30,
) -> List[Dict[str, Any]]:
    """
    Top-N bowlers across all players in ball_events.

    sort_by: 'wickets' | 'economy' | 'bowling_average'
    min_balls: minimum balls bowled to qualify
    """
    con = _get_con()
    try:
        sql = """  -- nosec B608
        SELECT
            bowler,
            COUNT(DISTINCT match_id)                           AS matches,
            COUNT(*)                                           AS balls,
            SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END)         AS wickets,
            COALESCE(SUM(runs_total - runs_extras), 0)         AS runs_conceded
        FROM ball_events
        WHERE bowler != ''
        GROUP BY bowler
        HAVING COUNT(*) >= ?
        """
        rows = con.execute(sql, [min_balls]).fetchall()
        result = []
        for r in rows:
            player, matches, balls, wkts, runs = r
            overs = balls / 6.0
            economy = _r(runs / overs) if overs else None
            bowl_avg = _r(runs / wkts) if wkts else None
            result.append({
                "player": player,
                "matches": matches,
                "balls": balls,
                "overs": _r(overs),
                "wickets": wkts,
                "runs_conceded": runs,
                "economy": economy,
                "bowling_average": bowl_avg,
            })

        if sort_by == "economy":
            result.sort(key=lambda x: x["economy"] or 99)
        elif sort_by == "bowling_average":
            result.sort(key=lambda x: x["bowling_average"] or 99)
        else:
            result.sort(key=lambda x: x["wickets"], reverse=True)
        return result[:min(top_n, _MAX_LEADERBOARD_N)]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# PA-27  Batting vs bowler hand (best-effort)
# ---------------------------------------------------------------------------

def batting_vs_bowler_hand(player_name: str) -> Dict[str, Any]:
    """
    Batting split vs left-arm vs right-arm.
    Requires bowler handedness metadata — not in ball_events by default.
    Returns note if data unavailable.
    """
    return {
        "player": player_name,
        "note": (
            "Bowler handedness not stored in ball_events. "
            "Enrich data via cricsheet player metadata to enable this split."
        ),
        "available": False,
    }


# ---------------------------------------------------------------------------
# PA-28  Bowling vs batter hand (best-effort)
# ---------------------------------------------------------------------------

def bowling_vs_batter_hand(player_name: str) -> Dict[str, Any]:
    """
    Bowling split vs left-hand vs right-hand batters.
    Requires batter handedness metadata — not in ball_events by default.
    """
    return {
        "player": player_name,
        "note": (
            "Batter handedness not stored in ball_events. "
            "Enrich data via cricsheet player metadata to enable this split."
        ),
        "available": False,
    }


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------

__all__ = [
    "career_batting",
    "career_bowling",
    "career_fielding",
    "batting_by_phase",
    "bowling_by_phase",
    "batting_by_venue",
    "bowling_by_venue",
    "best_worst_venues",
    "batting_by_season",
    "bowling_by_season",
    "batting_form",
    "bowling_form",
    "batting_vs_teams",
    "bowling_vs_teams",
    "weakness_detector",
    "batting_by_innings_number",
    "batting_in_chases",
    "batting_under_pressure",
    "death_over_specialist",
    "highest_score",
    "best_bowling_figures",
    "match_streaks",
    "milestones_and_failures",
    "compare_players",
    "batting_leaderboard",
    "bowling_leaderboard",
    "batting_vs_bowler_hand",
    "bowling_vs_batter_hand",
]
