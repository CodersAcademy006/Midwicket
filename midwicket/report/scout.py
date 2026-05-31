"""
Midwicket Scouting Report: Automated Player Scouting Insights

Aggregates multiple underlying player analytics endpoints into a single comprehensive,
actionable report analyzing strengths, weaknesses, phase splits, venue biases, and form.
"""

import logging
from typing import Dict, Any, Optional

# Internal Imports
from midwicket.api.session import MidwicketSession
from midwicket.api.player_analytics import (
    career_batting,
    career_bowling,
    batting_by_phase,
    bowling_by_phase,
    batting_by_venue,
    bowling_by_venue,
    weakness_detector,
    batting_form,
    bowling_form
)

logger = logging.getLogger(__name__)

def scouting_report(player_name: str) -> Dict[str, Any]:
    """
    Compiles a comprehensive, actionable scouting report for a given player.

    Args:
        player_name: The canonical spelling or resolved alias of the player.

    Returns:
        Dict containing strengths, weaknesses, venue performance, phase performance,
        and matchup summaries from both batting and bowling perspectives.
    """
    logger.info("Compiling high-fidelity scouting report for %r...", player_name)
    
    # Resolve exact casing as it exists in the active ball_events table dynamically
    session = MidwicketSession.get()
    resolved_name = None
    try:
        # Check if the name exists case-insensitively in batter column
        res_bat = session.engine.execute_sql(
            "SELECT DISTINCT batter FROM ball_events WHERE LOWER(batter) = LOWER(?) LIMIT 1",
            [player_name]
        ).to_pydict()
        if res_bat and res_bat.get("batter") and len(res_bat["batter"]) > 0:
            resolved_name = res_bat["batter"][0]
        else:
            # Check bowler column
            res_bowl = session.engine.execute_sql(
                "SELECT DISTINCT bowler FROM ball_events WHERE LOWER(bowler) = LOWER(?) LIMIT 1",
                [player_name]
            ).to_pydict()
            if res_bowl and res_bowl.get("bowler") and len(res_bowl["bowler"]) > 0:
                resolved_name = res_bowl["bowler"][0]
    except Exception as e:
        logger.debug("Failed dynamic name resolution: %s", e)

    if resolved_name:
        logger.info("Resolved name %r to exact database casing: %r", player_name, resolved_name)
        player_name = resolved_name

    # 1. Fetch baseline analytics data
    bat_career = None
    bowl_career = None
    try:
        bat_career = career_batting(player_name)
    except Exception:
        pass
        
    try:
        bowl_career = career_bowling(player_name)
    except Exception:
        pass
        
    # Check if we have any data at all
    if (not bat_career or bat_career.get("message") == "no data") and (not bowl_career or bowl_career.get("message") == "no data"):
        raise ValueError(f"No historical statistical records found for player: '{player_name}'")
        
    # 2. Extract Phase and Venue splits
    bat_phases = []
    bowl_phases = []
    try:
        bat_phases = batting_by_phase(player_name).get("phases", [])
    except Exception:
        pass
        
    try:
        bowl_phases = bowling_by_phase(player_name).get("phases", [])
    except Exception:
        pass
        
    bat_venues = []
    bowl_venues = []
    try:
        bat_venues = batting_by_venue(player_name).get("venues", [])
    except Exception:
        pass
        
    try:
        bowl_venues = bowling_by_venue(player_name).get("venues", [])
    except Exception:
        pass
        
    # 3. Detect weaknesses and matchups
    weaknesses = {}
    try:
        weaknesses = weakness_detector(player_name)
    except Exception:
        pass
        
    # 4. Form tracker
    bat_recent = {}
    bowl_recent = {}
    try:
        bat_recent = batting_form(player_name)
    except Exception:
        pass
        
    try:
        bowl_recent = bowling_form(player_name)
    except Exception:
        pass

    # 5. Synthesis: Strengths & Weaknesses
    strengths = []
    detected_weaknesses = []
    
    # Analyze batting strengths
    if bat_career and bat_career.get("runs", 0) > 0:
        runs = bat_career["runs"]
        sr = bat_career.get("strike_rate", 0)
        avg = bat_career.get("average", 0)
        
        if avg and avg >= 30.0:
            strengths.append(f"Anchor capability: averages {avg:.2f} runs per innings.")
        if sr and sr >= 130.0:
            strengths.append(f"Attacking option: scores at a strike rate of {sr:.2f}.")
            
        # Phase-based strengths
        for phase_data in bat_phases:
            p_name = phase_data.get("phase")
            p_sr = phase_data.get("strike_rate", 0)
            p_avg = phase_data.get("average", 0)
            if p_name == "Death" and p_sr and p_sr >= 150.0:
                strengths.append(f"Death overs: explosive scoring rate of {p_sr:.2f} in final overs.")
            if p_name == "Powerplay" and p_sr and p_sr >= 135.0:
                strengths.append(f"Powerplay: takes advantage of fielding restrictions (SR: {p_sr:.2f}).")
                
    # Analyze bowling strengths
    if bowl_career and bowl_career.get("wickets", 0) > 0:
        w = bowl_career["wickets"]
        econ = bowl_career.get("economy", 0)
        sr = bowl_career.get("strike_rate", 0)
        
        if econ and econ <= 8.0:
            strengths.append(f"Restrictive bowler: maintains economy rate of {econ:.2f} runs per over.")
        if sr and sr <= 20.0:
            strengths.append(f"Wicket threat: strikes every {sr:.2f} deliveries.")
            
        # Phase-based bowling strengths
        for phase_data in bowl_phases:
            p_name = phase_data.get("phase")
            p_econ = phase_data.get("economy", 0)
            if p_name == "Death" and p_econ and p_econ <= 9.0:
                strengths.append(f"Death overs: restrictive economy of {p_econ:.2f} defending runs.")
            if p_name == "Powerplay" and p_econ and p_econ <= 7.0:
                strengths.append(f"Powerplay control: keeps score down inside initial overs (Econ: {p_econ:.2f}).")

    # Analyze Weaknesses
    if weaknesses and "weaknesses" in weaknesses:
        for wk in weaknesses["weaknesses"]:
            detected_weaknesses.append(wk)
            
    # Default fallbacks if none are derived
    if not strengths:
        strengths.append("Steady contributor: standard baseline performance profiles.")
    if not detected_weaknesses:
        detected_weaknesses.append("No statistical vulnerabilities detected against current opponent thresholds.")

    # 6. Compose report payload
    return {
        "player": player_name,
        "role": "All-Rounder" if (bat_career and bat_career.get("runs", 0) > 0 and bowl_career and bowl_career.get("wickets", 0) > 0) else ("Batter" if (bat_career and bat_career.get("runs", 0) > 0) else "Bowler"),
        "strengths": strengths,
        "weaknesses": detected_weaknesses,
        "career_summary": {
            "batting": bat_career,
            "bowling": bowl_career
        },
        "phase_performance": {
            "batting": bat_phases,
            "bowling": bowl_phases
        },
        "venue_performance": {
            "batting": bat_venues,
            "bowling": bowl_venues
        },
        "recent_form": {
            "batting": bat_recent.get("form", []) if bat_recent else [],
            "bowling": bowl_recent.get("form", []) if bowl_recent else []
        }
    }
