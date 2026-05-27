"""
Team-level metrics calculation functions.
Pure vectorized functions (Agent 5: The Analyst).
"""
import pyarrow as pa
import pyarrow.compute as pc


def calculate_team_win_rate(wins: pa.Array, matches: pa.Array) -> pa.Array:
    """
    Calculate team win rate percentage.

    Args:
        wins: Number of wins
        matches: Total matches played

    Returns:
        Win rate as a percentage (0–100), 0.0 where matches == 0.
    """
    matches_f = matches.cast(pa.float64())
    wins_f = wins.cast(pa.float64())
    rate = pc.multiply(
        pc.divide(wins_f, matches_f),
        pa.scalar(100.0),
    )
    # Guard: when matches == 0, division produces Inf/NaN; return 0.0 instead
    return pc.if_else(pc.equal(matches_f, 0.0), 0.0, rate)


def calculate_team_run_rate(runs: pa.Array, overs: pa.Array) -> pa.Array:
    """
    Calculate team run rate (runs per over).

    Args:
        runs: Total runs scored
        overs: Total overs bowled/faced

    Returns:
        Run rate (runs per over), 0.0 where overs == 0.
    """
    overs_f = overs.cast(pa.float64())
    runs_f = runs.cast(pa.float64())
    rate = pc.divide(runs_f, overs_f)
    # Guard: when overs == 0, division produces Inf; return 0.0 instead
    return pc.if_else(pc.equal(overs_f, 0.0), 0.0, rate)


def calculate_average_first_innings_score(
    total_runs: pa.Array, matches: pa.Array
) -> pa.Array:
    """
    Calculate average first innings score across matches.

    Args:
        total_runs: Sum of first innings runs across the sample
        matches: Number of first innings played

    Returns:
        Average first innings score, 0.0 where matches == 0.
    """
    matches_f = matches.cast(pa.float64())
    avg = pc.divide(total_runs.cast(pa.float64()), matches_f)
    return pc.if_else(pc.equal(matches_f, 0.0), 0.0, avg)


def calculate_average_second_innings_score(
    total_runs: pa.Array, matches: pa.Array
) -> pa.Array:
    """
    Calculate average second innings score across matches.

    Args:
        total_runs: Sum of second innings runs across the sample
        matches: Number of second innings played

    Returns:
        Average second innings score, 0.0 where matches == 0.
    """
    matches_f = matches.cast(pa.float64())
    avg = pc.divide(total_runs.cast(pa.float64()), matches_f)
    return pc.if_else(pc.equal(matches_f, 0.0), 0.0, avg)


def calculate_team_boundary_percentage(
    boundary_runs: pa.Array, total_runs: pa.Array
) -> pa.Array:
    """
    Calculate the percentage of runs scored via boundaries (4s and 6s).

    Args:
        boundary_runs: Runs scored from 4s and 6s
        total_runs: Total runs scored by the team

    Returns:
        Boundary percentage (0–100), 0.0 where total_runs == 0.
    """
    total_f = total_runs.cast(pa.float64())
    pct = pc.multiply(
        pc.divide(boundary_runs.cast(pa.float64()), total_f),
        pa.scalar(100.0),
    )
    return pc.if_else(pc.equal(total_f, 0.0), 0.0, pct)


def calculate_runs_per_match(total_runs: pa.Array, matches: pa.Array) -> pa.Array:
    """
    Calculate average runs scored per match.

    Args:
        total_runs: Total runs over a set of matches
        matches: Number of matches

    Returns:
        Average runs per match, 0.0 where matches == 0.
    """
    matches_f = matches.cast(pa.float64())
    avg = pc.divide(total_runs.cast(pa.float64()), matches_f)
    return pc.if_else(pc.equal(matches_f, 0.0), 0.0, avg)


def calculate_wickets_per_match(total_wickets: pa.Array, matches: pa.Array) -> pa.Array:
    """
    Calculate average wickets taken per match.

    Args:
        total_wickets: Total wickets taken over a set of matches
        matches: Number of matches

    Returns:
        Average wickets per match, 0.0 where matches == 0.
    """
    matches_f = matches.cast(pa.float64())
    avg = pc.divide(total_wickets.cast(pa.float64()), matches_f)
    return pc.if_else(pc.equal(matches_f, 0.0), 0.0, avg)
