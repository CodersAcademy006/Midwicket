"""
Partnership metrics calculation functions.
Pure vectorized functions (Agent 5: The Analyst).
"""
import pyarrow as pa
import pyarrow.compute as pc


def calculate_partnership_run_rate(runs: pa.Array, balls: pa.Array) -> pa.Array:
    """
    Calculate partnership run rate (runs per over).

    Args:
        runs: Total runs scored in partnership
        balls: Total balls faced in partnership

    Returns:
        Partnership run rate (runs per over), 0.0 where balls == 0.
    """
    overs = pc.divide(balls.cast(pa.float64()), pa.scalar(6.0))
    run_rate = pc.divide(runs.cast(pa.float64()), overs)
    # Guard: when balls == 0, overs == 0 → division produces Inf; return 0.0 instead
    return pc.if_else(pc.equal(balls, 0), 0.0, run_rate)


def calculate_partnership_contribution(
    player_runs: pa.Array, partnership_runs: pa.Array
) -> pa.Array:
    """
    Calculate player's percentage contribution to an overall partnership.

    Args:
        player_runs: Runs scored by the individual player
        partnership_runs: Total partnership runs

    Returns:
        Contribution as a percentage (0–100), 0.0 where partnership_runs == 0.
    """
    pct = pc.multiply(
        pc.divide(
            player_runs.cast(pa.float64()),
            partnership_runs.cast(pa.float64()),
        ),
        pa.scalar(100.0),
    )
    # Guard: when partnership_runs == 0, division by zero produces Inf; return 0.0
    return pc.if_else(pc.equal(partnership_runs, 0), 0.0, pct)


def calculate_partnership_runs(batter1_runs: pa.Array, batter2_runs: pa.Array) -> pa.Array:
    """
    Calculate total partnership runs from both batters' individual run contributions.

    Args:
        batter1_runs: Runs scored by batter 1 during the partnership
        batter2_runs: Runs scored by batter 2 during the partnership

    Returns:
        Total partnership runs array.
    """
    return pc.add(batter1_runs.cast(pa.int64()), batter2_runs.cast(pa.int64()))


def calculate_powerplay_strike_rate(runs: pa.Array, balls: pa.Array) -> pa.Array:
    """
    Calculate strike rate specifically for the powerplay phase.

    Args:
        runs: Runs scored in the powerplay
        balls: Balls faced in the powerplay

    Returns:
        Powerplay strike rate, 0.0 where balls == 0.
    """
    sr = pc.multiply(
        pc.divide(runs.cast(pa.float64()), balls.cast(pa.float64())),
        pa.scalar(100.0),
    )
    return pc.if_else(pc.equal(balls, 0), 0.0, sr)


def calculate_death_overs_strike_rate(runs: pa.Array, balls: pa.Array) -> pa.Array:
    """
    Calculate strike rate specifically for the death overs phase.

    Args:
        runs: Runs scored in the death overs
        balls: Balls faced in the death overs

    Returns:
        Death overs strike rate, 0.0 where balls == 0.
    """
    sr = pc.multiply(
        pc.divide(runs.cast(pa.float64()), balls.cast(pa.float64())),
        pa.scalar(100.0),
    )
    return pc.if_else(pc.equal(balls, 0), 0.0, sr)
