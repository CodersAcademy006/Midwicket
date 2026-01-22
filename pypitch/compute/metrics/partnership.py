"""
Partnership metrics calculation functions.
"""
import pyarrow as pa
import pyarrow.compute as pc

def calculate_partnership_run_rate(runs: pa.Array, balls: pa.Array) -> pa.Array:
    """
    Calculate partnership run rate.
    
    Args:
        runs: Total runs scored in partnership
        balls: Total balls faced in partnership
    
    Returns:
        Partnership run rate (runs per over)
    """
    overs = pc.divide(balls.cast(pa.float64()), pa.scalar(6.0))
    return pc.divide(runs.cast(pa.float64()), overs)

def calculate_partnership_contribution(player_runs: pa.Array, partnership_runs: pa.Array) -> pa.Array:
    """
    Calculate player's contribution to partnership.
    
    Args:
        player_runs: Runs scored by player
        partnership_runs: Total partnership runs
    
    Returns:
        Contribution percentage
    """
    return pc.multiply(
        pc.divide(player_runs.cast(pa.float64()), partnership_runs.cast(pa.float64())),
        pa.scalar(100.0)
    )
