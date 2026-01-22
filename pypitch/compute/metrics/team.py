"""
Team-level metrics calculation functions.
"""
import pyarrow as pa
import pyarrow.compute as pc

def calculate_team_win_rate(wins: pa.Array, matches: pa.Array) -> pa.Array:
    """
    Calculate team win rate.
    
    Args:
        wins: Number of wins
        matches: Total matches played
    
    Returns:
        Win rate percentage
    """
    return pc.multiply(
        pc.divide(wins.cast(pa.float64()), matches.cast(pa.float64())),
        pa.scalar(100.0)
    )

def calculate_team_run_rate(runs: pa.Array, overs: pa.Array) -> pa.Array:
    """
    Calculate team run rate.
    
    Args:
        runs: Total runs scored
        overs: Total overs bowled/faced
    
    Returns:
        Run rate (runs per over)
    """
    return pc.divide(runs.cast(pa.float64()), overs.cast(pa.float64()))
