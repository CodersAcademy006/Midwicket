import pyarrow as pa
import pyarrow.compute as pc

def calculate_economy(runs_conceded: pa.Array, legal_balls: pa.Array) -> pa.Array:
    """
    Standard Econ Formula: (Runs / Overs).
    """
    overs = pc.divide(legal_balls.cast(pa.float64()), 6.0)  # type: ignore
    econ = pc.divide(runs_conceded.cast(pa.float64()), overs)  # type: ignore
    
    return pc.if_else(pc.equal(legal_balls, 0), 0.0, econ)  # type: ignore

def calculate_pressure_index(dots: pa.Array, balls: pa.Array) -> pa.Array:
    """
    Metric: Dot Ball Percentage.
    High Dot % correlates strongly with Wickets in T20s.
    """
    dot_pct = pc.divide(dots.cast(pa.float64()), balls.cast(pa.float64()))  # type: ignore
    pressure = pc.multiply(dot_pct, 100.0)  # type: ignore

    return pc.if_else(pc.equal(balls, 0), 0.0, pressure)  # type: ignore