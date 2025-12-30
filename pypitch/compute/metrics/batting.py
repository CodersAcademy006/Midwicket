import numpy as np
import pyarrow as pa

def calculate_strike_rate(runs: pa.Array, balls: pa.Array) -> pa.Array:
    """
    Standard SR Formula: (Runs / Balls) * 100.
    Handles DivisionByZero safely.
    """
    # Cast to float64 for precision
    runs_f = runs.cast(pa.float64())
    balls_f = balls.cast(pa.float64())
    
    # pc.divide handles nulls, but we need to handle zero-balls manually to avoid Inf
    sr = pc.divide(runs_f, balls_f)
    sr = pc.multiply(sr, 100.0)
    
    # Where balls == 0, SR is 0 (or null, depending on preference. We choose 0 for safety)
    sr = pc.if_else(pc.equal(balls, 0), 0.0, sr)
    
    return sr

def calculate_impact_score(
    runs: pa.Array, 
    balls: pa.Array, 
    phase: pa.Array
) -> pa.Array:
    """
    The 'Moneyball' Metric.
    Formula: (Actual Runs) - (Expected Runs based on Phase).
    
    Phase Baselines (Hardcoded for v1, loaded from config in v2):
    - Powerplay: 120 SR (1.2 RPB)
    - Middle: 110 SR (1.1 RPB)
    - Death: 160 SR (1.6 RPB)
    """
    # 1. Define Expected Run Per Ball (RPB) Map
    # We use pc.case_when or simple if_else chains for vectorization
    is_pp = pc.equal(phase, "Powerplay")
    is_death = pc.equal(phase, "Death")
    
    expected_rpb = pc.case_when(
        pc.make_struct([is_pp, is_death], field_names=["pp", "death"]),
        1.2,  # If PP
        1.6,  # If Death
        1.1   # Else (Middle)
    )

    # 2. Calculate Expected Runs
    expected_runs = pc.multiply(balls.cast(pa.float64()), expected_rpb)
    
    # 3. Impact = Actual - Expected
    impact = pc.subtract(runs.cast(pa.float64()), expected_runs)
    
    return impact