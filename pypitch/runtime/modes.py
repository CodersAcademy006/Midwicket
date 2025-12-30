from enum import Enum

class ExecutionMode(str, Enum):
    EXACT = "exact"     # Full compute, raw data scan allowed (High Cost)
    APPROX = "approx"   # Sampling or pre-aggregated data only (Low Cost)
    BUDGET = "budget"   # Hard limit on compute resources (Safe for public APIs)
