from functools import wraps
from typing import List, Callable, Dict, Any

class MetricSpec:
    """
    Metadata attached to a metric function.
    Describes WHAT it needs, not HOW to get it.
    """
    def __init__(self, func: Callable, requirements: List[Dict[str, str]]):
        self.func = func
        self.requirements = requirements
        self.name = func.__name__

def requires(dataset: str, join_key: str):
    """
    Decorator to declare data dependencies.
    
    Usage:
        @requires("venue_baselines", join_key="venue_id")
        def relative_strike_rate(events): ...
    """
    def decorator(func):
        if not hasattr(func, "_pypitch_spec"):
            func._pypitch_spec = MetricSpec(func, [])
        
        # Attach the dependency requirement
        func._pypitch_spec.requirements.append({
            "table": dataset,
            "key": join_key
        })
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator
