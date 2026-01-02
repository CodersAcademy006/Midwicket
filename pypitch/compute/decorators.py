from functools import wraps
from typing import List, Callable, Dict, Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])

class MetricSpec:
    """
    Metadata attached to a metric function.
    Describes WHAT it needs, not HOW to get it.
    """
    def __init__(self, func: Callable[..., Any], requirements: List[Dict[str, str]]) -> None:
        self.func = func
        self.requirements = requirements
        self.name = func.__name__

def requires(dataset: str, join_key: str) -> Callable[[F], F]:
    """
    Decorator to declare data dependencies.
    
    Usage:
        @requires("venue_baselines", join_key="venue_id")
        def relative_strike_rate(events): ...
    """
    def decorator(func: F) -> F:
        if not hasattr(func, "_pypitch_spec"):
            setattr(func, "_pypitch_spec", MetricSpec(func, []))
        
        # Attach the dependency requirement
        spec: MetricSpec = getattr(func, "_pypitch_spec")
        spec.requirements.append({
            "table": dataset,
            "key": join_key
        })
        
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)
        return cast(F, wrapper)
    return decorator
