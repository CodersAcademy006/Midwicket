from enum import Enum

class ExecutionMode(str, Enum):
    EXACT = "exact"     # Full compute, raw data scan allowed (High Cost)
    APPROX = "approx"   # Sampling or pre-aggregated data only (Low Cost)
    BUDGET = "budget"   # Hard limit on compute resources (Safe for public APIs)

# Global debug mode flag
debug_mode = False

def set_debug_mode(enabled: bool):
    """
    Enable or disable debug mode for eager execution.

    When debug mode is enabled, all queries are executed eagerly (materialized immediately)
    instead of lazily. This surfaces errors early during development, making debugging easier.

    Usage:
        import midwicket
        midwicket.set_debug_mode(True)  # Enable debug mode
        # Run queries - they will execute immediately
        midwicket.set_debug_mode(False)  # Disable for production
    """
    global debug_mode
    debug_mode = enabled
    # Unify duplicate debug flags (MW-033)
    try:
        import midwicket.config as config
        config.debug = enabled
    except Exception:
        pass
    try:
        import midwicket.express as express
        with express._debug_lock:
            express._DEBUG_MODE = enabled
    except Exception:
        pass
