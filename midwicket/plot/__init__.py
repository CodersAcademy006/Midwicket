"""
Midwicket Plot API
Convenience wrappers around `midwicket.visuals` that automatically inject the session
and display the plots.
"""

from typing import Optional, Any
from midwicket.api.session import MidwicketSession

def _get_session() -> MidwicketSession:
    return MidwicketSession.get()

def match_worm(match_id: str, ax: Optional[Any] = None) -> Any:
    """Plots the innings worm for a specific match."""
    from midwicket.visuals.worm import plot_match_worm
    return plot_match_worm(match_id, session=_get_session(), ax=ax)

def run_pressure(match_id: str, ax: Optional[Any] = None) -> Any:
    """Plots the run pressure graph (run rate vs required run rate) for a match."""
    from midwicket.visuals.worm import plot_run_pressure
    return plot_run_pressure(match_id, session=_get_session(), ax=ax)

def batter_pacing(match_id: str, batsman_id: int, ax: Optional[Any] = None) -> Any:
    """Plots how a batter paced their innings."""
    from midwicket.visuals.worm import plot_batter_pacing
    return plot_batter_pacing(match_id, batsman_id, session=_get_session(), ax=ax)

def momentum_swings(match_id: str, ax: Optional[Any] = None) -> Any:
    """Plots the momentum swings of a match."""
    from midwicket.visuals.worm import plot_momentum_swings
    return plot_momentum_swings(match_id, session=_get_session(), ax=ax)

def manhattan(match_id: str, ax: Optional[Any] = None) -> Any:
    """Plots a Manhattan chart (runs per over)."""
    from midwicket.visuals.worm import plot_manhattan
    return plot_manhattan(match_id, session=_get_session(), ax=ax)

def wagon_wheel(match_id: str, batter_id: int, ax: Optional[Any] = None) -> Any:
    """Plots a wagon wheel for a batter in a match."""
    from midwicket.visuals.worm import plot_wagon_wheel
    return plot_wagon_wheel(match_id, batter_id, session=_get_session(), ax=ax)

def show() -> None:
    """Helper to display the currently generated plots."""
    import matplotlib.pyplot as plt
    plt.show()
