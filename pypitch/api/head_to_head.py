"""
PyPitch Head-to-Head Analysis Module

Provides high-level head-to-head comparison between two players,
including batting, bowling, and combined performance summaries.
This is a missing convenience feature bridging the gap between the
low-level ``stats.matchup()`` and what users actually want.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from pypitch.api.session import get_executor, get_registry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeadToHeadSummary:
    """Immutable summary of a head-to-head contest."""

    batter: str
    bowler: str
    venue: Optional[str] = None

    # Batting stats
    innings: int = 0
    runs: int = 0
    balls: int = 0
    dismissals: int = 0
    dot_balls: int = 0
    boundaries: int = 0
    sixes: int = 0

    @property
    def average(self) -> Optional[float]:
        """Batting average (runs / dismissals)."""
        return round(self.runs / self.dismissals, 2) if self.dismissals else None

    @property
    def strike_rate(self) -> Optional[float]:
        """Batting strike rate ((runs / balls) * 100)."""
        return round((self.runs / self.balls) * 100, 2) if self.balls else None

    @property
    def dot_ball_pct(self) -> Optional[float]:
        """Dot ball percentage."""
        return round((self.dot_balls / self.balls) * 100, 1) if self.balls else None

    @property
    def boundary_pct(self) -> Optional[float]:
        """Boundary percentage ((4s + 6s) / balls)."""
        total_bounds = self.boundaries + self.sixes
        return round((total_bounds / self.balls) * 100, 1) if self.balls else None

    def as_dict(self) -> Dict[str, Any]:
        """Serialise to a dictionary (including computed properties)."""
        return {
            "batter": self.batter,
            "bowler": self.bowler,
            "venue": self.venue,
            "innings": self.innings,
            "runs": self.runs,
            "balls": self.balls,
            "dismissals": self.dismissals,
            "dot_balls": self.dot_balls,
            "boundaries": self.boundaries,
            "sixes": self.sixes,
            "average": self.average,
            "strike_rate": self.strike_rate,
            "dot_ball_pct": self.dot_ball_pct,
            "boundary_pct": self.boundary_pct,
        }

    def __repr__(self) -> str:
        sr = f"{self.strike_rate:.1f}" if self.strike_rate is not None else "N/A"
        avg = f"{self.average:.1f}" if self.average is not None else "N/A"
        return (
            f"H2H({self.batter} vs {self.bowler}) — "
            f"{self.runs} runs, {self.balls} balls, "
            f"SR {sr}, Avg {avg}, {self.dismissals} dismissals"
        )


def head_to_head(
    batter: str,
    bowler: str,
    venue: Optional[str] = None,
    *,
    date_context: Optional[date] = None,
) -> HeadToHeadSummary:
    """
    Get a comprehensive head-to-head summary between a batter and bowler.

    This is the recommended top-level API for matchup analysis.
    It resolves player identities, queries the engine, and returns
    a rich ``HeadToHeadSummary`` dataclass.

    Args:
        batter:  Batter name (fuzzy-matched via IdentityRegistry).
        bowler:  Bowler name (fuzzy-matched via IdentityRegistry).
        venue:   Optional venue filter.
        date_context: Date context for identity resolution (default: today).

    Returns:
        HeadToHeadSummary with batting stats and computed metrics.

    Example::

        import pypitch as pp
        h2h = pp.head_to_head("V Kohli", "JJ Bumrah")
        print(h2h)
        # H2H(V Kohli vs JJ Bumrah) — 42 runs, 38 balls, SR 110.5, Avg 21.0, 2 dismissals
    """
    from pypitch.query.base import MatchupQuery

    reg = get_registry()
    exc = get_executor()

    if date_context is None:
        date_context = date.today()

    b_id = str(reg.resolve_player(batter, date_context))
    bo_id = str(reg.resolve_player(bowler, date_context))

    v_id = None
    if venue:
        v_id = str(reg.resolve_venue(venue, date_context))

    query = MatchupQuery(
        snapshot_id="latest",
        batter_id=b_id,
        bowler_id=bo_id,
        venue_id=v_id,
    )

    response = exc.execute(query)
    data = response.data

    # Parse the Arrow/DataFrame result into summary fields
    if hasattr(data, "to_pandas"):
        df = data.to_pandas()
    elif hasattr(data, "to_pydict"):
        import pandas as pd
        df = pd.DataFrame(data.to_pydict())
    else:
        # Fallback — data might already be a dict
        return HeadToHeadSummary(batter=batter, bowler=bowler, venue=venue)

    if df.empty:
        logger.info("No head-to-head data found for %s vs %s", batter, bowler)
        return HeadToHeadSummary(batter=batter, bowler=bowler, venue=venue)

    runs = int(df["runs"].sum()) if "runs" in df.columns else 0
    balls = int(df["balls"].sum()) if "balls" in df.columns else 0
    dismissals = int(df["wickets"].sum()) if "wickets" in df.columns else 0

    # Extended stats — available if the engine returns ball-level data
    dot_balls = int((df.get("runs_batter", df.get("runs", None)) == 0).sum()) if balls else 0
    boundaries = int((df.get("runs_batter", df.get("runs", None)) == 4).sum()) if balls else 0
    sixes = int((df.get("runs_batter", df.get("runs", None)) == 6).sum()) if balls else 0

    return HeadToHeadSummary(
        batter=batter,
        bowler=bowler,
        venue=venue,
        innings=len(df),
        runs=runs,
        balls=balls,
        dismissals=dismissals,
        dot_balls=dot_balls,
        boundaries=boundaries,
        sixes=sixes,
    )


__all__ = ["head_to_head", "HeadToHeadSummary"]
