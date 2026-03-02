"""
PyPitch API Models

Pydantic models for API responses and data structures.
"""

from pydantic import BaseModel, Field
from typing import Any
from decimal import Decimal

class PlayerStats(BaseModel):
    """Player career statistics - hides internal column names"""
    name: str = Field(..., description="Player name")
    matches: int = Field(..., ge=0, description="Total matches played")
    runs: int = Field(..., ge=0, description="Total runs scored")
    balls_faced: int = Field(..., ge=0, description="Total balls faced")
    wickets: int = Field(..., ge=0, description="Total wickets taken")
    balls_bowled: int = Field(..., ge=0, description="Total balls bowled")
    runs_conceded: int = Field(..., ge=0, description="Total runs conceded")

    @property
    def average(self) -> float | None:
        """Batting average"""
        if self.matches == 0:
            return None
        return float(Decimal(self.runs) / Decimal(self.matches))

    @property
    def strike_rate(self) -> float | None:
        """Batting strike rate"""
        if self.balls_faced == 0:
            return None
        return float((Decimal(self.runs) / Decimal(self.balls_faced)) * 100)

    @property
    def economy(self) -> float | None:
        """Bowling economy"""
        if self.balls_bowled == 0:
            return None
        return float((Decimal(self.runs_conceded) / Decimal(self.balls_bowled)) * 6)

class MatchupResult(BaseModel):
    """Head-to-head matchup statistics"""
    batter_name: str = Field(..., description="Batter name")
    bowler_name: str = Field(..., description="Bowler name")
    venue_name: str | None = Field(None, description="Venue name")
    matches: int = Field(..., ge=0, description="Number of matches")
    runs_scored: int = Field(..., ge=0, description="Total runs scored")
    balls_faced: int = Field(..., ge=0, description="Total balls faced")
    dismissals: int = Field(..., ge=0, description="Number of dismissals")
    average: float | None = Field(None, ge=0, description="Batting average")
    strike_rate: float | None = Field(None, ge=0, description="Strike rate")

    @classmethod
    def from_dataframe(cls, df: Any, batter: str, bowler: str, venue: str | None = None) -> "MatchupResult":
        """Convert internal DataFrame to public model"""
        # Aggregate the data
        total_matches = len(df)
        total_runs = df['runs'].sum()
        total_balls = df['balls'].sum()
        total_dismissals = df['wickets'].sum()

        avg = float(total_runs / total_matches) if total_matches > 0 else None
        sr = float((total_runs / total_balls) * 100) if total_balls > 0 else None

        return cls(
            batter_name=batter,
            bowler_name=bowler,
            venue_name=venue,
            matches=total_matches,
            runs_scored=int(total_runs),
            balls_faced=int(total_balls),
            dismissals=int(total_dismissals),
            average=avg,
            strike_rate=sr
        )

class VenueStats(BaseModel):
    """Venue statistics"""
    name: str = Field(..., description="Venue name")
    matches: int = Field(..., ge=0, description="Total matches")
    average_first_innings: float | None = Field(None, ge=0, description="Average first innings score")
    average_total: float | None = Field(None, ge=0, description="Average total score")