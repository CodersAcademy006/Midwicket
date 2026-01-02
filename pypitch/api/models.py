from dataclasses import dataclass
from typing import List, Optional
from datetime import date

@dataclass
class PlayerStats:
    """Player career statistics - hides internal column names"""
    name: str
    matches: int
    runs: int
    balls_faced: int
    wickets: int
    balls_bowled: int
    runs_conceded: int
    
    @property
    def average(self) -> Optional[float]:
        """Batting average"""
        if self.matches == 0:
            return None
        return self.runs / self.matches
    
    @property
    def strike_rate(self) -> Optional[float]:
        """Batting strike rate"""
        if self.balls_faced == 0:
            return None
        return (self.runs / self.balls_faced) * 100
    
    @property
    def economy(self) -> Optional[float]:
        """Bowling economy"""
        if self.balls_bowled == 0:
            return None
        return (self.runs_conceded / self.balls_bowled) * 6

@dataclass
class MatchupResult:
    """Head-to-head matchup statistics"""
    batter_name: str
    bowler_name: str
    venue_name: Optional[str]
    matches: int
    runs_scored: int
    balls_faced: int
    dismissals: int
    average: Optional[float]
    strike_rate: Optional[float]
    
    @classmethod
    def from_dataframe(cls, df: 'pd.DataFrame', batter: str, bowler: str, venue: Optional[str] = None) -> 'MatchupResult':
        """Convert internal DataFrame to public model"""
        # Aggregate the data
        total_matches = len(df)
        total_runs = df['runs'].sum()
        total_balls = df['balls'].sum()
        total_dismissals = df['wickets'].sum()
        
        avg = total_runs / total_matches if total_matches > 0 else None
        sr = (total_runs / total_balls) * 100 if total_balls > 0 else None
        
        return cls(
            batter_name=batter,
            bowler_name=bowler,
            venue_name=venue,
            matches=total_matches,
            runs_scored=total_runs,
            balls_faced=total_balls,
            dismissals=total_dismissals,
            average=avg,
            strike_rate=sr
        )

@dataclass
class VenueStats:
    """Venue statistics"""
    name: str
    matches: int
    average_first_innings: Optional[float]
    average_total: Optional[float]