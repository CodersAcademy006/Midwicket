from typing import Optional
from dataclasses import dataclass

@dataclass
class MatchConfig:
    """
    Configuration object for match rules to avoid hardcoding.
    
    Handles different formats: T20 (20 overs), ODI (50), Test (unlimited),
    The Hundred (5-ball overs), etc.
    
    Also supports flexible player counts for rules like Impact Player.
    """
    total_overs: int
    balls_per_over: int = 6
    max_overs_per_bowler: Optional[int] = None  # Powerplay rules, etc.
    powerplay_overs: int = 6
    death_overs_start: int = 16  # For T20
    max_players_per_team: int = 11  # Standard cricket, but configurable for Impact Player
    
    @property
    def total_balls(self) -> int:
        """Total balls in a complete match"""
        return self.total_overs * self.balls_per_over
    
    @classmethod
    def t20(cls) -> 'MatchConfig':
        """Standard T20 configuration"""
        return cls(total_overs=20, balls_per_over=6, powerplay_overs=6, death_overs_start=16, max_players_per_team=11)
    
    @classmethod
    def odi(cls) -> 'MatchConfig':
        """Standard ODI configuration"""
        return cls(total_overs=50, balls_per_over=6, powerplay_overs=10, death_overs_start=41, max_players_per_team=11)
    
    @classmethod
    def test(cls) -> 'MatchConfig':
        """Test cricket (unlimited overs)"""
        return cls(total_overs=999, balls_per_over=6, powerplay_overs=0, death_overs_start=999, max_players_per_team=11)
    
    @classmethod
    def hundred(cls) -> 'MatchConfig':
        """The Hundred (5-ball overs)"""
        return cls(total_overs=20, balls_per_over=5, powerplay_overs=5, death_overs_start=15, max_players_per_team=11)
    
    @classmethod
    def t20_impact_player(cls) -> 'MatchConfig':
        """T20 with Impact Player rule (12th player)"""
        return cls(total_overs=20, balls_per_over=6, powerplay_overs=6, death_overs_start=16, max_players_per_team=12)