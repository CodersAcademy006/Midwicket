"""
Midwicket API Models

Pydantic models for API responses and data structures.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional, Union
from decimal import Decimal


class PlayerStats(BaseModel):
    """Player career statistics - hides internal column names"""
    name: str = Field(..., description="Player name")
    matches: int = Field(..., ge=0, description="Total matches played")
    runs: int = Field(..., ge=0, description="Total runs scored")
    balls_faced: int = Field(..., ge=0, description="Total balls faced")
    dismissals: int = Field(0, ge=0, description="Number of times dismissed (batting)")
    wickets: int = Field(..., ge=0, description="Total wickets taken")
    balls_bowled: int = Field(..., ge=0, description="Total balls bowled")
    runs_conceded: int = Field(..., ge=0, description="Total runs conceded")

    @property
    def average(self) -> Optional[float]:
        """Cricket batting average: runs per dismissal.

        Falls back to runs-per-match when dismissals == 0 (e.g. never-out players
        or datasets that don't track dismissals) to avoid returning None for
        players who have scored runs.
        """
        if self.dismissals > 0:
            return float(Decimal(self.runs) / Decimal(self.dismissals))
        if self.matches > 0:
            # Fallback: runs per match (labelled in the model docstring)
            return float(Decimal(self.runs) / Decimal(self.matches))
        return None

    @property
    def strike_rate(self) -> Optional[float]:
        """Batting strike rate"""
        if self.balls_faced == 0:
            return None
        return float((Decimal(self.runs) / Decimal(self.balls_faced)) * 100)

    @property
    def economy(self) -> Optional[float]:
        """Bowling economy"""
        if self.balls_bowled == 0:
            return None
        return float((Decimal(self.runs_conceded) / Decimal(self.balls_bowled)) * 6)


class MatchupResult(BaseModel):
    """Head-to-head matchup statistics"""
    batter_name: str = Field(..., description="Batter name")
    bowler_name: str = Field(..., description="Bowler name")
    venue_name: Optional[str] = Field(None, description="Venue name")
    matches: int = Field(..., ge=0, description="Number of matches")
    runs_scored: int = Field(..., ge=0, description="Total runs scored")
    balls_faced: int = Field(..., ge=0, description="Total balls faced")
    dismissals: int = Field(..., ge=0, description="Number of dismissals")
    average: Optional[float] = Field(None, ge=0, description="Batting average")
    strike_rate: Optional[float] = Field(None, ge=0, description="Strike rate")

    @classmethod
    def from_dataframe(cls, df: Any, batter: str, bowler: str, venue: Optional[str] = None) -> "MatchupResult":
        """Convert internal DataFrame to public model"""
        total_matches = len(df)
        total_runs = df['runs'].sum() if not df.empty and 'runs' in df.columns else 0
        total_balls = df['balls'].sum() if not df.empty and 'balls' in df.columns else 0
        total_dismissals = df['wickets'].sum() if not df.empty and 'wickets' in df.columns else 0

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
    average_first_innings: Optional[float] = Field(None, ge=0, description="Average first innings score")
    average_total: Optional[float] = Field(None, ge=0, description="Average total score")

class MidwicketResultSet(list):
    """
    A custom list class for Midwicket query results.
    Renders as a styled HTML table in Jupyter Notebooks.
    """
    def _repr_html_(self) -> str:
        if not self:
            return "<p><em>No results found</em></p>"
        
        # Get headers from first dict
        first = self[0]
        if not isinstance(first, dict):
            return super().__repr__()
            
        keys = list(first.keys())
        
        # Build HTML table
        html = ["<table style='border-collapse: collapse; width: 100%; border: 1px solid #ddd; font-family: sans-serif;'>"]
        
        # Header row
        html.append("<thead><tr style='background-color: #f2f2f2;'>")
        for key in keys:
            html.append(f"<th style='padding: 8px; text-align: left; border-bottom: 2px solid #ddd;'>{str(key).replace('_', ' ').title()}</th>")
        html.append("</tr></thead>")
        
        # Data rows
        html.append("<tbody>")
        for row in self:
            html.append("<tr>")
            for key in keys:
                val = row.get(key, "")
                if isinstance(val, float):
                    val = f"{val:.2f}"
                html.append(f"<td style='padding: 8px; border-bottom: 1px solid #ddd;'>{val}</td>")
            html.append("</tr>")
        html.append("</tbody></table>")
        
        return "".join(html)

    def __repr__(self) -> str:
        """Fallback to standard representation, or rich if available."""
        try:
            from rich.console import Console
            from rich.table import Table
            import sys
            
            # Check if running interactively
            if hasattr(sys, 'ps1') or sys.stdout.isatty():
                if not self:
                    return "[]"
                
                table = Table(show_header=True, header_style="bold magenta")
                first = self[0]
                if isinstance(first, dict):
                    for k in first.keys():
                        table.add_column(str(k).replace('_', ' ').title())
                        
                    for row in self:
                        table.add_row(*[f"{v:.2f}" if isinstance(v, float) else str(v) for v in row.values()])
                        
                    Console().print(table)
                    return ""
        except ImportError:
            pass
            
        return super().__repr__()

class MidwicketResultDict(dict):
    """
    A custom dict class for single-row Midwicket query results.
    Renders as a styled HTML table in Jupyter Notebooks.
    """
    def _repr_html_(self) -> str:
        if not self:
            return "<p><em>No results found</em></p>"
            
        # Build HTML table (vertical key-value)
        html = ["<table style='border-collapse: collapse; border: 1px solid #ddd; font-family: sans-serif;'>"]
        html.append("<tbody>")
        for k, v in self.items():
            html.append("<tr>")
            html.append(f"<th style='padding: 8px; text-align: left; border-bottom: 1px solid #ddd; background-color: #f9f9f9; width: 150px;'>{str(k).replace('_', ' ').title()}</th>")
            if isinstance(v, float):
                v = f"{v:.2f}"
            html.append(f"<td style='padding: 8px; border-bottom: 1px solid #ddd;'>{v}</td>")
            html.append("</tr>")
        html.append("</tbody></table>")
        
        return "".join(html)

    def __repr__(self) -> str:
        """Fallback to standard representation, or rich if available."""
        try:
            from rich.console import Console
            from rich.table import Table
            import sys
            
            if hasattr(sys, 'ps1') or sys.stdout.isatty():
                if not self:
                    return "{}"
                    
                table = Table(show_header=False)
                table.add_column("Property", style="cyan")
                table.add_column("Value", style="magenta")
                
                for k, v in self.items():
                    val = f"{v:.2f}" if isinstance(v, float) else str(v)
                    table.add_row(str(k).replace('_', ' ').title(), val)
                    
                Console().print(table)
                return ""
        except ImportError:
            pass
            
        return super().__repr__()
