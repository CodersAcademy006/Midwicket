"""
PyPitch Input Validation Models

Pydantic models for API input validation and request/response schemas.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import date
import re

class WinPredictionRequest(BaseModel):
    """Request model for win probability prediction."""
    target: int = Field(..., gt=0, le=500, description="Target score to chase")
    current_runs: int = Field(..., ge=0, description="Current runs scored")
    wickets_down: int = Field(..., ge=0, le=10, description="Wickets fallen")
    overs_done: float = Field(..., ge=0.0, le=20.0, description="Overs completed")
    venue: Optional[str] = Field(None, max_length=100, description="Venue name")

    @field_validator('overs_done')
    @classmethod
    def validate_overs_done(cls, v):
        if v < 0 or v > 20:
            raise ValueError('overs_done must be between 0 and 20')
        return v

    @field_validator('wickets_down')
    @classmethod
    def validate_wickets_down(cls, v):
        if v < 0 or v > 10:
            raise ValueError('wickets_down must be between 0 and 10')
        return v

class PlayerLookupRequest(BaseModel):
    """Request model for player lookup."""
    name: str = Field(..., min_length=1, max_length=100, description="Player name")
    date_context: Optional[date] = Field(None, description="Date context for identity resolution")

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        # Basic name validation - allow letters, spaces, hyphens, apostrophes
        if not re.match(r"^[a-zA-Z\s\-']+$", v):
            raise ValueError('Name contains invalid characters')
        return v.strip()

class VenueLookupRequest(BaseModel):
    """Request model for venue lookup."""
    name: str = Field(..., min_length=1, max_length=100, description="Venue name")

class MatchupRequest(BaseModel):
    """Request model for batter vs bowler matchup."""
    batter: str = Field(..., min_length=1, max_length=100, description="Batter name")
    bowler: str = Field(..., min_length=1, max_length=100, description="Bowler name")
    venue: Optional[str] = Field(None, max_length=100, description="Venue name (optional)")
    date_context: Optional[date] = Field(None, description="Date context")

class FantasyPointsRequest(BaseModel):
    """Request model for fantasy points calculation."""
    player_name: str = Field(..., min_length=1, max_length=100, description="Player name")
    match_id: Optional[str] = Field(None, description="Specific match ID")
    season: Optional[str] = Field(None, max_length=10, description="Season filter")

class StatsFilterRequest(BaseModel):
    """Request model for statistics with filters."""
    player_name: Optional[str] = Field(None, max_length=100, description="Filter by player")
    team: Optional[str] = Field(None, max_length=100, description="Filter by team")
    venue: Optional[str] = Field(None, max_length=100, description="Filter by venue")
    season: Optional[str] = Field(None, max_length=10, description="Filter by season")
    min_matches: Optional[int] = Field(None, ge=1, description="Minimum matches played")

class LiveMatchRegistrationRequest(BaseModel):
    """Request model for registering a live match."""
    match_id: str = Field(..., min_length=1, max_length=50, description="Unique match identifier")
    source: str = Field(..., pattern="^(webhook|api_poll|stream)$", description="Data source type")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional match metadata")

class DeliveryDataRequest(BaseModel):
    """Request model for live delivery data."""
    match_id: str = Field(..., min_length=1, max_length=50, description="Match identifier")
    inning: int = Field(..., ge=1, le=2, description="Inning number")
    over: int = Field(..., ge=0, le=20, description="Over number")
    ball: int = Field(..., ge=1, le=6, description="Ball number in over")
    runs_total: int = Field(..., ge=0, description="Total runs scored so far")
    wickets_fallen: int = Field(..., ge=0, le=10, description="Wickets fallen so far")
    target: Optional[int] = Field(None, gt=0, description="Target score (for second innings)")
    venue: Optional[str] = Field(None, max_length=100, description="Venue name")

# Response Models
class WinPredictionResponse(BaseModel):
    """Response model for win probability prediction."""
    win_prob: float = Field(..., ge=0.0, le=1.0, description="Win probability")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Prediction confidence")
    runs_remaining: int = Field(..., ge=0, description="Runs remaining")
    balls_remaining: int = Field(..., ge=0, description="Balls remaining")
    run_rate_required: float = Field(..., ge=0.0, description="Required run rate")
    venue_adjustment: float = Field(..., description="Venue-specific adjustment")

class ErrorResponse(BaseModel):
    """Standard error response model."""
    error: str = Field(..., description="Error message")
    error_type: str = Field(..., description="Error type/class name")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")

class HealthCheckResponse(BaseModel):
    """Health check response model."""
    status: str = Field(..., pattern="^(healthy|degraded|unhealthy)$", description="Service health status")
    version: str = Field(..., description="Service version")
    uptime_seconds: float = Field(..., ge=0, description="Service uptime")
    database_status: str = Field(..., description="Database connection status")
    active_connections: Optional[int] = Field(None, description="Active database connections")

# Plugin-related models
class PluginInfo(BaseModel):
    """Plugin information model."""
    name: str = Field(..., description="Plugin name")
    version: str = Field(..., description="Plugin version")
    description: str = Field(..., description="Plugin description")
    loaded: bool = Field(..., description="Whether plugin is loaded")

class PluginListResponse(BaseModel):
    """Response model for plugin listing."""
    plugins: List[PluginInfo] = Field(..., description="List of available plugins")
    loaded_count: int = Field(..., ge=0, description="Number of loaded plugins")

__all__ = [
    'WinPredictionRequest', 'PlayerLookupRequest', 'VenueLookupRequest',
    'MatchupRequest', 'FantasyPointsRequest', 'StatsFilterRequest',
    'LiveMatchRegistrationRequest', 'DeliveryDataRequest',
    'WinPredictionResponse', 'ErrorResponse', 'HealthCheckResponse',
    'PluginInfo', 'PluginListResponse'
]