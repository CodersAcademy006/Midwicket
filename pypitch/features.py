"""
PyPitch Feature Store: Standardized Feature Calculations

Provides standardized, ML-ready features for cricket analysis.
Ensures consistent calculations across teams and prevents "different answers" syndrome.

Key Features:
- Rolling statistics (e.g., strike rate over last 10 balls)
- Momentum indicators
- Fatigue metrics
- Standardized aggregations
"""

from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
import duckdb
from pypitch.api.session import PyPitchSession

@dataclass
class RollingFeature:
    """Configuration for rolling window features."""
    window: int = 10
    metric: str = "strike_rate"
    partition_by: str = "player_id"
    order_by: str = "match_date"

    def compute(self, session: PyPitchSession, player_id: Union[str, int]) -> Any:
        """
        Compute rolling feature for a player.

        Args:
            session: PyPitchSession
            player_id: Player ID or name

        Returns:
            DuckDB relation with rolling calculations
        """
        # Resolve player ID if name
        if isinstance(player_id, str):
            resolved = session.registry.resolve_player(player_id)
            if not resolved:
                raise ValueError(f"Player {player_id} not found")
            player_id = resolved

        # Build rolling window query
        query = f"""
        SELECT
            *,
            AVG({self._get_metric_column()}) OVER (
                PARTITION BY {self.partition_by}
                ORDER BY {self.order_by}
                ROWS BETWEEN {self.window - 1} PRECEDING AND CURRENT ROW
            ) as rolling_{self.metric}
        FROM deliveries
        WHERE player_id = ?
        """

        return session.engine.con.sql(query, [player_id])

    def _get_metric_column(self) -> str:
        """Map metric name to database column."""
        mappings = {
            "strike_rate": "runs",
            "economy": "runs_conceded",
            "average": "runs",
            "bowling_average": "runs_conceded"
        }
        return mappings.get(self.metric, self.metric)

@dataclass
class MomentumIndicator:
    """Momentum and form indicators."""
    lookback_periods: List[int] = None

    def __post_init__(self):
        if self.lookback_periods is None:
            self.lookback_periods = [5, 10, 20]

    def calculate_recent_form(self, session: PyPitchSession, player_id: Union[str, int]) -> Dict[str, float]:
        """
        Calculate recent form indicators.

        Returns dict with keys like 'form_5', 'form_10', etc.
        """
        # Resolve player ID
        if isinstance(player_id, str):
            resolved = session.registry.resolve_player(player_id)
            if not resolved:
                raise ValueError(f"Player {player_id} not found")
            player_id = resolved

        results = {}
        for period in self.lookback_periods:
            # Get recent matches
            query = f"""
            SELECT AVG(runs) as avg_runs, AVG(balls_faced) as avg_balls
            FROM player_match_stats
            WHERE player_id = ?
            ORDER BY match_date DESC
            LIMIT ?
            """
            df = session.engine.con.sql(query, [player_id, period]).df()
            if not df.empty:
                avg_runs = df['avg_runs'].iloc[0] or 0
                avg_balls = df['avg_balls'].iloc[0] or 1  # Avoid division by zero
                results[f'form_{period}'] = avg_runs / avg_balls * 100  # Strike rate
            else:
                results[f'form_{period}'] = 0.0

        return results

@dataclass
class FatigueMetric:
    """Player fatigue and workload metrics."""
    max_matches_threshold: int = 15

    def calculate_workload(self, session: PyPitchSession, player_id: Union[str, int]) -> Dict[str, Any]:
        """
        Calculate player workload and fatigue indicators.
        """
        # Resolve player ID
        if isinstance(player_id, str):
            resolved = session.registry.resolve_player(player_id)
            if not resolved:
                raise ValueError(f"Player {player_id} not found")
            player_id = resolved

        # Recent matches in last 30 days
        query = """
        SELECT
            COUNT(*) as recent_matches,
            AVG(balls_bowled) as avg_balls_bowled,
            AVG(balls_faced) as avg_balls_faced,
            MAX(match_date) as last_match_date
        FROM player_match_stats
        WHERE player_id = ?
        AND match_date >= date('now', '-30 days')
        """

        df = session.engine.con.sql(query, [player_id]).df()
        if df.empty:
            return {
                'fatigue_level': 'low',
                'workload_score': 0,
                'rest_days': 30
            }

        row = df.iloc[0]
        recent_matches = row['recent_matches'] or 0
        avg_balls_bowled = row['avg_balls_bowled'] or 0
        avg_balls_faced = row['avg_balls_faced'] or 0

        # Calculate workload score (simple heuristic)
        workload_score = recent_matches * 0.3 + avg_balls_bowled * 0.4 + avg_balls_faced * 0.3

        # Determine fatigue level
        if workload_score > 20:
            fatigue_level = 'high'
        elif workload_score > 10:
            fatigue_level = 'medium'
        else:
            fatigue_level = 'low'

        return {
            'fatigue_level': fatigue_level,
            'workload_score': workload_score,
            'recent_matches': recent_matches,
            'avg_workload': avg_balls_bowled + avg_balls_faced
        }

# Convenience functions
def get_rolling_strike_rate(session: PyPitchSession, player_id: Union[str, int], window: int = 10):
    """Get rolling strike rate for player."""
    feature = RollingFeature(window=window, metric="strike_rate")
    return feature.compute(session, player_id)

def get_recent_form(session: PyPitchSession, player_id: Union[str, int]):
    """Get recent form indicators."""
    indicator = MomentumIndicator()
    return indicator.calculate_recent_form(session, player_id)

def get_fatigue_level(session: PyPitchSession, player_id: Union[str, int]):
    """Get player fatigue metrics."""
    fatigue = FatigueMetric()
    return fatigue.calculate_workload(session, player_id)

__all__ = [
    'RollingFeature',
    'MomentumIndicator',
    'FatigueMetric',
    'get_rolling_strike_rate',
    'get_recent_form',
    'get_fatigue_level'
]
