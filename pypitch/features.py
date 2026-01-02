"""
Feature Store for PyPitch: Standardized feature generation for ML and analytics.
"""
from typing import Any

class RollingFeature:
    def __init__(self, window=10, metric="strike_rate"):
        self.window = window
        self.metric = metric

    def compute(self, session, player_id):
        """
        Compute rolling average for a player using DuckDB window functions.
        """
        # Example: Replace with actual query logic
        query = f"""
            SELECT avg({self.metric}) over (
                PARTITION BY player_id 
                ORDER BY match_date 
                ROWS BETWEEN {self.window} PRECEDING AND CURRENT ROW
            ) as rolling_{self.metric}
            FROM player_stats
            WHERE player_id = '{player_id}'
        """
        return session.query(query)
