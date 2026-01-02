import pyarrow as pa

def build_venue_stats(ball_events: pa.Table) -> pa.Table:
    """
    Aggregates data by Venue ID to calculate average scores.
    """
    # Group by Venue + Inning
    aggregated = ball_events.group_by(['venue_id', 'inning']).aggregate([
        ('runs_batter', 'sum'),
        ('runs_extras', 'sum'),
        ('match_id', 'count_distinct') # Needs pyarrow >= 14.0
    ])
    
    # Logic to calculate avg score would go here
    return aggregated