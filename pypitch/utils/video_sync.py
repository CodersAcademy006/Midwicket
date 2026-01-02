"""
Video Synchronization Utility for PyPitch.
Maps ball_index to video timestamp for easy video analysis.
"""
from typing import Optional

def get_video_timestamp(ball_index: int, mapping: dict) -> Optional[int]:
    """
    Returns the video timestamp (in seconds) for a given ball_index.
    mapping: dict of {ball_index: timestamp}
    """
    return mapping.get(ball_index)
