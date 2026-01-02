"""
Video Synchronization for PyPitch.

Links ball-by-ball data to video timestamps for seamless analysis.
Essential for coaching and broadcasting applications.
"""
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class VideoTimestamp:
    """Represents a video timestamp with ball mapping."""
    ball_index: int  # Which ball in the match (1-based)
    timestamp_seconds: float  # Video timestamp in seconds
    over: int
    ball: int
    description: str  # e.g., "Kohli cover drive for 4"

@dataclass
class MatchVideo:
    """Complete video mapping for a match."""
    match_id: str
    video_url: Optional[str] = None
    timestamps: List[VideoTimestamp] = None

    def __post_init__(self):
        if self.timestamps is None:
            self.timestamps = []

class VideoSynchronizer:
    """
    Synchronizes PyPitch data with video timestamps.

    Enables features like:
    - One-click video jumps from statistics
    - Video highlights generation
    - Live broadcast integration
    """

    def __init__(self):
        self.match_videos: Dict[str, MatchVideo] = {}

    def load_video_mapping(self, match_id: str, video_url: str, timestamp_file: str) -> None:
        """
        Load video timestamp mappings from a file.

        Format: CSV with columns: ball_index,timestamp_seconds,description
        """
        import csv

        match_video = MatchVideo(match_id=match_id, video_url=video_url)

        try:
            with open(timestamp_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert ball_index to over/ball
                    ball_index = int(row['ball_index'])
                    over = (ball_index - 1) // 6
                    ball = ((ball_index - 1) % 6) + 1

                    timestamp = VideoTimestamp(
                        ball_index=ball_index,
                        timestamp_seconds=float(row['timestamp_seconds']),
                        over=over,
                        ball=ball,
                        description=row['description']
                    )
                    match_video.timestamps.append(timestamp)

            self.match_videos[match_id] = match_video

        except FileNotFoundError:
            raise FileNotFoundError(f"Video timestamp file not found: {timestamp_file}")
        except Exception as e:
            raise ValueError(f"Error loading video mapping: {e}")

    def get_video_timestamp(self, match_id: str, ball_index: int) -> Optional[VideoTimestamp]:
        """
        Get video timestamp for a specific ball.

        Args:
            match_id: The match identifier
            ball_index: Which ball in the match (1-based)

        Returns:
            VideoTimestamp if available, None otherwise
        """
        if match_id not in self.match_videos:
            return None

        match_video = self.match_videos[match_id]
        for timestamp in match_video.timestamps:
            if timestamp.ball_index == ball_index:
                return timestamp

        return None

    def get_youtube_url(self, match_id: str, ball_index: int) -> Optional[str]:
        """
        Generate YouTube URL with timestamp for direct video jumping.

        Returns URL like: https://youtu.be/VIDEO_ID?t=4520
        """
        if match_id not in self.match_videos:
            return None

        match_video = self.match_videos[match_id]
        if not match_video.video_url:
            return None

        timestamp = self.get_video_timestamp(match_id, ball_index)
        if not timestamp:
            return None

        # Extract YouTube video ID from URL
        video_id = self._extract_youtube_id(match_video.video_url)
        if not video_id:
            return None

        # Create timestamped URL
        timestamp_int = int(timestamp.timestamp_seconds)
        return f"https://youtu.be/{video_id}?t={timestamp_int}"

    def _extract_youtube_id(self, url: str) -> Optional[str]:
        """Extract YouTube video ID from various URL formats."""
        import re

        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com\/v\/([a-zA-Z0-9_-]{11})'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        return None

    def generate_highlights(self, match_id: str, criteria: Dict[str, Any]) -> List[VideoTimestamp]:
        """
        Generate video highlights based on criteria.

        Args:
            match_id: Match to generate highlights for
            criteria: Dict with keys like 'wickets', 'boundaries', 'overs'

        Returns:
            List of VideoTimestamp objects for highlight clips
        """
        if match_id not in self.match_videos:
            return []

        match_video = self.match_videos[match_id]
        highlights = []

        for timestamp in match_video.timestamps:
            if self._matches_criteria(timestamp, criteria):
                highlights.append(timestamp)

        return highlights

    def _matches_criteria(self, timestamp: VideoTimestamp, criteria: Dict[str, Any]) -> bool:
        """Check if a timestamp matches highlight criteria."""
        # Example criteria matching
        if 'wickets' in criteria and 'wicket' in timestamp.description.lower():
            return True
        if 'boundaries' in criteria and 'boundary' in timestamp.description.lower():
            return True
        if 'overs' in criteria and timestamp.over in criteria['overs']:
            return True

        return False

# Global instance for easy access
_video_sync = VideoSynchronizer()

def get_video_timestamp(match_id: str, ball_index: int) -> Optional[VideoTimestamp]:
    """Get video timestamp for a ball. Returns None if not available."""
    return _video_sync.get_video_timestamp(match_id, ball_index)

def get_youtube_url(match_id: str, ball_index: int) -> Optional[str]:
    """Get YouTube URL with timestamp for direct video jumping."""
    return _video_sync.get_youtube_url(match_id, ball_index)

def load_video_mapping(match_id: str, video_url: str, timestamp_file: str) -> None:
    """Load video timestamp mappings for a match."""
    _video_sync.load_video_mapping(match_id, video_url, timestamp_file)