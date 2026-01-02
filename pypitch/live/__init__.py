"""
PyPitch Live Plugin: Broadcasting and Live Statistics

This plugin provides real-time statistics overlays for live streaming,
perfect for local leagues, YouTubers, and professional broadcasters.
"""

from .overlay import OverlayServer, LiveFeedSimulator, serve_overlay, simulate_live_match

__all__ = [
    'OverlayServer',
    'LiveFeedSimulator',
    'serve_overlay',
    'simulate_live_match'
]