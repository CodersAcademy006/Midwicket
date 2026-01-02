"""
PyPitch Live Plugin: Broadcaster Integration

Provides real-time statistics overlays for live streaming.
Perfect for local leagues, YouTubers, and broadcasters.
"""
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import json
import time
from pathlib import Path
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver

@dataclass
class LiveStats:
    """Real-time statistics for live broadcasting."""
    match_id: str
    current_over: float
    current_score: int
    wickets_fallen: int
    run_rate: float
    required_rr: Optional[float] = None
    batsman_on_strike: str = ""
    bowler: str = ""
    last_ball: str = ""
    recent_overs: List[str] = None

    def __post_init__(self):
        if self.recent_overs is None:
            self.recent_overs = []

class OverlayServer:
    """
    HTTP Server that serves live statistics as JSON for OBS Browser Source.

    Usage:
        server = OverlayServer(match_id="live_match")
        server.start()
        # OBS Browser Source: http://localhost:8000/overlay
    """

    def __init__(self, match_id: str, port: int = 8000, host: str = "localhost"):
        self.match_id = match_id
        self.port = port
        self.host = host
        self.server = None
        self.current_stats = LiveStats(
            match_id=match_id,
            current_over=0.0,
            current_score=0,
            wickets_fallen=0,
            run_rate=0.0
        )
        self.is_running = False

    def start(self):
        """Start the overlay server."""
        if self.is_running:
            return

        # Create custom request handler
        class OverlayHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/overlay":
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()

                    # Return current stats as JSON
                    stats_dict = {
                        "match_id": overlay_server.current_stats.match_id,
                        "current_over": overlay_server.current_stats.current_over,
                        "current_score": overlay_server.current_stats.current_score,
                        "wickets": overlay_server.current_stats.wickets_fallen,
                        "run_rate": f"{overlay_server.current_stats.run_rate:.2f}",
                        "required_rr": f"{overlay_server.current_stats.required_rr:.2f}" if overlay_server.current_stats.required_rr else None,
                        "batsman": overlay_server.current_stats.batsman_on_strike,
                        "bowler": overlay_server.current_stats.bowler,
                        "last_ball": overlay_server.current_stats.last_ball,
                        "recent_overs": overlay_server.current_stats.recent_overs[-5:],  # Last 5 overs
                        "timestamp": time.time()
                    }

                    self.wfile.write(json.dumps(stats_dict).encode())
                else:
                    self.send_response(404)
                    self.end_headers()

        # Store reference for handler
        global overlay_server
        overlay_server = self

        # Start server in background thread
        def run_server():
            with socketserver.TCPServer((self.host, self.port), OverlayHandler) as httpd:
                self.server = httpd
                self.is_running = True
                print(f"🎥 Live Overlay Server started at http://{self.host}:{self.port}/overlay")
                print("📺 Add this URL as Browser Source in OBS")
                httpd.serve_forever()

        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()

        # Give server time to start
        time.sleep(0.5)

    def stop(self):
        """Stop the overlay server."""
        if self.server:
            self.server.shutdown()
            self.is_running = False
            print("🎥 Live Overlay Server stopped")

    def update_stats(self, stats: LiveStats):
        """Update current live statistics."""
        self.current_stats = stats

class LiveFeedSimulator:
    """
    Simulates live match feed for testing overlay functionality.

    In production, this would connect to a real live feed API.
    """

    def __init__(self, match_id: str, overlay_server: OverlayServer):
        self.match_id = match_id
        self.overlay = overlay_server
        self.is_simulating = False

    def start_simulation(self):
        """Start simulating live match updates."""
        if self.is_simulating:
            return

        self.is_simulating = True

        def simulate_match():
            over = 0.0
            score = 0
            wickets = 0

            while self.is_simulating and over < 20.0:
                # Simulate ball-by-ball updates
                balls_in_over = 0
                over_score = 0

                while balls_in_over < 6 and wickets < 10:
                    # Random ball outcome (simplified)
                    import random
                    outcomes = [0, 1, 2, 3, 4, 6, 'W']
                    outcome = random.choice(outcomes)

                    if outcome == 'W':
                        wickets += 1
                        ball_desc = "W"
                    else:
                        score += outcome
                        over_score += outcome
                        ball_desc = str(outcome)

                    balls_in_over += 1
                    current_over = over + (balls_in_over / 6)

                    # Update overlay
                    stats = LiveStats(
                        match_id=self.match_id,
                        current_over=round(current_over, 1),
                        current_score=score,
                        wickets_fallen=wickets,
                        run_rate=round(score / current_over, 2) if current_over > 0 else 0.0,
                        batsman_on_strike=f"Batsman {wickets + 1}",
                        bowler=f"Bowler {(wickets % 5) + 1}",
                        last_ball=ball_desc
                    )

                    self.overlay.update_stats(stats)

                    time.sleep(2)  # 2 seconds per ball

                # Complete over
                over += 1
                print(f"End of over {int(over)}: {score}/{wickets}")

                time.sleep(10)  # 10 second break between overs

        sim_thread = threading.Thread(target=simulate_match, daemon=True)
        sim_thread.start()

    def stop_simulation(self):
        """Stop the simulation."""
        self.is_simulating = False

# Convenience functions
def serve_overlay(match_id: str, port: int = 8000) -> OverlayServer:
    """
    Start a live overlay server for broadcasting.

    Returns the server instance for updating stats.
    """
    server = OverlayServer(match_id, port)
    server.start()
    return server

def simulate_live_match(match_id: str, port: int = 8000):
    """
    Start both overlay server and live match simulation.

    Perfect for testing and demos.
    """
    server = serve_overlay(match_id, port)
    simulator = LiveFeedSimulator(match_id, server)
    simulator.start_simulation()

    print(f"📺 OBS Browser Source: http://localhost:{port}/overlay")
    print("\n🎮 Simulation running!")
    print(f"📺 OBS Browser Source: http://localhost:{port}/overlay")
    print("Press Ctrl+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        simulator.stop_simulation()
        server.stop()