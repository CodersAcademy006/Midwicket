"""
31_live_overlay_demo.py — Live Match Overlay for OBS/Streaming

Starts a local HTTP server that serves real-time match stats as JSON.
Add the URL as a Browser Source in OBS for live cricket broadcasts.

Usage:
    python examples/31_live_overlay_demo.py

Then open: http://localhost:8765/overlay
"""

import time
from pypitch.live.overlay import OverlayServer, LiveStats, LiveFeedSimulator


def main() -> None:
    print("PyPitch Live Overlay Demo")
    print("=" * 45)

    # Start server on a non-conflicting port
    server = OverlayServer(match_id="demo_ipl_2024", port=8765)
    server.start()

    print(f"\nOverlay URL : http://localhost:8765/overlay")
    print("Add this as a Browser Source in OBS Studio.\n")

    # Push a few manual stat updates to show the update mechanism
    updates = [
        LiveStats(
            match_id="demo_ipl_2024",
            current_over=6.0,
            current_score=58,
            wickets_fallen=1,
            run_rate=9.67,
            required_rr=8.4,
            batsman_on_strike="V Kohli",
            bowler="JJ Bumrah",
            last_ball="4",
            recent_overs=["1,0,4,1,2,W", "6,1,0,1,4,1", "2,0,1,6,1,0",
                          "4,1,1,0,2,1", "1,6,0,4,1,1", "2,1,4,0,W,4"],
        ),
        LiveStats(
            match_id="demo_ipl_2024",
            current_over=10.0,
            current_score=98,
            wickets_fallen=2,
            run_rate=9.8,
            required_rr=8.2,
            batsman_on_strike="KL Rahul",
            bowler="R Ashwin",
            last_ball="1",
        ),
        LiveStats(
            match_id="demo_ipl_2024",
            current_over=15.0,
            current_score=145,
            wickets_fallen=3,
            run_rate=9.67,
            required_rr=7.0,
            batsman_on_strike="KL Rahul",
            bowler="Hardik Pandya",
            last_ball="6",
        ),
    ]

    print("Pushing 3 manual stat updates (2 s apart)…\n")
    for stats in updates:
        server.update_stats(stats)
        data = server.get_stats_json()
        print(
            f"  Over {data['current_over']:.1f} | "
            f"{data['current_score']}/{data['wickets']} | "
            f"RR {data['run_rate']} | "
            f"Req {data['required_rr'] or '-'} | "
            f"Striker: {data['batsman']}"
        )
        time.sleep(2)

    # Optionally run the full simulator (uncomment to try)
    # sim = LiveFeedSimulator("demo_ipl_2024", server)
    # sim.start_simulation()
    # input("\nPress Enter to stop...\n")
    # sim.stop_simulation()

    server.stop()
    print("\nServer stopped.")


if __name__ == "__main__":
    main()
