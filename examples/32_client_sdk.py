"""
32_client_sdk.py — PyPitch REST Client SDK

Demonstrates the PyPitchClient — a thin HTTP wrapper for the PyPitch
API server. Run the server first with:

    uvicorn pypitch.serve.api:app --reload   # requires 'serve' extra

Usage:
    python examples/32_client_sdk.py
"""

import sys

# Ensure UTF-8 stdout on Windows (CP1252 crashes on non-ASCII output)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

from pypitch.client import PyPitchClient, connect, quick_health_check

SERVER_URL = "http://localhost:8000"


def main() -> None:
    print("PyPitch Client SDK Demo")
    print("=" * 45)
    print(f"Target server: {SERVER_URL}\n")

    # ------------------------------------------------------------------
    # 1. Quick health check (non-fatal if server is not running)
    # ------------------------------------------------------------------
    healthy = quick_health_check(SERVER_URL, timeout=3.0)
    print(f"[1] Server healthy: {healthy}")

    if not healthy:
        print("\nServer is offline. Start it with:")
        print("  pip install 'pypitch[serve]'")
        print("  uvicorn pypitch.serve.api:app --reload")
        print("\nShowing client API surface only (no live calls).")

    # ------------------------------------------------------------------
    # 2. Client object — shows full API surface
    # ------------------------------------------------------------------
    client = connect(SERVER_URL, timeout=5.0)
    print("\n[2] Client created:", client)

    # Demonstrate method signatures without executing (server is offline)
    print("\n[3] Available client methods:")
    methods = [
        ("health_check()",                         "GET /health"),
        ("list_matches()",                         "GET /matches"),
        ("get_match(match_id)",                    "GET /matches/{id}"),
        ("get_player_stats(player_id)",            "GET /players/{id}"),
        ("predict_win_probability(...)",           "GET /win_probability"),
        ("analyze_custom(query, params)",          "POST /analyze"),
        ("register_live_match(match_id, source)",  "POST /live/register"),
        ("ingest_live_delivery(...)",              "POST /live/ingest"),
        ("get_live_matches()",                     "GET /live/matches"),
    ]
    for method, route in methods:
        print(f"  client.{method:<42} → {route}")

    # ------------------------------------------------------------------
    # 4. If server is live, run a few calls
    # ------------------------------------------------------------------
    if healthy:
        print("\n[4] Live API calls:")
        try:
            health = client.health_check()
            print(f"  Health: {health}")

            matches = client.list_matches()
            print(f"  Matches available: {len(matches)}")

            prob = client.predict_win_probability(
                target=180,
                current_runs=95,
                wickets_down=3,
                overs_done=10.0,
                venue="Wankhede Stadium",
            )
            print(f"  Win probability: {prob}")
        except Exception as exc:
            print(f"  Error during live calls: {exc}")

    print("\nDone.")


if __name__ == "__main__":
    main()
