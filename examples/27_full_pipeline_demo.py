"""
27_full_pipeline_demo.py

Demonstrates the full end-to-end pipeline:
1. Starting the PyPitch Server
2. Registering a Live Match
3. Ingesting Delivery Data
4. Querying Live Matches

This script runs the server in a background thread to simulate a real deployment.
"""

import pypitch as pp
import threading
import time
import requests
import sys

def run_server():
    """Start the PyPitch server."""
    print("Starting server...")
    # In a real app, you would just call pp.serve()
    # Here we disable reload to avoid signal issues in threads
    pp.serve(port=8001, reload=False)

def run_client():
    """Simulate a client interacting with the server."""
    base_url = "http://localhost:8001"
    
    # Wait for server to start
    print("Waiting for server to start...")
    for _ in range(10):
        try:
            requests.get(f"{base_url}/health")
            break
        except requests.ConnectionError:
            time.sleep(1)
    else:
        print("Server failed to start.")
        return

    print("\n--- 1. Registering Live Match ---")
    match_id = "demo_match_2026"
    payload = {
        "match_id": match_id,
        "source": "demo_script",
        "metadata": {
            "venue": "Wankhede Stadium",
            "teams": ["MI", "CSK"]
        }
    }
    resp = requests.post(f"{base_url}/live/register", json=payload)
    print(f"Response: {resp.status_code} - {resp.json()}")

    print("\n--- 2. Ingesting Delivery Data ---")
    # Simulate an over
    for ball in range(1, 7):
        delivery = {
            "match_id": match_id,
            "inning": 1,
            "over": 0,
            "ball": ball,
            "runs_total": ball * 1, # 1 run per ball
            "wickets_fallen": 0,
            "target": None,
            "venue": "Wankhede Stadium"
        }
        resp = requests.post(f"{base_url}/live/ingest", json=delivery)
        print(f"Ball {ball}: {resp.status_code}")
        time.sleep(0.1)

    print("\n--- 3. Querying Live Matches ---")
    resp = requests.get(f"{base_url}/live/matches")
    matches = resp.json()
    print(f"Live Matches: {len(matches)}")
    for m in matches:
        print(f"- {m['match_id']} ({m['source']}): Last update {m.get('seconds_since_update', 0):.1f}s ago")

    print("\n--- Demo Complete ---")
    # In a real app, the server would keep running.
    # Here we just exit.
    sys.exit(0)

if __name__ == "__main__":
    # Initialize PyPitch
    pp.init()

    # Start server in background thread
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    # Run client
    run_client()
