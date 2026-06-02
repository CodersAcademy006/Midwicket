"""Midwicket end-to-end - server + client in one process."""

import threading, time, sys, requests
import midwicket as md

md.init()
threading.Thread(target=lambda: md.serve(port=8001, reload=False), daemon=True).start()

URL = "http://localhost:8001"
for _ in range(10):
    try:
        requests.get(f"{URL}/health"); break
    except requests.ConnectionError:
        time.sleep(1)
else:
    sys.exit("server failed to start")

match_id = "demo_match_2026"
print(requests.post(f"{URL}/live/register", json={
    "match_id": match_id, "source": "demo_script",
    "metadata": {"venue": "Wankhede Stadium", "teams": ["MI", "CSK"]},
}).json())

for ball in range(1, 7):
    print(f"ball {ball}:", requests.post(f"{URL}/live/ingest", json={
        "match_id": match_id, "inning": 1, "over": 0, "ball": ball,
        "runs_total": ball, "wickets_fallen": 0, "target": None, "venue": "Wankhede Stadium",
    }).status_code)
    time.sleep(0.1)

print("live matches:", requests.get(f"{URL}/live/matches").json())
sys.exit(0)
