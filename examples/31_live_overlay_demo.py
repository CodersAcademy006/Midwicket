"""Midwicket live overlay - start server, push 3 stat updates. OBS Browser Source: http://localhost:8765/overlay"""

import time
from midwicket.live.overlay import OverlayServer, LiveStats

server = OverlayServer(match_id="demo_ipl_2024", port=8765)
server.start()

for stats in [
    LiveStats(match_id="demo_ipl_2024", current_over=6.0,  current_score=58,  wickets_fallen=1,
              run_rate=9.67, required_rr=8.4, batsman_on_strike="V Kohli",  bowler="JJ Bumrah",
              last_ball="4",
              recent_overs=["1,0,4,1,2,W", "6,1,0,1,4,1", "2,0,1,6,1,0",
                            "4,1,1,0,2,1", "1,6,0,4,1,1", "2,1,4,0,W,4"]),
    LiveStats(match_id="demo_ipl_2024", current_over=10.0, current_score=98,  wickets_fallen=2,
              run_rate=9.8,  required_rr=8.2, batsman_on_strike="KL Rahul", bowler="R Ashwin",      last_ball="1"),
    LiveStats(match_id="demo_ipl_2024", current_over=15.0, current_score=145, wickets_fallen=3,
              run_rate=9.67, required_rr=7.0, batsman_on_strike="KL Rahul", bowler="Hardik Pandya", last_ball="6"),
]:
    server.update_stats(stats)
    print(server.get_stats_json())
    time.sleep(2)

server.stop()
