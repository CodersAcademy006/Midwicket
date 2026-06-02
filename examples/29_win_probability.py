"""Midwicket win probability - five chase scenarios, no session needed."""

from midwicket.compute.winprob import win_probability

for label, kw in [
    ("180/195 |  95/3 @ 10ov",     dict(target=180, current_runs=95,  wickets_down=3, overs_done=10.0)),
    ("180/195 | 120/5 @ 15ov",     dict(target=180, current_runs=120, wickets_down=5, overs_done=15.0)),
    ("180/195 |  50/7 @ 15ov",     dict(target=180, current_runs=50,  wickets_down=7, overs_done=15.0)),
    ("180/195 | 181/4 chase done", dict(target=180, current_runs=181, wickets_down=4, overs_done=19.3)),
    ("180/195 |  20/0 @  3ov",     dict(target=180, current_runs=20,  wickets_down=0, overs_done=3.0)),
]:
    print(f"{label:<32}", win_probability(**kw, venue=None))
