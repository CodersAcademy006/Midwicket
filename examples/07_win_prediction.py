"""Win probability across three venue/chase scenarios."""

import midwicket.express as px

for s in [
    dict(venue="Eden Gardens",     target=180, current_score=150, wickets_down=3, overs_done=16.0),
    dict(venue="Wankhede Stadium", target=165, current_score=80,  wickets_down=5, overs_done=12.0),
    dict(venue="Chinnaswamy",      target=200, current_score=50,  wickets_down=7, overs_done=10.0),
]:
    print(s["venue"], f"{s['target']}/{s['current_score']}/{s['wickets_down']} @ {s['overs_done']}ov ->", px.predict_win(**s))
