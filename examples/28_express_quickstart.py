"""Midwicket Express — three calls, one screen."""

import midwicket.express as px

print(px.get_player_stats("V Kohli"))
print(px.get_matchup("V Kohli", "JJ Bumrah"))
print(px.predict_win(venue="Wankhede Stadium", target=180,
                     current_score=95, wickets_down=3, overs_done=10.0))
