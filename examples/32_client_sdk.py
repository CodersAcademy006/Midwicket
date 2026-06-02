"""Midwicket REST client SDK - health check, client, method surface, live calls."""

from midwicket.client import connect, quick_health_check

URL = "http://localhost:8000"

print("server healthy:", quick_health_check(URL, timeout=3.0))
client = connect(URL, timeout=5.0)
print("client        :", client)

for method, route in [
    ("health_check()",                        "GET /health"),
    ("list_matches()",                        "GET /matches"),
    ("get_match(match_id)",                   "GET /matches/{id}"),
    ("get_player_stats(player_id)",           "GET /players/{id}"),
    ("predict_win_probability(...)",          "GET /win_probability"),
    ("analyze_custom(query, params)",         "POST /analyze"),
    ("register_live_match(match_id, source)", "POST /live/register"),
    ("ingest_live_delivery(...)",             "POST /live/ingest"),
    ("get_live_matches()",                    "GET /live/matches"),
]:
    print(f"  client.{method:<42} -> {route}")

try:
    print("health   :", client.health_check())
    print("matches  :", len(client.list_matches().get("items", [])))
    print("win_prob :", client.predict_win_probability(
        target=180, current_runs=95, wickets_down=3, overs_done=10.0, venue="Wankhede Stadium"))
except Exception:
    print("(server offline - start it with: uvicorn midwicket.serve.api:app --reload)")
