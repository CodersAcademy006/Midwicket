"""
Midwicket API Load Test Suite

Run with:
    locust -f tests/load/locustfile.py --host=http://localhost:8000
    locust -f tests/load/locustfile.py --host=http://localhost:8000 --headless -u 50 -r 5 -t 60s

Environment variables:
    MIDWICKET_API_KEY: API key for authenticated endpoints
    LOAD_TEST_INCLUDE_WRITES: include write-heavy endpoints (default: false)
"""
import os
import random
from locust import HttpUser, task, between, events


API_KEY = os.getenv("MIDWICKET_API_KEY", "test-key-for-load-testing")
INCLUDE_WRITES = os.getenv("LOAD_TEST_INCLUDE_WRITES", "false").lower() == "true"

SAMPLE_PLAYER_NAMES = ["Virat", "Rohit", "Bumrah", "Dhoni", "Sharma", "Williamson"]
SAMPLE_VENUES = ["Wankhede", "Eden Gardens", "MCG", "Lord's", "Mumbai"]
SAMPLE_TEAMS = ["MI", "CSK", "RCB", "KKR", "DC"]


class MidwicketUser(HttpUser):
    """Simulates a typical API consumer."""

    wait_time = between(0.5, 2.0)

    def on_start(self) -> None:
        """Setup auth headers for all requests."""
        self.client.headers.update({
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        })

    @task(10)
    def health_check(self) -> None:
        with self.client.get("/health", catch_response=True, name="/health") as resp:
            if resp.status_code != 200:
                resp.failure(f"Health check returned {resp.status_code}")

    @task(8)
    def search_players(self) -> None:
        name = random.choice(SAMPLE_PLAYER_NAMES)
        with self.client.get(
            f"/v1/players/search?q={name}",
            catch_response=True,
            name="/v1/players/search",
        ) as resp:
            if resp.status_code not in (200, 404):
                resp.failure(f"Player search failed: {resp.status_code}")

    @task(6)
    def list_venues(self) -> None:
        with self.client.get(
            "/v1/venues",
            catch_response=True,
            name="/v1/venues",
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"Venue list failed: {resp.status_code}")

    @task(5)
    def venue_resolve(self) -> None:
        venue = random.choice(SAMPLE_VENUES)
        with self.client.get(
            f"/v1/venues/resolve?name={venue}",
            catch_response=True,
            name="/v1/venues/resolve",
        ) as resp:
            if resp.status_code not in (200, 404):
                resp.failure(f"Venue resolve failed: {resp.status_code}")

    @task(4)
    def matchup_analysis(self) -> None:
        team_a = random.choice(SAMPLE_TEAMS)
        team_b = random.choice([t for t in SAMPLE_TEAMS if t != team_a])
        with self.client.get(
            f"/v1/matchup?team_a={team_a}&team_b={team_b}",
            catch_response=True,
            name="/v1/matchup",
        ) as resp:
            if resp.status_code not in (200, 404):
                resp.failure(f"Matchup failed: {resp.status_code}")

    @task(3)
    def win_probability(self) -> None:
        payload = {
            "target": random.randint(150, 220),
            "current_runs": random.randint(50, 150),
            "wickets_down": random.randint(1, 7),
            "overs_done": round(random.uniform(5.0, 18.0), 1),
        }
        with self.client.post(
            "/v1/predict/win",
            json=payload,
            catch_response=True,
            name="/v1/predict/win",
        ) as resp:
            if resp.status_code not in (200, 422):
                resp.failure(f"Win prediction failed: {resp.status_code}")

    @task(2)
    def analyze_query(self) -> None:
        """Read-only SQL query through the /analyze endpoint."""
        payload = {
            "sql": "SELECT COUNT(*) AS cnt FROM matches LIMIT 1",
            "params": [],
        }
        with self.client.post(
            "/v1/analyze",
            json=payload,
            catch_response=True,
            name="/v1/analyze",
        ) as resp:
            if resp.status_code not in (200, 400, 422):
                resp.failure(f"Analyze failed: {resp.status_code}")

    @task(1)
    def metrics_endpoint(self) -> None:
        with self.client.get("/metrics", catch_response=True, name="/metrics") as resp:
            if resp.status_code != 200:
                resp.failure(f"Metrics failed: {resp.status_code}")


class LiveIngestionUser(HttpUser):
    """Simulates a live ball-by-ball ingestion client. Only enabled if writes allowed."""

    wait_time = between(0.1, 0.5)

    def on_start(self) -> None:
        if not INCLUDE_WRITES:
            self.environment.runner.quit()
            return
        self.client.headers.update({
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        })
        self.match_id = f"load-test-match-{random.randint(1000, 9999)}"
        self._registered = False

    @task
    def ingest_delivery(self) -> None:
        if not self._registered:
            self._register_match()
            return

        delivery = {
            "match_id": self.match_id,
            "over": random.randint(1, 20),
            "ball": random.randint(1, 6),
            "batter": random.choice(SAMPLE_PLAYER_NAMES),
            "bowler": random.choice(SAMPLE_PLAYER_NAMES),
            "runs": random.choice([0, 1, 2, 4, 6]),
            "extras": 0,
            "wicket": False,
        }
        with self.client.post(
            "/v1/live/ingest",
            json=delivery,
            catch_response=True,
            name="/v1/live/ingest",
        ) as resp:
            if resp.status_code not in (200, 201, 202, 503):
                resp.failure(f"Live ingest failed: {resp.status_code}")

    def _register_match(self) -> None:
        payload = {
            "match_id": self.match_id,
            "team_a": "TeamA",
            "team_b": "TeamB",
            "venue": "LoadTest Ground",
            "format": "T20",
        }
        with self.client.post(
            "/v1/live/register",
            json=payload,
            catch_response=True,
            name="/v1/live/register",
        ) as resp:
            if resp.status_code in (200, 201, 202):
                self._registered = True
            elif resp.status_code == 503:
                resp.success()  # ingestor not configured — expected in test envs


@events.test_start.add_listener
def on_test_start(environment, **kwargs) -> None:
    print(f"Load test starting against {environment.host}")
    print(f"Include writes: {INCLUDE_WRITES}")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs) -> None:
    stats = environment.stats.total
    print("=" * 60)
    print("Load Test Summary")
    print("=" * 60)
    print(f"Total requests:   {stats.num_requests}")
    print(f"Total failures:   {stats.num_failures}")
    print(f"Median response:  {stats.median_response_time}ms")
    print(f"95th percentile:  {stats.get_response_time_percentile(0.95)}ms")
    print(f"99th percentile:  {stats.get_response_time_percentile(0.99)}ms")
    print(f"Requests/second:  {stats.total_rps:.2f}")
    print("=" * 60)
