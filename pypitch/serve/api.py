"""
PyPitch Serve Plugin: REST API Deployment

One-command deployment of PyPitch as a REST API.
Perfect for enterprise engineers and startups.
"""
from typing import Dict, Any, Optional, List
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
import json
from pathlib import Path
import time
import logging

from pypitch.live.ingestor import StreamIngestor
from pypitch.serve.auth import verify_api_key
from pypitch.serve.rate_limit import check_rate_limit, rate_limiter, get_client_key
from pypitch.serve.monitoring import record_request_metrics, record_error_metrics, metrics_collector
from pypitch.config import API_CORS_ORIGINS, is_production
from pypitch.api.session import PyPitchSession
from pypitch.compute.winprob import win_probability as wp_func

logger = logging.getLogger(__name__)

# Pydantic models for request validation
class LiveMatchRegistration(BaseModel):
    match_id: str
    source: str
    metadata: Optional[Dict[str, Any]] = None

class DeliveryData(BaseModel):
    match_id: str
    inning: int
    over: int
    ball: int
    runs_total: int
    wickets_fallen: int
    target: Optional[int] = None
    venue: Optional[str] = None
    timestamp: Optional[float] = None

class PyPitchAPI:
    """
    FastAPI wrapper for PyPitch deployment.

    Automatically creates endpoints for common operations.
    """

    ingestor: Optional["StreamIngestor"]

    def __init__(self, session=None, *, start_ingestor: bool = True) -> None:
        """
        Initialize the PyPitch API.

        Args:
            session: PyPitch session instance. If None, uses singleton.
            start_ingestor: Whether to start the live ingestor (disable for testing).
        """
        # Disable interactive docs in production — they expose the full API
        # surface and schema to unauthenticated users.  Set PYPITCH_ENV=development
        # (the default) to enable them locally.
        _prod = is_production()
        self.app = FastAPI(
            title="PyPitch API",
            description="Cricket Analytics API powered by PyPitch",
            version="1.0.0",
            docs_url=None if _prod else "/v1/docs",
            redoc_url=None if _prod else "/v1/redoc",
            openapi_url=None if _prod else "/v1/openapi.json",
        )

        # CORS — never default to wildcard; require explicit config in production.
        # When origins is empty (default) the middleware allows no cross-origin requests.
        origins = [o for o in API_CORS_ORIGINS if o and o != "*"]
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=bool(origins),  # credentials forbidden with wildcard
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type"],
        )

        # TrustedHost guard — protects against Host-header injection in
        # reverse-proxy deployments.  PYPITCH_ALLOWED_HOSTS is comma-separated;
        # defaults to localhost-only when not set.
        import os as _os
        _allowed_hosts_raw = _os.getenv("PYPITCH_ALLOWED_HOSTS", "localhost,127.0.0.1")
        _allowed_hosts = [h.strip() for h in _allowed_hosts_raw.split(",") if h.strip()]
        # In testing mode, also allow the "testserver" host used by FastAPI TestClient
        if _os.getenv("PYPITCH_ENV") == "testing" and "testserver" not in _allowed_hosts:
            _allowed_hosts.append("testserver")
        if _allowed_hosts:
            self.app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts)

        # Add rate limiting middleware
        @self.app.middleware("http")
        async def rate_limit_middleware(request: Request, call_next):
            # Skip rate limiting for docs and health endpoints
            if request.url.path in ["/v1/docs", "/v1/redoc", "/v1/openapi.json", "/health", "/"]:
                return await call_next(request)

            await check_rate_limit(request)
            return await call_next(request)

        # Add security headers
        @self.app.middleware("http")
        async def add_security_headers(request: Request, call_next):
            response = await call_next(request)
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

            # Add rate limit headers
            client_key = get_client_key(request)
            remaining = rate_limiter.get_remaining_requests(client_key)
            reset_time = rate_limiter.get_reset_time(client_key)

            response.headers["X-RateLimit-Limit"] = str(rate_limiter.requests_per_minute)
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(int(time.time() + reset_time))

            return response

        # Add request logging
        @self.app.middleware("http")
        async def log_requests(request: Request, call_next):
            start_time = time.time()
            response = await call_next(request)
            process_time = time.time() - start_time

            # Record metrics
            record_request_metrics(
                method=request.method,
                endpoint=request.url.path,
                status_code=response.status_code,
                duration=process_time
            )

            logger.info(f"{request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s")
            return response

        # Initialize session
        if session is None:
            self.session = PyPitchSession.get()
        else:
            self.session = session

        # Initialize Live Ingestor (conditionally)
        if start_ingestor and getattr(self.session, 'engine', None) is not None:
            self.ingestor = StreamIngestor(self.session.engine)
            # Start the ingestor background threads
            self.ingestor.start()
        else:
            self.ingestor = None

        self._setup_routes()

        # Add exception handlers for monitoring
        @self.app.exception_handler(HTTPException)
        async def http_exception_handler(request: Request, exc: HTTPException):
            record_error_metrics("HTTPException", str(exc.detail))
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail}
            )

        @self.app.exception_handler(Exception)
        async def general_exception_handler(request: Request, exc: Exception):
            record_error_metrics("Exception", str(exc))
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error"}
            )

    def close(self):
        """Explicitly close and cleanup resources."""
        if hasattr(self, 'ingestor') and self.ingestor is not None:
            self.ingestor.stop()
            self.ingestor = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        self.close()

    def predict_win_probability(self, request):
        """Calculate win probability for current match state."""
        try:
            result = wp_func(
                target=request.target,
                current_runs=request.current_runs,
                wickets_down=request.wickets_down,
                overs_done=request.overs_done
            )
            return result
        except Exception as e:
            raise Exception(f"Win probability calculation failed: {str(e)}")

    def lookup_player(self, request):
        """Lookup player information."""
        try:
            # This would need to be implemented based on your registry
            # For now, return a placeholder
            return {"player_name": request.name, "found": False}
        except Exception as e:
            raise Exception(f"Player lookup failed: {str(e)}")

    def lookup_venue(self, request):
        """Lookup venue information."""
        try:
            # This would need to be implemented based on your data
            # For now, return a placeholder
            return {"venue_name": request.name, "found": False}
        except Exception as e:
            raise Exception(f"Venue lookup failed: {str(e)}")

    def get_matchup_stats(self, request):
        """Get matchup statistics between batter and bowler."""
        try:
            # This would need to be implemented based on your data
            # For now, return a placeholder
            return {
                "batter": request.batter,
                "bowler": request.bowler,
                "matches": 0,
                "stats": {}
            }
        except Exception as e:
            raise Exception(f"Matchup stats retrieval failed: {str(e)}")

    def get_fantasy_points(self, request):
        """Calculate fantasy points for a player."""
        try:
            # This would need to be implemented based on your fantasy logic
            # For now, return a placeholder
            return {"player": request.player_name, "points": 0}
        except Exception as e:
            raise Exception(f"Fantasy points calculation failed: {str(e)}")

    def get_player_stats(self, request):
        """Get player statistics with filters."""
        try:
            # This would need to be implemented based on your stats logic
            # For now, return a placeholder
            return {"player": request.player_name, "stats": {}}
        except Exception as e:
            raise Exception(f"Player stats retrieval failed: {str(e)}")

    def register_live_match(self, request):
        """Register a match for live tracking."""
        try:
            # This would need to be implemented based on your live tracking
            # For now, return a placeholder
            return {"match_id": request.match_id, "registered": True}
        except Exception as e:
            raise Exception(f"Live match registration failed: {str(e)}")

    def ingest_delivery_data(self, request):
        """Ingest live delivery data."""
        try:
            # This would need to be implemented based on your live ingestion
            # For now, return a placeholder
            return {"match_id": request.match_id, "ingested": True}
        except Exception as e:
            raise Exception(f"Delivery data ingestion failed: {str(e)}")

    def get_live_matches(self):
        """Get list of currently live matches."""
        try:
            # This would need to be implemented based on your live tracking
            # For now, return a placeholder
            return {"matches": []}
        except Exception as e:
            raise Exception(f"Live matches retrieval failed: {str(e)}")

    def get_health_status(self):
        """Get health status of the API."""
        try:
            # Check database connectivity
            db_status = "healthy"
            active_connections = 0
            try:
                # Simple query to test DB connection
                self.session.engine.execute_sql("SELECT 1")
                active_connections = getattr(self.session.engine, '_active_connections', 0)
            except Exception:
                db_status = "unhealthy"

            return {
                "status": "healthy",
                "version": "1.0.0",
                "uptime_seconds": 0,  # Would need to track actual uptime
                "database_status": db_status,
                "active_connections": active_connections
            }
        except Exception as e:
            raise Exception(f"Health check failed: {str(e)}")

    def _setup_routes(self):
        """Setup all API routes."""

        # ── Internal health probe (no auth) — for Docker / k8s healthchecks ──
        # Bind this path to the PYPITCH_ALLOWED_HOSTS guard but not to auth so
        # that container orchestrators can probe liveness without an API key.
        @self.app.get("/_internal/health", include_in_schema=False)
        async def internal_health():
            """Unauthenticated liveness probe for orchestrators."""
            return {"status": "ok"}

        @self.app.get("/")
        async def root(authenticated: bool = Depends(verify_api_key)):
            """API root with available endpoints."""
            return {
                "message": "PyPitch API is running",
                "version": "1.0.0",
                "endpoints": {
                    "GET /": "This help message",
                    "GET /health": "Health check endpoint",
                    "GET /matches": "List available matches",
                    "GET /matches/{match_id}": "Get match details",
                    "GET /players/{player_id}": "Get player statistics",
                    "GET /teams/{team_id}": "Get team statistics",
                    "POST /analyze": "Run custom analysis",
                    "GET /win_probability": "Calculate win probability"
                }
            }

        @self.app.get("/v1/health")
        async def health_check_v1(authenticated: bool = Depends(verify_api_key)):
            """Health check endpoint (v1)."""
            try:
                # Check database connectivity
                db_status = "healthy"
                active_connections = 0
                try:
                    # Simple query to test DB connection
                    self.session.engine.execute_sql("SELECT 1")
                    active_connections = getattr(self.session.engine, '_active_connections', 0)
                except Exception:
                    db_status = "unhealthy"

                return {
                    "status": "healthy",
                    "version": "1.0.0",
                    "uptime_seconds": 0,  # Would need to track actual uptime
                    "database_status": db_status,
                    "active_connections": active_connections
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

        # Keep the old endpoint for backward compatibility
        @self.app.get("/health")
        async def health_check_legacy(authenticated: bool = Depends(verify_api_key)):
            """Health check endpoint (legacy)."""
            return await health_check_v1()

        @self.app.get("/v1/metrics")
        async def get_metrics(authenticated: bool = Depends(verify_api_key)):
            """Get API and system metrics."""

            api_metrics = metrics_collector.get_api_metrics()
            system_metrics = metrics_collector.get_system_metrics()

            return {
                "api": api_metrics,
                "system": system_metrics,
                "timestamp": time.time()
            }

        @self.app.get("/matches")
        async def list_matches(authenticated: bool = Depends(verify_api_key)):
            """List all available matches."""
            return {"matches": [], "count": 0}

        @self.app.get("/matches/{match_id}")
        async def get_match(match_id: str, authenticated: bool = Depends(verify_api_key)):
            """Get details for a specific match."""
            try:
                self.session.load_match(match_id)

                sql = """
                    SELECT
                        inning,
                        MAX(over) as overs,
                        SUM(runs_batter + runs_extras) as runs,
                        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
                    FROM ball_events
                    WHERE match_id = ?
                    GROUP BY inning
                """
                result = self.session.engine.execute_sql(sql, [match_id])
                df = result.to_pandas()
                return {"match_id": match_id, "innings": df.to_dict("records")}

            except HTTPException:
                raise
            except Exception as e:
                logger.warning("get_match(%s) failed: %s", match_id, e)
                raise HTTPException(status_code=404, detail="Match not found")

        @self.app.get("/players/{player_id}")
        async def get_player_stats(player_id: int, authenticated: bool = Depends(verify_api_key)):
            """Get statistics for a specific player."""
            try:
                stats = self.session.registry.get_player_stats(player_id)
                if stats:
                    return stats
                raise HTTPException(status_code=404, detail="Player not found")
            except HTTPException:
                raise
            except Exception as e:
                logger.warning("get_player_stats(%s) failed: %s", player_id, e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/win_probability")
        async def win_probability(
            target: int = 150,
            current_runs: int = 50,
            wickets_down: int = 2,
            overs_done: float = 10.0,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Calculate win probability for current match state."""
            try:
                result = wp_func(
                    target=target,
                    current_runs=current_runs,
                    wickets_down=wickets_down,
                    overs_done=overs_done,
                )
                return result
            except Exception as e:
                logger.warning("win_probability failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        _MAX_ANALYZE_ROWS = 100
        _ANALYZE_ROW_LIMIT = 500  # enforced at SQL level to prevent full-scan materialisation

        @self.app.post("/analyze")
        async def custom_analysis(query: Dict[str, Any], authenticated: bool = Depends(verify_api_key)):
            """Run a read-only SELECT query against ball_events."""
            import os as _os
            from pypitch.serve.sql_guard import validate_read_only_query, SQLValidationError
            if not _os.getenv("PYPITCH_ANALYZE_ENABLED", "false").lower() == "true":
                raise HTTPException(
                    status_code=403,
                    detail="Custom SQL analysis is disabled. Set PYPITCH_ANALYZE_ENABLED=true to enable.",
                )
            try:
                sql = query.get("sql", "").strip()
                if not sql:
                    raise HTTPException(status_code=400, detail="SQL query required")

                # C1: Use dedicated sql_guard validator — strict keyword blocklist,
                # comment stripping, single-statement enforcement, complexity bounds.
                try:
                    sql = validate_read_only_query(
                        sql,
                        max_selects=5,
                        max_joins=8,
                        max_unions=3,
                    )
                except SQLValidationError as exc:
                    raise HTTPException(status_code=403, detail=str(exc))

                # Inject a hard row-limit to prevent full-scan memory exhaustion.
                safe_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {_ANALYZE_ROW_LIMIT}"  # nosec B608 – sql_guard validated above

                result = self.session.engine.execute_sql(safe_sql, read_only=True)
                rows = result.to_pydict()
                keys = list(rows.keys())
                n = min(len(rows[keys[0]]) if keys else 0, _MAX_ANALYZE_ROWS)
                records = [{k: rows[k][i] for k in keys} for i in range(n)]

                return {"rows": n, "data": records}

            except HTTPException:
                raise
            except Exception as e:
                logger.warning("custom_analysis failed: %s", e)
                raise HTTPException(status_code=500, detail="Query execution failed")

        @self.app.post("/live/register")
        async def register_live_match(
            request: LiveMatchRegistration,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Register a match for live tracking."""
            try:
                if self.ingestor is None:
                    logger.warning("register_live_match: ingestor not available for match_id=%s", request.match_id)
                    return {"success": True, "match_id": request.match_id}

                success = self.ingestor.register_match(
                    match_id=request.match_id,
                    source=request.source,
                    metadata=request.metadata or {},
                )
                if not success:
                    raise HTTPException(status_code=409, detail="Match already registered")
                return {"success": True, "match_id": request.match_id}
            except HTTPException:
                raise
            except Exception as e:
                logger.warning("register_live_match failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.post("/live/ingest")
        async def ingest_delivery(
            data: DeliveryData,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Ingest live delivery data."""
            try:
                if self.ingestor is None:
                    logger.warning("ingest_delivery: ingestor not available for match_id=%s", data.match_id)
                    return {"success": True}

                delivery_dict = data.model_dump(exclude_none=True)
                match_id = delivery_dict.pop("match_id")
                self.ingestor.update_match_data(match_id, delivery_dict)
                return {"success": True}
            except HTTPException:
                raise
            except Exception as e:
                from pypitch.exceptions import DataIngestionError
                if isinstance(e, DataIngestionError):
                    raise HTTPException(status_code=429, detail=str(e))
                logger.warning("ingest_delivery failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/live/matches")
        async def get_live_matches(authenticated: bool = Depends(verify_api_key)):
            """Get list of currently live matches."""
            try:
                if self.ingestor is None:
                    logger.info("get_live_matches: ingestor not available")
                    return []

                return self.ingestor.get_live_matches()
            except Exception as e:
                logger.warning("get_live_matches failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        # ------------------------------------------------------------------
        # Player Analytics endpoints (PA-01 to PA-28)
        # ------------------------------------------------------------------

        @self.app.get("/v1/players/{player_name}/batting")
        async def player_batting(
            player_name: str,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Career batting + phase/venue/season/form/situation breakdown."""
            from pypitch.api.player_analytics import (
                career_batting, batting_by_phase, batting_by_venue,
                batting_by_season, batting_form, batting_in_chases,
                batting_under_pressure, death_over_specialist,
                batting_by_innings_number, weakness_detector,
            )
            try:
                return {
                    "career": career_batting(player_name),
                    "by_phase": batting_by_phase(player_name),
                    "by_venue": batting_by_venue(player_name),
                    "by_season": batting_by_season(player_name),
                    "form_last5": batting_form(player_name),
                    "chases": batting_in_chases(player_name),
                    "under_pressure": batting_under_pressure(player_name),
                    "death_specialist": death_over_specialist(player_name),
                    "innings_split": batting_by_innings_number(player_name),
                    "weaknesses": weakness_detector(player_name),
                }
            except Exception as e:
                logger.warning("player_batting failed for %s: %s", player_name, e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/players/{player_name}/bowling")
        async def player_bowling(
            player_name: str,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Career bowling + phase/venue/season/form breakdown."""
            from pypitch.api.player_analytics import (
                career_bowling, bowling_by_phase, bowling_by_venue,
                bowling_by_season, bowling_form,
            )
            try:
                return {
                    "career": career_bowling(player_name),
                    "by_phase": bowling_by_phase(player_name),
                    "by_venue": bowling_by_venue(player_name),
                    "by_season": bowling_by_season(player_name),
                    "form_last5": bowling_form(player_name),
                }
            except Exception as e:
                logger.warning("player_bowling failed for %s: %s", player_name, e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/players/{player_name}/milestones")
        async def player_milestones(
            player_name: str,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Highest scores, best bowling figures, streaks, ducks."""
            from pypitch.api.player_analytics import (
                highest_score, best_bowling_figures,
                match_streaks, milestones_and_failures,
            )
            try:
                return {
                    "highest_scores": highest_score(player_name),
                    "best_bowling_figures": best_bowling_figures(player_name),
                    "streaks": match_streaks(player_name),
                    "failures": milestones_and_failures(player_name),
                }
            except Exception as e:
                logger.warning("player_milestones failed for %s: %s", player_name, e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/players/{player_name}/vs-team/{team_name}")
        async def player_vs_team(
            player_name: str,
            team_name: str,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Batting and bowling vs specific opposition."""
            from pypitch.api.player_analytics import batting_vs_teams, bowling_vs_teams
            try:
                bat_all = batting_vs_teams(player_name)
                bowl_all = bowling_vs_teams(player_name)
                bat = next((r for r in bat_all if team_name.lower() in r["opposition"].lower()), None)
                bowl = next((r for r in bowl_all if team_name.lower() in r["opposition"].lower()), None)
                return {
                    "player": player_name,
                    "team": team_name,
                    "batting": bat or {"message": "no data"},
                    "bowling": bowl or {"message": "no data"},
                }
            except Exception as e:
                logger.warning("player_vs_team failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/players/compare")
        async def compare_players_endpoint(
            p1: str,
            p2: str,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Side-by-side career comparison. ?p1=name&p2=name"""
            from pypitch.api.player_analytics import compare_players
            try:
                return compare_players(p1, p2)
            except Exception as e:
                logger.warning("compare_players failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/players/leaderboard/batting")
        async def batting_leaderboard_endpoint(
            sort_by: str = "runs",
            top_n: int = 10,
            min_balls: int = 30,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Batting leaderboard. sort_by: runs | average | strike_rate"""
            from pypitch.api.player_analytics import batting_leaderboard
            try:
                return batting_leaderboard(sort_by=sort_by, top_n=top_n, min_balls=min_balls)
            except Exception as e:
                logger.warning("batting_leaderboard failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/players/leaderboard/bowling")
        async def bowling_leaderboard_endpoint(
            sort_by: str = "wickets",
            top_n: int = 10,
            min_balls: int = 30,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Bowling leaderboard. sort_by: wickets | economy | bowling_average"""
            from pypitch.api.player_analytics import bowling_leaderboard
            try:
                return bowling_leaderboard(sort_by=sort_by, top_n=top_n, min_balls=min_balls)
            except Exception as e:
                logger.warning("bowling_leaderboard failed: %s", e)
                raise HTTPException(status_code=500, detail="Internal server error")

    def run(self, host: str = "0.0.0.0", port: int = 8000, reload: bool = False):  # nosec B104
        """Run the API server."""
        logger.info("Starting PyPitch API server at http://%s:%d", host, port)
        logger.info("API documentation at http://%s:%d/docs", host, port)

        uvicorn.run(
            self.app,
            host=host,
            port=port,
            reload=reload
        )

def create_app(session=None, *, start_ingestor: bool = True) -> FastAPI:
    """
    Create and return a FastAPI application instance.

    This is the main entry point for creating the PyPitch API app.
    Useful for testing, deployment, and integration with other ASGI apps.
    """
    api = PyPitchAPI(session=session, start_ingestor=start_ingestor)
    return api.app

def serve(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):  # nosec B104
    """
    One-command API deployment.

    Usage:
        from pypitch.serve import serve
        serve()  # Starts API at http://localhost:8000
    """
    with PyPitchAPI() as api:
        api.run(host=host, port=port, reload=reload)

def create_dockerfile(output_dir: str = "."):
    """
    Generate Dockerfile for containerized deployment.

    Creates a production-ready Docker setup.
    """
    dockerfile_content = '''
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose port
EXPOSE 8000

# Run the API
CMD ["python", "-c", "from pypitch.serve.api import serve; serve()"]
'''

    dockerignore_content = '''
__pycache__
*.pyc
*.pyo
*.pyd
.Python
env
venv
.venv
pip-log.txt
pip-delete-this-directory.txt
.tox
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.log
.git
.mypy_cache
.pytest_cache
.hypothesis
'''

    output_path = Path(output_dir)

    # Write Dockerfile
    with open(output_path / "Dockerfile", "w") as f:
        f.write(dockerfile_content.strip())

    # Write .dockerignore
    with open(output_path / ".dockerignore", "w") as f:
        f.write(dockerignore_content.strip())

    print(f"Docker files created in {output_path}")
    print("Build with: docker build -t pypitch-api .")
    print("Run with: docker run -p 8000:8000 pypitch-api")