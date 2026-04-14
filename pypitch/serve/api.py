"""
PyPitch Serve Plugin: REST API Deployment

One-command deployment of PyPitch as a REST API.
Perfect for enterprise engineers and startups.
"""
from typing import Dict, Any, Optional, List
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
from datetime import date as _date_type
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
class PlayerLookupRequest(BaseModel):
    name: str
    match_date: Optional[_date_type] = None  # ISO date string; used for historical alias resolution

class VenueLookupRequest(BaseModel):
    name: str
    match_date: Optional[_date_type] = None

class MatchupRequest(BaseModel):
    batter: str
    bowler: str
    match_date: Optional[_date_type] = None  # pass match date for correct historical resolution

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
            if request.url.path in ["/v1/docs", "/v1/redoc", "/v1/openapi.json", "/health", "/_internal/health", "/"]:
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
        """Lookup player by name via identity registry.

        Uses ``request.match_date`` (date object) when present so that
        historical alias resolution is correct for old data queries.
        Falls back to today only if no date is supplied — callers should
        always supply a date for historical data queries.
        """
        from datetime import date as _date
        explicit_date = getattr(request, "match_date", None)
        if explicit_date is None:
            logger.warning(
                "lookup_player: no match_date supplied for %r — "
                "falling back to today(), which may give incorrect results for historical data.",
                request.name,
            )
        resolve_date = explicit_date or _date.today()
        try:
            player_id = self.session.registry.resolve_player(
                request.name, resolve_date
            )
            stats = self.session.registry.get_player_stats(player_id)
            return {
                "player_name": request.name,
                "player_id": player_id,
                "found": True,
                "stats": stats or {},
                "resolved_as_of": resolve_date.isoformat(),
            }
        except Exception as exc:
            logger.warning("lookup_player(%r) failed: %s", request.name, exc)
            return {"player_name": request.name, "found": False}

    def lookup_venue(self, request):
        """Lookup venue by name via identity registry.

        Uses ``request.match_date`` when present for correct historical alias
        resolution (e.g. venue renames across seasons).
        Logs a warning when falling back to today() for historical queries.
        """
        from datetime import date as _date
        explicit_date = getattr(request, "match_date", None)
        if explicit_date is None:
            logger.warning(
                "lookup_venue: no match_date supplied for %r — "
                "falling back to today(), which may be incorrect for historical data.",
                request.name,
            )
        resolve_date = explicit_date or _date.today()
        try:
            venue_id = self.session.registry.resolve_venue(
                request.name, resolve_date
            )
            stats = self.session.registry.get_venue_stats(venue_id)
            return {
                "venue_name": request.name,
                "venue_id": venue_id,
                "found": True,
                "stats": stats or {},
                "resolved_as_of": resolve_date.isoformat(),
            }
        except Exception as exc:
            logger.warning("lookup_venue(%r) failed: %s", request.name, exc)
            return {"venue_name": request.name, "found": False}

    def get_matchup_stats(self, request):
        """Get head-to-head matchup stats from registry.

        Uses ``request.match_date`` when present so resolution is historically
        correct (avoids mis-resolving a player who was known under a different
        alias in an earlier season).
        Logs a warning when falling back to today() without explicit date.
        """
        from datetime import date as _date
        explicit_date = getattr(request, "match_date", None)
        if explicit_date is None:
            logger.warning(
                "get_matchup_stats: no match_date supplied for %r vs %r — "
                "falling back to today(); pass match_date for accurate historical resolution.",
                request.batter, request.bowler,
            )
        resolve_date = explicit_date or _date.today()
        try:
            reg = self.session.registry
            b_id = reg.resolve_player(request.batter, resolve_date)
            bo_id = reg.resolve_player(request.bowler, resolve_date)
            stats = reg.get_matchup_stats(b_id, bo_id)
            return {
                "batter": request.batter,
                "bowler": request.bowler,
                "found": stats is not None,
                "stats": stats or {},
                "resolved_as_of": resolve_date.isoformat(),
            }
        except Exception as exc:
            logger.warning("get_matchup_stats failed: %s", exc)
            return {"batter": request.batter, "bowler": request.bowler, "found": False, "stats": {}}

    def get_fantasy_points(self, request):
        """Return per-match fantasy point estimate computed from ball_events."""
        from pypitch.api.fantasy import fantasy_score
        season = getattr(request, "season", None)
        try:
            result = fantasy_score(request.player_name, season=season)
            return {
                "player": request.player_name,
                "season": result.get("season", season or "all"),
                "points": result.get("per_match_avg", 0.0),
                "total_pts": result.get("total_pts", 0.0),
                "matches": result.get("matches", 0),
                "batting_breakdown": result.get("batting_breakdown", {}),
                "bowling_breakdown": result.get("bowling_breakdown", {}),
            }
        except Exception as exc:
            logger.warning("get_fantasy_points(%r) failed: %s", request.player_name, exc)
            return {"player": request.player_name, "points": 0.0, "season": season or "all"}

    def get_player_stats(self, request):
        """Get player career stats via player_analytics."""
        from pypitch.api.player_analytics import career_batting, career_bowling
        try:
            batting = career_batting(request.player_name)
            bowling = career_bowling(request.player_name)
            return {
                "player": request.player_name,
                "stats": {"batting": batting, "bowling": bowling},
            }
        except Exception as exc:
            logger.warning("get_player_stats(%r) failed: %s", request.player_name, exc)
            return {"player": request.player_name, "stats": {}}

    def register_live_match(self, request):
        """Register a match for live tracking via ingestor."""
        if self.ingestor is None:
            return {"match_id": request.match_id, "registered": False, "error": "ingestor not running"}
        try:
            ok = self.ingestor.register_match(
                request.match_id,
                source=getattr(request, "source", "webhook"),
                metadata=getattr(request, "metadata", {}),
            )
            return {"match_id": request.match_id, "registered": ok}
        except Exception as exc:
            logger.warning("register_live_match failed: %s", exc)
            return {"match_id": request.match_id, "registered": False, "error": str(exc)}

    def ingest_delivery_data(self, request):
        """Ingest a live delivery into the active match via ingestor."""
        if self.ingestor is None:
            return {"match_id": request.match_id, "ingested": False, "error": "ingestor not running"}
        try:
            self.ingestor.update_match_data(
                request.match_id,
                delivery_data=getattr(request, "delivery", {}),
            )
            return {"match_id": request.match_id, "ingested": True}
        except Exception as exc:
            logger.warning("ingest_delivery_data failed: %s", exc)
            return {"match_id": request.match_id, "ingested": False, "error": str(exc)}

    def get_live_matches(self):
        """Return live match list from ingestor."""
        if self.ingestor is None:
            return {"matches": []}
        try:
            return {"matches": self.ingestor.get_live_matches()}
        except Exception as exc:
            logger.warning("get_live_matches failed: %s", exc)
            return {"matches": []}

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
                except (RuntimeError, OSError, AttributeError):
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
            try:
                result = self.session.engine.execute_sql(
                    "SELECT DISTINCT match_id FROM ball_events ORDER BY match_id"
                )
                rows = result.to_pydict()
                match_ids = rows.get("match_id", [])
                return {"matches": match_ids, "count": len(match_ids)}
            except (RuntimeError, AttributeError, TypeError):
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

        @self.app.get("/teams/{team_id}")
        async def get_team_stats(team_id: str, authenticated: bool = Depends(verify_api_key)):
            """Get statistics for a specific team."""
            try:
                result = self.session.engine.execute_sql(
                    """
                    SELECT
                        batting_team AS team,
                        COUNT(DISTINCT match_id) AS matches,
                        SUM(runs_batter + runs_extras) AS total_runs,
                        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS total_wickets
                    FROM ball_events
                    WHERE LOWER(batting_team) = LOWER(?)
                    GROUP BY batting_team
                    """,
                    [team_id],
                )
                rows = result.to_pydict()
                if not rows.get("team"):
                    raise HTTPException(status_code=404, detail="Team not found")
                return {k: rows[k][0] for k in rows}
            except HTTPException:
                raise
            except (RuntimeError, AttributeError, TypeError, ValueError) as e:
                logger.warning("get_team_stats(%s) failed: %s", team_id, e)
                raise HTTPException(status_code=404, detail="Team not found")

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
            target: int = Query(150, gt=0, le=720, description="Target score (1-720)"),
            current_runs: int = Query(50, ge=0, le=720, description="Runs scored so far"),
            wickets_down: int = Query(2, ge=0, le=10, description="Wickets fallen (0-10)"),
            overs_done: float = Query(10.0, ge=0.0, le=20.0, description="Overs completed (0-20)"),
            authenticated: bool = Depends(verify_api_key),
        ):
            """Calculate win probability for current match state."""
            if current_runs > target:
                raise HTTPException(status_code=400, detail="current_runs cannot exceed target")
            try:
                result = wp_func(
                    target=target,
                    current_runs=current_runs,
                    wickets_down=wickets_down,
                    overs_done=overs_done,
                )
                return result
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
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
                # Accept both "sql" and legacy "query" key
                sql = (query.get("sql") or query.get("query") or "").strip()
                if not sql:
                    raise HTTPException(status_code=400, detail="SQL query required")

                # Validate params is a list when provided
                params = query.get("params")
                if params is not None and not isinstance(params, list):
                    raise HTTPException(status_code=400, detail="params must be a list of positional values")

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

                result = self.session.engine.execute_sql(safe_sql, params=params, read_only=True)
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

        @self.app.get("/v1/players/{player_name}/fantasy")
        async def player_fantasy(
            player_name: str,
            season: Optional[str] = None,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Per-match fantasy point estimate using standard T20 scoring rules."""
            from pypitch.api.fantasy import fantasy_score
            try:
                return fantasy_score(player_name, season=season)
            except Exception as e:
                logger.warning("player_fantasy failed for %s: %s", player_name, e)
                raise HTTPException(status_code=500, detail="Internal server error")

        @self.app.get("/v1/venues/{venue_name}/fantasy")
        async def venue_fantasy(
            venue_name: str,
            authenticated: bool = Depends(verify_api_key),
        ):
            """Fantasy cheat sheet + venue bias (bat-first vs chase win%) for a venue."""
            from pypitch.api.fantasy import cheat_sheet, venue_bias
            try:
                bias = venue_bias(venue_name)
                top_players = cheat_sheet(venue_name)
                return {
                    "venue": venue_name,
                    "venue_bias": bias,
                    "top_players": top_players.to_dict(orient="records") if not top_players.empty else [],
                }
            except Exception as e:
                logger.warning("venue_fantasy failed for %s: %s", venue_name, e)
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

        # ------------------------------------------------------------------
        # Identity resolution endpoints — typed routes with explicit date
        # ------------------------------------------------------------------

        @self.app.get("/v1/players/resolve")
        async def resolve_player(
            name: str = Query(..., description="Player name as stored in registry"),
            match_date: _date_type = Query(
                ...,
                description="ISO date of the match/query (YYYY-MM-DD). "
                            "Required for accurate historical alias resolution.",
            ),
            authenticated: bool = Depends(verify_api_key),
        ):
            """Resolve a player name to its canonical entity ID.

            Pass ``match_date`` for any historical queries — without it the
            resolution uses today's date which may return the wrong alias for
            players who changed teams or were renamed across seasons.
            """

            class _Req:
                pass

            req = _Req()
            req.name = name
            req.match_date = match_date
            return self.lookup_player(req)

        @self.app.get("/v1/venues/resolve")
        async def resolve_venue(
            name: str = Query(..., description="Venue name as stored in registry"),
            match_date: _date_type = Query(
                ...,
                description="ISO date of the match (YYYY-MM-DD). "
                            "Required for venues that were renamed across seasons.",
            ),
            authenticated: bool = Depends(verify_api_key),
        ):
            """Resolve a venue name to its canonical entity ID."""

            class _Req:
                pass

            req = _Req()
            req.name = name
            req.match_date = match_date
            return self.lookup_venue(req)

        @self.app.get("/v1/matchup")
        async def matchup_stats(
            batter: str = Query(..., description="Batter name"),
            bowler: str = Query(..., description="Bowler name"),
            match_date: _date_type = Query(
                ...,
                description="ISO date for historical alias resolution (YYYY-MM-DD). "
                            "Required for accurate historical data queries.",
            ),
            authenticated: bool = Depends(verify_api_key),
        ):
            """Return head-to-head matchup stats from the registry.

            Passing ``match_date`` ensures the batter and bowler names are
            resolved against aliases that were valid on that date.
            """

            class _Req:
                pass

            req = _Req()
            req.batter = batter
            req.bowler = bowler
            req.match_date = match_date
            return self.get_matchup_stats(req)

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