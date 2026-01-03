"""
PyPitch Serve Plugin: REST API Deployment

One-command deployment of PyPitch as a REST API.
Perfect for enterprise engineers and startups.
"""
from typing import Dict, Any, Optional, List
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
import json
from pathlib import Path
import time
import logging

from pypitch.live.ingestor import StreamIngestor
from pypitch.serve.auth import verify_api_key
from pypitch.serve.rate_limit import check_rate_limit
from pypitch.serve.monitoring import record_request_metrics, record_error_metrics
from pypitch.config import API_CORS_ORIGINS

import logging

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

    def __init__(self, session=None, *, start_ingestor: bool = True) -> None:
        """
        Initialize the PyPitch API.

        Args:
            session: PyPitch session instance. If None, uses singleton.
            start_ingestor: Whether to start the live ingestor (disable for testing).
        """
        self.app = FastAPI(
            title="PyPitch API",
            description="Cricket Analytics API powered by PyPitch",
            version="1.0.0",
            docs_url="/v1/docs",
            redoc_url="/v1/redoc",
            openapi_url="/v1/openapi.json"
        )

        # Enable CORS for web applications
        origins = API_CORS_ORIGINS if API_CORS_ORIGINS != ["*"] else ["*"]
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["*"],
        )

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
            from pypitch.serve.rate_limit import rate_limiter, get_client_key
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
            from pypitch.api.session import PyPitchSession
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
            return await self.app.default_exception_handler(request, exc)

        @self.app.exception_handler(Exception)
        async def general_exception_handler(request: Request, exc: Exception):
            record_error_metrics("Exception", str(exc))
            return await self.app.default_exception_handler(request, exc)

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

    def __del__(self):
        """Cleanup resources as final fallback."""
        self.close()

    def predict_win_probability(self, request):
        """Calculate win probability for current match state."""
        try:
            from pypitch.compute.winprob import win_probability as wp_func
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

        @self.app.get("/")
        async def root():
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
            from pypitch.serve.monitoring import metrics_collector

            api_metrics = metrics_collector.get_api_metrics()
            system_metrics = metrics_collector.get_system_metrics()

            return {
                "api": api_metrics,
                "system": system_metrics,
                "timestamp": time.time()
            }

        @self.app.get("/matches")
        async def list_matches():
            """List all available matches."""
            try:
                # This would need to be implemented based on your data structure
                # For now, return a placeholder
                return {"matches": [], "count": 0}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/matches/{match_id}")
        async def get_match(match_id: str):
            """Get details for a specific match."""
            try:
                # Load match data
                self.session.load_match(match_id)

                # Query basic match info
                query = f"""
                    SELECT
                        inning,
                        MAX(over) as overs,
                        SUM(runs_batter + runs_extras) as runs,
                        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
                    FROM ball_events
                    WHERE match_id = ?
                    GROUP BY inning
                """

                result = self.session.engine.execute_sql(query, [match_id])
                df = result.to_pandas()

                return {
                    "match_id": match_id,
                    "innings": df.to_dict('records')
                }

            except Exception as e:
                raise HTTPException(status_code=404, detail=f"Match {match_id} not found: {str(e)}")

        @self.app.get("/players/{player_id}")
        async def get_player_stats(player_id: int):
            """Get statistics for a specific player."""
            try:
                stats = self.session.registry.get_player_stats(player_id)
                if stats:
                    return stats
                else:
                    raise HTTPException(status_code=404, detail=f"Player {player_id} not found")

            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/win_probability")
        async def win_probability(
            target: int = 150,
            current_runs: int = 50,
            wickets_down: int = 2,
            overs_done: float = 10.0
        ):
            """Calculate win probability for current match state."""
            try:
                from pypitch.compute.winprob import win_probability as wp_func
                result = wp_func(
                    target=target,
                    current_runs=current_runs,
                    wickets_down=wickets_down,
                    overs_done=overs_done
                )
                return result

            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/analyze")
        async def custom_analysis(query: Dict[str, Any], authenticated: bool = Depends(verify_api_key)):
            """Run custom analysis query."""
            try:
                # Execute custom query (with safety checks)
                sql = query.get("sql")
                if not sql:
                    raise HTTPException(status_code=400, detail="SQL query required")

                # Enhanced safety checks
                sql_upper = sql.upper().strip()

                # Block dangerous operations
                dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE"]
                if any(keyword in sql_upper for keyword in dangerous_keywords):
                    raise HTTPException(status_code=403, detail="Dangerous SQL operations not allowed")

                # Only allow SELECT statements
                if not sql_upper.startswith("SELECT"):
                    raise HTTPException(status_code=403, detail="Only SELECT queries are allowed")

                # Block potential SQL injection patterns
                injection_patterns = ["--", "/*", "*/", "UNION", "EXEC", "EXECUTE", "XP_", "SP_"]
                if any(pattern in sql_upper for pattern in injection_patterns):
                    raise HTTPException(status_code=403, detail="Potentially dangerous SQL patterns detected")

                # Limit query complexity (basic check)
                if sql_upper.count("SELECT") > 3 or sql_upper.count("JOIN") > 5:
                    raise HTTPException(status_code=403, detail="Query too complex")

                result = self.session.engine.execute_sql(sql)
                df = result.to_pandas()

                return {
                    "query": sql,
                    "rows": len(df),
                    "data": df.to_dict('records')[:100]  # Limit to 100 rows
                }

            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

        @self.app.post("/live/register")
        async def register_live_match(request: LiveMatchRegistration):
            """Register a match for live tracking."""
            try:
                if self.ingestor is None:
                    # For testing, return success without actual registration
                    logger.warning("register_live_match: Ingestor not available, returning synthetic success for match_id=%s", request.match_id)
                    return {"success": True, "match_id": request.match_id}
                    
                success = self.ingestor.register_match(
                    match_id=request.match_id,
                    source=request.source,
                    metadata=request.metadata
                )
                
                if not success:
                    raise HTTPException(status_code=400, detail=f"Match {request.match_id} already registered")
                
                return {"success": True, "match_id": request.match_id}
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/live/ingest")
        async def ingest_delivery(data: DeliveryData):
            """Ingest live delivery data."""
            try:
                if self.ingestor is None:
                    # For testing, return success without actual ingestion
                    logger.warning("ingest_delivery: Ingestor not available, returning synthetic success for match_id=%s", data.match_id)
                    return {"success": True}
                    
                # Convert Pydantic model to dict
                delivery_dict = data.model_dump(exclude_none=True)
                match_id = delivery_dict.pop('match_id')
                
                self.ingestor.update_match_data(match_id, delivery_dict)
                
                return {"success": True}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/live/matches")
        async def get_live_matches():
            """Get list of currently live matches."""
            try:
                if self.ingestor is None:
                    # For testing, return empty list
                    logger.info("get_live_matches: Ingestor not available, returning empty list")
                    return []
                    
                return self.ingestor.get_live_matches()
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

    def run(self, host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
        """Run the API server."""
        print(f"🚀 Starting PyPitch API server at http://{host}:{port}")
        print(f"📚 API documentation at http://{host}:{port}/docs")

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

def serve(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
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

    print(f"🐳 Docker files created in {output_path}")
    print("Build with: docker build -t pypitch-api .")
    print("Run with: docker run -p 8000:8000 pypitch-api")