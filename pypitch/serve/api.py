"""
PyPitch Serve Plugin: REST API Deployment

One-command deployment of PyPitch as a REST API.
Perfect for enterprise engineers and startups.
"""
from typing import Dict, Any, Optional
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
from pathlib import Path

class PyPitchAPI:
    """
    FastAPI wrapper for PyPitch deployment.

    Automatically creates endpoints for common operations.
    """

    def __init__(self, session=None):
        self.app = FastAPI(
            title="PyPitch API",
            description="Cricket Analytics API powered by PyPitch",
            version="1.0.0"
        )

        # Enable CORS for web applications
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Initialize session
        if session is None:
            from pypitch.api.session import PyPitchSession
            self.session = PyPitchSession.get()
        else:
            self.session = session

        self._setup_routes()

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
                    "GET /matches": "List available matches",
                    "GET /matches/{match_id}": "Get match details",
                    "GET /players/{player_id}": "Get player statistics",
                    "GET /teams/{team_id}": "Get team statistics",
                    "POST /analyze": "Run custom analysis",
                    "GET /win_probability": "Calculate win probability"
                }
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
        async def custom_analysis(query: Dict[str, Any]):
            """Run custom analysis query."""
            try:
                # Execute custom query (with safety checks)
                sql = query.get("sql")
                if not sql:
                    raise HTTPException(status_code=400, detail="SQL query required")

                # Basic safety check (very basic - in production use proper SQL injection prevention)
                dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT"]
                if any(keyword in sql.upper() for keyword in dangerous_keywords):
                    raise HTTPException(status_code=403, detail="Dangerous SQL keywords not allowed")

                result = self.session.engine.execute_sql(sql)
                df = result.to_pandas()

                return {
                    "query": sql,
                    "rows": len(df),
                    "data": df.to_dict('records')[:100]  # Limit to 100 rows
                }

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

def create_app(session=None) -> FastAPI:
    """
    Create and return a FastAPI application instance.

    This is the main entry point for creating the PyPitch API app.
    Useful for testing, deployment, and integration with other ASGI apps.
    """
    api = PyPitchAPI(session=session)
    return api.app

def serve(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """
    One-command API deployment.

    Usage:
        from pypitch.serve import serve
        serve()  # Starts API at http://localhost:8000
    """
    api = PyPitchAPI()
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