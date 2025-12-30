import hashlib
import json
from typing import Dict, Optional, Any, List
from pydantic import BaseModel, Field, ConfigDict

class ExecutionOptions(BaseModel):
    """Runtime controls that do NOT affect the data definition."""
    timeout: int = 30
    verbose: bool = False
    mode: str = "exact"  # or "approx"

class BaseQuery(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    snapshot_id: str
    execution_opts: ExecutionOptions = Field(default_factory=ExecutionOptions, exclude=True)

    @property
    def requires(self) -> Dict[str, Any]:
        """
        Contract for the Planner.
        Must return:
        {
            "preferred_tables": ["list", "of", "materialized", "views"],
            "fallback_table": "raw_table_name",
            "entities": ["list", "of", "required", "columns"],
            "granularity": "ball" | "match"
        }
        """
        raise NotImplementedError("Query subclass must implement requires property.")

    @property
    def cache_key(self) -> str:
        """
        Generates a deterministic SHA256 hash of the INTENT only.
        Crucially, it excludes execution_opts because of the exclude=True above.
        """
        # 1. Dump model to dict, excluding runtime opts
        canonical_dict = self.model_dump(exclude={"execution_opts"})
        
        # 2. Dump to JSON with sort_keys=True for determinism
        canonical_json = json.dumps(canonical_dict, sort_keys=True)
        
        # 3. Hash it
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

class MatchupQuery(BaseQuery):
    batter_id: str
    bowler_id: str
    venue_id: Optional[str] = None

    @property
    def requires(self):
        return {
            "preferred_tables": ["matchup_stats", "phase_stats"],
            "fallback_table": "ball_events",
            "entities": ["batter", "bowler"],
            "granularity": "ball" 
        }

