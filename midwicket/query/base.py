import hashlib
import json
from typing import Dict, Optional, Any, List
from pydantic import BaseModel, Field, ConfigDict, field_validator

class ExecutionOptions(BaseModel):
    """Runtime controls that do NOT affect the data definition."""
    timeout: float = 30.0
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
        # 1. Dump model to dict, excluding runtime opts.
        # Include query type so distinct query classes with identical fields
        # cannot collide in cache.
        canonical_dict = self.model_dump(exclude={"execution_opts"})
        canonical_dict["__query_type__"] = (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )
        
        # 2. Dump to JSON with sort_keys=True for determinism
        canonical_json = json.dumps(canonical_dict, sort_keys=True)
        
        # 3. Hash it
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

class MatchupQuery(BaseQuery):
    """
    Query for batter-vs-bowler matchup analytics.

    IDs must align with Schema V1 which stores actor IDs as pa.int32().
    String-formatted IDs (e.g. from API query params) are automatically
    coerced to int via validators so callers never produce silent type
    mismatches in DuckDB WHERE clauses.
    """
    batter_id: int  # int32 in Schema V1
    bowler_id: int  # int32 in Schema V1
    venue_id: Optional[int] = None  # int32 in Schema V1

    @field_validator("batter_id", "bowler_id", mode="before")
    @classmethod
    def _coerce_actor_id(cls, v: Any) -> int:
        """Accept string representations of integer IDs (common from API layer)."""
        try:
            return int(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Actor ID must be an integer or integer-coercible string, got: {v!r}") from exc

    @field_validator("venue_id", mode="before")
    @classmethod
    def _coerce_venue_id(cls, v: Any) -> Optional[int]:
        """Accept string representations of venue IDs."""
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Venue ID must be an integer or integer-coercible string, got: {v!r}") from exc

    @property
    def requires(self) -> Dict[str, Any]:
        return {
            "preferred_tables": ["matchup_stats", "phase_stats"],
            "fallback_table": "ball_events",
            "entities": ["batter", "bowler"],
            "granularity": "ball" 
        }

