import hashlib
import json
from typing import Dict, Any
from pydantic import BaseModel, Field, field_validator

class QueryContext(BaseModel):
    """
    Represents the state of the system execution environment.
    Included in the hash to invalidate caches if the system updates.
    """
    schema_version: str
    snapshot_id: str
    planner_version: str
    derived_versions: Dict[str, str]

class BaseQuery(BaseModel):
    """
    Abstract base for all Pypitch queries.
    Enforces deterministic hashing and explicit dependency declaration.
    """
    mode: str = Field(default="exact", description="Execution mode: exact, approx, budget")
    
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
        raise NotImplementedError("Query subclass must implement 'requires' property.")

    def to_canonical_dict(self) -> Dict[str, Any]:
        """
        Returns a sorted, clean dictionary of the query intent.
        Excludes None values to keep hashes stable.
        """
        return self.model_dump(mode='json', exclude_none=True)

    def compute_cache_key(self, context: QueryContext) -> str:
        """
        Generates a deterministic SHA-256 hash.
        Combines: Intent (Query params) + Context (System state).
        """
        payload = {
            "intent": self.to_canonical_dict(),
            "context": context.model_dump(mode='json'),
            "query_class": self.__class__.__name__
        }
        
        # sort_keys=True is CRITICAL for determinism
        canonical_str = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

    @field_validator('mode')
    def validate_mode(cls, v):
        allowed = {'exact', 'approx', 'budget'}
        if v not in allowed:
            raise ValueError(f"Invalid execution mode '{v}'. Allowed: {allowed}")
        return v
