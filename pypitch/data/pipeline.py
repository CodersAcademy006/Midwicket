"""
Data pipeline utilities for building registry and summary statistics.

Implements the ETL pass that seeds the IdentityRegistry (Agent 4) and
pre-computes derived tables consumed by the Archivist (Agent 3).
"""
from typing import Any


def build_registry_stats(loader: Any, registry: Any) -> None:
    """
    Build registry and summary statistics from raw match data.

    This function must be implemented before session initialization can
    succeed.  Calling it in its unimplemented state raises an error
    immediately so that callers are never left with a silently empty
    registry that appears healthy but produces incorrect results.

    Args:
        loader:   DataLoader instance used to fetch raw match files.
        registry: IdentityRegistry instance to populate with entity IDs.

    Raises:
        NotImplementedError: Always — implementation is pending.
    """
    # Implementation checklist:
    #   1. Load raw match data via loader
    #   2. Extract player / team / venue entities
    #   3. Populate registry tables
    #   4. Build summary / derived statistics
    raise NotImplementedError(
        "build_registry_stats is not implemented yet. "
        "Provide a real implementation before calling session.initialize()."
    )
