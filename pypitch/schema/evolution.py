from typing import Dict, Any, Tuple

def validate_compatibility(old_schema_meta: Dict[str, Any], new_schema_meta: Dict[str, Any]) -> bool:
    """
    Checks if a schema upgrade is safe (backward compatible).
    
    Rules:
    1. Version cannot move backwards.
    2. (Future) Cannot remove required columns.
    """
    old_ver = old_schema_meta.get("version", "0.0.0")
    new_ver = new_schema_meta.get("version", "0.0.0")
    
    # Semantic version check logic
    if _parse_version(old_ver) > _parse_version(new_ver):
        raise ValueError(f"Schema Downgrade Detected: Cannot move from {old_ver} to {new_ver}")
        
    return True

def _parse_version(version_str: str) -> Tuple[int, ...]:
    """Helper to convert '1.2.0' -> (1, 2, 0)"""
    try:
        return tuple(map(int, version_str.split('.')))
    except ValueError:
        return (0, 0, 0)
