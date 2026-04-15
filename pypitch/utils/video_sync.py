"""Backward-compatible video timestamp helpers.

This lightweight utility maps a ball index to a video timestamp using an
in-memory mapping. It is intentionally simple and independent from the richer
`pypitch.core.video_sync` module.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Optional


def get_video_timestamp(ball_index: Any, mapping: Mapping[Any, Any]) -> Optional[Any]:
    """Return the mapped video timestamp for a ball index.

    Compatibility behavior:
    - Accepts integer and string ball indexes.
    - Tries integer and string forms of the key to support JSON-loaded maps.
    - Returns ``None`` when key is missing.
    """
    if not isinstance(mapping, Mapping):
        raise TypeError("mapping must be a mapping")

    candidates: list[Any] = [ball_index]

    try:
        normalized = int(ball_index)
        candidates.extend([normalized, str(normalized)])
    except (TypeError, ValueError):
        pass

    if ball_index is not None:
        candidates.append(str(ball_index))

    for key in candidates:
        try:
            if key in mapping:
                return mapping[key]
        except TypeError:
            continue

    return None


__all__ = ["get_video_timestamp"]
