"""
Semantic version helpers used by the model registry.

This is a deliberately small implementation that handles the subset of
semver we actually need (``MAJOR.MINOR.PATCH``) plus an optional
pre-release/build segment that is parsed but ignored for comparison.
We do not depend on a third-party package so that the model registry
has zero install footprint beyond joblib.
"""
from __future__ import annotations

import re
from typing import Tuple


_VERSION_RE = re.compile(
    r"^(?P<major>0|[1-9]\d*)\."
    r"(?P<minor>0|[1-9]\d*)\."
    r"(?P<patch>0|[1-9]\d*)"
    r"(?:[-+].*)?$"
)


def parse_version(v: str) -> Tuple[int, int, int]:
    """Parse ``MAJOR.MINOR.PATCH`` into a tuple.

    Any trailing pre-release or build segment (``-rc1`` / ``+build42``)
    is permitted but discarded.

    Raises:
        ValueError: if ``v`` is not a valid semantic version string.
    """
    if not isinstance(v, str):
        raise ValueError(f"version must be a string, got {type(v).__name__}")
    m = _VERSION_RE.match(v.strip())
    if not m:
        raise ValueError(f"invalid semantic version: {v!r}")
    return int(m["major"]), int(m["minor"]), int(m["patch"])


def compare_versions(a: str, b: str) -> int:
    """Return -1 if ``a`` < ``b``, 0 if equal, 1 if greater.

    Pre-release / build metadata is ignored.
    """
    ta = parse_version(a)
    tb = parse_version(b)
    if ta < tb:
        return -1
    if ta > tb:
        return 1
    return 0


def next_patch(v: str) -> str:
    """Bump the patch component (``1.2.3`` -> ``1.2.4``)."""
    major, minor, patch = parse_version(v)
    return f"{major}.{minor}.{patch + 1}"


def next_minor(v: str) -> str:
    """Bump the minor component and reset patch (``1.2.3`` -> ``1.3.0``)."""
    major, minor, _ = parse_version(v)
    return f"{major}.{minor + 1}.0"


def next_major(v: str) -> str:
    """Bump the major component and reset minor/patch (``1.2.3`` -> ``2.0.0``)."""
    major, _, _ = parse_version(v)
    return f"{major + 1}.0.0"


def previous_patch(v: str) -> str:
    """Return the previous patch version, or raise if patch is 0."""
    major, minor, patch = parse_version(v)
    if patch == 0:
        raise ValueError(f"no previous patch for {v} (patch is 0)")
    return f"{major}.{minor}.{patch - 1}"


def previous_minor(v: str) -> str:
    """Return the previous minor (patch reset to 0), or raise if minor is 0."""
    major, minor, _ = parse_version(v)
    if minor == 0:
        raise ValueError(f"no previous minor for {v} (minor is 0)")
    return f"{major}.{minor - 1}.0"


def previous_major(v: str) -> str:
    """Return the previous major (minor/patch reset to 0), or raise if major is 0."""
    major, _, _ = parse_version(v)
    if major == 0:
        raise ValueError(f"no previous major for {v} (major is 0)")
    return f"{major - 1}.0.0"


def is_valid_version(v: str) -> bool:
    """Return True if ``v`` parses as a semantic version."""
    try:
        parse_version(v)
    except ValueError:
        return False
    return True


__all__ = [
    "parse_version",
    "compare_versions",
    "next_patch",
    "next_minor",
    "next_major",
    "previous_patch",
    "previous_minor",
    "previous_major",
    "is_valid_version",
]
