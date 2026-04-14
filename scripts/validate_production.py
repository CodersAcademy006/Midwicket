"""
Thin wrapper — delegates to the canonical validator at the repo root.

The authoritative production readiness checks live in validate_production.py
at the repo root (6 checks: imports, session lifecycle, metrics, win probability,
schema enforcement, performance).  This script exists only for convenience so
that `python scripts/validate_production.py` still works.

Usage:
    python scripts/validate_production.py
    # or equivalently:
    python validate_production.py
"""
import sys
import os

# Ensure the repo root is on sys.path so the root-level module is importable
# whether this script is run from the repo root or from scripts/.
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.dirname(_scripts_dir)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from validate_production import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
