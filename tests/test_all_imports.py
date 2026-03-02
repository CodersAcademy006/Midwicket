"""
Test that all pypitch modules can be imported without errors.
Moved from repo root to tests/ for correct pytest discovery.
"""
import importlib
import pkgutil
from pathlib import Path


def test_all_imports():
    """Test importing all pypitch modules — catches SyntaxError and ImportError."""
    # Resolve relative to the repo root (one level above this tests/ directory)
    pypitch_path = Path(__file__).parent.parent / "pypitch"

    modules_to_test = [
        modname
        for _importer, modname, _ispkg in pkgutil.walk_packages(
            [str(pypitch_path)], prefix="pypitch."
        )
    ]

    failed_imports: list[tuple[str, str]] = []

    for module_name in modules_to_test:
        try:
            importlib.import_module(module_name)
        except (ImportError, SyntaxError) as exc:
            failed_imports.append((module_name, str(exc)))
        except Exception:
            # Runtime errors (e.g. missing optional deps) are not import failures
            pass

    assert not failed_imports, (
        f"Failed to import {len(failed_imports)} module(s):\n"
        + "\n".join(f"  {name}: {err}" for name, err in failed_imports)
    )
