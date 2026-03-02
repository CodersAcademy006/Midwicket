"""
Production readiness validation script.
Moved from repo root to scripts/ — this is a standalone runner, not a pytest suite.

Usage:
    python scripts/validate_production.py
"""
import sys
import time


def validate_imports() -> bool:
    """Validate all core imports work."""
    print("Validating imports...")
    try:
        from pypitch.api.session import PyPitchSession          # noqa: F401
        from pypitch.storage.engine import QueryEngine           # noqa: F401
        from pypitch.query.defs import WinProbQuery, FantasyQuery  # noqa: F401
        from pypitch.runtime.executor import RuntimeExecutor     # noqa: F401
        from pypitch.compute.metrics import batting, bowling     # noqa: F401
        print("  PASS  All core imports successful")
        return True
    except Exception as exc:
        print(f"  FAIL  Import validation failed: {exc}")
        return False


def validate_session() -> bool:
    """Validate session creation and cleanup."""
    print("\nValidating session lifecycle...")
    try:
        from pypitch.api.session import PyPitchSession

        session = PyPitchSession()
        print("  PASS  Session created")
        session.close()
        print("  PASS  Session closed")
        return True
    except Exception as exc:
        import traceback
        print(f"  FAIL  Session validation failed: {exc}")
        traceback.print_exc()
        return False


def validate_performance() -> bool:
    """Validate basic session-init performance."""
    print("\nValidating performance...")
    try:
        from pypitch.api.session import PyPitchSession

        start = time.time()
        session = PyPitchSession()
        init_time = time.time() - start
        session.close()

        threshold = 5.0
        if init_time > threshold:
            print(f"  WARN  Session initialization took {init_time:.2f}s (>{threshold}s threshold)")
            return False

        print(f"  PASS  Performance acceptable (init: {init_time:.3f}s)")
        return True
    except Exception as exc:
        print(f"  FAIL  Performance validation failed: {exc}")
        return False


def main() -> int:
    print("=" * 60)
    print("PYPITCH PRODUCTION VALIDATION")
    print("=" * 60)

    checks = [
        ("Imports", validate_imports),
        ("Session Lifecycle", validate_session),
        ("Performance", validate_performance),
    ]

    results: list[tuple[str, bool]] = []
    for name, fn in checks:
        try:
            results.append((name, fn()))
        except Exception as exc:
            print(f"\n  CRASH  {name} check crashed: {exc}")
            results.append((name, False))

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, ok in results if ok)
    total = len(results)

    for name, ok in results:
        print(f"{'PASS' if ok else 'FAIL'} - {name}")

    print("=" * 60)
    print(f"Result: {passed}/{total} checks passed")

    if passed == total:
        print("All validation checks passed. Production ready.")
        return 0

    print("Some validation checks failed. Review before deployment.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
