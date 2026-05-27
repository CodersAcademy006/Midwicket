"""
Production readiness validator for Midwicket.

Usage:
    python validate_production.py

Exit code 0 = all checks passed.
Exit code 1 = one or more checks failed.
"""
import os
import sys
import time

# Run as development so SECRET_KEY is not required during validation
os.environ.setdefault("MIDWICKET_ENV", "development")


def check_imports() -> bool:
    print("  Checking core imports...")
    try:
        import midwicket as md                                          # noqa: F401
        from midwicket.api.session import MidwicketSession               # noqa: F401
        from midwicket.storage.engine import QueryEngine               # noqa: F401
        from midwicket.runtime.executor import RuntimeExecutor         # noqa: F401
        from midwicket.compute.metrics import batting, bowling         # noqa: F401
        from midwicket.compute.winprob import win_probability          # noqa: F401
        from midwicket.query.defs import FantasyQuery, WinProbQuery    # noqa: F401
        from midwicket.query.base import MatchupQuery                  # noqa: F401
        from midwicket.storage.registry import IdentityRegistry        # noqa: F401
        print("    OK")
        return True
    except Exception as exc:
        print(f"    FAIL: {exc}")
        return False


def check_session_lifecycle() -> bool:
    print("  Checking session lifecycle...")
    try:
        import tempfile
        from midwicket.api.session import MidwicketSession
        from midwicket.storage.engine import QueryEngine

        # Use a temp dir + in-memory engine so we never touch real data or
        # trigger network-dependent operations (downloads, migrations, etc.).
        with tempfile.TemporaryDirectory(prefix="midwicket_validate_") as tmp:
            mem_engine = QueryEngine(":memory:")
            session = MidwicketSession(
                data_dir=tmp,
                skip_registry_build=True,
                engine=mem_engine,
            )
            assert session.engine is not None
            assert session.registry is not None
            assert session.cache is not None
            session.close()
        print("    OK")
        return True
    except Exception as exc:
        print(f"    FAIL: {exc}")
        import traceback
        traceback.print_exc()
        return False


def check_compute_metrics() -> bool:
    print("  Checking compute metrics (no DB)...")
    try:
        import pyarrow as pa
        from midwicket.compute.metrics.batting import calculate_strike_rate
        from midwicket.compute.metrics.bowling import calculate_economy
        from midwicket.compute.metrics.team import calculate_team_win_rate

        runs  = pa.array([50, 30], type=pa.int64())
        balls = pa.array([40, 20], type=pa.int64())
        sr = calculate_strike_rate(runs, balls)
        assert abs(sr[0].as_py() - 125.0) < 0.01

        wins    = pa.array([8], type=pa.int64())
        matches = pa.array([14], type=pa.int64())
        wr = calculate_team_win_rate(wins, matches)
        assert abs(wr[0].as_py() - (8 / 14 * 100)) < 0.01

        print("    OK")
        return True
    except Exception as exc:
        print(f"    FAIL: {exc}")
        return False


def check_win_probability() -> bool:
    print("  Checking win probability model...")
    try:
        from midwicket.compute.winprob import win_probability
        result = win_probability(
            target=180, current_runs=90, wickets_down=3,
            overs_done=10.0, venue=None,
        )
        assert isinstance(result, dict)
        assert "win_prob" in result
        assert 0.0 <= result["win_prob"] <= 1.0
        print("    OK")
        return True
    except Exception as exc:
        print(f"    FAIL: {exc}")
        return False


def check_schema_validation() -> bool:
    print("  Checking schema enforcement...")
    try:
        import pyarrow as pa
        from midwicket.schema.v1 import BALL_EVENT_SCHEMA
        from midwicket.storage.engine import QueryEngine

        engine = QueryEngine(":memory:")
        bad = pa.table({"col_a": [1, 2]})
        try:
            engine.ingest_events(bad, "bad")
            engine.close()
            print("    FAIL: expected ValueError for bad schema")
            return False
        except ValueError:
            pass
        engine.close()
        print("    OK")
        return True
    except Exception as exc:
        print(f"    FAIL: {exc}")
        return False


def check_performance() -> bool:
    print("  Checking session init performance...")
    try:
        import tempfile
        from midwicket.api.session import MidwicketSession
        from midwicket.storage.engine import QueryEngine

        with tempfile.TemporaryDirectory(prefix="midwicket_perf_") as tmp:
            mem_engine = QueryEngine(":memory:")
            t0 = time.perf_counter()
            s = MidwicketSession(
                data_dir=tmp,
                skip_registry_build=True,
                engine=mem_engine,
            )
            elapsed = time.perf_counter() - t0
            s.close()
        if elapsed > 5.0:
            print(f"    WARN: init took {elapsed:.2f}s (threshold: 5s)")
            return False
        print(f"    OK ({elapsed:.3f}s)")
        return True
    except Exception as exc:
        print(f"    FAIL: {exc}")
        return False


def main() -> int:
    print("=" * 55)
    print("  Midwicket — Production Readiness Validator")
    print("=" * 55)

    checks = [
        ("Core imports",         check_imports),
        ("Session lifecycle",    check_session_lifecycle),
        ("Compute metrics",      check_compute_metrics),
        ("Win probability",      check_win_probability),
        ("Schema enforcement",   check_schema_validation),
        ("Init performance",     check_performance),
    ]

    results = []
    for name, fn in checks:
        print(f"\n[{name}]")
        try:
            ok = fn()
        except Exception as exc:
            print(f"    CRASH: {exc}")
            ok = False
        results.append((name, ok))

    print("\n" + "=" * 55)
    passed = sum(ok for _, ok in results)
    total  = len(results)
    for name, ok in results:
        mark = "PASS" if ok else "FAIL"
        print(f"  {mark:<4}  {name}")
    print("=" * 55)
    print(f"  {passed}/{total} checks passed")

    if passed == total:
        print("  All checks passed. Ready for release.")
        return 0
    print("  Fix failing checks before publishing.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
