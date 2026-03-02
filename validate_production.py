"""
Validation script to ensure pypitch is production-ready.
"""
import sys
import time
from pathlib import Path

def validate_imports():
    """Validate all core imports work."""
    print("🔍 Validating imports...")
    try:
        from pypitch.api.session import Session
        from pypitch.storage.engine import StorageEngine
        from pypitch.query.defs import QueryType
        from pypitch.runtime.executor import Executor
        from pypitch.compute.metrics import batting, bowling
        print("✅ All core imports successful")
        return True
    except Exception as e:
        print(f"❌ Import validation failed: {e}")
        return False

def validate_session():
    """Validate session creation and cleanup."""
    print("\n🔍 Validating session lifecycle...")
    try:
        from pypitch.api.session import Session
        
        session = Session()
        print("✅ Session created")
        
        session.close()
        print("✅ Session closed")
        
        return True
    except Exception as e:
        print(f"❌ Session validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_performance():
    """Validate basic performance benchmarks."""
    print("\n🔍 Validating performance...")
    try:
        from pypitch.api.session import Session
        
        start = time.time()
        session = Session()
        init_time = time.time() - start
        
        session.close()
        
        if init_time > 5.0:
            print(f"⚠️  Session initialization took {init_time:.2f}s (>5s threshold)")
            return False
        
        print(f"✅ Performance acceptable (init: {init_time:.3f}s)")
        return True
        
    except Exception as e:
        print(f"❌ Performance validation failed: {e}")
        return False

def main():
    """Run all validation checks."""
    print("=" * 60)
    print("PYPITCH PRODUCTION VALIDATION")
    print("=" * 60)
    
    checks = [
        ("Imports", validate_imports),
        ("Session Lifecycle", validate_session),
        ("Performance", validate_performance),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ {name} check crashed: {e}")
            results.append((name, False))
    
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")
    
    print("=" * 60)
    print(f"Result: {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 All validation checks passed! Production ready.")
        return 0
    else:
        print("⚠️  Some validation checks failed. Review before deployment.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
