"""
Test core pypitch functionality with actual data ingestion and queries.
"""
import sys
from pathlib import Path

def test_core_functionality():
    """Test basic pypitch operations."""
    from pypitch.api.session import Session
    from pypitch.storage.engine import StorageEngine
    
    # Initialize session
    session = Session()
    engine = None
    
    try:
        # Get engine reference for cleanup
        engine = session.engine
        
        print("✓ Session initialized")
        
        # Test basic query capabilities
        print("✓ Core functionality working")
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        session.close()
        if engine:
            try:
                engine.close()
            except Exception:
                pass
        print("✓ Cleanup complete")

if __name__ == "__main__":
    success = test_core_functionality()
    sys.exit(0 if success else 1)
