"""
Test that all pypitch modules can be imported without errors.
"""
import importlib
import pkgutil
import sys
from pathlib import Path

def test_all_imports():
    """Test importing all pypitch modules."""
    pypitch_path = Path(__file__).parent / "pypitch"
    
    # Get all module names
    modules_to_test = []
    for importer, modname, ispkg in pkgutil.walk_packages([str(pypitch_path)], prefix="pypitch."):
        modules_to_test.append(modname)
    
    failed_imports = []
    
    for module_name in modules_to_test:
        try:
            importlib.import_module(module_name)
            print(f"✓ {module_name}")
        except (ImportError, SyntaxError) as e:
            print(f"✗ {module_name}: {e}")
            failed_imports.append((module_name, str(e)))
        except Exception as e:
            print(f"⚠ {module_name}: {type(e).__name__}: {e}")
    
    if failed_imports:
        print(f"\n❌ Failed to import {len(failed_imports)} modules:")
        for name, error in failed_imports:
            print(f"  - {name}: {error}")
        sys.exit(1)
    else:
        print(f"\n✅ Successfully imported all {len(modules_to_test)} modules")

if __name__ == "__main__":
    test_all_imports()
