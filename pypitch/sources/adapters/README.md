# PyPitch Adapter Registry

This directory contains the base contract and registry system for all data source adapters in PyPitch. The adapter pattern enables flexible, plug-and-play data ingestion from various cricket data sources.

## Overview

The Adapter Registry provides:
- **Standardized Interface**: Consistent API for all data sources
- **Dynamic Registration**: Add new adapters at runtime
- **Plugin System**: Extend PyPitch without modifying core code
- **Type Safety**: Protocol-based contracts for type checking

## BaseAdapter Contract

All adapters must implement the `BaseAdapter` protocol interface:

```python
from typing import Protocol, List, Dict, Any

class BaseAdapter(Protocol):
    """Protocol defining the adapter contract."""
    
    def get_match_ids(self) -> List[str]:
        """
        Return list of available match identifiers.
        
        Returns:
            List of match ID strings
        """
        ...
    
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Retrieve complete match data for the given ID.
        
        Args:
            match_id: Unique match identifier
            
        Returns:
            Dictionary with match data in PyPitch schema format
        """
        ...
```

### Required Data Format

Adapters must return a dictionary containing:

| Key | Type | Description |
|-----|------|-------------|
| `match_id` | str | Unique match identifier |
| `format` | str | Match format (T20, ODI, Test) |
| `info` | dict | Match metadata (teams, venue, date, etc.) |
| `events` | list | Ball-by-ball event data |
| `raw` | dict | Original raw data (optional, for debugging) |

**Example Return Structure:**
```python
{
    "match_id": "123456",
    "format": "T20",
    "info": {
        "teams": ["Team A", "Team B"],
        "venue": "Stadium Name",
        "date": "2023-05-15"
    },
    "events": [
        {
            "over": 1,
            "ball": 1,
            "batter": "Player Name",
            "bowler": "Bowler Name",
            "runs": 4
            # ... more fields
        }
    ],
    "raw": {}  # Optional
}
```

## AdapterRegistry

The registry manages all available adapters and provides discovery mechanisms.

### Registering Adapters

```python
from pypitch.sources.adapters.registry import AdapterRegistry

# Register a custom adapter
AdapterRegistry.register("my_source", MyAdapter)

# Retrieve registered adapter
adapter_class = AdapterRegistry.get("my_source")

# Instantiate and use
adapter = adapter_class()
matches = adapter.get_match_ids()
```

### Example: Custom Adapter Registration

```python
from typing import List, Dict, Any
from pypitch.sources.adapters.registry import AdapterRegistry

class MyCustomAdapter:
    """Custom adapter for proprietary data source."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key
    
    def get_match_ids(self) -> List[str]:
        # Fetch match IDs from your source
        return ["match_1", "match_2", "match_3"]
    
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        # Fetch and transform match data
        return {
            "match_id": match_id,
            "format": "T20",
            "info": {...},
            "events": [...]
        }

# Register the adapter
AdapterRegistry.register("custom", MyCustomAdapter)

# Use the adapter
adapter = AdapterRegistry.get("custom")()
data = adapter.get_match_data("match_1")
```

## Built-in Adapters

PyPitch includes several built-in adapters:

### Cricsheet (Primary)
- **Name**: `cricsheet`
- **Source**: Cricsheet.org JSON data
- **Status**: Production-ready
- **Features**: Ball-by-ball data for all formats

### Retrosheet (Example)
- **Name**: `retrosheet`
- **Source**: Baseball data (demonstration)
- **Status**: Stub implementation
- **Purpose**: Cross-sport extensibility example

### CricAPI (Planned)
- **Name**: `cricapi`
- **Source**: Live cricket API
- **Status**: Planned for future release
- **Purpose**: Real-time match data

## Working with the Registry

### List All Adapters

```python
from pypitch.sources.adapters.registry import AdapterRegistry

# Get list of all registered adapters
adapters = AdapterRegistry.list_adapters()
print(f"Available adapters: {adapters}")
```

### Check Adapter Availability

```python
from pypitch.sources.adapters.registry import AdapterRegistry

# Check if adapter is registered (using try/except pattern)
try:
    adapter = AdapterRegistry.get("my_source")()
    # Use the adapter
except KeyError:
    print("Adapter not available")
```

### Clear Registry (Advanced)

```python
# Note: Unregister functionality not currently implemented
# The registry is designed to be write-once for adapter registration
# For testing purposes, you can clear the entire registry:
from pypitch.sources.adapters.registry import AdapterRegistry

# Clear all registrations (use with caution)
AdapterRegistry._registry.clear()
```

## Advanced Usage

### Factory Pattern

```python
class AdapterFactory:
    """Factory for creating adapter instances."""
    
    @staticmethod
    def create(source_type: str, **kwargs):
        adapter_class = AdapterRegistry.get(source_type)
        return adapter_class(**kwargs)

# Use factory
adapter = AdapterFactory.create("cricsheet", data_dir="./data")
matches = adapter.get_match_ids()
```

### Adapter Chaining

```python
class CompositeAdapter:
    """Adapter that aggregates multiple sources."""
    
    def __init__(self, *adapters):
        self.adapters = adapters
    
    def get_match_ids(self):
        all_ids = []
        for adapter in self.adapters:
            all_ids.extend(adapter.get_match_ids())
        return all_ids

# Use composite adapter
cricsheet = AdapterRegistry.get("cricsheet")()
custom = AdapterRegistry.get("custom")()
composite = CompositeAdapter(cricsheet, custom)
```

## Best Practices

1. **Implement Full Contract**: All protocol methods must be implemented
2. **Error Handling**: Raise appropriate exceptions for errors
3. **Data Validation**: Validate data before returning
4. **Documentation**: Document your adapter's requirements
5. **Testing**: Write unit tests for your adapter
6. **Registration**: Register adapters at module import time

## Testing Adapters

```python
import unittest

class TestMyAdapter(unittest.TestCase):
    def setUp(self):
        self.adapter = MyCustomAdapter()
    
    def test_get_match_ids(self):
        ids = self.adapter.get_match_ids()
        self.assertIsInstance(ids, list)
    
    def test_get_match_data(self):
        data = self.adapter.get_match_data("test_id")
        self.assertIn("match_id", data)
        self.assertIn("format", data)
        self.assertIn("info", data)
        self.assertIn("events", data)
```

## See Also

- [Adapter Documentation](../../docs/adapters.md) - Complete adapter development guide
- [API Documentation](../../docs/api.md) - PyPitch API reference
- [Cricsheet Format](https://cricsheet.org/format/) - Data format specification

---

For questions about the adapter system, visit [GitHub Discussions](https://github.com/CodersAcademy006/PyPitch/discussions).
