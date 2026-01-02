# PyPitch Data Source Adapters

PyPitch supports flexible data ingestion through the Adapter pattern, allowing you to integrate cricket data from various sources. This guide covers built-in adapters and how to create custom ones for your specific data sources.

## Overview

Data source adapters provide a consistent interface for ingesting cricket data from different sources. PyPitch ships with several adapters and makes it easy to create custom ones.

### Benefits of the Adapter Pattern

- **Plug-and-Play**: Easily switch between data sources
- **Consistency**: Uniform API regardless of data source
- **Extensibility**: Add new data sources without modifying core code
- **Testability**: Mock data sources for testing

## Built-in Adapters

### CricsheetLoader (Default)

The primary adapter for loading cricket data from Cricsheet.org.

**Features:**
- Zero configuration required
- Supports all Cricsheet JSON formats
- Automatic schema detection
- Ball-by-ball data ingestion

**Usage:**
```python
from pypitch.sources.cricsheet_loader import CricsheetLoader

# Initialize loader
loader = CricsheetLoader()

# Get available match IDs
match_ids = loader.get_match_ids()
print(f"Found {len(match_ids)} matches")

# Load specific match data
match_data = loader.get_match_data(match_ids[0])
print(f"Loaded match: {match_data['info']['match_type']}")
```

**Configuration Options:**
```python
# Custom data directory
loader = CricsheetLoader(data_dir="./custom_data")

# Filter by competition
loader = CricsheetLoader(competition="ipl")

# Filter by season
loader = CricsheetLoader(season=2023)
```

### RetrosheetAdapter (Experimental)

Adapter for baseball data from Retrosheet (example implementation).

**Status:** Stub implementation - demonstrates cross-sport extensibility

**Usage:**
```python
from pypitch.sources.adapters.retrosheet import RetrosheetAdapter

adapter = RetrosheetAdapter()
# Adapt baseball data to PyPitch schema
```

### CricAPIAdapter (Planned)

Adapter for live cricket data from cricket APIs.

**Status:** Planned for future release

**Planned Features:**
- Real-time match data
- Live score updates
- Player statistics API
- Match schedule integration

## Custom Adapter Interface

Create your own adapter by implementing the `DataSource` interface:

### Interface Definition

```python
from pypitch.data.sources.adapter import DataSource
from typing import List, Dict, Any

class DataSource:
    """Base interface for all data source adapters."""
    
    def get_match_ids(self) -> List[str]:
        """
        Return list of available match identifiers.
        
        Returns:
            List of match ID strings
        """
        raise NotImplementedError
    
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """
        Load complete match data for given ID.
        
        Args:
            match_id: Match identifier
            
        Returns:
            Dictionary containing match data in PyPitch schema format
        """
        raise NotImplementedError
    
    def get_competition_name(self) -> str:
        """
        Return the competition/tournament name.
        
        Returns:
            Competition name string
        """
        raise NotImplementedError
```

### Implementation Example

```python
from pypitch.data.sources.adapter import DataSource
import requests

class MyCustomAdapter(DataSource):
    """Custom adapter for proprietary cricket data API."""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
    
    def get_match_ids(self) -> List[str]:
        """Fetch match IDs from API."""
        response = requests.get(
            f"{self.base_url}/matches",
            headers={"Authorization": f"Bearer {self.api_key}"}
        )
        data = response.json()
        return [match["id"] for match in data["matches"]]
    
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """Fetch and transform match data."""
        response = requests.get(
            f"{self.base_url}/matches/{match_id}",
            headers={"Authorization": f"Bearer {self.api_key}"}
        )
        raw_data = response.json()
        
        # Transform to PyPitch schema format
        return self._transform_to_pypitch_schema(raw_data)
    
    def get_competition_name(self) -> str:
        """Return competition name."""
        return "Custom Cricket League"
    
    def _transform_to_pypitch_schema(self, raw_data: Dict) -> Dict:
        """Transform API data to PyPitch schema."""
        # Implement your transformation logic
        return {
            "info": {
                "match_type": raw_data["format"],
                "teams": raw_data["teams"],
                # ... more fields
            },
            "innings": [
                # Transform innings data
            ]
        }
```

## Using Custom Adapters

### Register and Use

```python
# Create custom adapter instance
my_adapter = MyCustomAdapter(
    api_key="your_api_key",
    base_url="https://api.example.com"
)

# Register with the adapter registry
from pypitch.sources.adapters.registry import AdapterRegistry
AdapterRegistry.register("custom", MyCustomAdapter)

# Use the adapter directly
match_ids = my_adapter.get_match_ids()
match_data = my_adapter.get_match_data(match_ids[0])

# Or retrieve from registry
adapter_class = AdapterRegistry.get("custom")
adapter_instance = adapter_class(api_key="your_api_key", base_url="https://api.example.com")
```

### Integration with DataLoader

```python
from pypitch.data.loader import DataLoader

# Use custom adapter with DataLoader
# Note: Direct adapter integration may require custom implementation
my_adapter = MyCustomAdapter(api_key="key", base_url="https://api.example.com")

# Use adapter methods to fetch and process data
for match_id in my_adapter.get_match_ids():
    match_data = my_adapter.get_match_data(match_id)
    # Process match data as needed
```

## Advanced Topics

### Schema Transformation

Ensure your adapter transforms data to PyPitch's Schema V1 format:

```python
def _transform_to_pypitch_schema(self, raw_data: Dict) -> Dict:
    """Transform to PyPitch Schema V1."""
    return {
        "info": {
            "match_type": raw_data["type"],  # T20, ODI, Test
            "dates": [raw_data["date"]],
            "teams": raw_data["teams"],
            "venue": raw_data["venue"],
            # Required schema fields...
        },
        "innings": [
            {
                "team": team_name,
                "overs": [
                    {
                        "over": over_number,
                        "deliveries": [
                            {
                                "batter": batter_name,
                                "bowler": bowler_name,
                                "runs": {"batter": runs, "total": total},
                                # More ball-level data...
                            }
                        ]
                    }
                ]
            }
        ]
    }
```

### Error Handling

Implement robust error handling in your adapter:

```python
from pypitch.exceptions import DataNotFoundError, InvalidQueryError

class MyCustomAdapter(DataSource):
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        try:
            response = requests.get(f"{self.base_url}/matches/{match_id}")
            response.raise_for_status()
            return self._transform_to_pypitch_schema(response.json())
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                raise DataNotFoundError(f"Match {match_id} not found")
            raise
        except Exception as e:
            raise InvalidQueryError(f"Failed to fetch match data: {e}")
```

### Caching

Implement caching for better performance:

```python
from functools import lru_cache

class MyCustomAdapter(DataSource):
    @lru_cache(maxsize=100)
    def get_match_data(self, match_id: str) -> Dict[str, Any]:
        """Cached match data retrieval."""
        # Fetch and return data
        pass
```

## Testing Custom Adapters

```python
import unittest
from pypitch.data.sources.adapter import DataSource

class TestMyCustomAdapter(unittest.TestCase):
    def setUp(self):
        self.adapter = MyCustomAdapter(api_key="test_key")
    
    def test_get_match_ids(self):
        match_ids = self.adapter.get_match_ids()
        self.assertIsInstance(match_ids, list)
        self.assertTrue(len(match_ids) > 0)
    
    def test_get_match_data(self):
        match_data = self.adapter.get_match_data("test_match_id")
        self.assertIn("info", match_data)
        self.assertIn("innings", match_data)
```

## Best Practices

1. **Follow the Interface**: Implement all required methods
2. **Schema Compliance**: Ensure data matches PyPitch Schema V1
3. **Error Handling**: Provide clear error messages
4. **Documentation**: Document your adapter's requirements
5. **Testing**: Write unit tests for your adapter
6. **Performance**: Implement caching when appropriate
7. **Validation**: Validate data before returning

## Example Use Cases

### Use Case 1: Private League Data

```python
class PrivateLeagueAdapter(DataSource):
    """Adapter for private cricket league database."""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def get_match_ids(self):
        return self.db.execute("SELECT match_id FROM matches").fetchall()
    
    def get_match_data(self, match_id):
        # Query database and transform to PyPitch schema
        pass
```

### Use Case 2: CSV Files

```python
class CSVAdapter(DataSource):
    """Adapter for CSV cricket data files."""
    
    def __init__(self, csv_directory):
        self.directory = csv_directory
    
    def get_match_ids(self):
        return [f.stem for f in Path(self.directory).glob("*.csv")]
    
    def get_match_data(self, match_id):
        # Read CSV and transform to PyPitch schema
        pass
```

## Further Reading

- [PyPitch Schema Documentation](../../schema/)
- [Cricsheet Format Specification](https://cricsheet.org/format/)
- [PyPitch API Documentation](api.md)

---

For questions about creating custom adapters, please visit [GitHub Discussions](https://github.com/CodersAcademy006/PyPitch/discussions).
