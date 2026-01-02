# PyPitch API Documentation

This comprehensive API reference provides detailed documentation for all PyPitch functions, classes, and modules. The API is organized into multiple layers to support different use cases, from quick prototyping to production-grade applications.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Express API](#express-api)
3. [Core API](#core-api)
4. [Data Management](#data-management)
5. [Analytics & Statistics](#analytics--statistics)
6. [Simulation & Prediction](#simulation--prediction)
7. [Visualization](#visualization)
8. [Configuration](#configuration)
9. [Error Handling](#error-handling)
10. [Advanced Usage](#advanced-usage)

## Overview

PyPitch provides three API levels:

- **Express API** (`pypitch.express`): Simplified one-liner functions with automatic setup
- **Core API** (`pypitch.api`): Full-featured session-based API for production use
- **Direct Engine Access** (`pypitch.storage`, `pypitch.compute`): Low-level access for custom analytics

## Quick Start

### Installation

```bash
pip install pypitch
```

### Basic Usage

```python
import pypitch.express as px

# Quick load with bundled sample data
session = px.quick_load()

# Get player statistics
stats = px.get_player_stats("Virat Kohli")
print(f"Matches: {stats.matches}, Runs: {stats.runs}")

# Predict win probability
prob = px.predict_win("Wankhede", 180, 120, 5, 15.0)
print(f"Win probability: {prob['win_prob']:.2%}")
```

## Express API

The Express API provides one-liner access to PyPitch features with sensible defaults.

### `pypitch.express`

#### `quick_load() -> PyPitchSession`

Load PyPitch with bundled sample data instantly (no download required).

**Returns:** `PyPitchSession` with sample cricket data ready for analysis.

**Example:**
```python
import pypitch.express as px

session = px.quick_load()
# Instantly ready for analysis
```

#### `get_player_stats(player_name: str, data_dir: Optional[str] = None) -> Optional[PlayerStats]`

Get comprehensive player statistics by name.

**Parameters:**
- `player_name` (str): Player name (fuzzy matched)
- `data_dir` (Optional[str]): Custom data directory path

**Returns:** `PlayerStats` dataclass or `None` if player not found.

**Example:**
```python
stats = px.get_player_stats("Virat Kohli")
print(f"Matches: {stats.matches}, Runs: {stats.runs}, Average: {stats.runs/stats.matches:.1f}")
```

#### `predict_win(venue: str, target: int, current_score: int, wickets_down: int, overs_done: float, data_dir: Optional[str] = None) -> Dict[str, float]`

Predict win probability for a cricket match.

**Parameters:**
- `venue` (str): Venue name
- `target` (int): Target score to chase
- `current_score` (int): Current team score
- `wickets_down` (int): Number of wickets fallen
- `overs_done` (float): Overs completed (0-20)
- `data_dir` (Optional[str]): Custom data directory path

**Returns:** Dictionary with `'win_prob'` key containing probability (0.0 to 1.0).

**Example:**
```python
prob = px.predict_win("Wankhede", 180, 120, 5, 15.0)
print(f"Win probability: {prob['win_prob']:.2%}")
```

#### `set_debug_mode(enabled: bool = True)`

Enable debug mode for detailed logging and eager execution.

**Parameters:**
- `enabled` (bool): Whether to enable debug mode

**Example:**
```python
px.set_debug_mode(True)  # Enable verbose logging
```

## Core API

### Session Management

#### `PyPitchSession`

The main entry point for PyPitch operations.

**Initialization:**
```python
from pypitch.api.session import PyPitchSession

# With bundled data
session = PyPitchSession("./data", skip_registry_build=True)

# With downloaded data
session = PyPitchSession("./data")
```

**Methods:**

##### `get_player_stats(player_name: str) -> Optional[PlayerStats]`

Get player statistics by name.

##### `load_match(match_id: str) -> Optional[MatchData]`

Load match data by ID.

##### `close()`

Clean up database connections and resources.

**Example:**
```python
with PyPitchSession("./data") as session:
    stats = session.get_player_stats("V Kohli")
    match = session.load_match("980959")
```

### Data Classes

#### `PlayerStats`

```python
@dataclass
class PlayerStats:
    name: str
    matches: int
    runs: int
    balls_faced: int
    wickets: int
    balls_bowled: int
    runs_conceded: int
```

#### `MatchData`

Contains ball-by-ball match information.

## Data Management

### Data Loading

#### `DataLoader`

Handles downloading and processing cricket data.

```python
from pypitch.data.loader import DataLoader

loader = DataLoader("./data")
loader.download()  # Download IPL 2023 data
```

### Storage Engine

#### `QueryEngine`

Executes analytical queries on cricket data.

```python
from pypitch.storage.engine import QueryEngine

engine = QueryEngine("./data/pypitch.duckdb")
results = engine.execute_query("SELECT * FROM balls WHERE batsman = 'V Kohli'")
```

## Analytics & Statistics

### Fantasy Points

#### `fantasy_points(player_name: str, match_id: str) -> Dict[str, float]`

Calculate fantasy cricket points for a player in a specific match.

**Parameters:**
- `player_name` (str): Player name
- `match_id` (str): Match identifier

**Returns:** Dictionary with points breakdown.

**Example:**
```python
from pypitch.api.fantasy import fantasy_points

points = fantasy_points("Virat Kohli", "980959")
print(f"Total points: {points['total']}")
```

### Matchup Analysis

#### `matchup(batter: str, bowler: str) -> pd.DataFrame`

Analyze head-to-head statistics between batter and bowler.

**Parameters:**
- `batter` (str): Batter name
- `bowler` (str): Bowler name

**Returns:** DataFrame with matchup statistics.

**Example:**
```python
from pypitch.api.stats import matchup

df = matchup("V Kohli", "JJ Bumrah")
print(df[['matches', 'runs', 'balls', 'dismissals']])
```

## Simulation & Prediction

### Win Probability Model

#### `predict_win(venue: str, target: int, current_score: int, wickets_down: int, overs_done: float) -> Dict[str, float]`

Predict match win probability using machine learning.

**Parameters:**
- `venue` (str): Match venue
- `target` (int): Target score
- `current_score` (int): Current score
- `wickets_down` (int): Wickets fallen
- `overs_done` (float): Overs completed

**Returns:** Dictionary with win probability and confidence.

**Example:**
```python
from pypitch.api.sim import predict_win

result = predict_win("Wankhede", 180, 120, 5, 15.0)
print(f"Win prob: {result['win_prob']:.3f}, Confidence: {result['confidence']:.3f}")
```

## Visualization

### Charts and Plots

#### `plot_career_runs(player_name: str) -> matplotlib.figure.Figure`

Plot player's career runs progression.

#### `plot_match_timeline(match_id: str) -> matplotlib.figure.Figure`

Plot run rate and wicket timeline for a match.

**Example:**
```python
from pypitch.visuals.charts import plot_career_runs

fig = plot_career_runs("Virat Kohli")
fig.savefig("kohli_career.png")
```

### Report Generation

#### `create_match_report(match_id: str) -> PDFReport`

Generate comprehensive match analysis report.

**Example:**
```python
from pypitch.report.pdf import create_match_report

report = create_match_report("980959")
report.save("match_report.pdf")
```

## Configuration

PyPitch can be configured using environment variables or a configuration file.

### Environment Variables

Configure PyPitch behavior using the following environment variables:

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `PYPITCH_DATA_DIR` | Default data directory path | `~/.pypitch_data` |
| `PYPITCH_DEBUG` | Enable debug mode with verbose logging | `false` |
| `PYPITCH_CACHE_SIZE` | Cache size in megabytes | `100` |

**Example:**
```bash
export PYPITCH_DATA_DIR="/path/to/data"
export PYPITCH_DEBUG="true"
export PYPITCH_CACHE_SIZE="200"
```

### Configuration File

Create a `pypitch.toml` file in your project root for persistent configuration:

```toml
[general]
data_dir = "./data"
debug = false
cache_size = 100

[database]
pool_size = 5
timeout = 30

[api]
rate_limit = 100
timeout = 10
```

**Configuration Precedence:**
1. Explicit function parameters (highest priority)
2. Environment variables
3. Configuration file
4. Default values (lowest priority)

## Error Handling

PyPitch provides specific exception types for different error conditions, enabling precise error handling in your applications.

### Exception Types

| Exception | Description | When Raised |
|-----------|-------------|-------------|
| `DataNotFoundError` | Requested data is not available | Player/match not found in database |
| `InvalidQueryError` | Query parameters are invalid | Malformed query or invalid parameters |
| `ConnectionError` | Database connection failed | Cannot connect to DuckDB |
| `SchemaValidationError` | Data schema mismatch | Data doesn't match Schema V1 contract |

### Best Practices

**Handle specific exceptions:**
```python
from pypitch.exceptions import DataNotFoundError, InvalidQueryError

try:
    stats = session.get_player_stats("Unknown Player")
except DataNotFoundError as e:
    print(f"Player not found: {e}")
except InvalidQueryError as e:
    print(f"Invalid query: {e}")
```

**Use context managers for automatic cleanup:**
```python
from pypitch.api.session import PyPitchSession

try:
    with PyPitchSession("./data") as session:
        stats = session.get_player_stats("V Kohli")
except Exception as e:
    print(f"An error occurred: {e}")
    # Session is automatically closed
```

## Performance Tips

Optimize your PyPitch usage with these performance recommendations:

1. **Use Express API for Simple Operations**: The Express API is optimized for common use cases and handles setup automatically
   ```python
   # Preferred for simple queries
   stats = px.get_player_stats("V Kohli")
   ```

2. **Cache Sessions for Multiple Calls**: Reuse session objects instead of creating new ones
   ```python
   session = px.quick_load()
   # Reuse session for multiple queries
   stats1 = session.get_player_stats("V Kohli")
   stats2 = session.get_player_stats("R Sharma")
   ```

3. **Use Bundled Data for Development**: Avoid download overhead during development
   ```python
   session = px.quick_load()  # Instant setup
   ```

4. **Close Sessions Explicitly**: Use context managers or explicit cleanup
   ```python
   with PyPitchSession("./data") as session:
       # Session automatically closed
       pass
   ```

5. **Enable Debug Mode Only When Troubleshooting**: Debug mode adds overhead
   ```python
   px.set_debug_mode(False)  # Disable in production
   ```

6. **Leverage Query Caching**: Identical queries are automatically cached
   ```python
   # First call hits database
   stats1 = px.get_player_stats("V Kohli")
   # Second call uses cache
   stats2 = px.get_player_stats("V Kohli")
   ```

## Advanced Usage

For advanced users who need more control or custom analytics capabilities.

### Custom SQL Queries

Execute custom SQL queries directly against the DuckDB database:

```python
from pypitch.storage.engine import QueryEngine

# Initialize query engine
engine = QueryEngine("./data/pypitch.duckdb")

# Execute custom SQL with parameters
query = """
    SELECT batsman, SUM(batsman_runs) as runs
    FROM balls
    WHERE match_id = ?
    GROUP BY batsman
    ORDER BY runs DESC
    LIMIT 10
"""
results = engine.execute_query(query, [match_id])
```

### Plugin System

Extend PyPitch functionality with custom plugins:

```python
from pypitch.api.plugins import register_plugin

@register_plugin("custom_analytics")
class CustomAnalytics:
    """Custom analytics plugin for specialized analysis."""
    
    def analyze_match(self, match_data):
        """
        Perform custom match analysis.
        
        Args:
            match_data: Match data object
            
        Returns:
            Custom analysis results
        """
        # Your custom analysis logic
        return analysis_results

# Use the plugin
from pypitch.api.plugins import get_plugin

analytics = get_plugin("custom_analytics")
results = analytics.analyze_match(match_data)
```

### Direct PyArrow Access

Work directly with PyArrow tables for maximum performance:

```python
from pypitch.storage.engine import QueryEngine

engine = QueryEngine("./data/pypitch.duckdb")

# Get PyArrow table directly
arrow_table = engine.execute_arrow_query("""
    SELECT * FROM balls WHERE match_id = ?
""", [match_id])

# Perform custom PyArrow operations
import pyarrow.compute as pc

# Filter and compute
filtered = arrow_table.filter(pc.field('batsman_runs') >= 4)
boundary_count = len(filtered)
```

## Migration Guide

### Future Version Planning

PyPitch is currently at version 0.1.0. When v1.0 is released, the following changes are planned:

#### Planned Changes for v1.0

1. **Express API Enhancement**: The Express API (`pypitch.express`) will become the primary recommended interface
   ```python
   # Current (v0.1.0)
   from pypitch.api.session import PyPitchSession
   session = PyPitchSession("./data")
   
   # Planned (v1.0)
   import pypitch.express as px
   session = px.quick_load()  # Enhanced features
   ```

2. **Session Management**: Enhanced session lifecycle management
   ```python
   # Recommended approach (already supported)
   with PyPitchSession("./data") as session:
       stats = session.get_player_stats("V Kohli")
   # Session automatically closed
   ```

3. **Bundled Sample Data**: Currently available, will be expanded in v1.0
   ```python
   # Already available in v0.1.0
   session = px.quick_load()
   ```

4. **Function Signature Improvements**: Some functions may have updated signatures for consistency
   - `get_player_stats()` returns `Optional[PlayerStats]` (current behavior)
   - `predict_win()` returns dictionary with `win_prob` and `confidence` (current behavior)

#### Current Features (v0.1.0)

- ✅ Express API available and functional
- ✅ Bundled sample data support
- ✅ Win probability ML model
- ✅ Enhanced caching system
- ✅ Comprehensive error messages

For migration assistance when v1.0 is released, refer to the release notes and updated documentation.

---

**Note**: This API documentation is for PyPitch v0.1.0. For the latest documentation, visit the [PyPitch repository](https://github.com/CodersAcademy006/PyPitch).