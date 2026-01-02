# PyPitch API Documentation

## Table of Contents

1. [Quick Start](#quick-start)
2. [Express API](#express-api)
3. [Core API](#core-api)
4. [Data Management](#data-management)
5. [Analytics & Statistics](#analytics--statistics)
6. [Simulation & Prediction](#simulation--prediction)
7. [Visualization](#visualization)
8. [Configuration](#configuration)

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

### Environment Variables

- `PYPITCH_DATA_DIR`: Default data directory (default: `~/.pypitch_data`)
- `PYPITCH_DEBUG`: Enable debug mode (default: `false`)
- `PYPITCH_CACHE_SIZE`: Cache size in MB (default: `100`)

### Configuration File

Create `pypitch.toml` in your project root:

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

## Error Handling

PyPitch uses specific exception types for different error conditions:

- `DataNotFoundError`: When requested data is not available
- `InvalidQueryError`: When query parameters are invalid
- `ConnectionError`: When database connections fail
- `SchemaValidationError`: When data doesn't match expected schema

**Example:**
```python
from pypitch.exceptions import DataNotFoundError

try:
    stats = session.get_player_stats("Unknown Player")
except DataNotFoundError:
    print("Player not found in database")
```

## Performance Tips

1. **Use Express API** for simple operations - it's optimized for common use cases
2. **Cache sessions** when making multiple calls
3. **Use bundled data** for development and testing
4. **Close sessions** explicitly or use context managers
5. **Enable debug mode** only when troubleshooting

## Advanced Usage

### Custom Queries

```python
# Direct SQL access
results = engine.execute_query("""
    SELECT batsman, SUM(batsman_runs) as runs
    FROM balls
    WHERE match_id = ?
    GROUP BY batsman
    ORDER BY runs DESC
""", [match_id])
```

### Plugin System

PyPitch supports custom plugins for specialized analytics:

```python
from pypitch.api.plugins import register_plugin

@register_plugin("custom_analytics")
class CustomAnalytics:
    def analyze_match(self, match_data):
        # Your custom analysis logic
        return analysis_results
```

## Migration Guide

### From v0.x to v1.0

- Express API is now the recommended interface
- Session management requires explicit cleanup
- Some function signatures have changed for consistency
- Bundled data is now available for instant setup

See [Migration Guide](migration.md) for detailed changes.</content>
<parameter name="filePath">d:\Srijan\PyPitch\pypitch\docs\api.md