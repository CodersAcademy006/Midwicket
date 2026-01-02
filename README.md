# PyPitch

The Open Source Cricket Intelligence SDK

PyPitch is a comprehensive Python library for cricket analytics, providing a robust, agent-based architecture for querying, processing, and analyzing cricket data. Built on top of PyArrow, DuckDB, and Pydantic, it offers deterministic, cacheable queries with strict schema enforcement.

## Features

- **Agent-Based Architecture**: Specialized agents (Gatekeeper, Planner, Archivist, Identity Manager, Analyst) handle different aspects of data processing.
- **Deterministic Queries**: All queries are hashed for reproducible results and caching.
- **Schema V1 Contract**: Immutable data schema with evolution rules for backward compatibility.
- **High Performance**: Vectorized operations using PyArrow and analytical queries via DuckDB.
- **Time-Aware Identity**: Consistent player/team/venue resolution across historical data.
- **Express API**: One-liner access to common operations with sensible defaults.
- **Bundled Data**: Instant setup with sample cricket data (no download required).
- **Win Probability Model**: ML-powered match outcome predictions.
- **Rich Visualizations**: Charts, reports, and interactive dashboards.

## Quick Start

### Instant Setup with Bundled Data

```python
import pypitch.express as px

# Load with sample data instantly (no download!)
session = px.quick_load()

# Get player statistics
stats = px.get_player_stats("Virat Kohli")
print(f"🏏 {stats.name}: {stats.runs} runs in {stats.matches} matches")

# Predict win probability
prob = px.predict_win("Wankhede", 180, 120, 5, 15.0)
print(f"🎯 Win probability: {prob['win_prob']:.1%}")
```

### Full Installation & Setup

```bash
pip install pypitch
```

```python
import pypitch as pp

# Initialize session
session = pp.api.session.PyPitchSession("./data")

# Download sample data (IPL 2023)
from pypitch.data.loader import DataLoader
loader = DataLoader("./data")
loader.download()

# Analyze player performance
stats = session.get_player_stats("V Kohli")
print(f"Player: {stats.name}")
print(f"Matches: {stats.matches}, Runs: {stats.runs}")
```

## API Overview

PyPitch provides multiple API levels for different use cases:

### Express API (`pypitch.express`)
- **Best for**: Quick analysis, prototyping, beginners
- **Features**: One-liner functions, automatic setup, sensible defaults
- **Example**: `px.get_player_stats("V Kohli")`

### Core API (`pypitch.api`)
- **Best for**: Production applications, custom workflows
- **Features**: Full control, session management, advanced features
- **Example**: `PyPitchSession("./data").get_player_stats("V Kohli")`

### Direct Engine Access (`pypitch.storage`, `pypitch.compute`)
- **Best for**: Custom analytics, high-performance computing
- **Features**: Raw data access, custom queries, plugin system

## Key Capabilities

### Player Analytics
```python
# Career statistics
stats = px.get_player_stats("Steve Smith")

# Head-to-head matchups
matchup = px.get_matchup("V Kohli", "JJ Bumrah")

# Fantasy points calculation
points = pp.api.fantasy.fantasy_points("V Kohli", "980959")
```

### Match Analysis
```python
# Load match data
match = session.load_match("980959")

# Win probability throughout innings
probs = pp.compute.winprob.calculate_win_probability(match)

# Performance reports
report = pp.report.pdf.create_match_report("980959")
```

### Predictive Modeling
```python
# Win probability prediction
result = px.predict_win("Eden Gardens", 180, 120, 5, 15.0)
print(f"Win chance: {result['win_prob']:.1%}")

# Player impact analysis
impact = pp.compute.attribution.calculate_impact("V Kohli", match)
```

### Data Management
```python
# Download competition data
loader = pp.data.loader.DataLoader("./data")
loader.download_competition("ipl", 2023)

# Custom data ingestion
pp.data.pipeline.ingest_match_data(custom_match_json)

# Query optimization
results = pp.storage.engine.QueryEngine("./data").execute_query(sql)
```

## Architecture Overview

```
pypitch/
├── api/             # User-Facing APIs (Express, Core, Plugins)
├── schema/          # Immutable Data Definitions (Schema V1)
├── query/           # Explicit Query Objects with Hashing
├── storage/         # I/O & State Management (DuckDB/Parquet)
├── runtime/         # Execution & Planning (Cache, Modes)
├── compute/         # Pure Math & Analytics (PyArrow)
├── core/            # Raw Data Processing (Cricsheet → Arrow)
├── data/            # External Data Fetching & Loading
├── models/          # ML Models (Win Probability, etc.)
├── visuals/         # Charts, Reports, Dashboards
├── report/          # PDF/Interactive Report Generation
├── live/            # Live Broadcasting Overlays
├── serve/           # REST API Server
└── tests/           # Comprehensive Test Suite
```

## Data Sources

PyPitch uses [Cricsheet](https://cricsheet.org/) as its primary data source, providing comprehensive ball-by-ball data for international and domestic cricket matches. The library also supports custom data ingestion and includes bundled sample data for instant setup.

## Documentation

- **[Complete API Reference](pypitch/docs/api.md)**: Detailed function documentation with examples
- **[Architecture Guide](Agents.md)**: Agent-based system design
- **[Win Probability Model](pypitch/docs/winprob_model.md)**: ML model details
- **[Debug Mode](pypitch/docs/debug_mode.md)**: Troubleshooting guide
- **[Examples](examples/)**: Jupyter notebooks and sample scripts

## Examples

### Basic Analysis
```python
import pypitch.express as px

# Quick analysis with bundled data
session = px.quick_load()

# Top run scorers
players = ["V Kohli", "S Dhawan", "RG Sharma", "DA Warner", "AB de Villiers"]
for player in players:
    stats = px.get_player_stats(player)
    avg = stats.runs / stats.matches
    print(f"{player}: {stats.runs} runs ({avg:.1f} avg)")
```

### Match Prediction
```python
# Real-time win probability
venue = "Wankhede Stadium"
target = 180
current_score = 120
wickets_down = 5
overs_completed = 15.0

prob = px.predict_win(venue, target, current_score, wickets_down, overs_completed)
print(f"Current win probability: {prob['win_prob']:.1%}")
print(f"Model confidence: {prob['confidence']:.1%}")
```

### Fantasy Cricket
```python
from pypitch.api.fantasy import fantasy_points

# Calculate points for a player in a match
points = fantasy_points("Virat Kohli", "980959")
print("Fantasy Points Breakdown:")
for category, pts in points.items():
    if pts > 0:
        print(f"  {category}: {pts}")
print(f"  Total: {points['total']}")
```

## Performance

PyPitch is designed for high performance:

- **Vectorized Operations**: PyArrow for fast data processing
- **Analytical Queries**: DuckDB for sub-second aggregations
- **Smart Caching**: Deterministic query hashing and result caching
- **Memory Efficient**: Lazy loading and streaming for large datasets

Benchmark results (on sample IPL 2023 data):
- Player stats query: ~400μs
- Match loading: ~6.5ms
- Registry resolution: ~800μs
- Win probability prediction: ~50μs

## Stability & Compatibility

### Versioning
PyPitch follows [Semantic Versioning](https://semver.org/):

- **Major (1.x → 2.x)**: Breaking architecture changes
- **Minor (0.1 → 0.2)**: New features (backward compatible)
- **Patch (0.1.1 → 0.1.2)**: Bug fixes only

### API Stability
- **Express API**: Stable for v1.0+ with backward compatibility
- **Core API**: Structurally stable, parameter additions only
- **Internal APIs**: May change between minor versions

## Contributing

We welcome contributions! See our [Contributing Guide](CONTRIBUTING.md) for details on:
- Setting up development environment
- Running tests and benchmarks
- Code style and documentation
- Submitting pull requests

### Development Setup
```bash
git clone https://github.com/yourusername/pypitch.git
cd pypitch
pip install -r requirements.txt
pip install -e .
pytest  # Run test suite
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [docs.pypitch.org](https://docs.pypitch.org)
- **Issues**: [GitHub Issues](https://github.com/yourusername/pypitch/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/pypitch/discussions)
- **Discord**: [PyPitch Community](https://discord.gg/pypitch)

## Roadmap

### v1.0 (Current)
- ✅ Express API with one-liner access
- ✅ Bundled sample data for instant setup
- ✅ Win probability ML model
- ✅ Comprehensive test suite (47% coverage)
- ✅ Performance benchmarks
- ✅ PDF report generation
- ✅ Live broadcasting overlays

### v1.1 (Next)
- Enhanced ML models (player impact, pitch analysis)
- Real-time data streaming
- Advanced visualizations
- Plugin ecosystem
- REST API server improvements

### Future
- Multi-sport support
- Cloud deployment options
- Mobile SDK
- Advanced AI analytics