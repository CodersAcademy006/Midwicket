# PyPitch

**The Open Source Cricket Intelligence SDK**

PyPitch is a comprehensive Python library for cricket analytics, providing a robust, agent-based architecture for querying, processing, and analyzing cricket data. Built on top of PyArrow, DuckDB, and Pydantic, it offers deterministic, cacheable queries with strict schema enforcement.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
  - [API Overview](#api-overview)
  - [Key Capabilities](#key-capabilities)
- [Examples](#examples)
- [Architecture Overview](#architecture-overview)
- [Data Sources](#data-sources)
- [Documentation](#documentation)
- [Performance](#performance)
- [Stability & Compatibility](#stability--compatibility)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)
- [Roadmap](#roadmap)

## Introduction

PyPitch is a powerful cricket analytics SDK designed for developers, data scientists, and cricket enthusiasts. It provides a complete solution for ingesting, querying, and analyzing cricket data with a focus on performance, reliability, and ease of use.

The library leverages modern data engineering tools (PyArrow, DuckDB) and architectural patterns (agent-based design, deterministic queries) to deliver a professional-grade analytics platform. Whether you're building a fantasy cricket application, conducting statistical research, or creating interactive dashboards, PyPitch provides the tools you need.

### Target Audience

- **Data Scientists**: Perform advanced cricket analytics with Python
- **Application Developers**: Build cricket-related applications with robust APIs
- **Cricket Enthusiasts**: Explore and analyze cricket data programmatically
- **Researchers**: Conduct statistical analysis on cricket matches

## Features

- **Agent-Based Architecture**: Specialized agents (Gatekeeper, Planner, Archivist, Identity Manager, Analyst) handle different aspects of data processing
- **Deterministic Queries**: All queries are hashed for reproducible results and caching
- **Schema V1 Contract**: Immutable data schema with evolution rules for backward compatibility
- **High Performance**: Vectorized operations using PyArrow and analytical queries via DuckDB
- **Time-Aware Identity**: Consistent player/team/venue resolution across historical data
- **Express API**: One-liner access to common operations with sensible defaults
- **Bundled Data**: Instant setup with sample cricket data (no download required)
- **Win Probability Model**: ML-powered match outcome predictions
- **Rich Visualizations**: Charts, reports, and interactive dashboards

## Installation

### Prerequisites

- Python 3.9 or higher
- pip package manager

### Install from PyPI

```bash
pip install pypitch
```

### Install from Source

For development or to get the latest features:

```bash
git clone https://github.com/CodersAcademy006/PyPitch.git
cd pypitch
pip install -r requirements.txt
pip install -e .
```

### Verify Installation

```python
import pypitch as pp
print(pp.__version__)  # Should print: 0.1.0
```

## Deployment

### Docker Deployment

PyPitch includes production-ready Docker configuration for easy deployment:

#### Quick Start with Docker

```bash
# Clone the repository
git clone https://github.com/CodersAcademy006/PyPitch.git
cd pypitch

# Copy environment configuration
cp .env.example .env
# Edit .env with your production values

# Start all services
docker-compose up -d

# Check health
curl http://localhost:8000/health
```

#### Services Included

- **PyPitch API**: FastAPI-based REST API (`http://localhost:8000`)
- **PostgreSQL**: Production database for metadata
- **Prometheus**: Metrics collection (`http://localhost:9090`)
- **Grafana**: Monitoring dashboards (`http://localhost:3000`)

#### API Endpoints

```
GET  /health          - Health check
GET  /v1/metrics      - System and API metrics
GET  /matches         - List matches
POST /analyze         - Custom analysis
GET  /win_probability - Win probability predictions
```

#### Authentication

Include your API key in requests:
```bash
curl -H "X-API-Key: your-api-key" http://localhost:8000/health
```

#### Rate Limiting

- 60 requests per minute per API key/IP
- Rate limit headers included in responses
- 429 status code when exceeded

### Manual Deployment

For custom deployment scenarios:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SECRET_KEY="your-secret-key"
export API_CORS_ORIGINS='["http://yourdomain.com"]'

# Run the API
python -m uvicorn pypitch.serve.api:PyPitchAPI().app --host 0.0.0.0 --port 8000
```

## Quick Start

The fastest way to get started is using the Express API with bundled sample data:

### Instant Setup with Bundled Data

```python
import pypitch.express as px

# Load with sample data instantly (no download required)
session = px.quick_load()

# Get player statistics
stats = px.get_player_stats("Virat Kohli")
print(f"🏏 {stats.name}: {stats.runs} runs in {stats.matches} matches")

# Predict win probability
prob = px.predict_win("Wankhede", 180, 120, 5, 15.0)
print(f"🎯 Win probability: {prob['win_prob']:.1%}")
```

### Full Setup with Custom Data

For production use or custom datasets:

```bash
pip install pypitch
```

```python
import pypitch as pp

# Initialize session with data directory
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

## Usage

### API Overview

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

PyPitch uses a modular, agent-based architecture with clear separation of concerns:

```
Data Flow: Cricsheet JSON → Ingestion → DuckDB Cache → PyArrow Table → Pandas
```

### Module Structure

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

For detailed architecture information, see [Agents.md](Agents.md).

## Data Sources

PyPitch uses [Cricsheet](https://cricsheet.org/) as its primary data source, providing comprehensive ball-by-ball data for international and domestic cricket matches. The library also supports:

- **Custom Data Ingestion**: Import your own cricket data in JSON format
- **Bundled Sample Data**: Pre-packaged IPL 2023 matches for instant setup
- **Live Data Streaming**: Real-time match data (upcoming feature)

## Documentation

### Core Documentation

- **[Complete API Reference](pypitch/docs/api.md)**: Detailed function documentation with examples
- **[Architecture Guide](Agents.md)**: Agent-based system design and philosophy
- **[Win Probability Model](pypitch/docs/winprob_model.md)**: ML model implementation details
- **[Debug Mode](pypitch/docs/debug_mode.md)**: Troubleshooting and debugging guide

### Additional Resources

- **[Examples](examples/)**: Jupyter notebooks and sample scripts (25+ examples)
- **[Adapters](pypitch/docs/adapters.md)**: Custom data source integration guide
- **[Impact Player](pypitch/docs/impact_player.md)**: Player impact analysis documentation

## Examples

PyPitch includes a comprehensive collection of examples to help you get started. All examples are located in the [examples/](examples/) directory.

### Basic Analysis

Analyze player statistics across multiple players:

```python
import pypitch.express as px

# Quick analysis with bundled data
session = px.quick_load()

# Compare top run scorers
players = ["V Kohli", "S Dhawan", "RG Sharma", "DA Warner", "AB de Villiers"]
for player in players:
    stats = px.get_player_stats(player)
    if stats:
        avg = stats.runs / stats.matches if stats.matches > 0 else 0
        print(f"{player}: {stats.runs} runs ({avg:.1f} avg)")
```

### Match Win Prediction

Predict match outcomes using real-time data:

```python
import pypitch.express as px

# Real-time win probability calculation
venue = "Wankhede Stadium"
target = 180
current_score = 120
wickets_down = 5
overs_completed = 15.0

prob = px.predict_win(venue, target, current_score, wickets_down, overs_completed)
print(f"Current win probability: {prob['win_prob']:.1%}")
print(f"Model confidence: {prob['confidence']:.1%}")
```

### Fantasy Cricket Points

Calculate fantasy cricket points for players:

```python
from pypitch.api.fantasy import fantasy_points

# Calculate points for a player in a specific match
points = fantasy_points("Virat Kohli", "980959")
print("Fantasy Points Breakdown:")
for category, pts in points.items():
    if pts > 0:
        print(f"  {category}: {pts}")
print(f"  Total: {points['total']}")
```

### Advanced Analytics

Perform custom analytics using direct SQL queries:

```python
from pypitch.storage.engine import QueryEngine

# Initialize query engine
engine = QueryEngine("./data/pypitch.duckdb")

# Custom SQL query for detailed analysis
query = """
    SELECT batsman, SUM(batsman_runs) as total_runs, COUNT(*) as balls_faced
    FROM balls
    WHERE match_id = ?
    GROUP BY batsman
    ORDER BY total_runs DESC
    LIMIT 10
"""
results = engine.execute_query(query, ["980959"])
print(results)
```

For more examples, see the [examples/](examples/) directory which contains 25+ scripts covering various use cases.

## Performance

PyPitch is engineered for high performance with modern data processing technologies:

### Performance Features

- **Vectorized Operations**: Leverages PyArrow for fast columnar data processing
- **Analytical Queries**: Uses DuckDB for sub-second analytical queries on large datasets
- **Smart Caching**: Implements deterministic query hashing for efficient result caching
- **Memory Efficient**: Employs lazy loading and streaming for handling large datasets
- **Optimized I/O**: Parquet file format for fast reads and minimal storage

### Benchmark Results

Performance metrics on sample IPL 2023 dataset:

| Operation | Execution Time |
|-----------|---------------|
| Player stats query | ~400μs |
| Match loading | ~6.5ms |
| Registry resolution | ~800μs |
| Win probability prediction | ~50μs |

*Note: Benchmarks performed on standard hardware. Actual performance may vary based on dataset size and hardware specifications.*

## Stability & Compatibility

### Versioning
PyPitch follows [Semantic Versioning](https://semver.org/):

- **Major (1.x → 2.x)**: Breaking architecture changes
- **Minor (0.1 → 0.2)**: New features (backward compatible)
- **Patch (0.1.1 → 0.1.2)**: Bug fixes only

### API Stability

- **Express API**: Designed to be stable with backward compatibility maintained in future versions
- **Core API**: Structurally stable, with parameter additions only in minor versions
- **Internal APIs**: May change between minor versions (use at your own risk)

## Contributing

We welcome contributions from the community! PyPitch is an open-source project and we appreciate help in the following areas:

- Bug fixes and issue reporting
- Feature development and enhancements
- Documentation improvements
- Test coverage expansion
- Performance optimizations

### How to Contribute

1. **Fork the Repository**: Create your own fork of the PyPitch repository
2. **Create a Branch**: Make a feature branch for your changes
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make Changes**: Implement your changes with clear, documented code
4. **Write Tests**: Add tests for new features or bug fixes
5. **Run Tests**: Ensure all tests pass
   ```bash
   pytest
   ```
6. **Submit Pull Request**: Create a PR with a clear description of your changes

### Development Setup

```bash
# Clone the repository
git clone https://github.com/CodersAcademy006/PyPitch.git
cd pypitch

# Install dependencies
pip install -r requirements.txt

# Install in editable mode
pip install -e .

# Run tests
pytest

# Run with coverage
pytest --cov=pypitch
```

### Code Style Guidelines

- Follow PEP 8 Python style guidelines
- Use type hints for function signatures
- Write docstrings for all public functions and classes
- Keep functions focused and modular
- Add comments for complex logic

### Reporting Issues

When reporting issues, please include:
- Python version and operating system
- PyPitch version
- Minimal code example to reproduce the issue
- Expected vs. actual behavior
- Error messages and stack traces

## License

PyPitch is released under the **MIT License**. This means you are free to use, modify, and distribute the software, subject to the terms and conditions of the MIT License.

For full license details, see the [LICENSE](LICENSE) file in the repository.

## Support

### Getting Help

If you need assistance or have questions about PyPitch:

- **Documentation**: Comprehensive guides available in the [docs](pypitch/docs/) directory
- **GitHub Issues**: [Report bugs or request features](https://github.com/CodersAcademy006/PyPitch/issues)
- **GitHub Discussions**: [Ask questions and share ideas](https://github.com/CodersAcademy006/PyPitch/discussions)
- **Examples**: Browse [25+ example scripts](examples/) for common use cases

### Community

Join the PyPitch community to connect with other users and contributors:

- Share your cricket analytics projects
- Get help from experienced users
- Contribute to the project's development
- Stay updated on new features and releases

## Roadmap

PyPitch is under active development with a clear roadmap for future enhancements.

### Current Version: v0.1.0

**Completed Features:**
- ✅ Express API with one-liner access patterns
- ✅ Bundled sample data for instant setup
- ✅ Win probability ML model implementation
- ✅ Comprehensive test suite (47% coverage)
- ✅ Performance benchmarks and optimizations
- ✅ PDF report generation capabilities
- ✅ Live broadcasting overlay support
- ✅ Agent-based architecture with clear separation of concerns

### Upcoming: v1.1

**Planned Features:**
- 🔄 Enhanced ML models (player impact analysis, pitch condition predictions)
- 🔄 Real-time data streaming capabilities
- 🔄 Advanced data visualizations and interactive charts
- 🔄 Plugin ecosystem for extensibility
- 🔄 REST API server improvements and optimizations
- 🔄 Expanded test coverage (target: 75%+)

### Future Releases

**Long-term Goals:**
- Multi-sport support (extending beyond cricket)
- Cloud deployment options and scalability
- Mobile SDK for iOS and Android
- Advanced AI-powered analytics and insights
- Enhanced caching strategies
- Support for additional data sources

We welcome community input on the roadmap. Feel free to suggest features or vote on priorities in our [GitHub Discussions](https://github.com/CodersAcademy006/PyPitch/discussions).

---

**Built with ❤️ for the cricket analytics community**
