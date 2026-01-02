# PyPitch

The Open Source Cricket Intelligence SDK

PyPitch is a comprehensive Python library for cricket analytics, providing a robust, agent-based architecture for querying, processing, and analyzing cricket data. Built on top of PyArrow, DuckDB, and Pydantic, it offers deterministic, cacheable queries with strict schema enforcement.

## Features

- **Agent-Based Architecture**: Specialized agents (Gatekeeper, Planner, Archivist, Identity Manager, Analyst) handle different aspects of data processing.
- **Deterministic Queries**: All queries are hashed for reproducible results and caching.
- **Schema V1 Contract**: Immutable data schema with evolution rules for backward compatibility.
- **High Performance**: Vectorized operations using PyArrow and analytical queries via DuckDB.
- **Time-Aware Identity**: Consistent player/team/venue resolution across historical data.

## Stability Contract

### Q1: Is the API stable?

**Official Answer:**

"As of version 0.1.0, the Core Engine APIs (`pypitch.engine`, `pypitch.session`) are structurally stable. We do not anticipate changing the fundamental 'Session -> Match -> Delivery' hierarchy. However, until the v1.0.0 release, high-level helper functions in `pypitch.stats` may be renamed or refactored for clarity based on user feedback."

### Q2: What breaks between versions?

**Official Answer:**

"We strictly follow Semantic Versioning (SemVer).

- **Major (1.x -> 2.x):** Fundamental architecture changes (e.g., dropping DuckDB support).
- **Minor (0.1 -> 0.2):** New features (e.g., adding a 'Win Probability' model). These are backward compatible.
- **Patch (0.1.1 -> 0.1.2):** Bug fixes only.

Note: During the 0.x.x Alpha phase, we reserve the right to modify function arguments if necessary, but will provide Deprecation Warnings for at least one cycle before removal."

### Q3: Do minor versions introduce breaking changes?

**Official Answer:**

"No. Once we hit v1.0, minor versions will never break existing code. New parameters will always be optional (with default values), and return types will remain consistent. If you write a script on v1.2, it is guaranteed to run on v1.9."

## Installation

### From PyPI (Recommended)
```bash
pip install pypitch
```

### From Source
```bash
git clone https://github.com/yourusername/pypitch.git
cd pypitch
pip install -r requirements.txt
pip install -e .
```

## Quick Start

```python
import pypitch as pp

# Initialize the session
pp.init()

# Analyze a matchup
df = pp.stats.matchup("V Kohli", "JJ Bumrah")
print(df)

# Get fantasy cheat sheet for a venue
cheat_sheet = pp.fantasy.cheat_sheet("Wankhede Stadium")
print(cheat_sheet)

# Predict win probability
prob = pp.sim.predict_win("Eden Gardens", 180, 120, 5, 15.0)
print(f"Win probability: {prob}")
```

## Architecture Overview

```
pypitch/
├── schema/          # Immutable Data Definitions (Schema V1)
├── query/           # Explicit Query Objects with Deterministic Hashing
├── storage/         # I/O & State Management (DuckDB/Parquet)
├── runtime/         # Execution & Planning (Cache, Modes)
├── compute/         # Pure Math & Transformation (PyArrow)
├── core/            # Raw Data Processing (Cricsheet -> Arrow)
├── data/            # External Data Fetching
├── api/             # User-Facing Sugar (Wrappers)
└── tests/           # Reliability Suite
```

## Data Sources

PyPitch uses [Cricsheet](https://cricsheet.org/) as its primary data source, providing comprehensive ball-by-ball data for international and domestic cricket matches.

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- Documentation: [docs.pypitch.org](https://docs.pypitch.org)
- Issues: [GitHub Issues](https://github.com/yourusername/pypitch/issues)
- Discussions: [GitHub Discussions](https://github.com/yourusername/pypitch/discussions)