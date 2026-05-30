<div align="center">
  <img src="https://img.icons8.com/color/256/cricket.png" alt="Midwicket Logo" width="150" />

  # Midwicket
  
  **The Open-Source Cricket Intelligence SDK**

  <p align="center">
    <a href="https://colab.research.google.com/github/CodersAcademy006/Midwicket/blob/main/notebooks/quickstart.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab" height="20"></a>
    <a href="https://pypi.org/project/midwicket/"><img src="https://img.shields.io/pypi/v/midwicket?color=0052CC&style=flat-square&logo=python&logoColor=white" alt="PyPI version" /></a>
    <a href="https://github.com/CodersAcademy006/Midwicket/actions"><img src="https://img.shields.io/github/actions/workflow/status/CodersAcademy006/Midwicket/ci.yml?color=238636&style=flat-square&logo=github&logoColor=white&label=CI" alt="Build Status" /></a>
    <a href="https://pypi.org/project/midwicket/"><img src="https://img.shields.io/pypi/pyversions/midwicket?color=0052CC&style=flat-square&logo=python&logoColor=white" alt="Python Versions" /></a>
  </p>

  <p align="center">
    <i>Fast, deterministic cricket analytics powered by PyArrow and DuckDB.</i>
  </p>
</div>

---

## The Problem
Processing unstructured sports telemetry is historically a nightmare. Traditional APIs are slow, schemas constantly break, and calculating complex metrics like "venue bias" or "live win probability" across millions of events requires expensive cloud data warehouses.

## The Midwicket Solution
Midwicket brings the data warehouse to your laptop. It is a high-performance cricket intelligence SDK built on a structured pipeline architecture: a query planner routes requests between the PyArrow in-memory layer and a materialized DuckDB cache, keeping aggregations fast without cloud costs.

By leveraging vectorized **PyArrow** operations and an embedded **DuckDB** engine, Midwicket processes over 10 years of play-by-play data locally.

### Key Capabilities

*   **Fast Local Queries:** PyArrow and DuckDB power sub-second aggregations on cached, materialized views. Raw event scans are available for arbitrary flexibility.
*   **Pipeline Architecture:** Specialized components (Executor, Planner, Storage Engine, Registry) isolate concerns and route queries along the most efficient path.
*   **Predictive Machine Learning:** Logistic regression win probability model trained on IPL data (AUC 0.843), running entirely in memory with no external call.
*   **Type-Safe & Deterministic:** Immutable V1 schemas enforced via Pydantic. Queries are hashed and cached; identical inputs always produce identical outputs.
*   **FastAPI Backend:** Production-ready REST API with auth, rate limiting, CORS, and Prometheus metrics.

---

## Architecture

The Midwicket engine separates concerns across a structured pipeline: incoming data flows from Cricsheet JSON through a PyArrow ingestion layer into a DuckDB cache, where a query planner decides whether to scan raw events or serve a pre-computed view.

```mermaid
graph LR
    A[Cricsheet JSON] -->|Ingestion| B(PyArrow Pipeline)
    B -->|Parquet| C{DuckDB Cache}
    C -->|SQL Queries| D[Query Planner]
    D -->|Express API| E[Jupyter / Colab]
    D -->|FastAPI| F[Web / Mobile Clients]
```

---

## Quick Start

**Try it instantly in your browser — no install required:**

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/CodersAcademy006/Midwicket/blob/main/notebooks/quickstart.ipynb)

---

### Step 1 — Install

```bash
pip install midwicket
```

---

### Step 2 — Run a prediction (no data download needed)

The win probability model runs entirely in memory. No dataset, no waiting.

```python
import midwicket.express as px

result = px.predict_win(
    venue="Wankhede Stadium",
    target=180,
    current_score=120,
    wickets_down=5,
    overs_done=15.0,
)
print(f"Win Probability: {result['win_prob']:.1%}")
# Win Probability: 22.5%
```

The result also includes a `confidence` field — a heuristic certainty
indicator (0.1–0.95) that reflects how extreme the prediction is and how
much situational information is available. It is not a statistical confidence
interval; treat it as a qualitative signal.

---

### Step 3 — Query player stats and head-to-head matchups

Midwicket ships with a bundled in-memory dataset. Player stats and matchups
work out of the box — no download needed:

```python
import midwicket.express as px

stats = px.get_player_stats("Virat Kohli")
print(f"Player: {stats.name} | Runs: {stats.runs} | Strike Rate: {stats.strike_rate}")

matchup = px.get_matchup("V Kohli", "JJ Bumrah")
print(f"Head-to-head | Matches: {matchup.matches} | Average: {matchup.average:.1f}")
```

**How the data layer works:**

1. **Bundled data (default):** The in-memory ZIP ships with the package. Stats
   and matchups read from it automatically with no setup.
2. **Download full history (optional):** For 10+ years of ball-by-ball IPL
   data (~50 MB), run this once and it persists to disk:
   ```python
   px.download_data()          # downloads to ./data by default
   # px.download_data("~/cricket-data")  # or a custom path
   ```
3. **Registry:** Player resolution and matchup stats are indexed in an in-memory
   `IdentityRegistry` built from the loaded data. If a player name isn't found,
   `get_player_stats` raises `EntityNotFoundError` with the missing name.

---

## Enterprise Deployment

Midwicket includes a FastAPI backend, Prometheus scrape config, and a Grafana
dashboard definition. The observability stack is provisioned via Docker Compose.

> **Status:** The FastAPI service and Prometheus integration are production-ready.
> The Grafana dashboard is provided as a starting point and may need metric
> name adjustments to match your environment.

```bash
# Clone the repository
git clone https://github.com/CodersAcademy006/Midwicket.git
cd Midwicket

# Configure environment variables
cp .env.example .env
# Edit .env: set MIDWICKET_SECRET_KEY, MIDWICKET_API_KEYS, GRAFANA_PASSWORD

# Start the FastAPI server + Prometheus + Grafana
docker-compose up -d
```

---

## Examples

The `examples/` directory contains 36 runnable scripts covering the full SDK:

| Range | Topic |
|-------|-------|
| `01`–`03` | Setup, basic session, data ingest |
| `03b`–`08` | Player lookup, venue stats, win prediction |
| `09`–`20` | Fantasy points, raw SQL, season filters, leaderboards |
| `21`–`27` | Partnership stats, consistency, reports, pipelines |
| `28`–`36` | Express API, config, full library tour |

Browse [`examples/`](examples/) or start with
[`28_express_quickstart.py`](examples/28_express_quickstart.py).

---

## Contributing
Contributions are highly encouraged! We are actively looking for help with:
- Expanding the built-in machine learning models.
- Optimizing DuckDB materialized views.
- Writing tests for the query planner.

Before submitting code, please review the component architecture in
[`Agents.md`](Agents.md).

## License
Midwicket is open-source software released under the MIT License.
