# PyPitch Examples

This folder contains 36 example scripts covering every layer of `pypitch`,
from one-liner express usage to raw SQL and live broadcasting overlays.

## Quick Start

```bash
# Install the library (editable for local dev)
pip install -e .

# Optional extras for the API server
pip install -e ".[serve]"

# Download IPL data (~50 MB, run once)
python examples/01_setup_data.py
```

---

## Script Index

### Getting Started (no data download required)

| Script | What it shows |
|:---|:---|
| `28_express_quickstart.py` | Express API one-liners — player stats, matchup, win probability |
| `29_win_probability.py` | Win-probability model across multiple chase scenarios |
| `30_metrics_showcase.py` | All compute metrics (batting, bowling, partnership, team) on synthetic data |
| `33_session_and_registry.py` | IdentityRegistry — register and resolve players/venues/teams in-memory |
| `34_query_engine_demo.py` | Schema validation, QueryEngine ingest, RuntimeExecutor + caching |
| `35_config_and_debug.py` | Environment config, debug mode, structured logging |
| `36_full_library_tour.py` | **Comprehensive end-to-end tour** of every major feature |

### Live & Streaming

| Script | What it shows |
|:---|:---|
| `31_live_overlay_demo.py` | OBS Browser Source overlay — push live stats to an HTTP endpoint |
| `32_client_sdk.py` | REST client SDK — connect to a running PyPitch API server |

### Foundation (requires `01_setup_data.py` first)

| Script | What it shows |
|:---|:---|
| `01_setup_data.py` | Download and ingest IPL dataset |
| `02_basic_session.py` | Initialize `PyPitchSession` |
| `03_player_lookup.py` | Resolve player names to IDs |
| `04_venue_lookup.py` | Resolve venue names to IDs |
| `05_batter_vs_bowler.py` | Basic matchup analysis |
| `06_venue_stats.py` | Venue cheat sheet |
| `07_win_prediction.py` | Win probability via Sim API |
| `08_custom_matchup.py` | Custom `MatchupQuery` object |
| `09_fantasy_points.py` | `FantasyQuery` for fantasy cricket |
| `10_raw_sql.py` | Execute raw SQL against the engine |

### SQL Power (requires `01_setup_data.py`)

| Script | What it shows |
|:---|:---|
| `11_filter_season.py` | Filter by year |
| `12_filter_phase.py` | Filter by Powerplay / Death overs |
| `13_top_run_scorers.py` | JOIN with registry to get player names |
| `14_top_wicket_takers.py` | Top wicket-takers |
| `15_economy_rates.py` | Economy-rate calculation |
| `16_boundary_percentage.py` | Boundary % analysis |
| `17_dot_ball_percentage.py` | Dot ball % analysis |
| `18_batting_average.py` | Batting average |
| `19_bowling_average.py` | Bowling average |
| `20_team_stats.py` | Team-level aggregates |
| `21_innings_comparison.py` | 1st vs 2nd innings comparison |
| `22_partnership_stats.py` | Partnership analysis (window functions) |
| `23_player_consistency.py` | Standard deviation of scores |
| `24_match_result.py` | Determine match winners |
| `25_full_analysis.py` | Multi-step venue/phase analysis |
| `26_report_plugin_demo.py` | PDF report generation |

---

## Recommended Learning Path

```
36_full_library_tour.py          ← start here (standalone, no downloads)
    ↓
30_metrics_showcase.py           ← understand the compute layer
29_win_probability.py            ← explore the probability model
33_session_and_registry.py       ← understand identity resolution
34_query_engine_demo.py          ← understand schema + execution
    ↓
01_setup_data.py                 ← download real IPL data
28_express_quickstart.py         ← use the high-level express API
13_top_run_scorers.py            ← SQL + registry joins
25_full_analysis.py              ← complex analysis
    ↓
31_live_overlay_demo.py          ← live broadcasting
32_client_sdk.py                 ← REST API client
```
