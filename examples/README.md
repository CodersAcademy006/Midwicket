# PyPitch Examples

This folder contains 25 example scripts to help you get started with `pypitch`.

## Prerequisites

Before running the examples, you must install the package in editable mode so that Python can find the `pypitch` module.

```bash
pip install -e .
```

## Getting Started

1.  **Setup Data**: Run `01_setup_data.py` first! This downloads the dataset and populates your local database.
    ```bash
    python examples/01_setup_data.py
    ```

2.  **Basic Usage**: Explore scripts `02` to `05` to understand the Session, Registry, and basic Stats API.

3.  **Analysis**: Scripts `06` to `10` show how to use the Fantasy and Simulation APIs.

4.  **SQL Power**: Scripts `11` to `25` demonstrate the full power of the SQL engine, including:
    - Filtering by Season and Phase
    - Joining with the Registry to get Names
    - Advanced Aggregations (Economy, Averages)
    - Window Functions (Partnerships)

## List of Examples

| Script | Description |
| :--- | :--- |
| `01_setup_data.py` | Download and Ingest Data (Run First) |
| `02_basic_session.py` | Initialize Session |
| `03_player_lookup.py` | Resolve Player Names to IDs |
| `04_venue_lookup.py` | Resolve Venue Names to IDs |
| `05_batter_vs_bowler.py` | Basic Matchup Analysis |
| `06_venue_stats.py` | Venue Cheat Sheet |
| `07_win_prediction.py` | Win Probability (Sim) |
| `08_custom_matchup.py` | Custom Query Object |
| `09_fantasy_points.py` | Fantasy Query Object |
| `10_raw_sql.py` | Execute Raw SQL |
| `11_filter_season.py` | Filter by Year |
| `12_filter_phase.py` | Filter by Phase (Powerplay/Death) |
| `13_top_run_scorers.py` | Join with Registry (Batters) |
| `14_top_wicket_takers.py` | Join with Registry (Bowlers) |
| `15_economy_rates.py` | Calculate Economy Rates |
| `16_boundary_percentage.py` | Boundary % Analysis |
| `17_dot_ball_percentage.py` | Dot Ball % Analysis |
| `18_batting_average.py` | Batting Average Calculation |
| `19_bowling_average.py` | Bowling Average Calculation |
| `20_team_stats.py` | Team Aggregates |
| `21_innings_comparison.py` | 1st vs 2nd Innings |
| `22_partnership_stats.py` | Partnership Analysis (Window Functions) |
| `23_player_consistency.py` | Standard Deviation of Runs |
| `24_match_result.py` | Determine Match Winners |
| `25_full_analysis.py` | Complex Multi-step Analysis |
