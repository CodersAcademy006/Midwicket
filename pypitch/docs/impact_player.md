# Impact Player Rule Support in PyPitch

PyPitch is architected with flexibility in mind, supporting evolving cricket rules such as the "Impact Player" rule introduced in IPL 2023. The system can adapt to new rules without requiring code changes.

## Overview

The Impact Player rule allows teams to substitute a player during a match, effectively creating a 12th player scenario. PyPitch's architecture inherently supports this and similar rule changes through its flexible schema design.

### What is the Impact Player Rule?

The Impact Player rule, introduced in IPL 2023, allows teams to:
- Designate up to 5 substitute players before the match
- Introduce one substitute player at any point during the match
- The substitute can bat and bowl (unlike traditional substitutes)
- Creates scenarios with more than 11 active players per team

## Architecture Support

PyPitch supports new cricket rules through several key design principles:

### 1. Schema Flexibility

**No Hardcoded Player Limits**
- The schema doesn't enforce a fixed 11-player roster
- Supports any number of unique player IDs per match
- Automatically handles additional players in match data

```python
# PyPitch handles 12+ players seamlessly
import pypitch as pp

session = pp.api.session.PyPitchSession("./data")
match = session.load_match("ipl_2023_match_with_impact_player")

# All players, including impact player, are automatically processed
stats = session.get_player_stats("Impact Player Name")
```

### 2. Registry Evolution

**Schema-Evolution Awareness**
- Registry system supports new player roles and participation types
- No code changes needed for new player classifications
- Automatic identification and tracking of impact players

### 3. Dynamic Data Processing

**Player ID-Based Logic**
- All simulation and analytics logic uses player IDs, not fixed roster size
- Automatically adapts to varying number of players
- Maintains data integrity across rule changes

## Implementation Guide

### Ingesting Impact Player Data

PyPitch automatically handles impact player data from Cricsheet:

```python
from pypitch.data.loader import DataLoader

# Load IPL 2023 data with impact player information
loader = DataLoader("./data")
loader.download_competition("ipl", 2023)

# Impact player data is automatically ingested
# No special configuration needed
```

### Analyzing Impact Player Performance

```python
import pypitch.express as px

# Get stats for an impact player
session = px.quick_load()
impact_player_stats = px.get_player_stats("Impact Player Name")

if impact_player_stats:
    print(f"Matches: {impact_player_stats.matches}")
    print(f"Runs: {impact_player_stats.runs}")
    print(f"Wickets: {impact_player_stats.wickets}")
```

### Custom Queries for Impact Players

```python
from pypitch.storage.engine import QueryEngine

engine = QueryEngine("./data/pypitch.duckdb")

# Query to identify impact player substitutions
query = """
    SELECT match_id, player_id, MIN(ball_number) as first_ball
    FROM balls
    WHERE is_impact_player = true
    GROUP BY match_id, player_id
    ORDER BY match_id
"""
results = engine.execute_query(query)
```

## Supporting New Cricket Rules

PyPitch's flexible architecture makes it easy to support future rule changes:

### Step 1: Update Data Schema (if needed)

If new fields are required:

```python
# New schema fields are automatically recognized
# Example: Cricsheet adds "impact_player_role" field
# PyPitch will ingest it automatically
```

### Step 2: Update Registry Configuration (optional)

For special player role tracking (note: This is a conceptual example):

```python
# The registry automatically handles new player participation patterns
# No manual configuration typically needed for impact players
import pypitch.express as px

session = px.quick_load()
# Impact players are automatically recognized from data
```

### Step 3: Use Existing APIs

No code changes needed for most use cases:

```python
import pypitch.express as px

# Existing APIs work with new rules
session = px.quick_load()
stats = px.get_player_stats("Any Player")  # Works for all player types
```

## Examples

### Example 1: Impact Player Statistics

```python
import pypitch.express as px

session = px.quick_load()

# Get statistics for a player who has played as impact player
stats = px.get_player_stats("Shivam Dube")

print(f"Total matches: {stats.matches}")
print(f"Total runs: {stats.runs}")
# Statistics include both regular and impact player appearances
```

### Example 2: Team Analysis with Impact Players

```python
from pypitch.storage.engine import QueryEngine

engine = QueryEngine("./data/pypitch.duckdb")

# Analyze team performance with impact players
query = """
    SELECT 
        team,
        COUNT(DISTINCT match_id) as matches,
        COUNT(DISTINCT CASE WHEN is_impact_player THEN player_id END) as impact_players_used,
        AVG(CASE WHEN is_impact_player THEN batsman_runs END) as avg_impact_runs
    FROM balls
    WHERE season = 2023
    GROUP BY team
    ORDER BY impact_players_used DESC
"""
results = engine.execute_query(query)
print(results)
```

### Example 3: Match-Level Impact Analysis

Analyze match-level impact (conceptual example):

```python
import pypitch as pp

session = pp.api.session.PyPitchSession("./data")
match = session.load_match("ipl_2023_final")

# Query impact player statistics directly
from pypitch.storage.engine import QueryEngine

engine = QueryEngine("./data/pypitch.duckdb")
query = """
    SELECT 
        player_id,
        SUM(batsman_runs) as runs,
        COUNT(*) as balls
    FROM balls
    WHERE match_id = ? AND is_impact_player = true
    GROUP BY player_id
"""
impact_stats = engine.execute_query(query, ["ipl_2023_final"])
print(f"Impact player statistics: {impact_stats}")
```

## Data Model

PyPitch's data model for handling impact players:

```
Match
├── Players (dynamic count, not fixed at 11)
│   ├── Regular Players
│   └── Impact Player (if used)
├── Balls
│   ├── player_id (references any player)
│   └── is_impact_player (boolean flag)
└── Registry
    └── Player metadata (includes participation type)
```

## Limitations & Considerations

### Current Implementation

- ✅ Automatic ingestion of impact player data
- ✅ Support for unlimited players per match
- ✅ Player statistics include all appearances
- ✅ No code changes needed for new rules

### Future Enhancements

- 🔄 Explicit impact player role tagging
- 🔄 Impact player substitution timing analysis
- 🔄 Comparative analysis: impact vs regular players
- 🔄 Strategic impact player usage recommendations

## Best Practices

1. **Use Latest Data**: Ensure Cricsheet data includes impact player information
2. **Check Player Roles**: Some queries may need to distinguish impact players
3. **Update Regularly**: Keep PyPitch updated for the latest rule support
4. **Validate Data**: Verify impact player data is correctly ingested

## FAQ

**Q: Do I need to modify my code for impact player support?**  
A: No, PyPitch handles impact players automatically through existing APIs.

**Q: How does PyPitch identify impact players?**  
A: PyPitch uses data from Cricsheet which includes impact player flags.

**Q: Can I distinguish between regular and impact player statistics?**  
A: Yes, use custom SQL queries on the `is_impact_player` field.

**Q: Will PyPitch support future rule changes?**  
A: Yes, PyPitch's flexible architecture is designed to adapt to new rules.

## Further Reading

- [Cricsheet Data Format](https://cricsheet.org/)
- [IPL Impact Player Rule](https://www.iplt20.com/about/impact-player-rule)
- [PyPitch API Documentation](api.md)
- [PyPitch Schema Documentation](../../schema/)

---

For technical questions about impact player support, please open an issue on [GitHub](https://github.com/CodersAcademy006/PyPitch/issues).
